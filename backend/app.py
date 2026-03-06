import json
import logging
import os
import uuid
from datetime import datetime, timezone

from flask import Flask, jsonify, request
from flask_cors import CORS
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from db import (
    authenticate_user,
    change_user_password,
    get_user_by_id,
    init_db,
    insert_sensor_payload,
    get_current_metrics,
    get_iot_overview,
    list_refill_requests,
    list_sla_alerts,
    list_events,
    list_solenoid_commands,
    process_refill_request,
    refresh_sla_alerts,
    update_sla_alert_state,
)
from solenoid import send_solenoid_command


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key.startswith("_"):
                continue
            if key in {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
            }:
                continue
            base_payload[key] = value

        return json.dumps(base_payload)


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(logging.INFO)


configure_logging()

app = Flask(__name__)
CORS(app)
init_db()
REFILL_APPROVAL_REQUIRED = os.getenv("REFILL_APPROVAL_REQUIRED", "true").lower() != "false"
AUTH_TOKEN_EXPIRES_SECONDS = int(os.getenv("AUTH_TOKEN_EXPIRES_SECONDS", "43200"))
AUTH_SECRET = os.getenv("AUTH_SECRET", "oil-libya-ethiopia-auth-secret")
AUTH_REQUIRED = os.getenv("AUTH_REQUIRED", "true").lower() != "false"


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(secret_key=AUTH_SECRET, salt="auth-token")


def _create_token(user: dict) -> str:
    return _serializer().dumps({"user_id": user["id"], "role": user["role"]})


def _decode_token(token: str) -> dict | None:
    try:
        return _serializer().loads(token, max_age=AUTH_TOKEN_EXPIRES_SECONDS)
    except (BadSignature, SignatureExpired):
        return None


def _extract_bearer_token() -> str | None:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header[7:].strip()
    return token or None


def _auth_context() -> tuple[str, str, bool, int]:
    token = _extract_bearer_token()
    if not token:
        return "", "", False, 0

    payload = _decode_token(token)
    if payload is None:
        return "", "", False, 0

    user = get_user_by_id(int(payload.get("user_id", 0)))
    if user is None:
        return "", "", False, 0

    role = str(user["role"]).lower()
    if role not in {"worker", "manager", "admin"}:
        return "", "", False, 0
    return str(user["username"]), role, bool(user.get("must_change_password", False)), int(user["id"])


def _require_role(minimum_role: str, *, allow_if_password_change_required: bool = False) -> tuple[str, str] | tuple[dict, int]:
    hierarchy = {"worker": 1, "manager": 2, "admin": 3}
    user_id, role, must_change_password, _ = _auth_context()
    if AUTH_REQUIRED and (not user_id or not role):
        return {"error": "authentication required"}, 401

    if not user_id or not role:
        user_id, role = ("system", "admin")

    if hierarchy.get(role, 0) < hierarchy.get(minimum_role, 99):
        return {"error": f"{minimum_role} role required"}, 403

    if must_change_password and not allow_if_password_change_required:
        return {"error": "password change required", "code": "PASSWORD_CHANGE_REQUIRED"}, 403
    return user_id, role


@app.get("/api/health")
def health() -> tuple:
    return jsonify({"status": "ok", "service": "OIL LIBYA ETHIOPIA API"}), 200


@app.get("/api/metrics")
def metrics() -> tuple:
    access = _require_role("worker")
    if isinstance(access, tuple) and len(access) == 2 and isinstance(access[0], dict):
        return jsonify(access[0]), access[1]

    payload = get_current_metrics()
    if payload is None:
        return jsonify({"error": "No metrics available yet"}), 404
    return jsonify(payload), 200


@app.get("/api/events")
def events() -> tuple:
    access = _require_role("worker")
    if isinstance(access, tuple) and len(access) == 2 and isinstance(access[0], dict):
        return jsonify(access[0]), access[1]

    return jsonify({"events": list_events(limit=50)}), 200


@app.get("/api/solenoid/commands")
def solenoid_commands() -> tuple:
    access = _require_role("worker")
    if isinstance(access, tuple) and len(access) == 2 and isinstance(access[0], dict):
        return jsonify(access[0]), access[1]

    return jsonify({"commands": list_solenoid_commands(limit=50)}), 200


@app.get("/api/iot/overview")
def iot_overview() -> tuple:
    access = _require_role("worker")
    if isinstance(access, tuple) and len(access) == 2 and isinstance(access[0], dict):
        return jsonify(access[0]), access[1]

    return jsonify(get_iot_overview()), 200


@app.get("/api/alerts")
def alerts() -> tuple:
    access = _require_role("worker")
    if isinstance(access, tuple) and len(access) == 2 and isinstance(access[0], dict):
        return jsonify(access[0]), access[1]

    should_refresh = request.args.get("refresh", "true").lower() != "false"
    include_resolved = request.args.get("include_resolved", "false").lower() == "true"
    try:
        limit = max(1, min(200, int(request.args.get("limit", "50"))))
    except ValueError:
        return jsonify({"error": "limit must be an integer"}), 400

    if should_refresh:
        refresh_sla_alerts()

    return jsonify(list_sla_alerts(limit=limit, include_resolved=include_resolved)), 200


@app.post("/api/alerts/<path:alert_key>/action")
def alert_action(alert_key: str) -> tuple:
    access = _require_role("manager")
    if isinstance(access, tuple) and len(access) == 2 and isinstance(access[0], dict):
        return jsonify(access[0]), access[1]

    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action", "")).strip().lower()
    silence_minutes = payload.get("silence_minutes")

    operator_id, operator_role, _, _ = _auth_context()

    try:
        result = update_sla_alert_state(
            alert_key=alert_key,
            action=action,
            operator_id=operator_id,
            operator_role=operator_role,
            silence_minutes=silence_minutes,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except LookupError as exc:
        return jsonify({"error": str(exc)}), 404

    return jsonify({"status": "ok", "alerts": result}), 200


@app.post("/api/ingest")
def ingest() -> tuple:
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    payload = request.get_json(silent=True) or {}

    required_fields = ["fuel_level"]
    missing_fields = [field for field in required_fields if field not in payload]
    if missing_fields:
        return jsonify({"error": f"Missing fields: {', '.join(missing_fields)}"}), 400

    try:
        user_id, _, _, _ = _auth_context()
        if not user_id:
            user_id = "telemetry-gateway"
        result = insert_sensor_payload(
            payload,
            require_refill_approval=REFILL_APPROVAL_REQUIRED,
            requested_by=user_id,
        )
    except (TypeError, ValueError) as exc:
        return jsonify({"error": f"Invalid payload: {exc}"}), 400

    solenoid_result = None
    auto_refill = result.get("auto_refill")
    if auto_refill:
        solenoid_result = send_solenoid_command(
            command="OPEN",
            reason="generator_below_minimum",
            metadata={
                "generator_level": auto_refill.get("generator_level"),
                "external_level": auto_refill.get("external_level"),
                "transfer_amount": auto_refill.get("transfer_amount"),
            },
            request_id=f"auto-{uuid.uuid4()}",
        )

    return jsonify({"status": "accepted", "data": result, "solenoid_command": solenoid_result}), 201


@app.post("/api/solenoid/command")
def manual_solenoid_command() -> tuple:
    access = _require_role("manager")
    if isinstance(access, tuple) and len(access) == 2 and isinstance(access[0], dict):
        return jsonify(access[0]), access[1]

    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    payload = request.get_json(silent=True) or {}
    command = str(payload.get("command", "")).upper()
    reason = str(payload.get("reason", "manual_override"))
    request_id = payload.get("request_id") or str(uuid.uuid4())
    metadata = payload.get("metadata") or {}

    if command not in {"OPEN", "CLOSE"}:
        return jsonify({"error": "command must be OPEN or CLOSE"}), 400

    try:
        result = send_solenoid_command(
            command=command,
            reason=reason,
            request_id=str(request_id),
            metadata=metadata,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    return jsonify({"status": "accepted", "command_result": result}), 200


@app.get("/api/refill/requests")
def refill_requests() -> tuple:
    access = _require_role("worker")
    if isinstance(access, tuple) and len(access) == 2 and isinstance(access[0], dict):
        return jsonify(access[0]), access[1]

    status = str(request.args.get("status", "")).strip().lower() or None
    allowed = {"pending", "executed", "rejected"}
    if status is not None and status not in allowed:
        return jsonify({"error": "status must be one of: pending, executed, rejected"}), 400

    try:
        limit = max(1, min(200, int(request.args.get("limit", "50"))))
    except ValueError:
        return jsonify({"error": "limit must be an integer"}), 400

    return jsonify(list_refill_requests(limit=limit, status=status)), 200


@app.post("/api/refill/requests/<int:request_id>/action")
def refill_request_action(request_id: int) -> tuple:
    access = _require_role("manager")
    if isinstance(access, tuple) and len(access) == 2 and isinstance(access[0], dict):
        return jsonify(access[0]), access[1]

    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action", "")).strip().lower()
    actor_id, actor_role, _, _ = _auth_context()

    try:
        result = process_refill_request(
            request_id=request_id,
            action=action,
            actor_id=actor_id,
            actor_role=actor_role,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except LookupError as exc:
        return jsonify({"error": str(exc)}), 404

    if action == "approve":
        send_solenoid_command(
            command="OPEN",
            reason="manager_approved_refill",
            metadata={"request_id": request_id},
            request_id=f"refill-{request_id}-{uuid.uuid4()}",
        )

    return jsonify({"status": "ok", "refill_requests": result}), 200


@app.post("/api/auth/login")
def login() -> tuple:
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    payload = request.get_json(silent=True) or {}
    username = str(payload.get("username", "")).strip()
    password = str(payload.get("password", "")).strip()

    if not username or not password:
        return jsonify({"error": "username and password are required"}), 400

    user = authenticate_user(username, password)
    if user is None:
        return jsonify({"error": "invalid credentials"}), 401

    token = _create_token(user)
    return jsonify(
        {
            "token": token,
            "expires_in_seconds": AUTH_TOKEN_EXPIRES_SECONDS,
            "user": {
                "id": user["id"],
                "username": user["username"],
                "role": user["role"],
                "must_change_password": bool(user.get("must_change_password", False)),
            },
        }
    ), 200


@app.get("/api/auth/me")
def me() -> tuple:
    user_id, role, must_change_password, _ = _auth_context()
    if not user_id or not role:
        return jsonify({"error": "authentication required"}), 401
    return jsonify({"user": {"username": user_id, "role": role, "must_change_password": must_change_password}}), 200


@app.post("/api/auth/change-password")
def auth_change_password() -> tuple:
    access = _require_role("worker", allow_if_password_change_required=True)
    if isinstance(access, tuple) and len(access) == 2 and isinstance(access[0], dict):
        return jsonify(access[0]), access[1]

    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    payload = request.get_json(silent=True) or {}
    current_password = str(payload.get("current_password", "")).strip()
    new_password = str(payload.get("new_password", "")).strip()

    if not current_password or not new_password:
        return jsonify({"error": "current_password and new_password are required"}), 400

    _, _, _, user_id = _auth_context()

    try:
        updated_user = change_user_password(
            user_id=user_id,
            current_password=current_password,
            new_password=new_password,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except LookupError as exc:
        return jsonify({"error": str(exc)}), 404

    token = _create_token(updated_user)
    return jsonify({
        "status": "ok",
        "token": token,
        "user": {
            "id": updated_user["id"],
            "username": updated_user["username"],
            "role": updated_user["role"],
            "must_change_password": False,
        },
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
