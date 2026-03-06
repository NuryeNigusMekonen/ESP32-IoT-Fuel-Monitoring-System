import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import requests
from paho.mqtt import publish as mqtt_publish

from db import get_solenoid_command_by_request_id, save_solenoid_command

logger = logging.getLogger("oil_libya_ethiopia.solenoid")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_command_payload(command: str, reason: str, metadata: dict[str, Any], request_id: str) -> dict[str, Any]:
    return {
        "command": command,
        "reason": reason,
        "request_id": request_id,
        "issued_at": _utc_now(),
        **metadata,
    }


def _publish_http(payload: dict[str, Any], target: str, timeout_seconds: float, retry_count: int) -> tuple[str, dict[str, Any], int]:
    token = os.getenv("SOLENOID_HTTP_TOKEN", "")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    attempts = 0
    last_error = ""
    for _ in range(retry_count + 1):
        attempts += 1
        try:
            response = requests.post(target, json=payload, timeout=timeout_seconds, headers=headers)
            if response.ok:
                try:
                    body = response.json()
                except ValueError:
                    body = {"raw": response.text}
                return "sent", {"http_status": response.status_code, "body": body}, attempts

            last_error = f"HTTP {response.status_code}: {response.text}"
        except requests.RequestException as exc:
            last_error = str(exc)

    return "failed", {"error": last_error}, attempts


def _publish_mqtt(payload: dict[str, Any], target: str, timeout_seconds: float, retry_count: int) -> tuple[str, dict[str, Any], int]:
    broker = os.getenv("MQTT_BROKER", "localhost")
    port = int(os.getenv("MQTT_PORT", "1883"))

    attempts = 0
    last_error = ""

    for _ in range(retry_count + 1):
        attempts += 1
        try:
            mqtt_publish.single(
                topic=target,
                payload=json.dumps(payload),
                hostname=broker,
                port=port,
                retain=False,
                qos=1,
            )
            return "sent", {"topic": target, "broker": broker, "port": port}, attempts
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)

    return "failed", {"error": last_error, "topic": target, "broker": broker, "port": port}, attempts


def send_solenoid_command(
    *,
    command: str,
    reason: str,
    metadata: dict[str, Any] | None = None,
    request_id: str | None = None,
) -> dict[str, Any]:
    mode = os.getenv("SOLENOID_MODE", "disabled").lower().strip()
    request_id = request_id or str(uuid.uuid4())
    existing = get_solenoid_command_by_request_id(request_id)

    if existing is not None:
        return {
            "status": "duplicate",
            "request_id": request_id,
            "mode": existing["mode"],
            "result": existing,
        }

    metadata = metadata or {}
    timeout_seconds = float(os.getenv("SOLENOID_TIMEOUT_SECONDS", "5"))
    retry_count = int(os.getenv("SOLENOID_RETRY_COUNT", "2"))

    if command not in {"OPEN", "CLOSE"}:
        raise ValueError("command must be OPEN or CLOSE")

    payload = _build_command_payload(command, reason, metadata, request_id)

    target = ""
    publish_status = "skipped"
    response_payload: dict[str, Any] = {"message": "Solenoid mode is disabled"}
    attempts = 0

    if mode == "http":
        target = os.getenv("SOLENOID_HTTP_ENDPOINT", "http://localhost:8080/api/solenoid/command")
        publish_status, response_payload, attempts = _publish_http(payload, target, timeout_seconds, retry_count)
    elif mode == "mqtt":
        target = os.getenv("SOLENOID_MQTT_TOPIC", "oil/libya-ethiopia/solenoid/cmd")
        publish_status, response_payload, attempts = _publish_mqtt(payload, target, timeout_seconds, retry_count)

    record = {
        "request_id": request_id,
        "command": command,
        "mode": mode,
        "target": target,
        "reason": reason,
        "status": publish_status,
        "payload": payload,
        "response": response_payload,
        "attempts": attempts,
        "created_at": _utc_now(),
    }
    save_solenoid_command(record)

    logger.info(
        "solenoid_command_processed",
        extra={
            "request_id": request_id,
            "command": command,
            "mode": mode,
            "target": target,
            "status": publish_status,
            "attempts": attempts,
            "reason": reason,
        },
    )

    return {
        "status": publish_status,
        "request_id": request_id,
        "mode": mode,
        "target": target,
        "attempts": attempts,
        "response": response_payload,
    }
