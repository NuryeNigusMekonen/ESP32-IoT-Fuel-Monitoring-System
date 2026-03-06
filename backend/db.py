import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from werkzeug.security import check_password_hash, generate_password_hash

DATABASE_PATH = Path(__file__).parent / "fuel_monitor.db"
GENERATOR_TANK = "Generator Tank"
EXTERNAL_TANK = "External Tank"
GENERATOR_MIN_LEVEL = 25.0
GENERATOR_REFILL_TARGET = 60.0
DEFAULT_DEVICE_INTERVAL_SECONDS = 180
DEVICE_OFFLINE_MULTIPLIER = 3
HEALTH_DEGRADED_THRESHOLD = 65.0
AUTONOMY_WARNING_HOURS = 4.0
AUTONOMY_CRITICAL_HOURS = 2.0

logger = logging.getLogger("oil_libya_ethiopia.db")


def _default_users_config() -> list[tuple[str, str, str]]:
    return [
        ("worker", os.getenv("DEFAULT_WORKER_PASSWORD", "Worker@123"), "worker"),
        ("manager", os.getenv("DEFAULT_MANAGER_PASSWORD", "Manager@123"), "manager"),
        ("admin", os.getenv("DEFAULT_ADMIN_PASSWORD", "Admin@123"), "admin"),
    ]


def _default_password_for_username(username: str) -> str | None:
    for configured_username, configured_password, _ in _default_users_config():
        if configured_username == username:
            return configured_password
    return None


def _clamp_level(value: float) -> float:
    return max(0.0, min(100.0, value))


def _parse_timestamp(timestamp_text: str | None) -> datetime:
    if not timestamp_text:
        return datetime.now(timezone.utc)

    normalized = timestamp_text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@contextmanager
def get_connection():
    connection = sqlite3.connect(DATABASE_PATH)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def init_db() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tank_name TEXT NOT NULL,
                fuel_level REAL NOT NULL,
                consumption_rate REAL NOT NULL,
                recorded_at TEXT NOT NULL,
                raw_payload TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tank_name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                fuel_level REAL NOT NULL,
                delta REAL NOT NULL,
                recorded_at TEXT NOT NULL,
                details TEXT
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS solenoid_commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT UNIQUE NOT NULL,
                command TEXT NOT NULL,
                mode TEXT NOT NULL,
                target TEXT,
                reason TEXT,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                response TEXT,
                attempts INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS iot_devices (
                device_id TEXT PRIMARY KEY,
                tank_name TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                expected_interval_seconds INTEGER NOT NULL,
                battery_voltage REAL,
                signal_rssi INTEGER,
                firmware_version TEXT,
                quality_score REAL NOT NULL,
                health_score REAL NOT NULL,
                last_fuel_level REAL NOT NULL,
                last_consumption_rate REAL NOT NULL,
                last_event_type TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS refill_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tank_name TEXT NOT NULL,
                source_tank TEXT NOT NULL,
                requested_level REAL NOT NULL,
                target_level REAL NOT NULL,
                estimated_transfer_amount REAL NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                requested_by TEXT NOT NULL,
                approved_by TEXT,
                rejected_by TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                approved_at TEXT,
                rejected_at TEXT,
                execution_result TEXT
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                is_active INTEGER NOT NULL,
                must_change_password INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        user_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(users)").fetchall()
        }
        if "must_change_password" not in user_columns:
            connection.execute(
                "ALTER TABLE users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0"
            )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_key TEXT UNIQUE NOT NULL,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                status TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                tank_name TEXT,
                device_id TEXT,
                metadata TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                resolved_at TEXT,
                acknowledged_by TEXT,
                acknowledged_at TEXT,
                silenced_until TEXT,
                updated_by TEXT
            )
            """
        )

        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(alerts)").fetchall()
        }
        column_migrations = {
            "acknowledged_by": "ALTER TABLE alerts ADD COLUMN acknowledged_by TEXT",
            "acknowledged_at": "ALTER TABLE alerts ADD COLUMN acknowledged_at TEXT",
            "silenced_until": "ALTER TABLE alerts ADD COLUMN silenced_until TEXT",
            "updated_by": "ALTER TABLE alerts ADD COLUMN updated_by TEXT",
        }
        for column_name, migration_sql in column_migrations.items():
            if column_name not in existing_columns:
                connection.execute(migration_sql)

        _bootstrap_default_users(connection)


def _bootstrap_default_users(connection: sqlite3.Connection) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    for username, password, role in _default_users_config():
        existing = connection.execute(
            """
            SELECT id, password_hash, must_change_password
            FROM users
            WHERE username = ?
            LIMIT 1
            """,
            (username,),
        ).fetchone()
        if existing is not None:
            if check_password_hash(existing["password_hash"], password) and int(existing["must_change_password"]) != 1:
                connection.execute(
                    """
                    UPDATE users
                    SET must_change_password = 1,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now_iso, int(existing["id"])),
                )
            continue

        connection.execute(
            """
            INSERT INTO users (username, password_hash, role, is_active, must_change_password, created_at, updated_at)
            VALUES (?, ?, ?, 1, 1, ?, ?)
            """,
            (username, generate_password_hash(password), role, now_iso, now_iso),
        )


def authenticate_user(username: str, password: str) -> dict[str, Any] | None:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, username, password_hash, role, is_active, must_change_password
            FROM users
            WHERE username = ?
            LIMIT 1
            """,
            (username,),
        ).fetchone()

    if row is None:
        return None

    if int(row["is_active"]) != 1:
        return None

    if not check_password_hash(row["password_hash"], password):
        return None

    must_change_password = bool(int(row["must_change_password"]))
    configured_default_password = _default_password_for_username(username)
    logged_in_with_default_password = configured_default_password is not None and password == configured_default_password

    if logged_in_with_default_password and not must_change_password:
        with get_connection() as connection:
            connection.execute(
                """
                UPDATE users
                SET must_change_password = 1,
                    updated_at = ?
                WHERE id = ?
                """,
                (datetime.now(timezone.utc).isoformat(), int(row["id"])),
            )
        must_change_password = True

    return {
        "id": int(row["id"]),
        "username": row["username"],
        "role": row["role"],
        "must_change_password": must_change_password,
    }


def get_user_by_id(user_id: int) -> dict[str, Any] | None:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, username, role, is_active, must_change_password
            FROM users
            WHERE id = ?
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()

    if row is None or int(row["is_active"]) != 1:
        return None

    return {
        "id": int(row["id"]),
        "username": row["username"],
        "role": row["role"],
        "must_change_password": bool(int(row["must_change_password"])),
    }


def change_user_password(*, user_id: int, current_password: str, new_password: str) -> dict[str, Any]:
    if len(new_password) < 8:
        raise ValueError("new_password must be at least 8 characters")

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, username, password_hash, role, is_active
            FROM users
            WHERE id = ?
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()

        if row is None or int(row["is_active"]) != 1:
            raise LookupError("user not found")

        if not check_password_hash(row["password_hash"], current_password):
            raise ValueError("current_password is invalid")

        if check_password_hash(row["password_hash"], new_password):
            raise ValueError("new_password must be different from current password")

        now_iso = datetime.now(timezone.utc).isoformat()
        connection.execute(
            """
            UPDATE users
            SET password_hash = ?,
                must_change_password = 0,
                updated_at = ?
            WHERE id = ?
            """,
            (generate_password_hash(new_password), now_iso, user_id),
        )

    return {
        "id": int(row["id"]),
        "username": row["username"],
        "role": row["role"],
        "must_change_password": False,
    }


def _derive_device_id(tank_name: str, payload: dict[str, Any]) -> str:
    provided = str(payload.get("device_id") or "").strip()
    if provided:
        return provided
    normalized_tank = tank_name.lower().replace(" ", "-")
    return f"sensor-{normalized_tank}"


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _compute_quality_score(
    *,
    fuel_level: float,
    consumption_rate: float,
    previous_level: float | None,
    within_window: bool,
    event_type: str,
    event_delta: float,
    battery_voltage: float | None,
    signal_rssi: int | None,
    recorded_at: str,
) -> float:
    score = 100.0

    if fuel_level <= 0.2 or fuel_level >= 99.8:
        score -= 5

    if consumption_rate < 0:
        score -= 30

    if previous_level is not None and within_window:
        if abs(event_delta) > 25 and event_type != "refill":
            score -= 25

    recorded_time = _parse_timestamp(recorded_at)
    now_utc = datetime.now(timezone.utc)
    if recorded_time > now_utc + timedelta(minutes=2):
        score -= 20
    if now_utc - recorded_time > timedelta(hours=24):
        score -= 20

    if battery_voltage is not None:
        if battery_voltage < 3.3:
            score -= 20
        elif battery_voltage < 3.5:
            score -= 10

    if signal_rssi is not None:
        if signal_rssi < -95:
            score -= 20
        elif signal_rssi < -85:
            score -= 10

    return round(max(0.0, min(100.0, score)), 2)


def _compute_health_score(
    *,
    quality_score: float,
    battery_voltage: float | None,
    signal_rssi: int | None,
    age_seconds: int,
    expected_interval_seconds: int,
) -> float:
    score = float(quality_score)

    if battery_voltage is not None:
        if battery_voltage < 3.3:
            score -= 20
        elif battery_voltage < 3.5:
            score -= 10

    if signal_rssi is not None:
        if signal_rssi < -95:
            score -= 20
        elif signal_rssi < -85:
            score -= 10

    if age_seconds > expected_interval_seconds * DEVICE_OFFLINE_MULTIPLIER:
        score -= 35
    elif age_seconds > expected_interval_seconds * 2:
        score -= 15

    return round(max(0.0, min(100.0, score)), 2)


def _predict_hours_to_empty(level: float, consumption_rate: float) -> float | None:
    if consumption_rate <= 0:
        return None
    return round(level / consumption_rate, 2)


def _upsert_device_state(
    connection: sqlite3.Connection,
    *,
    device_id: str,
    tank_name: str,
    recorded_at: str,
    expected_interval_seconds: int,
    battery_voltage: float | None,
    signal_rssi: int | None,
    firmware_version: str | None,
    quality_score: float,
    health_score: float,
    fuel_level: float,
    consumption_rate: float,
    event_type: str,
) -> None:
    connection.execute(
        """
        INSERT INTO iot_devices (
            device_id,
            tank_name,
            last_seen_at,
            expected_interval_seconds,
            battery_voltage,
            signal_rssi,
            firmware_version,
            quality_score,
            health_score,
            last_fuel_level,
            last_consumption_rate,
            last_event_type,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(device_id) DO UPDATE SET
            tank_name = excluded.tank_name,
            last_seen_at = excluded.last_seen_at,
            expected_interval_seconds = excluded.expected_interval_seconds,
            battery_voltage = excluded.battery_voltage,
            signal_rssi = excluded.signal_rssi,
            firmware_version = excluded.firmware_version,
            quality_score = excluded.quality_score,
            health_score = excluded.health_score,
            last_fuel_level = excluded.last_fuel_level,
            last_consumption_rate = excluded.last_consumption_rate,
            last_event_type = excluded.last_event_type,
            updated_at = excluded.updated_at
        """,
        (
            device_id,
            tank_name,
            recorded_at,
            expected_interval_seconds,
            battery_voltage,
            signal_rssi,
            firmware_version,
            quality_score,
            health_score,
            fuel_level,
            consumption_rate,
            event_type,
            datetime.now(timezone.utc).isoformat(),
        ),
    )


def _detect_event_type(previous_level: float, current_level: float, within_window: bool) -> tuple[str, float]:
    delta = current_level - previous_level

    if within_window and delta <= -10.0:
        return "abnormal_drop", delta
    if within_window and delta >= 8.0:
        return "refill", delta
    return "normal_usage", delta


def _latest_tank_snapshot(connection: sqlite3.Connection, tank_name: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT fuel_level, recorded_at
        FROM sensor_data
        WHERE tank_name = ?
        ORDER BY recorded_at DESC
        LIMIT 1
        """,
        (tank_name,),
    ).fetchone()


def _insert_event(
    connection: sqlite3.Connection,
    *,
    tank_name: str,
    event_type: str,
    fuel_level: float,
    delta: float,
    recorded_at: str,
    details: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO events (tank_name, event_type, fuel_level, delta, recorded_at, details)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (tank_name, event_type, fuel_level, delta, recorded_at, json.dumps(details)),
    )


def _insert_sensor_row(
    connection: sqlite3.Connection,
    *,
    tank_name: str,
    fuel_level: float,
    consumption_rate: float,
    recorded_at: str,
    raw_payload: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO sensor_data (tank_name, fuel_level, consumption_rate, recorded_at, raw_payload)
        VALUES (?, ?, ?, ?, ?)
        """,
        (tank_name, fuel_level, consumption_rate, recorded_at, json.dumps(raw_payload)),
    )


def _apply_generator_auto_refill(
    connection: sqlite3.Connection,
    *,
    tank_name: str,
    fuel_level: float,
    recorded_at: str,
    reason: str = "policy_auto_refill",
    request_id: str | None = None,
) -> dict[str, float] | None:
    if tank_name != GENERATOR_TANK or fuel_level >= GENERATOR_MIN_LEVEL:
        return None

    external_snapshot = _latest_tank_snapshot(connection, EXTERNAL_TANK)
    if external_snapshot is None:
        return None

    external_level = _clamp_level(float(external_snapshot["fuel_level"]))
    needed = max(0.0, _clamp_level(GENERATOR_REFILL_TARGET) - _clamp_level(fuel_level))
    transfer_amount = min(needed, external_level)

    if transfer_amount <= 0:
        return None

    updated_generator_level = round(_clamp_level(fuel_level + transfer_amount), 2)
    updated_external_level = round(_clamp_level(external_level - transfer_amount), 2)

    transfer_payload = {
        "auto_transfer": True,
        "source_tank": EXTERNAL_TANK,
        "target_tank": GENERATOR_TANK,
        "transfer_amount": round(transfer_amount, 2),
        "timestamp": recorded_at,
    }

    _insert_sensor_row(
        connection,
        tank_name=GENERATOR_TANK,
        fuel_level=updated_generator_level,
        consumption_rate=0.0,
        recorded_at=recorded_at,
        raw_payload=transfer_payload,
    )
    _insert_event(
        connection,
        tank_name=GENERATOR_TANK,
        event_type="refill",
        fuel_level=updated_generator_level,
        delta=round(transfer_amount, 2),
        recorded_at=recorded_at,
        details={
            "auto_transfer": True,
            "from_tank": EXTERNAL_TANK,
            "transfer_amount": round(transfer_amount, 2),
            "trigger_min_level": GENERATOR_MIN_LEVEL,
            "target_level": GENERATOR_REFILL_TARGET,
            "reason": reason,
            "request_id": request_id,
        },
    )

    _insert_sensor_row(
        connection,
        tank_name=EXTERNAL_TANK,
        fuel_level=updated_external_level,
        consumption_rate=0.0,
        recorded_at=recorded_at,
        raw_payload=transfer_payload,
    )
    _insert_event(
        connection,
        tank_name=EXTERNAL_TANK,
        event_type="normal_usage",
        fuel_level=updated_external_level,
        delta=round(-transfer_amount, 2),
        recorded_at=recorded_at,
        details={
            "transfer_to": GENERATOR_TANK,
            "auto_transfer": True,
            "transfer_amount": round(transfer_amount, 2),
            "reason": reason,
            "request_id": request_id,
        },
    )

    logger.info(
        "auto_refill_applied",
        extra={
            "generator_level_before": fuel_level,
            "generator_level_after": updated_generator_level,
            "external_level_after": updated_external_level,
            "transfer_amount": round(transfer_amount, 2),
            "recorded_at": recorded_at,
        },
    )

    return {
        "generator_level": updated_generator_level,
        "external_level": updated_external_level,
        "transfer_amount": round(transfer_amount, 2),
    }


def _estimate_refill_transfer(connection: sqlite3.Connection, generator_level: float) -> tuple[float, float] | None:
    external_snapshot = _latest_tank_snapshot(connection, EXTERNAL_TANK)
    if external_snapshot is None:
        return None

    external_level = _clamp_level(float(external_snapshot["fuel_level"]))
    needed = max(0.0, _clamp_level(GENERATOR_REFILL_TARGET) - _clamp_level(generator_level))
    transfer_amount = min(needed, external_level)

    if transfer_amount <= 0:
        return None

    return round(external_level, 2), round(transfer_amount, 2)


def _ensure_refill_request(
    connection: sqlite3.Connection,
    *,
    generator_level: float,
    reason: str,
    requested_by: str,
    created_at: str,
) -> dict[str, Any] | None:
    if generator_level >= GENERATOR_MIN_LEVEL:
        return None

    pending = connection.execute(
        """
        SELECT id, tank_name, source_tank, requested_level, target_level, estimated_transfer_amount,
               status, reason, requested_by, approved_by, rejected_by, created_at, updated_at,
               approved_at, rejected_at, execution_result
        FROM refill_requests
        WHERE status = 'pending'
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    if pending is not None:
        return {
            "id": int(pending["id"]),
            "tank_name": pending["tank_name"],
            "source_tank": pending["source_tank"],
            "requested_level": float(pending["requested_level"]),
            "target_level": float(pending["target_level"]),
            "estimated_transfer_amount": float(pending["estimated_transfer_amount"]),
            "status": pending["status"],
            "reason": pending["reason"],
            "requested_by": pending["requested_by"],
            "approved_by": pending["approved_by"],
            "rejected_by": pending["rejected_by"],
            "created_at": pending["created_at"],
            "updated_at": pending["updated_at"],
            "approved_at": pending["approved_at"],
            "rejected_at": pending["rejected_at"],
            "execution_result": json.loads(pending["execution_result"] or "{}"),
        }

    estimate = _estimate_refill_transfer(connection, generator_level)
    if estimate is None:
        return None

    external_level, transfer_amount = estimate
    cursor = connection.execute(
        """
        INSERT INTO refill_requests (
            tank_name,
            source_tank,
            requested_level,
            target_level,
            estimated_transfer_amount,
            status,
            reason,
            requested_by,
            approved_by,
            rejected_by,
            created_at,
            updated_at,
            approved_at,
            rejected_at,
            execution_result
        )
        VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, NULL, NULL, ?, ?, NULL, NULL, ?)
        """,
        (
            GENERATOR_TANK,
            EXTERNAL_TANK,
            round(generator_level, 2),
            GENERATOR_REFILL_TARGET,
            transfer_amount,
            reason,
            requested_by,
            created_at,
            created_at,
            json.dumps({"external_level": external_level}),
        ),
    )

    raw_request_id = cursor.lastrowid
    if raw_request_id is None:
        raise ValueError("unable to create refill request")
    request_id = int(raw_request_id)
    return {
        "id": request_id,
        "tank_name": GENERATOR_TANK,
        "source_tank": EXTERNAL_TANK,
        "requested_level": round(generator_level, 2),
        "target_level": GENERATOR_REFILL_TARGET,
        "estimated_transfer_amount": transfer_amount,
        "status": "pending",
        "reason": reason,
        "requested_by": requested_by,
        "approved_by": None,
        "rejected_by": None,
        "created_at": created_at,
        "updated_at": created_at,
        "approved_at": None,
        "rejected_at": None,
        "execution_result": {"external_level": external_level},
    }


def insert_sensor_payload(
    payload: dict[str, Any],
    *,
    require_refill_approval: bool = False,
    requested_by: str = "system-policy",
) -> dict[str, Any]:
    tank_name = str(payload.get("tank_name", GENERATOR_TANK))
    device_id = _derive_device_id(tank_name, payload)
    fuel_level = _clamp_level(float(payload["fuel_level"]))
    consumption_rate = float(payload.get("consumption_rate", 0.0))
    battery_voltage = _optional_float(payload.get("battery_voltage"))
    signal_rssi = _optional_int(payload.get("signal_rssi"))
    firmware_version = str(payload.get("firmware_version") or "").strip() or None
    expected_interval_seconds = max(30, int(payload.get("expected_interval_seconds") or DEFAULT_DEVICE_INTERVAL_SECONDS))
    recorded_at = _parse_timestamp(payload.get("timestamp")).isoformat()

    with get_connection() as connection:
        previous_row = _latest_tank_snapshot(connection, tank_name)

        _insert_sensor_row(
            connection,
            tank_name=tank_name,
            fuel_level=fuel_level,
            consumption_rate=consumption_rate,
            recorded_at=recorded_at,
            raw_payload=payload,
        )

        event_type = "normal_usage"
        delta = 0.0

        if previous_row is not None:
            previous_level = float(previous_row["fuel_level"])
            previous_time = _parse_timestamp(previous_row["recorded_at"])
            current_time = _parse_timestamp(recorded_at)
            within_window = current_time - previous_time <= timedelta(minutes=10)
            event_type, delta = _detect_event_type(previous_level, fuel_level, within_window)
        else:
            previous_level = None
            within_window = False

        quality_score = _compute_quality_score(
            fuel_level=fuel_level,
            consumption_rate=consumption_rate,
            previous_level=previous_level,
            within_window=within_window,
            event_type=event_type,
            event_delta=delta,
            battery_voltage=battery_voltage,
            signal_rssi=signal_rssi,
            recorded_at=recorded_at,
        )

        health_score = _compute_health_score(
            quality_score=quality_score,
            battery_voltage=battery_voltage,
            signal_rssi=signal_rssi,
            age_seconds=0,
            expected_interval_seconds=expected_interval_seconds,
        )

        details = {
            "consumption_rate": consumption_rate,
            "rule_window_minutes": 10,
            "device_id": device_id,
            "quality_score": quality_score,
            "health_score": health_score,
            "thresholds": {
                "abnormal_drop_percent": -10,
                "refill_percent": 8,
            },
        }

        _insert_event(
            connection,
            tank_name=tank_name,
            event_type=event_type,
            fuel_level=fuel_level,
            delta=delta,
            recorded_at=recorded_at,
            details=details,
        )

        auto_refill = None
        refill_request = None
        if require_refill_approval:
            generator_snapshot = _latest_tank_snapshot(connection, GENERATOR_TANK)
            generator_level_for_request = fuel_level
            if tank_name != GENERATOR_TANK and generator_snapshot is not None:
                generator_level_for_request = _clamp_level(float(generator_snapshot["fuel_level"]))

            refill_request = _ensure_refill_request(
                connection,
                generator_level=generator_level_for_request,
                reason="generator_below_minimum",
                requested_by=requested_by,
                created_at=recorded_at,
            )
        else:
            auto_refill = _apply_generator_auto_refill(
                connection,
                tank_name=tank_name,
                fuel_level=fuel_level,
                recorded_at=recorded_at,
            )

        _upsert_device_state(
            connection,
            device_id=device_id,
            tank_name=tank_name,
            recorded_at=recorded_at,
            expected_interval_seconds=expected_interval_seconds,
            battery_voltage=battery_voltage,
            signal_rssi=signal_rssi,
            firmware_version=firmware_version,
            quality_score=quality_score,
            health_score=health_score,
            fuel_level=fuel_level,
            consumption_rate=consumption_rate,
            event_type=event_type,
        )

        logger.info(
            "sensor_ingested",
            extra={
                "device_id": device_id,
                "tank_name": tank_name,
                "fuel_level": fuel_level,
                "consumption_rate": consumption_rate,
                "quality_score": quality_score,
                "health_score": health_score,
                "event_type": event_type,
                "delta": delta,
                "recorded_at": recorded_at,
            },
        )

    return {
        "tank_name": tank_name,
        "device_id": device_id,
        "fuel_level": fuel_level,
        "consumption_rate": consumption_rate,
        "battery_voltage": battery_voltage,
        "signal_rssi": signal_rssi,
        "firmware_version": firmware_version,
        "quality_score": quality_score,
        "health_score": health_score,
        "recorded_at": recorded_at,
        "event_type": event_type,
        "delta": delta,
        "auto_refill": auto_refill,
        "refill_request": refill_request,
    }


def list_refill_requests(limit: int = 50, status: str | None = None) -> dict[str, Any]:
    with get_connection() as connection:
        if status:
            rows = connection.execute(
                """
                SELECT id, tank_name, source_tank, requested_level, target_level, estimated_transfer_amount,
                       status, reason, requested_by, approved_by, rejected_by, created_at, updated_at,
                       approved_at, rejected_at, execution_result
                FROM refill_requests
                WHERE status = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (status, limit),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT id, tank_name, source_tank, requested_level, target_level, estimated_transfer_amount,
                       status, reason, requested_by, approved_by, rejected_by, created_at, updated_at,
                       approved_at, rejected_at, execution_result
                FROM refill_requests
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    requests_payload: list[dict[str, Any]] = []
    for row in rows:
        requests_payload.append(
            {
                "id": int(row["id"]),
                "tank_name": row["tank_name"],
                "source_tank": row["source_tank"],
                "requested_level": float(row["requested_level"]),
                "target_level": float(row["target_level"]),
                "estimated_transfer_amount": float(row["estimated_transfer_amount"]),
                "status": row["status"],
                "reason": row["reason"],
                "requested_by": row["requested_by"],
                "approved_by": row["approved_by"],
                "rejected_by": row["rejected_by"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "approved_at": row["approved_at"],
                "rejected_at": row["rejected_at"],
                "execution_result": json.loads(row["execution_result"] or "{}"),
            }
        )

    summary = {
        "total": len(requests_payload),
        "pending": sum(1 for request in requests_payload if request["status"] == "pending"),
        "executed": sum(1 for request in requests_payload if request["status"] == "executed"),
        "rejected": sum(1 for request in requests_payload if request["status"] == "rejected"),
    }

    return {"summary": summary, "requests": requests_payload}


def process_refill_request(
    *,
    request_id: int,
    action: str,
    actor_id: str,
    actor_role: str,
) -> dict[str, Any]:
    normalized_action = action.lower().strip()
    normalized_role = actor_role.lower().strip()

    if normalized_role not in {"manager", "admin"}:
        raise ValueError("manager or admin role required")

    if normalized_action not in {"approve", "reject"}:
        raise ValueError("action must be approve or reject")

    now_iso = datetime.now(timezone.utc).isoformat()

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, status
            FROM refill_requests
            WHERE id = ?
            LIMIT 1
            """,
            (request_id,),
        ).fetchone()

        if row is None:
            raise LookupError("refill request not found")

        if row["status"] != "pending":
            raise ValueError("only pending refill requests can be processed")

        if normalized_action == "reject":
            connection.execute(
                """
                UPDATE refill_requests
                SET status = 'rejected',
                    rejected_by = ?,
                    rejected_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (actor_id, now_iso, now_iso, request_id),
            )
        else:
            generator_snapshot = _latest_tank_snapshot(connection, GENERATOR_TANK)
            if generator_snapshot is None:
                raise ValueError("cannot approve refill: generator snapshot unavailable")

            current_level = _clamp_level(float(generator_snapshot["fuel_level"]))
            transfer = _apply_generator_auto_refill(
                connection,
                tank_name=GENERATOR_TANK,
                fuel_level=current_level,
                recorded_at=now_iso,
                reason="manager_approved_refill",
                request_id=f"refill-request-{request_id}",
            )

            if transfer is None:
                raise ValueError("cannot execute refill: no transfer capacity available")

            connection.execute(
                """
                UPDATE refill_requests
                SET status = 'executed',
                    approved_by = ?,
                    approved_at = ?,
                    updated_at = ?,
                    execution_result = ?
                WHERE id = ?
                """,
                (
                    actor_id,
                    now_iso,
                    now_iso,
                    json.dumps(transfer),
                    request_id,
                ),
            )

    return list_refill_requests(limit=100)


def get_iot_overview() -> dict[str, Any]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                device_id,
                tank_name,
                last_seen_at,
                expected_interval_seconds,
                battery_voltage,
                signal_rssi,
                firmware_version,
                quality_score,
                health_score,
                last_fuel_level,
                last_consumption_rate,
                last_event_type
            FROM iot_devices
            ORDER BY last_seen_at DESC
            """
        ).fetchall()

    now_utc = datetime.now(timezone.utc)
    device_payloads: list[dict[str, Any]] = []

    for row in rows:
        last_seen = _parse_timestamp(row["last_seen_at"])
        age_seconds = max(0, int((now_utc - last_seen).total_seconds()))
        expected_interval_seconds = int(row["expected_interval_seconds"] or DEFAULT_DEVICE_INTERVAL_SECONDS)

        refreshed_health = _compute_health_score(
            quality_score=float(row["quality_score"]),
            battery_voltage=_optional_float(row["battery_voltage"]),
            signal_rssi=_optional_int(row["signal_rssi"]),
            age_seconds=age_seconds,
            expected_interval_seconds=expected_interval_seconds,
        )

        if age_seconds > expected_interval_seconds * DEVICE_OFFLINE_MULTIPLIER:
            status = "Offline"
        elif refreshed_health < HEALTH_DEGRADED_THRESHOLD or float(row["quality_score"]) < HEALTH_DEGRADED_THRESHOLD:
            status = "Degraded"
        else:
            status = "Online"

        last_fuel_level = _clamp_level(float(row["last_fuel_level"]))
        last_consumption_rate = float(row["last_consumption_rate"])

        device_payloads.append(
            {
                "device_id": row["device_id"],
                "tank_name": row["tank_name"],
                "status": status,
                "last_seen_at": row["last_seen_at"],
                "telemetry_age_seconds": age_seconds,
                "expected_interval_seconds": expected_interval_seconds,
                "battery_voltage": row["battery_voltage"],
                "signal_rssi": row["signal_rssi"],
                "firmware_version": row["firmware_version"],
                "quality_score": float(row["quality_score"]),
                "health_score": refreshed_health,
                "fuel_level": last_fuel_level,
                "consumption_rate": last_consumption_rate,
                "predicted_hours_to_empty": _predict_hours_to_empty(last_fuel_level, last_consumption_rate),
                "last_event_type": row["last_event_type"],
            }
        )

    summary = {
        "total": len(device_payloads),
        "online": sum(1 for device in device_payloads if device["status"] == "Online"),
        "degraded": sum(1 for device in device_payloads if device["status"] == "Degraded"),
        "offline": sum(1 for device in device_payloads if device["status"] == "Offline"),
    }

    return {"fleet": summary, "devices": device_payloads}


def _upsert_sla_alert(
    connection: sqlite3.Connection,
    *,
    alert_key: str,
    alert_type: str,
    severity: str,
    title: str,
    message: str,
    tank_name: str | None,
    device_id: str | None,
    metadata: dict[str, Any],
    now_iso: str,
) -> None:
    existing = connection.execute(
        """
        SELECT first_seen_at
        FROM alerts
        WHERE alert_key = ?
        LIMIT 1
        """,
        (alert_key,),
    ).fetchone()

    first_seen_at = existing["first_seen_at"] if existing else now_iso

    connection.execute(
        """
        INSERT INTO alerts (
            alert_key,
            alert_type,
            severity,
            status,
            title,
            message,
            tank_name,
            device_id,
            metadata,
            first_seen_at,
            last_seen_at,
            resolved_at,
            acknowledged_by,
            acknowledged_at,
            silenced_until,
            updated_by
        )
        VALUES (?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL)
        ON CONFLICT(alert_key) DO UPDATE SET
            alert_type = excluded.alert_type,
            severity = excluded.severity,
            status = CASE
                WHEN alerts.silenced_until IS NOT NULL AND alerts.silenced_until > excluded.last_seen_at THEN 'silenced'
                WHEN alerts.status = 'acknowledged' THEN 'acknowledged'
                ELSE 'open'
            END,
            title = excluded.title,
            message = excluded.message,
            tank_name = excluded.tank_name,
            device_id = excluded.device_id,
            metadata = excluded.metadata,
            first_seen_at = ?,
            last_seen_at = excluded.last_seen_at,
            resolved_at = CASE
                WHEN alerts.silenced_until IS NOT NULL AND alerts.silenced_until > excluded.last_seen_at THEN alerts.resolved_at
                ELSE NULL
            END,
            acknowledged_by = CASE
                WHEN alerts.status = 'acknowledged' THEN alerts.acknowledged_by
                ELSE NULL
            END,
            acknowledged_at = CASE
                WHEN alerts.status = 'acknowledged' THEN alerts.acknowledged_at
                ELSE NULL
            END,
            updated_by = CASE
                WHEN alerts.status IN ('acknowledged', 'silenced') THEN alerts.updated_by
                ELSE NULL
            END
        """,
        (
            alert_key,
            alert_type,
            severity,
            title,
            message,
            tank_name,
            device_id,
            json.dumps(metadata),
            first_seen_at,
            now_iso,
            first_seen_at,
        ),
    )


def _resolve_inactive_alerts(
    connection: sqlite3.Connection,
    *,
    managed_types: tuple[str, ...],
    active_alert_keys: set[str],
    now_iso: str,
) -> None:
    if not managed_types:
        return

    type_placeholders = ",".join("?" for _ in managed_types)

    if active_alert_keys:
        key_placeholders = ",".join("?" for _ in active_alert_keys)
        connection.execute(
            f"""
            UPDATE alerts
            SET status = 'resolved', resolved_at = ?, last_seen_at = ?
            WHERE status = 'open'
              AND alert_type IN ({type_placeholders})
              AND alert_key NOT IN ({key_placeholders})
            """,
            (now_iso, now_iso, *managed_types, *active_alert_keys),
        )
        return

    connection.execute(
        f"""
        UPDATE alerts
        SET status = 'resolved', resolved_at = ?, last_seen_at = ?
        WHERE status = 'open'
          AND alert_type IN ({type_placeholders})
        """,
        (now_iso, now_iso, *managed_types),
    )


def refresh_sla_alerts() -> dict[str, Any]:
    overview = get_iot_overview()
    metrics = get_current_metrics()
    now_iso = datetime.now(timezone.utc).isoformat()

    managed_types = (
        "device_offline",
        "device_degraded",
        "device_low_battery",
        "generator_low_autonomy",
        "generator_low_fuel",
    )

    active_alert_keys: set[str] = set()

    with get_connection() as connection:
        for device in overview["devices"]:
            device_id = str(device["device_id"])
            tank_name = str(device["tank_name"])
            health_score = float(device["health_score"])
            quality_score = float(device["quality_score"])
            status = str(device["status"])

            if status == "Offline":
                alert_key = f"device-offline:{device_id}"
                active_alert_keys.add(alert_key)
                _upsert_sla_alert(
                    connection,
                    alert_key=alert_key,
                    alert_type="device_offline",
                    severity="critical",
                    title="Device Offline",
                    message=f"{device_id} stopped sending telemetry for {device['telemetry_age_seconds']} seconds.",
                    tank_name=tank_name,
                    device_id=device_id,
                    metadata={
                        "telemetry_age_seconds": device["telemetry_age_seconds"],
                        "expected_interval_seconds": device["expected_interval_seconds"],
                        "health_score": health_score,
                        "quality_score": quality_score,
                    },
                    now_iso=now_iso,
                )

            elif status == "Degraded":
                alert_key = f"device-degraded:{device_id}"
                active_alert_keys.add(alert_key)
                _upsert_sla_alert(
                    connection,
                    alert_key=alert_key,
                    alert_type="device_degraded",
                    severity="high",
                    title="Device Degraded",
                    message=f"{device_id} quality/health dropped below operational threshold.",
                    tank_name=tank_name,
                    device_id=device_id,
                    metadata={
                        "health_score": health_score,
                        "quality_score": quality_score,
                        "signal_rssi": device.get("signal_rssi"),
                    },
                    now_iso=now_iso,
                )

            battery_voltage = _optional_float(device.get("battery_voltage"))
            if battery_voltage is not None and battery_voltage < 3.35:
                alert_key = f"device-low-battery:{device_id}"
                active_alert_keys.add(alert_key)
                _upsert_sla_alert(
                    connection,
                    alert_key=alert_key,
                    alert_type="device_low_battery",
                    severity="high",
                    title="Low Battery Voltage",
                    message=f"{device_id} battery is {battery_voltage:.2f}V and requires maintenance.",
                    tank_name=tank_name,
                    device_id=device_id,
                    metadata={"battery_voltage": battery_voltage},
                    now_iso=now_iso,
                )

        generator_device = next(
            (device for device in overview["devices"] if device["tank_name"] == GENERATOR_TANK),
            overview["devices"][0] if overview["devices"] else None,
        )
        if generator_device:
            predicted_hours = generator_device.get("predicted_hours_to_empty")
            if predicted_hours is not None and float(predicted_hours) <= AUTONOMY_WARNING_HOURS:
                severity = "critical" if float(predicted_hours) <= AUTONOMY_CRITICAL_HOURS else "warning"
                alert_key = "generator-low-autonomy"
                active_alert_keys.add(alert_key)
                _upsert_sla_alert(
                    connection,
                    alert_key=alert_key,
                    alert_type="generator_low_autonomy",
                    severity=severity,
                    title="Low Generator Autonomy",
                    message=f"Estimated runtime is {float(predicted_hours):.2f} hours.",
                    tank_name=GENERATOR_TANK,
                    device_id=str(generator_device["device_id"]),
                    metadata={
                        "predicted_hours_to_empty": float(predicted_hours),
                        "consumption_rate": generator_device.get("consumption_rate"),
                        "fuel_level": generator_device.get("fuel_level"),
                    },
                    now_iso=now_iso,
                )

        if metrics and float(metrics.get("fuel_level", 0.0)) < GENERATOR_MIN_LEVEL:
            alert_key = "generator-low-fuel"
            active_alert_keys.add(alert_key)
            _upsert_sla_alert(
                connection,
                alert_key=alert_key,
                alert_type="generator_low_fuel",
                severity="critical",
                title="Generator Fuel Below Minimum",
                message=(
                    f"Generator level {float(metrics['fuel_level']):.2f}% is below minimum "
                    f"threshold {GENERATOR_MIN_LEVEL:.2f}%."
                ),
                tank_name=GENERATOR_TANK,
                device_id=None,
                metadata={
                    "fuel_level": float(metrics["fuel_level"]),
                    "minimum_level": GENERATOR_MIN_LEVEL,
                },
                now_iso=now_iso,
            )

        _resolve_inactive_alerts(
            connection,
            managed_types=managed_types,
            active_alert_keys=active_alert_keys,
            now_iso=now_iso,
        )

    return list_sla_alerts(limit=100, include_resolved=False)


def list_sla_alerts(limit: int = 50, include_resolved: bool = False) -> dict[str, Any]:
    with get_connection() as connection:
        status_clause = "" if include_resolved else "WHERE status != 'resolved'"
        rows = connection.execute(
            f"""
            SELECT
                alert_key,
                alert_type,
                severity,
                status,
                title,
                message,
                tank_name,
                device_id,
                metadata,
                first_seen_at,
                last_seen_at,
                resolved_at,
                acknowledged_by,
                acknowledged_at,
                silenced_until,
                updated_by
            FROM alerts
            {status_clause}
            ORDER BY last_seen_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    alerts: list[dict[str, Any]] = []
    for row in rows:
        alerts.append(
            {
                "alert_key": row["alert_key"],
                "alert_type": row["alert_type"],
                "severity": row["severity"],
                "status": row["status"],
                "title": row["title"],
                "message": row["message"],
                "tank_name": row["tank_name"],
                "device_id": row["device_id"],
                "metadata": json.loads(row["metadata"] or "{}"),
                "first_seen_at": row["first_seen_at"],
                "last_seen_at": row["last_seen_at"],
                "resolved_at": row["resolved_at"],
                "acknowledged_by": row["acknowledged_by"],
                "acknowledged_at": row["acknowledged_at"],
                "silenced_until": row["silenced_until"],
                "updated_by": row["updated_by"],
            }
        )

    summary = {
        "total": len(alerts),
        "critical": sum(1 for alert in alerts if alert["severity"] == "critical"),
        "high": sum(1 for alert in alerts if alert["severity"] == "high"),
        "warning": sum(1 for alert in alerts if alert["severity"] == "warning"),
        "open": sum(1 for alert in alerts if alert["status"] == "open"),
        "acknowledged": sum(1 for alert in alerts if alert["status"] == "acknowledged"),
        "silenced": sum(1 for alert in alerts if alert["status"] == "silenced"),
    }

    return {"summary": summary, "alerts": alerts}


def update_sla_alert_state(
    *,
    alert_key: str,
    action: str,
    operator_id: str,
    operator_role: str,
    silence_minutes: int | None = None,
) -> dict[str, Any]:
    normalized_role = operator_role.lower().strip()
    normalized_action = action.lower().strip()

    if normalized_role not in {"manager", "admin"}:
        raise ValueError("operator_role must be one of: manager, admin")

    if normalized_action not in {"acknowledge", "silence", "resolve"}:
        raise ValueError("action must be one of: acknowledge, silence, resolve")

    if normalized_action == "resolve" and normalized_role != "admin":
        raise ValueError("resolve action requires admin role")

    now_iso = datetime.now(timezone.utc).isoformat()
    with get_connection() as connection:
        existing = connection.execute(
            """
            SELECT alert_key
            FROM alerts
            WHERE alert_key = ?
            LIMIT 1
            """,
            (alert_key,),
        ).fetchone()

        if existing is None:
            raise LookupError("alert not found")

        if normalized_action == "acknowledge":
            connection.execute(
                """
                UPDATE alerts
                SET
                    status = 'acknowledged',
                    acknowledged_by = ?,
                    acknowledged_at = ?,
                    updated_by = ?,
                    silenced_until = NULL,
                    resolved_at = NULL,
                    last_seen_at = ?
                WHERE alert_key = ?
                """,
                (operator_id, now_iso, operator_id, now_iso, alert_key),
            )

        elif normalized_action == "silence":
            minutes = max(1, min(1440, int(silence_minutes or 30)))
            silenced_until = (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()
            connection.execute(
                """
                UPDATE alerts
                SET
                    status = 'silenced',
                    silenced_until = ?,
                    updated_by = ?,
                    resolved_at = NULL,
                    last_seen_at = ?
                WHERE alert_key = ?
                """,
                (silenced_until, operator_id, now_iso, alert_key),
            )

        elif normalized_action == "resolve":
            connection.execute(
                """
                UPDATE alerts
                SET
                    status = 'resolved',
                    resolved_at = ?,
                    updated_by = ?,
                    silenced_until = NULL,
                    last_seen_at = ?
                WHERE alert_key = ?
                """,
                (now_iso, operator_id, now_iso, alert_key),
            )

    return list_sla_alerts(limit=200, include_resolved=True)


def get_current_metrics() -> dict[str, Any] | None:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT tank_name, fuel_level, consumption_rate, recorded_at
            FROM sensor_data
            ORDER BY recorded_at DESC
            """
        ).fetchall()

        if not rows:
            return None

        latest_by_tank: dict[str, sqlite3.Row] = {}
        for row in rows:
            tank_name = row["tank_name"]
            if tank_name not in latest_by_tank:
                latest_by_tank[tank_name] = row

        generator_row = latest_by_tank.get(GENERATOR_TANK)
        if generator_row is None:
            generator_row = next(iter(latest_by_tank.values()))

        external_row = latest_by_tank.get(EXTERNAL_TANK)
        external_level = _clamp_level(float(external_row["fuel_level"])) if external_row else 0.0
        generator_level = _clamp_level(float(generator_row["fuel_level"]))

        tank_status = "Stable"
        if generator_level < GENERATOR_MIN_LEVEL:
            tank_status = "Low"
        elif external_level <= 0:
            tank_status = "External Empty"

        return {
            "tank_name": GENERATOR_TANK,
            "fuel_level": generator_level,
            "consumption_rate": float(generator_row["consumption_rate"]),
            "last_update_time": generator_row["recorded_at"],
            "tank_status": tank_status,
            "minimum_level": GENERATOR_MIN_LEVEL,
            "tank_levels": {
                GENERATOR_TANK: generator_level,
                EXTERNAL_TANK: external_level,
            },
        }


def list_events(limit: int = 50) -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT tank_name, event_type, fuel_level, delta, recorded_at, details
            FROM events
            ORDER BY recorded_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        events = []
        for row in rows:
            events.append(
                {
                    "tank_name": row["tank_name"],
                    "event_type": row["event_type"],
                    "fuel_level": row["fuel_level"],
                    "delta": row["delta"],
                    "recorded_at": row["recorded_at"],
                    "details": json.loads(row["details"] or "{}"),
                }
            )

        return events


def get_solenoid_command_by_request_id(request_id: str) -> dict[str, Any] | None:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT request_id, command, mode, target, reason, status, payload, response, attempts, created_at
            FROM solenoid_commands
            WHERE request_id = ?
            LIMIT 1
            """,
            (request_id,),
        ).fetchone()

        if row is None:
            return None

        return {
            "request_id": row["request_id"],
            "command": row["command"],
            "mode": row["mode"],
            "target": row["target"],
            "reason": row["reason"],
            "status": row["status"],
            "payload": json.loads(row["payload"] or "{}"),
            "response": json.loads(row["response"] or "{}"),
            "attempts": row["attempts"],
            "created_at": row["created_at"],
        }


def save_solenoid_command(command_record: dict[str, Any]) -> None:
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO solenoid_commands (
                request_id,
                command,
                mode,
                target,
                reason,
                status,
                payload,
                response,
                attempts,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(request_id) DO UPDATE SET
                command = excluded.command,
                mode = excluded.mode,
                target = excluded.target,
                reason = excluded.reason,
                status = excluded.status,
                payload = excluded.payload,
                response = excluded.response,
                attempts = excluded.attempts,
                created_at = excluded.created_at
            """,
            (
                command_record["request_id"],
                command_record["command"],
                command_record["mode"],
                command_record.get("target"),
                command_record.get("reason"),
                command_record["status"],
                json.dumps(command_record.get("payload") or {}),
                json.dumps(command_record.get("response") or {}),
                int(command_record.get("attempts", 0)),
                command_record["created_at"],
            ),
        )


def list_solenoid_commands(limit: int = 50) -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT request_id, command, mode, target, reason, status, payload, response, attempts, created_at
            FROM solenoid_commands
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        command_records: list[dict[str, Any]] = []
        for row in rows:
            command_records.append(
                {
                    "request_id": row["request_id"],
                    "command": row["command"],
                    "mode": row["mode"],
                    "target": row["target"],
                    "reason": row["reason"],
                    "status": row["status"],
                    "payload": json.loads(row["payload"] or "{}"),
                    "response": json.loads(row["response"] or "{}"),
                    "attempts": row["attempts"],
                    "created_at": row["created_at"],
                }
            )

        return command_records
