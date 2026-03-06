import random
from datetime import datetime, timedelta, timezone

from db import EXTERNAL_TANK, GENERATOR_TANK, get_connection, init_db, insert_sensor_payload


def seed_data() -> None:
    init_db()

    with get_connection() as connection:
        connection.execute("DELETE FROM sensor_data")
        connection.execute("DELETE FROM events")
        connection.execute("DELETE FROM iot_devices")
        connection.execute("DELETE FROM alerts")
        connection.execute("DELETE FROM refill_requests")

    base_time = datetime.now(timezone.utc) - timedelta(hours=6)
    generator_level = 52.0

    insert_sensor_payload(
        {
            "tank_name": EXTERNAL_TANK,
            "device_id": "esp32-external-01",
            "fuel_level": 100.0,
            "consumption_rate": 0.0,
            "battery_voltage": 3.97,
            "signal_rssi": -68,
            "firmware_version": "1.4.2",
            "expected_interval_seconds": 180,
            "timestamp": (base_time - timedelta(minutes=1)).isoformat(),
        }
    )

    for index in range(120):
        timestamp = base_time + timedelta(minutes=3 * index)

        if generator_level < 30:
            generator_level += random.uniform(20.0, 32.0)
        elif index in {35, 82}:
            generator_level -= random.uniform(13.0, 18.0)
        elif index in {52}:
            generator_level += random.uniform(9.0, 13.0)
        elif index in {45, 101}:
            generator_level -= random.uniform(9.0, 12.0)
        else:
            generator_level -= random.uniform(0.4, 1.6)

        generator_level = max(5.0, min(100.0, generator_level))

        payload = {
            "tank_name": GENERATOR_TANK,
            "device_id": "esp32-generator-01",
            "fuel_level": round(generator_level, 2),
            "consumption_rate": round(random.uniform(0.2, 1.8), 2),
            "battery_voltage": round(random.uniform(3.32, 4.08), 2),
            "signal_rssi": random.randint(-98, -62),
            "firmware_version": "1.4.2",
            "expected_interval_seconds": 180,
            "timestamp": timestamp.isoformat(),
        }

        insert_sensor_payload(payload)


if __name__ == "__main__":
    seed_data()
    print("Seed data generated successfully.")
