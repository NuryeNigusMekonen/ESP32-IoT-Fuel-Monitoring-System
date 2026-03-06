import argparse
import random
import time
from datetime import datetime, timezone

import requests


def clamp_level(value: float) -> float:
    return max(0.0, min(100.0, value))


def build_payload(
    *,
    device_id: str,
    tank_name: str,
    fuel_level: float,
    consumption_rate: float,
    interval_seconds: int,
    firmware_version: str,
    inject_anomaly: bool,
) -> dict:
    signal_rssi = random.randint(-98, -62)
    battery_voltage = round(random.uniform(3.30, 4.12), 2)

    if inject_anomaly:
        fuel_level = clamp_level(fuel_level - random.uniform(12.5, 18.5))

    return {
        "device_id": device_id,
        "tank_name": tank_name,
        "fuel_level": round(fuel_level, 2),
        "consumption_rate": round(consumption_rate, 2),
        "battery_voltage": battery_voltage,
        "signal_rssi": signal_rssi,
        "firmware_version": firmware_version,
        "expected_interval_seconds": interval_seconds,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run_simulation(args: argparse.Namespace) -> None:
    endpoint = args.endpoint.rstrip("/") + "/api/ingest"
    level = args.start_level

    print(f"Simulator started: {args.device_id} -> {endpoint}")

    for index in range(1, args.count + 1):
        usage_drop = random.uniform(args.min_drop, args.max_drop)
        level = clamp_level(level - usage_drop)

        inject_anomaly = args.anomaly_every > 0 and index % args.anomaly_every == 0

        payload = build_payload(
            device_id=args.device_id,
            tank_name=args.tank_name,
            fuel_level=level,
            consumption_rate=random.uniform(args.min_consumption, args.max_consumption),
            interval_seconds=args.interval,
            firmware_version=args.firmware,
            inject_anomaly=inject_anomaly,
        )

        try:
            response = requests.post(endpoint, json=payload, timeout=args.timeout)
            response.raise_for_status()
            body = response.json()
            event_type = body.get("data", {}).get("event_type", "unknown")
            print(
                f"[{index:03d}/{args.count}] level={payload['fuel_level']:.2f}% "
                f"rssi={payload['signal_rssi']} battery={payload['battery_voltage']:.2f}V "
                f"event={event_type} status={response.status_code}"
            )
        except requests.RequestException as exc:
            print(f"[{index:03d}/{args.count}] request failed: {exc}")

        if index < args.count:
            time.sleep(args.interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ESP32-like telemetry simulator for production readiness and SLA testing."
    )
    parser.add_argument("--endpoint", default="http://localhost:5000", help="Base API URL")
    parser.add_argument("--device-id", default="esp32-generator-sim-01", help="Device identifier")
    parser.add_argument("--tank-name", default="Generator Tank", help="Target tank name")
    parser.add_argument("--firmware", default="1.5.0", help="Firmware version string")
    parser.add_argument("--start-level", type=float, default=72.0, help="Starting fuel level in percent")
    parser.add_argument("--count", type=int, default=30, help="Number of telemetry messages to send")
    parser.add_argument("--interval", type=int, default=5, help="Seconds between messages")
    parser.add_argument("--timeout", type=float, default=5.0, help="HTTP timeout in seconds")
    parser.add_argument("--min-drop", type=float, default=0.2, help="Minimum level drop per step")
    parser.add_argument("--max-drop", type=float, default=1.8, help="Maximum level drop per step")
    parser.add_argument("--min-consumption", type=float, default=0.4, help="Minimum consumption rate")
    parser.add_argument("--max-consumption", type=float, default=2.2, help="Maximum consumption rate")
    parser.add_argument("--anomaly-every", type=int, default=9, help="Inject abnormal drop every N samples")

    run_simulation(parser.parse_args())
