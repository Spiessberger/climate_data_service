#!/usr/bin/env python3
"""Show complete weather, unavailable quantities, source changes and retained history."""
import json
import os
from pathlib import Path
import pty
import tempfile
import time

from demo import get_json, start_service, stop_service


def wait_for(url, predicate):
    deadline = time.monotonic() + 3
    while True:
        value = get_json(url)
        if predicate(value):
            return value
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Unexpected HTTP result: {value}")
        time.sleep(0.01)


def main():
    root = Path(__file__).resolve().parents[1]
    master, slave = pty.openpty()
    process = None
    with tempfile.TemporaryDirectory() as directory:
        database = Path(directory) / "climate.sqlite3"
        try:
            binary = root / "target/debug/climate-data-service"
            process, base = start_service(binary, os.ttyname(slave), database)
            indoor = dict(v=1, type="indoor", boot_id="6a9d3c1f80b24e67a511d92cb837046e",
                          seq=1, temperature_celsius=21.5, relative_humidity_percent=48.2)
            data = ("INFO - Simulated indoor reading\nDATA " + json.dumps(indoor) + "\n").encode()
            for fixture in ("weather.data", "weather-unavailable.data"):
                data += (root / "tests/fixtures" / fixture).read_bytes()
            while data:
                data = data[os.write(master, data):]
            live = wait_for(base + "/live", lambda v: v["weather"] and v["weather"]["seq"] == 2)
            assert live["indoor"]["seq"] == 1
            assert live["weather"]["temperature_celsius"] is None
            assert live["weather"]["station_id"] == 1
            history_path = "/history/weather/updates?after_id=0&limit=10"
            history = wait_for(base + history_path, lambda v: len(v["readings"]) == 2)
            assert [row["station_id"] for row in history["readings"]] == [191, 1]
            print("Live:", json.dumps(live))
            print("Weather history:", json.dumps(history))
            stop_service(process)
            process = None
            process, base = start_service(binary, os.ttyname(slave), database)
            assert get_json(base + "/live")["weather"] is None
            assert get_json(base + history_path) == history
            print("Restart verified: live weather absent, both weather rows retained.")
        finally:
            if process is not None:
                stop_service(process)
            os.close(master)
            os.close(slave)


if __name__ == "__main__":
    main()
