#!/usr/bin/env python3
"""Ingest simulated indoor readings, query them, and verify restart persistence."""
import argparse
import json
import os
from pathlib import Path
import pty
import subprocess
import tempfile
import time
import urllib.request


def start_service(binary, serial, database):
    process = subprocess.Popen(
        [str(binary), "--serial", serial, "--listen", "127.0.0.1:0",
         "--database", str(database)],
        stderr=subprocess.PIPE, text=True,
    )
    startup = process.stderr.readline().strip()
    if not startup.startswith("Listening on http://"):
        process.wait()
        raise RuntimeError(f"Service failed to start: {startup}")
    return process, startup.removeprefix("Listening on ")


def stop_service(process):
    process.terminate()
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    process.stderr.close()


def get_json(url):
    with urllib.request.urlopen(url, timeout=2) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=0, help="0 runs until Ctrl-C")
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()
    if args.samples < 0 or args.interval < 0:
        parser.error("samples and interval must be nonnegative")
    binary = Path(__file__).resolve().parents[1] / "target/debug/climate-data-service"
    master, slave = pty.openpty()
    process = None
    with tempfile.TemporaryDirectory() as directory:
        database = Path(directory) / "climate.sqlite3"
        try:
            process, base_url = start_service(binary, os.ttyname(slave), database)
            live_url = base_url + "/live"
            history_url = base_url + "/history/indoor/updates?after_id=0&limit=1000"
            print(f"Polling {live_url}", flush=True)
            seq = 1
            while args.samples == 0 or seq <= args.samples:
                reading = dict(
                    v=1,
                    type="indoor",
                    boot_id="6a9d3c1f80b24e67a511d92cb837046e",
                    seq=seq,
                    temperature_celsius=21.5 + (seq - 1) / 10,
                    relative_humidity_percent=48.2,
                )
                record = (
                    "INFO - Simulated indoor measurement\nDATA "
                    + json.dumps(reading, separators=(",", ":"))
                    + "\n"
                ).encode()
                while record:
                    record = record[os.write(master, record):]
                deadline = time.monotonic() + 2
                while True:
                    live = get_json(live_url)
                    if live["indoor"] is not None and live["indoor"]["seq"] == seq:
                        print("Live:", json.dumps(live), flush=True)
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"Reading {seq} did not reach HTTP")
                    time.sleep(0.01)
                seq += 1
                if args.samples == 0 or seq <= args.samples:
                    time.sleep(args.interval)

            deadline = time.monotonic() + 2
            while True:
                history = get_json(history_url)
                if len(history["readings"]) == args.samples:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"Only {len(history['readings'])} readings reached history"
                    )
                time.sleep(0.01)
            print("Stored:", json.dumps(history), flush=True)
            if args.samples:
                stop_service(process)
                process = None
                process, base_url = start_service(binary, os.ttyname(slave), database)
                live = get_json(base_url + "/live")
                history = get_json(
                    base_url + "/history/indoor/updates?after_id=0&limit=1000"
                )
                print("After restart live:", json.dumps(live), flush=True)
                print("After restart stored:", json.dumps(history), flush=True)
        except KeyboardInterrupt:
            pass
        finally:
            if process is not None:
                stop_service(process)
            os.close(master)
            os.close(slave)


if __name__ == "__main__":
    main()
