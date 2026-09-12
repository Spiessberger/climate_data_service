#!/usr/bin/env python3
"""Run the real service with simulated USB serial input and poll live HTTP."""
import argparse
import json
import os
from pathlib import Path
import pty
import subprocess
import time
import urllib.request


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
    try:
        process = subprocess.Popen(
            [str(binary), "--serial", os.ttyname(slave), "--listen", "127.0.0.1:0"],
            stderr=subprocess.PIPE, text=True,
        )
        startup = process.stderr.readline().strip()
        if not startup.startswith("Listening on http://"):
            raise RuntimeError(f"Service failed to start: {startup}")
        url = startup.removeprefix("Listening on ") + "/live"
        print(f"Polling {url}", flush=True)
        seq = 1
        while args.samples == 0 or seq <= args.samples:
            reading = dict(v=1, type="indoor", boot_id="6a9d3c1f80b24e67a511d92cb837046e",
                           seq=seq, temperature_celsius=21.5 + (seq - 1) / 10,
                           relative_humidity_percent=48.2)
            record = ("INFO - Simulated indoor measurement\nDATA "
                      + json.dumps(reading, separators=(",", ":")) + "\n").encode()
            while record:
                record = record[os.write(master, record):]
            deadline = time.monotonic() + 2
            while True:
                with urllib.request.urlopen(url, timeout=2) as response:
                    live = json.load(response)
                if live["indoor"] is not None and live["indoor"]["seq"] == seq:
                    print(json.dumps(live), flush=True)
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"Reading {seq} did not reach HTTP")
                time.sleep(0.01)
            seq += 1
            if args.samples == 0 or seq <= args.samples:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            process.stderr.close()
        os.close(master)
        os.close(slave)


if __name__ == "__main__":
    main()
