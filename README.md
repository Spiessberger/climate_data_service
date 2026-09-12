# Climate Data Service

A foreground Rust/Linux service receiving one explicitly selected gateway's indoor
readings over USB serial, retaining them in SQLite, and exposing live and historical
readings through read-only HTTP.

```sh
cargo build --locked
cargo run --locked -- --serial /dev/serial/by-id/YOUR_GATEWAY \
  --database ./data/climate.sqlite3
curl http://127.0.0.1:8080/live
curl 'http://127.0.0.1:8080/history/indoor/updates?after_id=0&limit=100'
```

`--serial` is required. `--database` defaults to `./data/climate.sqlite3`, relative
to the invocation directory, and its parent directories are created as needed.
`--listen` defaults to `127.0.0.1:8080`; an explicit trusted network address permits
remote clients. HTTP is read-only and unauthenticated.
Poll once per second. Stop with Ctrl-C or SIGTERM. The serial connection is opened
at 115200 baud, 8N1 without flow control (native USB Serial/JTAG ignores baud rate).
Run only one reader on the selected device; stop any serial monitor first.

Before the first valid reading in this process, `/live` returns `{"indoor":null}`.
After reception it returns:

```json
{"indoor":{"v":1,"type":"indoor","boot_id":"6a9d3c1f80b24e67a511d92cb837046e","seq":42,"temperature_celsius":21.5,"relative_humidity_percent":48.2,"received_at_unix_ms":1800000000123}}
```

Reception time is the Linux host's UTC Unix milliseconds, not sensor measurement
time. Each valid arrival replaces the complete live reading. Responses disable
caching. Unsupported input leaves both values and their reception time unchanged.
See [the wire contract](docs/indoor-wire.md).

Successfully committed readings are available from `GET /history/indoor` by UTC
time range and from `GET /history/indoor/updates` after a database row ID. Both
routes use bounded pages. See [indoor history](docs/indoor-history.md) for the
query parameters, ordering, and cursor rules. Live responses never contain a
database row ID and do not wait for SQLite commits.

## Simulated serial demonstration

No gateway hardware or additional Python packages are required:

```sh
cargo build --locked
python3 scripts/demo.py
# Finite verification including retained history after process restart:
python3 scripts/demo.py --samples 3 --interval 0.1
```

The script creates a Linux pseudo-terminal and temporary database, launches the
service against the slave path, sends mixed operational text and DATA records, and
prints live and stored HTTP responses. A finite run restarts the service with the
same database, showing empty live state and retained history. It prints a local
HTTP address that can also be polled with curl. Ctrl-C stops the process.

## Scope and continuation

This implements tickets 01 and 02: indoor live HTTP plus retained SQLite history.
There is no heartbeat/connection health, reconnect, or permanent log storage yet.
Weather reports remain operational text until the weather slice. An absent
serial device currently fails startup; a detected disconnection ends ingestion
while HTTP retains the last reading and original reception time. Restart with the
configured device to resume. A service restart starts with no live reading.

The parser offers original non-data, rejected, overlong and partial bytes to a
bounded-chunk diagnostic callback. The foreground binary currently discards these
chunks. The later log slice supplies a nonblocking persistence handoff; it must not
write files synchronously from this callback. No diagnostic replay backlog exists.

Serial ingestion, database writes, and HTTP run independently. SQLite is bundled
so the application controls the runtime version; startup enforces SQLite 3.51.3 or
newer. History uses WAL, FULL synchronous commits, a 1000-page passive automatic
checkpoint threshold, short read connections, and pages capped at 1000 rows.
Parser storage is bounded by the 1024-byte record limit, including long ordinary
text. HTTP admits up to 64 active connections with bounded header buffers and a
five-second connection deadline; slow clients cannot hold live or database locks.

## Development

```sh
cargo fmt --check
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

Integration tests exercise real pseudo-terminals, temporary SQLite databases, and
loopback HTTP, including the foreground executable, committed visibility, restart
persistence, range/incremental ordering, malformed input, byte-preservation
callbacks, framing recovery, reception time, and delayed writes. They need
permission to bind loopback sockets and use `/dev/pts`. No network downloads are
needed once dependencies in `Cargo.lock` are cached (`--offline` may be added).

This repository has its own Cargo manifest, lockfile and toolchain configuration,
and no dependency on a neighboring firmware checkout. Build here, independently
of the gateway's embedded Cargo configuration. Native builds were exercised on
Fedora x86_64. For Raspberry Pi OS or Yocto, use a Rust target and linker matching
that installation's CPU and libc, or build natively there. Pi/Yocto deployment,
Linux provisioning and real USB smoke verification are separate work.
