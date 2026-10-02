# Climate Data Service

A foreground Rust/Linux service receiving one explicitly selected gateway's indoor
readings over USB serial, retaining them in SQLite, and exposing live and historical
readings through read-only HTTP.

```sh
cargo build --locked
cargo run --locked -- --serial /dev/serial/by-id/YOUR_GATEWAY \
  --database ./data/climate.sqlite3 \
  --web-root /absolute/path/to/weatherstation_web/dist
curl http://127.0.0.1:8080/live
curl http://127.0.0.1:8080/api/v1/dashboard
curl 'http://127.0.0.1:8080/history/indoor/updates?after_id=0&limit=100'
```

`--serial` is required. `--database` defaults to `./data/climate.sqlite3`, relative
to the invocation directory, and its parent directories are created as needed.
Logs go to stdout; see [Logging](#logging) for levels and `RUST_LOG`.
`--listen` defaults to `127.0.0.1:8080`; an explicit trusted network address permits
remote clients. HTTP is read-only and unauthenticated.
`--web-root` is optional and names the contents of a built web application. When
set, the service hosts its SPA and hashed assets on the same origin as the APIs.
See [dashboard and weather summary](docs/dashboard-api.md) for the aggregate API,
Vienna calendar/rain semantics, resource bounds, error responses and static-file
rules.
Poll once per second. Stop with Ctrl-C or SIGTERM. The serial connection is opened
at 115200 baud, 8N1 without flow control (native USB Serial/JTAG ignores baud rate).
Run only one reader on the selected device; stop any serial monitor first.

Before the first valid reading in this process, `/live` has `"indoor":null` and
gateway availability is false until a valid heartbeat or indoor reading arrives.
After reception it returns:

```json
{"indoor":{"v":1,"type":"indoor","boot_id":"6a9d3c1f80b24e67a511d92cb837046e","seq":42,"temperature_celsius":21.5,"relative_humidity_percent":48.2,"received_at_unix_ms":1800000000123},"weather":null,"gateway":{"available":true,"boot_id":"6a9d3c1f80b24e67a511d92cb837046e","last_received_at_unix_ms":1800000000123,"restart_count":0,"indoor":{"last_seq":42,"observed_missing_readings":0},"weather":{"last_seq":null,"observed_missing_readings":0}},"storage":{"database":{"available":true,"last_error":null}}}
```

Reception time is the Linux host's UTC Unix milliseconds, not sensor measurement
time. Each valid arrival replaces the complete live reading. Responses disable
caching. Unsupported input leaves both values and their reception time unchanged.
See [the wire contract](docs/indoor-wire.md).

Five-second gateway heartbeats keep communication available during sensor silence.
After 15 seconds without valid supported DATA, or immediately on detected USB
disconnection, availability becomes false. Last live values keep their original
reception times. Only valid returning DATA restores availability. Missing or
disconnected hardware is retried once per second at the configured path while
HTTP remains available. See [gateway health](docs/gateway-health.md) for restart,
counter-gap and diagnostic semantics.

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

This implements tickets 01–06 plus the persisted dashboard/summary view: indoor
and weather live HTTP, retained SQLite history, gateway health/reconnect,
damaged-input recovery, storage failure recovery, and bounded web-facing
aggregates. `/live` reports database health.
See [weather readings](docs/weather.md) for the complete weather schema, independent
live/health state, weather history routes, and the finite weather demonstration. A service restart starts with no live reading or counter baseline.

The parser offers original non-data, rejected, overlong and partial bytes in
bounded chunks to the diagnostic callback, and the service logs them.

Serial ingestion, database writes, and HTTP run independently. SQLite is bundled
so the application controls the runtime version; startup enforces SQLite 3.51.3 or
newer. History uses WAL, FULL synchronous commits, a 1000-page passive automatic
checkpoint threshold, short read connections, and pages capped at 1000 rows.
Parser storage is bounded by the 1024-byte record limit, including long ordinary
text. HTTP admits up to 64 active connections with bounded header buffers and a
five-second connection deadline; slow clients cannot hold live or database locks.
Aggregate reads admit at most two jobs, scan no more than 2,000,000 rows for at
most four seconds, and stream into bounded statistics state. Static files use a
separate four-job blocking limit and a 16 MiB per-file limit.

## Logging

The service logs through the `log` crate, and the binary writes the records to
stdout with `env_logger`, one line each:

```text
[2026-10-02T13:31:42.053Z WARN  climate_data_service::health] Missed 2 indoor reading(s): seq 1 -> 4 (boot 6a9d…)
```

The default level is `info`. Set `RUST_LOG` to change it, globally or per target:

```sh
RUST_LOG=debug climate-data-service --serial …
RUST_LOG=info,climate_data_service::http=debug climate-data-service --serial …
```

| Level | What is logged |
| ----- | -------------- |
| `error` | Database open/write failures (once per new failure), history and aggregate query failures, HTTP accept failure, startup failure |
| `warn` | Serial unavailable/disconnected, gateway timeout and restarts, missed or repeated sequence numbers, rejected/oversized/partial input, readings dropped because storage is full or unavailable, database write retries, busy aggregate/static workers |
| `info` | Startup configuration, listening address, database opened/reopened/recovered, serial connected, gateway available, shutdown |
| `debug` | Every indoor and weather reading with its values, every stored row ID, every HTTP request (peer, method, URI, status, duration), HTTP connection errors/deadlines, repeated serial/database retry failures, shutdown steps |
| `trace` | Gateway heartbeats and serial byte counts |

Gateway firmware output uses the `gateway` target. The service keeps the gateway's
own `ERROR`/`WARN`/`INFO`/`DEBUG`/`TRACE - ` prefixes as the record level (lines
containing `PANIC` are errors, other text is `info`), so for example
`RUST_LOG=info,gateway=warn` hides routine firmware messages. Logging happens on
the thread producing the event, so stdout must be drained (a terminal, journald or
a reading pipe); write errors are ignored.

## Development

```sh
cargo fmt --check
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

Integration tests exercise real pseudo-terminals, temporary SQLite databases, and
loopback HTTP, including the foreground executable, committed visibility, restart
persistence, range/incremental ordering, malformed input, framing recovery,
reception time, and delayed writes.
Controlled monotonic and UTC clocks also exercise exact heartbeat deadlines,
sensor silence, restart/counter continuity and configured-device reconnects.
The tests need
permission to bind loopback sockets and use `/dev/pts`. No network downloads are
needed once dependencies in `Cargo.lock` are cached (`--offline` may be added).

This repository has its own Cargo manifest, lockfile and toolchain configuration,
and no dependency on a neighboring firmware checkout. Build here, independently
of the gateway's embedded Cargo configuration. Native builds were exercised on
Fedora x86_64. For Raspberry Pi OS or Yocto, use a Rust target and linker matching
that installation's CPU and libc, or build natively there. Pi/Yocto deployment,
Linux provisioning and real USB smoke verification are separate work.
