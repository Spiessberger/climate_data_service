# Gateway health and reading continuity

`GET /live` includes a `gateway` object alongside `indoor`. These fields describe
communication and observations in this service run:

| Field | Meaning |
| --- | --- |
| `available` | A valid supported heartbeat or reading was received less than 15 seconds ago, and no disconnection has since been detected. Initially false. |
| `boot_id` | Boot identifier from the last valid record, or null before the first. |
| `last_received_at_unix_ms` | UTC reception time of the last valid heartbeat or reading, or null. |
| `restart_count` | Observed boot-identifier changes during this service run. |
| `indoor.last_seq` | Last indoor counter observed in the current gateway boot, or null. |
| `indoor.observed_missing_readings` | Sum of observable within-boot indoor gaps during this service run, saturating at the unsigned 64-bit maximum. |

Availability uses a monotonic clock, independent of UTC corrections. The serial
loop checks deadlines at most every 100 ms while idle and before admitting each
received chunk; exactly 15 seconds of silence meets the timeout. Ordinary logs,
partial input, malformed DATA and unsupported types/versions cannot refresh it.
Five-second heartbeats keep communication available without changing indoor values,
reception times, counters or history. They give no evidence of sensor health.

The service opens only `--serial`, including an explicitly selected stable symlink.
An absent or disconnected device is retried once per second; no device discovery
or alternate path is used. Opening a device does not restore availability until
valid DATA arrives. A read error or end of stream immediately marks it unavailable,
offers partial bytes to diagnostics and resets framing before reconnecting.
HTTP remains available throughout. The last live reading and its original UTC
reception time survive gateway loss and restart. A service process restart clears
live state and continuity even when SQLite history exists.

The first valid record establishes a boot without inferring a restart. Later boot
changes, whether first seen on a heartbeat or reading, count as restarts and clear
stream counter baselines. Each stream uses its own continuity state; weather will
join this behavior in its feature slice. The first reading in a boot establishes
its baseline even if its counter is already large. Losses before this observation
or across boots cannot be established.

Within a boot, forward distances are modulo 2^32: `4294967295 → 0` is contiguous;
`4294967294 → 1` exposes two missed produced readings. A distance greater than one
adds `distance - 1` to the observed loss total. Equal counters are ambiguous and
produce a diagnostic without inventing a loss count. Whole unseen counter cycles
cannot be counted. These observations concern readings produced by the gateway,
not radio transmissions it never accepted. There is no replay or recovery backlog.

Operational events are JSON objects offered as `DiagnosticKind::Operational`
through the normal diagnostic callback: `serial_connected`, `serial_unavailable`,
`serial_disconnected`, `gateway_available`, `gateway_timeout`, `gateway_restart`,
`reading_gap` and `reading_sequence_ambiguous`. Serial events identify the configured
path; restarts identify both boots; gaps identify stream, boot, counters and the
observed missing count. Repeated failed open attempts and steady valid traffic do
not repeat state-transition events.

The foreground binary sends these events with UTC Unix millisecond timestamps to
stderr through a 128-event nonblocking queue. A stalled sink may drop events; it
cannot stall live acquisition or shutdown. Daily permanent files are a later slice.
`Service::start_with_clock` accepts independent UTC and monotonic clocks for tests
at the serial/HTTP/log boundary. Production `Service::start` uses `Instant` for
elapsed time. Integration coverage uses Linux pseudo-terminals and real temporary
SQLite; it does not claim physical USB or flashed-firmware verification.
