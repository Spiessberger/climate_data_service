# Daily operational logs and diagnostic input

`--log-dir` defaults to `./logs`, relative to the invocation directory. Missing
directories are created by the log worker. For example:

```sh
cargo run --locked -- --serial /dev/serial/by-id/YOUR_GATEWAY --log-dir ./retained-logs
```

The service appends two files per UTC reception date:

- `YYYY-MM-DD.operational.jsonl`: readable records with `received_at_unix_ms`
  (UTC Unix milliseconds), `source` (`gateway` or `service`), and `message`.
  Gateway text, service connection/health events, shutdown, and rejection notices
  use this path. Messages are JSON strings, so embedded control characters cannot
  introduce false records.
- `YYYY-MM-DD.diagnostic.jsonl`: lossless input chunks with
  `received_at_unix_ms`, `kind`, and `bytes` (an array of integers from 0 to 255).
  This includes ordinary text, startup/panic output, malformed/unsupported DATA,
  oversized candidates and disconnected partial input. The original bytes are
  retained even when the readable operational message replaces invalid UTF-8.

Each diagnostic record contains at most 1024 input bytes. Concatenate its `bytes`
arrays in file order to reconstruct the retained non-data input for that date.
Chunk boundaries are not necessarily line boundaries; timestamps belong to chunk
reception, and a line crossing midnight can appear in two dates. Accepted DATA
records are handled as readings or heartbeats, not copied to these files.
`Text` identifies ordinary input; `RejectedData` identifies complete rejected
DATA; `OversizedLine` identifies chunks of an overlong DATA candidate; and
`PartialLine` marks an unterminated tail delivered on disconnect or shutdown.
All non-DATA text is also retained readably as gateway operational chunks.

Only whole validated DATA lines can update live state. Oversized DATA remains
rejected through its next LF, including any appended DATA substring. Parsing then
resumes at the next line. Long ordinary text streams in bounded chunks without
changing into DATA. Version-1 extension fields remain allowed; duplicate keys,
invalid UTF-8/JSON, unsupported versions/types and damaged lines remain rejected.

Files are opened in append mode. Restart, UTC midnight and backward clock
movement reopen the corresponding date without truncating or deleting anything.
Only the current date's two handles remain open; changing dates attempts to
synchronize the old handles before closing them.

The log worker writes independently of serial ingestion, live HTTP and SQLite.
Its nonblocking handoff holds at most 128 chunks of up to 1024 input bytes each;
full handoffs drop new chunks, with no retry backlog. Persistent storage stalls
therefore cannot cause unbounded memory growth or stop valid live readings.
Writable normal-operation storage preserves input; an overwhelmed or failed
storage path can leave gaps, including within a long line.

Every 60 seconds of monotonic elapsed time, the worker calls `sync_all` on dirty
files. A continuous input stream does not postpone the deadline, and UTC clock
changes do not change it. A successful append is not a successful synchronization.
Failed synchronization leaves the file dirty for the next attempt. New file
entries and the log directory's ancestor chain are synchronized, leaf first, to
cover nested directory creation as well as file contents. Clean files need no
periodic synchronization.

On ordinary shutdown the service stops serial ingestion, offers its partial tail,
and lets the worker drain accepted chunks and synchronize. It waits at most one
second for the log worker; a stalled system call can outlive that wait. The minute
cadence is a normal-operation target, not a power-loss guarantee during stalls or
errors. Recent unsynchronized bytes can be lost on a power cut.

The library's `DailyLogs::status()` reports whether the most recent file operation
was successful, completed synchronization batches, and the last worker I/O error.
The foreground service passes this shared status to `/live`, where it appears as
the independent `storage.logs` health object. A failed open or append drops the
current `DayFiles` handle and retries the configured paths on a later entry or
synchronization tick. A failed synchronization leaves retained files in place,
reopens them before a later tick, and keeps the health error until a successful
write or synchronization.

Database health appears beside it as `storage.database`. A database startup or
write failure leaves live readings, heartbeats and `/live` available; the failed
arrival is discarded and the writer retries the configured database path without
replaying it. History continues to return committed rows when the read path is
available and returns HTTP 500 independently when it is not. These health fields
remain in memory, so they are available even when the corresponding error cannot
be appended to a log file. The `LogSync` filesystem boundary and elapsed-clock
input let integration tests observe/fail/block real file and directory
synchronization while exercising serial input and live HTTP. These host tests do
not demonstrate physical power-loss durability or hardware USB behavior.
