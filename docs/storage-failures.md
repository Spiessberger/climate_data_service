# Storage failure recovery

`GET /live` remains available while SQLite or daily-log storage is unavailable.
Its `storage` object reports the two destinations independently:

```json
"storage": {
  "database": {"available": true, "last_error": null},
  "logs": {"available": true, "last_error": null}
}
```

`available` describes the latest known persistence state. `last_error` is the
most recent open, write or synchronization error and is cleared after that
destination succeeds again. The service keeps these values in memory, so an
error remains visible when the failing destination cannot retain an operational
event.

The serial path updates each stream's live reading before handing a copy to the
bounded SQLite worker. A failed database write drops that arrival, closes the
writer connection and retries opening the explicitly configured database once per
second. New arrivals are accepted after recovery; failed arrivals are never
replayed. SQLite transaction setup and busy write attempts are bounded. A commit
error is reported as an unconfirmed outcome and is never blindly retried.
The storage worker is given the same one-second shutdown join bound as the log
worker, so a stalled database call cannot hold ordinary service shutdown forever.

History reads use independent short-lived read-only connections. They return
committed rows already present in the database, or HTTP 500 with
`{"error":"history unavailable"}` when the read destination cannot be opened or
queried. This does not affect live readings, gateway health or heartbeat handling.

Daily-log handoff remains bounded and nonblocking. Open or append failures cause
the worker to reopen the configured directory and files on a later entry. Sync
failures leave existing files untouched, keep them dirty, and cause a reopen and
retry at the next synchronization tick. No alternate destination is selected and
no retained file is deleted or truncated.
