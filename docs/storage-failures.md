# Storage failure recovery

`GET /live` remains available while SQLite storage is unavailable. Its
`storage` object reports the database state:

```json
"storage": {
  "database": {"available": true, "last_error": null}
}
```

`available` describes the latest known persistence state. `last_error` is the
most recent open or write error and is cleared after the database succeeds again.

The serial path updates each stream's live reading before handing a copy to the
bounded SQLite worker. A failed database write drops that arrival, closes the
writer connection and retries opening the explicitly configured database once per
second. New arrivals are accepted after recovery; failed arrivals are never
replayed. SQLite transaction setup and busy write attempts are bounded. A commit
error is reported as an unconfirmed outcome and is never blindly retried.
The storage worker is given a one-second shutdown join bound, so a stalled
database call cannot hold ordinary service shutdown forever.

History reads use independent short-lived read-only connections. They return
committed rows already present in the database, or HTTP 500 with
`{"error":"history unavailable"}` when the read destination cannot be opened or
queried. This does not affect live readings, gateway health or heartbeat handling.
