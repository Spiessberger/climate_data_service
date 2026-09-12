# Indoor history HTTP contract

The service retains every successfully committed indoor reading in the configured
local SQLite database. Stored rows include all version-1 wire fields, UTC reception
milliseconds, and an independent service-assigned `id`. Gateway `seq` values can
wrap or repeat and are never used as database identities.

All history responses have the form `{"readings":[...]}`. Each reading contains
`id`, `received_at_unix_ms`, `v`, `type`, `boot_id`, `seq`,
`temperature_celsius`, and `relative_humidity_percent`. Pages default to 100 rows;
`limit` must be from 1 through 1000. Invalid, duplicate, missing, or unknown query
parameters return HTTP 400. Only GET is accepted.

## UTC time range

`GET /history/indoor` requires `from_unix_ms` and `to_unix_ms`. The interval is
half-open: reception time is greater than or equal to `from_unix_ms` and less than
`to_unix_ms`. Results are ordered by `(received_at_unix_ms, id)`.

To continue a full page, pass both `after_received_at_unix_ms` and `after_id` from
its last row together with the original range. The compound cursor keeps paging
stable when readings have equal reception times or the host clock moves backward.

```text
/history/indoor?from_unix_ms=1800000000000&to_unix_ms=1800086400000&limit=100
/history/indoor?from_unix_ms=1800000000000&to_unix_ms=1800086400000&after_received_at_unix_ms=1800000060000&after_id=42&limit=100
```

## Incremental stored readings

`GET /history/indoor/updates` requires a nonnegative `after_id`. Results have
`id > after_id`, are ordered by `id`, and are limited by the page size. Start with
zero for all stored readings and continue with the last returned row's `id`.

```text
/history/indoor/updates?after_id=0&limit=100
```

History contains committed rows only. The separate `/live` capability updates
before and independently of database writes, has no database row ID, and starts
with `{"indoor":null}` after every service process restart.
