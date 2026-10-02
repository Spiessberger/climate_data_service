# Dashboard and weather summary HTTP contract

The service exposes two persisted-data views for the same-origin weather web
application. They read committed SQLite rows, not the process-local `/live`
state. Both routes are read-only, unauthenticated GET endpoints and return
`Cache-Control: no-store`.

The station timezone is currently fixed to `Europe/Vienna`. Calendar dates,
night boundaries and daylight-saving transitions use that named timezone.

## Dashboard

`GET /api/v1/dashboard` accepts no query string and returns:

```json
{
  "generated_at_unix_ms": 1790251200000,
  "station_timezone": "Europe/Vienna",
  "stale_after_ms": 300000,
  "latest_stored": {"weather": null, "indoor": null},
  "history": {"weather": null, "indoor": null},
  "rain_last_24_hours": {
    "total_mm": null,
    "coverage": "unavailable",
    "excluded_transitions": 0,
    "largest_gap_ms": null
  },
  "night_temperature": {
    "from_unix_ms": 1790186400000,
    "to_unix_ms": 1790229600000,
    "state": "completed",
    "min_celsius": null,
    "observations": 0
  }
}
```

Non-null `latest_stored` values use the existing raw-history reading schema,
including the database `id` and `received_at_unix_ms`. Each non-null history
extent has `first_received_at_unix_ms` and `last_received_at_unix_ms`.

The rain window is `[generated_at_unix_ms - 24 hours,
generated_at_unix_ms)`. The night window uses local 18:00–06:00. At or after
18:00, and before 06:00, the current night is `ongoing` and ends at the request
time. From 06:00 until 18:00 the preceding 18:00–06:00 window is `completed`.

## Weather summary

```text
GET /api/v1/history/weather/summary?from_date=2026-09-01&through_date=2026-09-24&max_points=360
GET /api/v1/history/weather/summary?from_unix_ms=1790114400000&to_unix_ms=1790118000000&max_points=360
```

Each request uses exactly one range mode. Date mode requires both `from_date` and
`through_date` as inclusive `YYYY-MM-DD` station-local dates. Their normalized
UTC interval is returned as `[from_unix_ms,to_unix_ms)`. Instant mode instead
requires integer `from_unix_ms` and `to_unix_ms` and aggregates that exact UTC
half-open interval. Mixed modes and partial parameter pairs are invalid.

The response range identifies the selected mode as `dates` or `instants`; its UTC
bounds are authoritative. In instant mode, `from_date` is the Vienna date
containing `from_unix_ms`, while `through_date` is the Vienna date containing
`to_unix_ms - 1`. These date strings are informational, so an exact range ending
at local midnight does not name the following day. `max_points` defaults to 360
and must be 1 through 600.

The response has this shape:

```json
{
  "range": {
    "mode": "dates",
    "from_date": "2026-09-01",
    "through_date": "2026-09-24",
    "from_unix_ms": 1788213600000,
    "to_unix_ms": 1790287200000,
    "timezone": "Europe/Vienna"
  },
  "sample_count": 0,
  "statistics": {
    "temperature_celsius": {"min": null, "max": null, "average": null},
    "rain": {"total_mm": null, "coverage": "unavailable", "excluded_transitions": 0, "largest_gap_ms": null},
    "wind_speed_mps": {"average": null, "max": null},
    "gust_speed_mps": {"max": null}
  },
  "buckets": [
    {
      "from_unix_ms": 1788213600000,
      "to_unix_ms": 1788219360000,
      "sample_count": 0,
      "temperature_celsius": {"min": null, "max": null, "average": null},
      "relative_humidity_percent": {"average": null},
      "wind_speed_mps": {"average": null, "max": null},
      "gust_speed_mps": {"max": null},
      "rain": {"total_mm": null, "coverage": "unavailable", "excluded_transitions": 0, "largest_gap_ms": null}
    }
  ]
}
```

The single bucket shown above is illustrative. Date mode contains exactly
`max_points` bucket objects. Instant mode contains
`min(max_points, to_unix_ms - from_unix_ms)` buckets so every bucket is at least
one millisecond wide.

The service emits contiguous, half-open UTC buckets, including empty buckets. The
first bucket starts at `from_unix_ms` and the last ends at `to_unix_ms`. Buckets
narrower than one hour are equally wide. When equal buckets would be at least one
hour wide, every inner boundary is rounded to the nearest whole UTC hour, so
bucket widths differ by up to an hour and the edge buckets may be up to half an
hour shorter. Clients must use each bucket's own bounds. Each bucket adds `from_unix_ms`, `to_unix_ms`,
`sample_count`, temperature min/max/average, relative-humidity average,
sustained-wind average/max, gust max and rain metadata. Sensor nulls are omitted
from their statistic; all-null and empty aggregates remain null. Wind average is
the arithmetic mean of the stored sustained-wind samples.

## Rain calculation and coverage

`rain_mm` is a cumulative station counter. A nonnegative delta between two
successive readings from the same `station_id` is assigned to the later reading.
The service seeds the first delta with the most recent row before the requested
range. A valid flat pair contributes `0`; if there is no comparable pair,
`total_mm` is null and coverage is `unavailable`.

A negative delta or station transition is excluded, increments
`excluded_transitions`, and makes otherwise available coverage `partial`.
Missing boundary evidence, a gap over five minutes between successive evidence,
or over five minutes from the last reading to the range end also makes coverage
`partial`. `largest_gap_ms` includes observed leading, internal and trailing
boundary gaps. These rules apply independently to full-range and bucket rain.

## Resource and error behavior

Each aggregate response comes from one SQLite read transaction. Rows stream into
fixed-size accumulators; they are not retained as a result-sized in-memory list.
At most two aggregate jobs run concurrently. Each job has a four-second elapsed
budget and a 2,000,000-row scan budget; hourly summary rows count toward it. Indexed
probes obtain latest readings, range predecessors and history extents.

Range length is not limited directly, but at least one rain grouping must fit into
600 periods, so a range may touch at most 600 calendar months. Dates and the
Vienna dates of exact bounds must lie in the years 1 through 9999, and
`to_unix_ms` must be greater than `from_unix_ms`. A local midnight skipped by a
daylight-saving change starts the day at the end of the gap.

## Hourly summaries

The `weather_hourly` table holds one row per UTC hour with stored weather
readings: counts, sums, minimums and maximums of each statistic, the first and
last reading's rain evidence, and the rain pairs inside the hour. Summaries with
hour-wide buckets combine these rows with the pairs across hour boundaries and
read raw readings only for hours that are cut by the range bounds, a bucket
boundary or a rain period boundary. The result is the same as aggregating the raw
readings, apart from floating-point rounding of sums.

`service_metadata` records the newest weather reading id included
(`weather_hourly_through_id`) and the summary definition
(`weather_hourly_version`). A request uses the summaries only when that id equals
the newest stored reading in the same read transaction; otherwise it aggregates
raw readings as before.

The storage worker maintains the table. Each stored weather reading recomputes its
hour in the same transaction, so readings received out of order after a host
clock change are included correctly. A failed summary update never discards the
reading. Readings not yet included, such as all existing readings after an upgrade
or rows written by other processes, are backfilled in transactions of 5,000
readings between arrivals and checked again every ten seconds. A changed summary
definition, or a progress marker beyond the newest reading, discards the table and
rebuilds it.

Invalid parameters and range or row-budget violations return HTTP 422. Busy,
timed-out or unavailable aggregate reads return HTTP 503. Aggregate errors use:

```json
{"error":{"code":"invalid_query","message":"invalid weather summary query"}}
```

The existing `/live`, `/history/indoor`, `/history/weather` and incremental raw
history routes are unchanged.

## Same-origin web files

Pass `--web-root /absolute/path/to/weatherstation_web/dist` to serve a built web
application. The path names the contents of `dist`; no sibling repository path is
assumed. `/` and client-side routes serve `index.html` with `no-store`. Existing
files are served with an appropriate content type, and `/assets/...` files use
`public, max-age=31536000, immutable`. GET and HEAD are supported.

Static reads run in a separate bounded blocking pool and individual files are
limited to 16 MiB. Encoded or literal traversal is rejected, symlinks cannot
escape the canonical web root, missing assets return 404, and `/api`, `/live` and
`/history` paths never fall back to the SPA. If `--web-root` is omitted, the
service retains its API-only behavior.
