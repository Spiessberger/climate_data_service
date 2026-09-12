# Live and historical weather

Version-1 `weather` DATA records carry every decoded WH24 quantity and receiver
status. See the complete [wire fixture](../tests/fixtures/weather.data) and the
[independently prepared unavailable fixture](../tests/fixtures/weather-unavailable.data).
These same files are checked against the firmware's reporting boundary; the
service has no build dependency on the gateway checkout.

All fields are required, including nullable fields. Extra version-1 fields are
accepted. Integer fields must use unsigned integer JSON values within their
specified representation; no additional physical-range filtering is applied.

| Field | Representation and meaning |
| --- | --- |
| `v`, `type`, `boot_id`, `seq` | Version 1, `weather`, 32 lowercase hex digits, unsigned 32-bit stream counter |
| `station_id` | Unsigned 8-bit transmitted source ID; may change after battery replacement |
| `temperature_celsius` | Nullable number, °C |
| `relative_humidity_percent` | Nullable unsigned 8-bit integer, percent |
| `wind_direction_degrees` | Nullable unsigned 16-bit integer, degrees |
| `wind_speed_mps`, `gust_speed_mps` | Nullable numbers, metres per second |
| `rain_mm` | Number, cumulative station rain counter, millimetres |
| `uv_microwatts_per_cm2` | Nullable unsigned 16-bit integer, µW/cm² |
| `uv_index` | Nullable unsigned 8-bit integer, dimensionless |
| `light_lux` | Nullable number, lux |
| `battery_low` | Boolean |
| `rssi_dbm` | Number, dBm |
| `lqi` | Unsigned 8-bit integer, receiver link quality |

`GET /live` includes independent `indoor` and `weather` objects, initially null.
Every accepted weather arrival replaces the whole weather object; nulls never
inherit earlier values. The service adds `received_at_unix_ms`, with no database
row ID. Heartbeats update gateway communication health without changing either
reading or its reception time. `gateway.weather` exposes the same per-stream
continuity fields as `gateway.indoor`; a boot change clears both baselines.
A station-ID change does not reset the weather counter or identify a permanent
physical device. Weather loss leaves the last live reading with its original age.

Weather history uses the [same parameters and cursor rules as indoor history](indoor-history.md):

- `GET /history/weather?from_unix_ms=0&to_unix_ms=2000000000000&limit=100`
- `GET /history/weather/updates?after_id=0&limit=100`

Time ranges are half-open and ordered by `(received_at_unix_ms, id)`. Continue a
range using both `after_received_at_unix_ms` and `after_id`; incremental queries
order by row ID. Pages default to 100 rows and allow 1–1000. IDs belong to each
stream's separate table, so do not reuse an indoor cursor for weather. Only
committed rows appear. All wire fields, reception milliseconds and row IDs are
retained in `weather_readings`, with INTEGER/REAL/NULL storage and an index on
`(received_at_unix_ms, id)`. Existing databases gain the weather table at startup.
Rain is preserved as received, including decreases after counter reset or source
change; the service does not compute interval rainfall.

```sh
cargo build --locked
python3 scripts/weather_demo.py
```

The finite pseudo-terminal demo sends both streams, switches from station 191 to
station 1 with unavailable quantities, queries live/history HTTP, then restarts
and verifies retained weather history with absent live weather. This is simulated
wire integration, not physical radio/USB verification. Full storage-failure
recovery and complete-system hardware acceptance remain subsequent tickets.
