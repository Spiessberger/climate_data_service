# Indoor DATA wire contract, version 1

One complete line begins with exact ASCII `DATA `, followed by a compact UTF-8
JSON object and LF. Its maximum size is 1024 bytes including prefix and LF.
There is no log-level prefix, ANSI escape, literal embedded newline or CR.

```text
DATA {"v":1,"type":"indoor","boot_id":"6a9d3c1f80b24e67a511d92cb837046e","seq":42,"temperature_celsius":21.5,"relative_humidity_percent":48.2}
```

Every listed field is required. `v` is integer 1; `type` is `indoor`. `boot_id` is
16 random bytes encoded as 32 lowercase hexadecimal characters, fixed for one
boot. Firmware generates it with an enabled hardware entropy source. `seq` is an
unsigned 32-bit integer: start at 1, increment when producing the paired reading
before latest-reading overwrite, and wrap from 4294967295 to 0. Sensor failures
produce no reading. Unread measurements may be overwritten; USB has no delivery
acknowledgment or replay guarantee.

`temperature_celsius` and `relative_humidity_percent` are finite JSON numbers in
°C and percent respectively. There is no added physical-range filtering.
The host adds UTC reception time; it does not infer sensor measurement time.

Validate the entire object before publishing. Reject missing/wrongly typed fields,
invalid identifiers, duplicate keys (including in extensions), invalid UTF-8,
non-finite/invalid JSON numbers, trailing non-JSON input, and unknown versions or
types. Accept additional fields on valid version-1 indoor records. Integer fields
must use integer JSON representations. Weather and heartbeat become supported in
later slices; they cannot update indoor live readings in this implementation.

Only complete LF-terminated lines can become readings. Do not salvage embedded
JSON or a later DATA prefix within a damaged line. Discard parser state for an
oversized line until LF; forward original rejected bytes incrementally in chunks
of at most 1024 bytes. Treat long ordinary text with the same memory bound.
Forward incomplete bytes on disconnection or shutdown to the diagnostic boundary.
Subsequent complete lines can update live state normally.

The gateway formats a complete record in bounded storage, then passes it to one
synchronized printer write shared with operational logging. Formatting failure
produces an operational warning and no DATA write. Transport can still truncate
or drop bytes. Operational logs retain the existing level-prefixed text format.
