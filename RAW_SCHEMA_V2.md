# Raw Parquet schema v2

Schema v2 keeps the existing context/path/day partitioning and ZSTD level 3,
but removes the duplicated ISO timestamp strings from durable raw files.

## Time columns

| Column | Parquet type | Meaning |
| --- | --- | --- |
| `event_time` | `TIMESTAMP_MICROS` | Signal K measurement time |
| `received_delay_us` | `INT64` | `received_time - event_time`, in signed microseconds |

Receipt time is losslessly reconstructed at the precision supplied by Signal K:

```sql
event_time + received_delay_us * INTERVAL '1 microsecond'
```

The design intentionally does not split event time into epoch/day/subsecond
columns. A native timestamp is directly filterable and sortable, while the
single delta column eliminates the costly second timestamp without complicating
every reader.

## Boundary with SQLite

SQLite is a short-lived ingestion and crash-recovery buffer. It retains
`signalk_timestamp` and `received_timestamp` as received. Export converts them
to the two durable columns above. History federation casts the SQLite event time
to `TIMESTAMP` before unioning it with Parquet, preventing text comparison or
timezone coercion.

## Compatibility

This is a clean schema boundary. Raw v1 Parquet files are not read alongside
v2 files. A deployment must remove the old raw/aggregate archive and allow the
plugin to begin a v2 archive; no backup or compatibility copy is created.
