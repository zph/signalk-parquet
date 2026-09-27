# Optimized raw archive baseline

On 2026-09-27, the benchmark used the home-server raw archive and the latest
local tiered-history worktree (`17104f3`). The source contained 6,519,174 rows
in 4,658 Parquet files across 299 path/year groups. All output was written to
temporary container storage; the production archive was not modified.

The existing raw files already use native `TIMESTAMP_MS` event time and
`received_delay_us`. Rewriting them as the typed path/day control therefore
produced exactly the same byte count as the current path/day control. There is
no remaining UTF-8 timestamp saving to claim for this archive.

The optimized variants use:

- unsigned integer IDs plus four Parquet catalogs for context, path, source,
  and metadata;
- one shared file per day and compatible typed payload family (25 data files
  plus four catalogs in this snapshot);
- rows sorted by `(path_id, context_id, event_time)`;
- DuckDB's Parquet writer and native `TIMESTAMP_MS` event time;
- `received_delay_us` instead of a second timestamp;
- the original typed payload columns, including JSON only when it already
  represents a value or metadata that cannot be represented by the typed
  payload family.

## Storage and compaction

| Layout | ZSTD | Row group | Files | Bytes | Change from sealed control | Write time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Raw source | mixed | mixed | 4,658 | 64,370,828 | +59.9% | n/a |
| Path/day sealed control | 9 | 16K | 585 | 40,261,860 | baseline | 24.35 s |
| Shared daily typed | 9 | 16K | 29 | 32,933,874 | -18.2% | 9.16 s |
| Shared daily typed | 9 | 64K | 29 | 31,025,644 | -22.9% | 8.97 s |
| Shared daily typed | 6 | 262K | 29 | 28,840,236 | -28.4% | 8.82 s |
| Shared daily typed | 9 | 262K | 29 | 28,596,554 | -29.0% | 9.22 s |

Building the four identity catalogs took 3.98 seconds. Even including that
step, the recommended 64K layout completed in 12.95 seconds, versus 24.35
seconds for the path/day sealed control. Compaction is offline, so this write
time is informational rather than a serving constraint.

ZSTD-9 saved only 0.84% over ZSTD-6 at 262K and took 4.5% longer to write. It
is still a reasonable sealed-partition default, but shared files and
dictionary IDs account for almost all of the material saving.

## Read performance

Each result below is the mean of the warmed median latency for the eight
largest path/context groups, querying their most recent 24 hours. Each query
was run 14 times. Candidate order was rotated in both directions to limit
cache-order bias.

| Layout | Mean warmed latency | Change from sealed control |
| --- | ---: | ---: |
| Raw source | 5.401 ms | +51.3% |
| Path/day sealed control | 3.571 ms | baseline |
| Shared daily, ZSTD-9/16K | 4.562 ms | +27.8% |
| Shared daily, ZSTD-9/64K | 3.596 ms | +0.7% |
| Shared daily, ZSTD-6/262K | 5.262 ms | +47.4% |
| Shared daily, ZSTD-9/262K | 5.103 ms | +42.9% |

The 64K row group is the best balanced baseline: it is within 0.7% of the
current sealed layout's mean warmed latency, uses 22.9% fewer bytes, and uses
95.0% fewer files. Relative to the unsealed raw source, it uses 51.8% fewer
bytes and is 33.4% faster for these queries.

The 262K layout is the size floor measured here, but it is not the default:
the additional 2.43 MB saving costs roughly 42.9% query latency versus the
sealed control. A future coldest tier could choose that trade-off separately.

## Integrity and limitations

Every optimized data file was checked against its input with the row count
and two order-independent whole-row fingerprints (XOR and sum of DuckDB's row
hash), including the timestamp normalized to epoch microseconds. All
6,519,174 rows matched. The path/day controls were additionally checked with
bidirectional `EXCEPT ALL` comparisons.

The measurements use DuckDB against the home-server's local filesystem. They
establish format size, local scan cost, and file-count reduction; they do not
measure S3 request latency. The drop from 585 sealed files to 29 should reduce
serverless object-open overhead, but that needs a separate S3 benchmark before
making a latency claim.

Reproduce after building the plugin with:

```sh
npm run benchmark:optimized-archive -- <archive-root> <temporary-output-root>
```
