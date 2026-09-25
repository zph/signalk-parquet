# Shared AIS pre-deployment benchmark

On 2026-09-25 UTC, a read-only snapshot of the home-server raw Parquet archive
was copied to local temporary storage. The benchmark generated one ZSTD-3
shared AIS file per path/day, preserving every row and vessel context. Query
times below are local DuckDB medians over 20 warmed count queries; they are
not a prediction of home-server CPU latency.

| Measure | Per-vessel files | Shared files |
| --- | ---: | ---: |
| Files | 3,440 | 17 |
| Bytes | 11,415,650 | 544,826 |
| AIS rows verified by migration | 50,069 | 50,069 |
| One vessel's position count (94 rows) | 0.275 ms | 0.206 ms |
| All AIS position count (7,347 rows) | 15.481 ms | 0.123 ms |

The size reduction is 95.23%. The benchmark used
`node dist/cli/benchmark-shared-ais.js <snapshot-directory>`; the
one-way migration was then applied to that copy and verified all rows before
moving old directories into a recoverable backup. No production archive was
modified during this benchmark.
