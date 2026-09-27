# Compatible 64K row-group benchmark

On 2026-09-27, a shadow benchmark compared the existing path/day Parquet
layout at 16K and 64K row groups. Both candidates used ZSTD level 9 and
preserved the source schema, directory structure, and rows exactly. No
production archive files were changed.

The input contained 6,778,593 rows in 4,883 raw Parquet files. Each candidate
produced 585 path/day files. Every output file was verified against its source
with bidirectional `EXCEPT ALL` comparisons.

| Measure | ZSTD-9 / 16K | ZSTD-9 / 64K | Change |
| --- | ---: | ---: | ---: |
| Bytes | 41,857,191 | 40,192,731 | -3.98% |
| Files | 585 | 585 | none |
| Offline write time | 24.76 s | 25.10 s | +1.38% |
| Mean warmed query latency | 4.060 ms | 3.909 ms | -3.72% |

Query latency is the mean of warmed medians for the eight largest
path/context groups over their latest 24 hours. Each query ran 14 times with
candidate order rotated in both directions. Individual paths varied in both
directions, so the small aggregate speed improvement should be treated as
equivalence rather than a guaranteed latency gain.

The selected setting is therefore ZSTD level 9 with 65,536-row groups. It is
a physical writer setting only: timestamp columns, value types, file layout,
History API behavior, and S3 object naming remain unchanged. The 64K output is
3.98% smaller with no demonstrated read penalty; the 1.38% write cost is in an
offline operation.

Earlier experiments with shared files, integer identity catalogs, and native
timestamp schema changes are not part of the selected implementation because
they require new reader behavior and introduce compatibility risk.
