# Explore the archive with DuckDB

DuckDB can query the Parquet files in place; it does not import or rewrite the
archive. Install the DuckDB CLI, edit `load_archive_views.sql` to point
`archive_root` at the plugin's data directory, then run:

```sh
duckdb -init tools/duckdb/load_archive_views.sql
```

The current legacy exact layout is exposed as `archive_legacy_exact`:

```sql
SELECT signalk_timestamp, received_timestamp, value
FROM archive_legacy_exact
WHERE context = 'vessels.self'
  AND path = 'navigation.speedOverGround'
  AND signalk_timestamp >= '2026-09-25T00:00:00Z'
ORDER BY signalk_timestamp;
```

The view uses a recursive Parquet glob and filters out quarantine and repair
directories. You can inspect columns and Parquet metadata directly with
`DESCRIBE SELECT * FROM archive_legacy_exact` and `parquet_metadata(...)`.

The plugin keeps one path per file. That matters for cloud reads because its
S3 cache downloads whole objects: a request for one path does not download
unrelated paths from a shared hourly file.

## Compact v1 trial on boat-pi

The existing path-per-file layout can be rewritten losslessly with DuckDB,
keeping the same columns, ZSTD compression, and 4,096-row groups. Run the
matrix on a representative file with:

```sh
npm run benchmark:compact-v1 -- /path/to/file.parquet
```

The script tests Parquet V1 and V2 physical encodings at ZSTD levels 3 and 9,
plus several row-group sizes for V1/ZSTD-9. It checks every row in both
directions with `EXCEPT ALL`, reports size and read latency, and removes its
temporary rewrites. It never edits the input file.

Four real boat-pi samples (two larger historical files and two current hourly
files) all used Snappy in their original files. With Parquet V1 and 4,096-row
groups, ZSTD-3 made them 60–65% smaller; ZSTD-9 made them 65–69% smaller.
The larger files gained another 11–12% relative to ZSTD-3 at level 9, with
roughly 1.5–2× the write time. Short-range warm reads stayed around 2–4 ms.
Parquet V2 physical encodings changed size by less than 0.5% on the larger
files and were sometimes larger on the small hourly files. These observations
support a compact v1 rollout for the current archive, but do not establish
the same reduction against archives that are already ZSTD-compressed.

For two existing ZSTD-3 daily compactions of roughly 45,000 rows each,
rewriting the same V1 schema at ZSTD-9 with 16,384-row groups saved about
5% and reduced a 10-minute read from roughly 5–6 ms to roughly 3 ms.
Using one large row group saved only another 0.3–1.4% but returned to
roughly 5–6 ms reads. On a larger Snappy wind file, ZSTD-9 with 16,384-row
groups saved 70% and read in 3.6 ms versus 3.1 ms originally. This makes
16,384 rows the daily-compaction setting used by the current writer.

For direct S3 exploration, DuckDB's `httpfs` extension supports S3 URLs; the
plugin's own S3 reader instead verifies committed manifests and stages verified
objects in its bounded local read cache. See the official
[DuckDB Parquet guide](https://duckdb.org/docs/data/parquet) and
[S3 API guide](https://duckdb.org/docs/current/core_extensions/httpfs/s3api).
