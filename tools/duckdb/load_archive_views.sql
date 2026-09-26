-- Load with DuckDB CLI after replacing the root below:
--   duckdb -init signalk-parquet/tools/duckdb/load_archive_views.sql
-- Then explore with: SELECT * FROM archive_values LIMIT 20;
--
-- Legacy exact files are immediately queryable.

SET VARIABLE archive_root = '/path/to/signalk-parquet-data';

CREATE OR REPLACE VIEW archive_legacy_exact AS
SELECT *
FROM read_parquet(
  getvariable('archive_root') || '/tier=raw/context=*/path=*/year=*/day=*/*.parquet',
  union_by_name = true,
  hive_partitioning = false,
  filename = true
)
WHERE filename NOT LIKE '%/processed/%'
  AND filename NOT LIKE '%/quarantine/%'
  AND filename NOT LIKE '%/failed/%'
  AND filename NOT LIKE '%/repaired/%';
