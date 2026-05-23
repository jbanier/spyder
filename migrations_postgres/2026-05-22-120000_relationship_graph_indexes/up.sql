-- This migration rewrites a large page_link column and builds two large
-- relationship indexes. Keep the tuning scoped to Diesel's migration
-- transaction so it does not persist after the migration completes.
SET LOCAL statement_timeout = 0;
SET LOCAL work_mem = '4GB';
SET LOCAL maintenance_work_mem = '8GB';
SET LOCAL max_parallel_maintenance_workers = 8;

ALTER TABLE page_link
  ADD COLUMN source_host TEXT NOT NULL DEFAULT '';

UPDATE page_link pl
SET source_host = lower(split_part(
  CASE
    WHEN position('://' IN p.url) > 0 THEN split_part(split_part(p.url, '://', 2), '/', 1)
    ELSE ''
  END,
  ':',
  1
))
FROM page p
WHERE p.id = pl.source_page_id
  AND pl.source_host = '';

CREATE INDEX idx_page_link_relationship_source_target
  ON page_link(source_host, lower(target_host))
  WHERE source_host <> ''
    AND target_host <> ''
    AND source_host <> lower(target_host);

CREATE INDEX idx_page_link_relationship_target_source
  ON page_link(lower(target_host), source_host)
  WHERE source_host <> ''
    AND target_host <> ''
    AND source_host <> lower(target_host);
