ALTER TABLE site_profile
  ADD COLUMN first_found_at TEXT NOT NULL DEFAULT to_char(timezone('UTC', now()), 'YYYY-MM-DD HH24:MI:SS'),
  ADD COLUMN last_scanned_at TEXT NOT NULL DEFAULT to_char(timezone('UTC', now()), 'YYYY-MM-DD HH24:MI:SS');

WITH page_stats AS (
  SELECT
    split_part(split_part(url, '://', 2), '/', 1) AS host,
    COUNT(*)::INTEGER AS page_count,
    MIN(created_at) AS first_found_at,
    MAX(last_scanned_at) AS last_scanned_at
  FROM page
  WHERE position('://' IN url) > 0
  GROUP BY split_part(split_part(url, '://', 2), '/', 1)
)
UPDATE site_profile sp
SET
  page_count = page_stats.page_count,
  first_found_at = page_stats.first_found_at,
  last_scanned_at = page_stats.last_scanned_at
FROM page_stats
WHERE page_stats.host = sp.host;

CREATE INDEX idx_site_profile_last_scanned_at ON site_profile(last_scanned_at DESC);
