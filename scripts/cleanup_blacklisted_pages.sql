\if :{?batch_size}
\else
\set batch_size 50000
\endif

\echo Cleaning up blacklisted pages in batches of :batch_size

BEGIN;

CREATE TEMP TABLE cleanup_blacklisted_page_batch ON COMMIT DROP AS
WITH page_hosts AS (
  SELECT
    p.id,
    lower(split_part(
      CASE
        WHEN position('://' IN p.url) > 0 THEN split_part(split_part(p.url, '://', 2), '/', 1)
        ELSE ''
      END,
      ':',
      1
    )) AS host
  FROM page p
),
matching_pages AS (
  SELECT ph.id, ph.host
  FROM page_hosts ph
  WHERE ph.host <> ''
    AND EXISTS (
      SELECT 1
      FROM domain_blacklist db
      WHERE ph.host = lower(db.domain)
         OR ph.host LIKE ('%.' || lower(db.domain))
    )
  ORDER BY ph.id
  LIMIT :batch_size
)
SELECT id, host
FROM matching_pages;

SELECT COUNT(*) AS selected_page_count
FROM cleanup_blacklisted_page_batch;

WITH deleted_pages AS (
  DELETE FROM page p
  USING cleanup_blacklisted_page_batch batch
  WHERE p.id = batch.id
  RETURNING p.id
)
SELECT COUNT(*) AS deleted_page_count
FROM deleted_pages;

WITH candidate_hosts AS (
  SELECT DISTINCT host
  FROM cleanup_blacklisted_page_batch
  WHERE host <> ''
),
deleted_profiles AS (
  DELETE FROM site_profile sp
  USING candidate_hosts ch
  WHERE sp.host = ch.host
    AND NOT EXISTS (
      SELECT 1
      FROM page p
      WHERE lower(split_part(
        CASE
          WHEN position('://' IN p.url) > 0 THEN split_part(split_part(p.url, '://', 2), '/', 1)
          ELSE ''
        END,
        ':',
        1
      )) = ch.host
    )
  RETURNING sp.host
)
SELECT COUNT(*) AS deleted_site_profile_count
FROM deleted_profiles;

COMMIT;

\echo Re-run this script until selected_page_count is 0.
