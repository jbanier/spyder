DROP INDEX IF EXISTS idx_site_profile_last_scanned_at;

ALTER TABLE site_profile
  DROP COLUMN last_scanned_at,
  DROP COLUMN first_found_at;
