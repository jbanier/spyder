-- Drop indexes
DROP INDEX IF EXISTS idx_site_profile_title_pages;
DROP INDEX IF EXISTS idx_site_profile_title_notnull;
DROP INDEX IF EXISTS idx_site_profile_title;

-- Drop title column
ALTER TABLE site_profile DROP COLUMN IF EXISTS title;
