-- Remove screenshot support
DROP INDEX IF EXISTS idx_page_screenshot_path;
ALTER TABLE page DROP COLUMN IF EXISTS screenshot_size_bytes;
ALTER TABLE page DROP COLUMN IF EXISTS screenshot_taken_at;
ALTER TABLE page DROP COLUMN IF EXISTS screenshot_path;
