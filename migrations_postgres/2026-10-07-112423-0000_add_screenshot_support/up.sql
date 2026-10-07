-- Add screenshot support to page table
ALTER TABLE page ADD COLUMN screenshot_path VARCHAR;
ALTER TABLE page ADD COLUMN screenshot_taken_at TIMESTAMP;
ALTER TABLE page ADD COLUMN screenshot_size_bytes INTEGER;

-- Index for finding pages with screenshots
CREATE INDEX idx_page_screenshot_path ON page(screenshot_path) WHERE screenshot_path IS NOT NULL;
