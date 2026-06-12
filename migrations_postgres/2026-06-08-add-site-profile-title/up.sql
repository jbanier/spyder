-- Add title column to site_profile table
ALTER TABLE site_profile ADD COLUMN title TEXT;

-- Create indexes for performance
CREATE INDEX idx_site_profile_title ON site_profile(title);
CREATE INDEX idx_site_profile_title_notnull ON site_profile(title)
    WHERE title IS NOT NULL;
CREATE INDEX idx_site_profile_title_pages ON site_profile(title, page_count DESC)
    WHERE title IS NOT NULL;
