-- Additional Performance Indexes
-- Strategic indexes to improve query performance on large tables
-- Note: CONCURRENTLY removed to allow running inside migration transaction
-- Note: Regular indexes (no partial filtering) for reliability and simplicity

-- Composite index on page table for recent scans sorting
-- Optimizes dashboard queries showing recently scanned pages
CREATE INDEX IF NOT EXISTS idx_page_last_scanned_desc
    ON page(last_scanned_at DESC, id);

-- Index on work_unit for pending work queries
-- Critical for crawler performance when loading work queue
CREATE INDEX IF NOT EXISTS idx_work_unit_status_created
    ON work_unit(status, created_at, id);

-- Composite index on page_scan for recent scan analysis
-- Speeds up scan history and diff views
CREATE INDEX IF NOT EXISTS idx_page_scan_scanned_desc
    ON page_scan(scanned_at DESC, page_id);

-- Composite index on site_profile for active site queries
-- Optimizes analytics queries sorting by recent activity
CREATE INDEX IF NOT EXISTS idx_site_profile_last_scanned_desc
    ON site_profile(last_scanned_at DESC, id);
