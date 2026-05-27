-- Rollback: Remove additional performance indexes

DROP INDEX IF EXISTS idx_page_last_scanned_desc;
DROP INDEX IF EXISTS idx_work_unit_status_created;
DROP INDEX IF EXISTS idx_page_scan_scanned_desc;
DROP INDEX IF EXISTS idx_site_profile_last_scanned_desc;
