-- Fix the unique constraint to handle NULL source_page_id values correctly.
-- The previous COALESCE-based index caused constraint violations when multiple pages
-- from the same domain were deleted, as all NULLs became 0 and created duplicates.
-- A partial unique index allows multiple NULLs while still preventing duplicates
-- when source_page_id is not NULL.

DROP INDEX IF EXISTS idx_auto_blacklist_event_unique_page;

CREATE UNIQUE INDEX idx_auto_blacklist_event_unique_page
  ON auto_blacklist_event(domain, rule_id, source_page_id)
  WHERE source_page_id IS NOT NULL;
