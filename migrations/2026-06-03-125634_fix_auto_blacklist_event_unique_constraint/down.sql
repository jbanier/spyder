-- Revert to the original COALESCE-based unique constraint
-- (Note: This will restore the bug, but needed for rollback)

DROP INDEX IF EXISTS idx_auto_blacklist_event_unique_page;

CREATE UNIQUE INDEX idx_auto_blacklist_event_unique_page
  ON auto_blacklist_event(domain, rule_id, COALESCE(source_page_id, 0));
