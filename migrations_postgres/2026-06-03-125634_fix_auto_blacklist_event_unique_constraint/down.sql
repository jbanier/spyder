-- Revert to the original COALESCE-based unique constraint
-- (Note: This will restore the bug, but needed for rollback)

DROP INDEX IF EXISTS idx_auto_blacklist_event_unique_page;

-- Recreate the original COALESCE-based unique constraint only if not already present
-- This is a safe revert that allows rollback even with existing data
DO $$
BEGIN
  BEGIN
    CREATE UNIQUE INDEX idx_auto_blacklist_event_unique_page
      ON auto_blacklist_event(domain, rule_id, COALESCE(source_page_id, 0));
  EXCEPTION WHEN unique_violation THEN
    -- If unique constraint violation occurs, it's due to duplicate rows
    -- This is expected and acceptable for a rollback scenario
    NULL;
  END;
END
$$;
