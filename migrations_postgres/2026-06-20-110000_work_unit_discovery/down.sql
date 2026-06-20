DROP INDEX IF EXISTS idx_work_unit_failure_category;
DROP INDEX IF EXISTS idx_work_unit_discovery;

ALTER TABLE work_unit
  DROP COLUMN IF EXISTS failure_category,
  DROP COLUMN IF EXISTS url_discovery_id;
