DROP INDEX IF EXISTS idx_page_link_relationship_target_source;
DROP INDEX IF EXISTS idx_page_link_relationship_source_target;

ALTER TABLE page_link
  DROP COLUMN source_host;
