ALTER TABLE page_link
  ADD COLUMN source_host VARCHAR NOT NULL DEFAULT '';

UPDATE page_link
SET source_host = lower(
  CASE
    WHEN instr((SELECT url FROM page WHERE page.id = page_link.source_page_id), '://') > 0 THEN
      CASE
        WHEN instr(substr((SELECT url FROM page WHERE page.id = page_link.source_page_id), instr((SELECT url FROM page WHERE page.id = page_link.source_page_id), '://') + 3), '/') > 0 THEN
          substr(
            substr((SELECT url FROM page WHERE page.id = page_link.source_page_id), instr((SELECT url FROM page WHERE page.id = page_link.source_page_id), '://') + 3),
            1,
            instr(substr((SELECT url FROM page WHERE page.id = page_link.source_page_id), instr((SELECT url FROM page WHERE page.id = page_link.source_page_id), '://') + 3), '/') - 1
          )
        ELSE substr((SELECT url FROM page WHERE page.id = page_link.source_page_id), instr((SELECT url FROM page WHERE page.id = page_link.source_page_id), '://') + 3)
      END
    ELSE ''
  END
)
WHERE source_host = '';

CREATE INDEX idx_page_link_relationship_source_target
  ON page_link(source_host, lower(target_host))
  WHERE source_host <> ''
    AND target_host <> ''
    AND source_host <> lower(target_host);

CREATE INDEX idx_page_link_relationship_target_source
  ON page_link(lower(target_host), source_host)
  WHERE source_host <> ''
    AND target_host <> ''
    AND source_host <> lower(target_host);
