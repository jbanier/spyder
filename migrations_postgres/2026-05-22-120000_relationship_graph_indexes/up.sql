ALTER TABLE page_link
  ADD COLUMN source_host TEXT NOT NULL DEFAULT '';

UPDATE page_link pl
SET source_host = lower(split_part(
  CASE
    WHEN position('://' IN p.url) > 0 THEN split_part(split_part(p.url, '://', 2), '/', 1)
    ELSE ''
  END,
  ':',
  1
))
FROM page p
WHERE p.id = pl.source_page_id
  AND pl.source_host = '';

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
