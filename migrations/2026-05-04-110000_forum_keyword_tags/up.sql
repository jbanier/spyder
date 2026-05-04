CREATE TABLE forum_keyword_rule(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  label VARCHAR NOT NULL,
  pattern VARCHAR NOT NULL,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(label, pattern)
);

CREATE INDEX idx_forum_keyword_rule_label ON forum_keyword_rule(label);

CREATE TABLE page_keyword_tag(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  tag VARCHAR NOT NULL,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(page_id) REFERENCES page(id) ON DELETE CASCADE,
  UNIQUE(page_id, tag)
);

CREATE INDEX idx_page_keyword_tag_page_id ON page_keyword_tag(page_id);
CREATE INDEX idx_page_keyword_tag_tag ON page_keyword_tag(tag);
