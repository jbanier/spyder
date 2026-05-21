CREATE TABLE auto_blacklist_rule(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  rule_type VARCHAR NOT NULL,
  value VARCHAR NOT NULL,
  label VARCHAR NOT NULL DEFAULT '',
  enabled BOOLEAN NOT NULL DEFAULT 1,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CHECK (rule_type IN ('site_category', 'keyword')),
  CHECK (value <> '')
);

CREATE UNIQUE INDEX idx_auto_blacklist_rule_type_value
  ON auto_blacklist_rule(rule_type, lower(value));

CREATE INDEX idx_auto_blacklist_rule_enabled_type
  ON auto_blacklist_rule(enabled, rule_type, id);

CREATE TABLE auto_blacklist_event(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  rule_id INTEGER NOT NULL REFERENCES auto_blacklist_rule(id) ON DELETE CASCADE,
  domain VARCHAR NOT NULL,
  source_page_id INTEGER REFERENCES page(id) ON DELETE SET NULL,
  rule_type VARCHAR NOT NULL,
  matched_value VARCHAR NOT NULL,
  evidence VARCHAR NOT NULL DEFAULT '',
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CHECK (domain <> ''),
  CHECK (rule_type IN ('site_category', 'keyword')),
  CHECK (matched_value <> '')
);

CREATE UNIQUE INDEX idx_auto_blacklist_event_unique_page
  ON auto_blacklist_event(domain, rule_id, COALESCE(source_page_id, 0));

CREATE INDEX idx_auto_blacklist_event_domain
  ON auto_blacklist_event(domain, created_at DESC);

CREATE INDEX idx_auto_blacklist_event_created_at
  ON auto_blacklist_event(created_at DESC, id DESC);
