CREATE TABLE page_classification(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL UNIQUE,
  host VARCHAR NOT NULL,
  category VARCHAR NOT NULL,
  confidence VARCHAR NOT NULL,
  score INTEGER NOT NULL DEFAULT 0,
  evidence VARCHAR NOT NULL DEFAULT '',
  last_classified_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(page_id) REFERENCES page(id) ON DELETE CASCADE
);

CREATE INDEX idx_page_classification_host ON page_classification(host);
CREATE INDEX idx_page_classification_category ON page_classification(category);

CREATE TABLE site_profile(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  host VARCHAR NOT NULL UNIQUE,
  category VARCHAR NOT NULL,
  confidence VARCHAR NOT NULL,
  score INTEGER NOT NULL DEFAULT 0,
  page_count INTEGER NOT NULL DEFAULT 0,
  evidence VARCHAR NOT NULL DEFAULT '',
  source_page_id INTEGER,
  last_classified_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(source_page_id) REFERENCES page(id) ON DELETE SET NULL
);

CREATE INDEX idx_site_profile_category ON site_profile(category);
CREATE INDEX idx_site_profile_host ON site_profile(host);
