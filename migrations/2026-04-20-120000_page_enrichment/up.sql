PRAGMA foreign_keys = OFF;

CREATE TABLE page_new(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  title VARCHAR NOT NULL,
  url VARCHAR NOT NULL UNIQUE,
  links VARCHAR NOT NULL,
  emails VARCHAR NOT NULL,
  coins VARCHAR NOT NULL,
  language VARCHAR NOT NULL DEFAULT '',
  last_scanned_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO page_new (id, title, url, links, emails, coins, language, last_scanned_at, created_at)
SELECT id, title, url, links, emails, coins, '', created_at, created_at
FROM page;

DROP TABLE page;
ALTER TABLE page_new RENAME TO page;

CREATE TABLE page_link(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  source_page_id INTEGER NOT NULL,
  target_url VARCHAR NOT NULL,
  target_host VARCHAR NOT NULL DEFAULT '',
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(source_page_id) REFERENCES page(id) ON DELETE CASCADE,
  UNIQUE(source_page_id, target_url)
);

CREATE INDEX idx_page_link_source_page_id ON page_link(source_page_id);
CREATE INDEX idx_page_link_target_url ON page_link(target_url);
CREATE INDEX idx_page_link_target_host ON page_link(target_host);

CREATE TABLE page_email(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  email VARCHAR NOT NULL,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(page_id) REFERENCES page(id) ON DELETE CASCADE,
  UNIQUE(page_id, email)
);

CREATE INDEX idx_page_email_page_id ON page_email(page_id);
CREATE INDEX idx_page_email_email ON page_email(email);

CREATE TABLE page_crypto(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  asset_type VARCHAR NOT NULL,
  reference VARCHAR NOT NULL,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(page_id) REFERENCES page(id) ON DELETE CASCADE,
  UNIQUE(page_id, asset_type, reference)
);

CREATE INDEX idx_page_crypto_page_id ON page_crypto(page_id);
CREATE INDEX idx_page_crypto_reference ON page_crypto(reference);
CREATE INDEX idx_page_crypto_asset_type ON page_crypto(asset_type);

PRAGMA foreign_keys = ON;
