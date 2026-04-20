CREATE TABLE page_scan(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  title VARCHAR NOT NULL,
  language VARCHAR NOT NULL DEFAULT '',
  scanned_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(page_id) REFERENCES page(id) ON DELETE CASCADE
);

CREATE INDEX idx_page_scan_page_id_scanned_at ON page_scan(page_id, scanned_at, id);

CREATE TABLE page_scan_link(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  scan_id INTEGER NOT NULL,
  target_url VARCHAR NOT NULL,
  target_host VARCHAR NOT NULL DEFAULT '',
  FOREIGN KEY(scan_id) REFERENCES page_scan(id) ON DELETE CASCADE,
  UNIQUE(scan_id, target_url)
);

CREATE INDEX idx_page_scan_link_scan_id ON page_scan_link(scan_id);
CREATE INDEX idx_page_scan_link_target_url ON page_scan_link(target_url);

CREATE TABLE page_scan_email(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  scan_id INTEGER NOT NULL,
  email VARCHAR NOT NULL,
  FOREIGN KEY(scan_id) REFERENCES page_scan(id) ON DELETE CASCADE,
  UNIQUE(scan_id, email)
);

CREATE INDEX idx_page_scan_email_scan_id ON page_scan_email(scan_id);
CREATE INDEX idx_page_scan_email_email ON page_scan_email(email);

CREATE TABLE page_scan_crypto(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  scan_id INTEGER NOT NULL,
  asset_type VARCHAR NOT NULL,
  reference VARCHAR NOT NULL,
  FOREIGN KEY(scan_id) REFERENCES page_scan(id) ON DELETE CASCADE,
  UNIQUE(scan_id, asset_type, reference)
);

CREATE INDEX idx_page_scan_crypto_scan_id ON page_scan_crypto(scan_id);
CREATE INDEX idx_page_scan_crypto_reference ON page_scan_crypto(reference);
