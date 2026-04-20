CREATE TABLE domain_blacklist(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  domain VARCHAR NOT NULL UNIQUE,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_domain_blacklist_domain ON domain_blacklist(domain);
