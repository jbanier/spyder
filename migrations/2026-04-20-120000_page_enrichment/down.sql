PRAGMA foreign_keys = OFF;

DROP TABLE IF EXISTS page_crypto;
DROP TABLE IF EXISTS page_email;
DROP TABLE IF EXISTS page_link;

CREATE TABLE page_old(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  title VARCHAR NOT NULL,
  url VARCHAR NOT NULL UNIQUE,
  links VARCHAR NOT NULL,
  emails VARCHAR NOT NULL,
  coins VARCHAR NOT NULL,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO page_old (id, title, url, links, emails, coins, created_at)
SELECT id, title, url, links, emails, coins, created_at
FROM page;

DROP TABLE page;
ALTER TABLE page_old RENAME TO page;

PRAGMA foreign_keys = ON;
