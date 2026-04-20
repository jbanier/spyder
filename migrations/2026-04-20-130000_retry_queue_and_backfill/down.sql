PRAGMA foreign_keys = OFF;

DROP INDEX IF EXISTS idx_work_unit_status_next_attempt_at;

CREATE TABLE work_unit_old(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  url VARCHAR NOT NULL UNIQUE,
  status VARCHAR NOT NULL DEFAULT 'pending',
  retry_count INTEGER NOT NULL DEFAULT 0,
  last_error VARCHAR,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO work_unit_old (id, url, status, retry_count, last_error, created_at)
SELECT id, url, status, retry_count, last_error, created_at
FROM work_unit;

DROP TABLE work_unit;
ALTER TABLE work_unit_old RENAME TO work_unit;

PRAGMA foreign_keys = ON;
