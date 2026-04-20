PRAGMA foreign_keys = OFF;

CREATE TABLE work_unit_new(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  url VARCHAR NOT NULL UNIQUE,
  status VARCHAR NOT NULL DEFAULT 'pending',
  retry_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_attempt_at VARCHAR,
  last_error VARCHAR,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO work_unit_new (
  id,
  url,
  status,
  retry_count,
  next_attempt_at,
  last_attempt_at,
  last_error,
  created_at
)
SELECT
  id,
  url,
  status,
  retry_count,
  created_at,
  NULL,
  last_error,
  created_at
FROM work_unit;

DROP TABLE work_unit;
ALTER TABLE work_unit_new RENAME TO work_unit;

CREATE INDEX idx_work_unit_status_next_attempt_at ON work_unit(status, next_attempt_at);

WITH RECURSIVE link_parts(page_id, value, rest) AS (
  SELECT
    id,
    trim(CASE
      WHEN instr(links, ',') > 0 THEN substr(links, 1, instr(links, ',') - 1)
      ELSE links
    END),
    CASE
      WHEN instr(links, ',') > 0 THEN substr(links, instr(links, ',') + 1)
      ELSE ''
    END
  FROM page
  WHERE trim(links) != ''
  UNION ALL
  SELECT
    page_id,
    trim(CASE
      WHEN instr(rest, ',') > 0 THEN substr(rest, 1, instr(rest, ',') - 1)
      ELSE rest
    END),
    CASE
      WHEN instr(rest, ',') > 0 THEN substr(rest, instr(rest, ',') + 1)
      ELSE ''
    END
  FROM link_parts
  WHERE rest != ''
),
normalized_links AS (
  SELECT page_id, trim(value) AS target_url
  FROM link_parts
  WHERE trim(value) != ''
)
INSERT OR IGNORE INTO page_link(source_page_id, target_url, target_host)
SELECT
  page_id,
  target_url,
  CASE
    WHEN instr(target_url, '://') > 0 THEN
      CASE
        WHEN instr(substr(target_url, instr(target_url, '://') + 3), '/') > 0 THEN
          substr(
            substr(target_url, instr(target_url, '://') + 3),
            1,
            instr(substr(target_url, instr(target_url, '://') + 3), '/') - 1
          )
        ELSE substr(target_url, instr(target_url, '://') + 3)
      END
    ELSE ''
  END
FROM normalized_links;

WITH RECURSIVE email_parts(page_id, value, rest) AS (
  SELECT
    id,
    trim(CASE
      WHEN instr(emails, ',') > 0 THEN substr(emails, 1, instr(emails, ',') - 1)
      ELSE emails
    END),
    CASE
      WHEN instr(emails, ',') > 0 THEN substr(emails, instr(emails, ',') + 1)
      ELSE ''
    END
  FROM page
  WHERE trim(emails) != ''
  UNION ALL
  SELECT
    page_id,
    trim(CASE
      WHEN instr(rest, ',') > 0 THEN substr(rest, 1, instr(rest, ',') - 1)
      ELSE rest
    END),
    CASE
      WHEN instr(rest, ',') > 0 THEN substr(rest, instr(rest, ',') + 1)
      ELSE ''
    END
  FROM email_parts
  WHERE rest != ''
)
INSERT OR IGNORE INTO page_email(page_id, email)
SELECT page_id, lower(trim(value))
FROM email_parts
WHERE trim(value) != '';

WITH RECURSIVE crypto_parts(page_id, value, rest) AS (
  SELECT
    id,
    trim(CASE
      WHEN instr(coins, ',') > 0 THEN substr(coins, 1, instr(coins, ',') - 1)
      ELSE coins
    END),
    CASE
      WHEN instr(coins, ',') > 0 THEN substr(coins, instr(coins, ',') + 1)
      ELSE ''
    END
  FROM page
  WHERE trim(coins) != ''
  UNION ALL
  SELECT
    page_id,
    trim(CASE
      WHEN instr(rest, ',') > 0 THEN substr(rest, 1, instr(rest, ',') - 1)
      ELSE rest
    END),
    CASE
      WHEN instr(rest, ',') > 0 THEN substr(rest, instr(rest, ',') + 1)
      ELSE ''
    END
  FROM crypto_parts
  WHERE rest != ''
),
normalized_crypto AS (
  SELECT
    page_id,
    trim(value) AS raw_value
  FROM crypto_parts
  WHERE trim(value) != ''
)
INSERT OR IGNORE INTO page_crypto(page_id, asset_type, reference)
SELECT
  page_id,
  CASE
    WHEN instr(raw_value, ':') > 0 THEN lower(trim(substr(raw_value, 1, instr(raw_value, ':') - 1)))
    ELSE 'unknown'
  END,
  CASE
    WHEN instr(raw_value, ':') > 0 THEN trim(substr(raw_value, instr(raw_value, ':') + 1))
    ELSE raw_value
  END
FROM normalized_crypto
WHERE trim(raw_value) != '';

PRAGMA foreign_keys = ON;
