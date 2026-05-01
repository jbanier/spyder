PRAGMA foreign_keys = OFF;

CREATE TEMP TABLE normalized_pages AS
WITH fragmentless AS (
  SELECT
    id AS old_page_id,
    CASE
      WHEN instr(url, '#') > 0 THEN substr(url, 1, instr(url, '#') - 1)
      ELSE url
    END AS without_fragment,
    title,
    language,
    last_scanned_at,
    created_at
  FROM page
),
authority_parts AS (
  SELECT
    old_page_id,
    title,
    language,
    last_scanned_at,
    created_at,
    without_fragment,
    CASE
      WHEN instr(without_fragment, '://') > 0 THEN substr(without_fragment, 1, instr(without_fragment, '://') + 2)
      ELSE ''
    END AS scheme_prefix,
    CASE
      WHEN instr(without_fragment, '://') > 0 THEN substr(without_fragment, instr(without_fragment, '://') + 3)
      ELSE ''
    END AS authority_source
  FROM fragmentless
)
SELECT
  old_page_id,
  CASE
    WHEN scheme_prefix != '' THEN
      scheme_prefix || substr(
        authority_source,
        1,
        min(
          CASE WHEN instr(authority_source, '/') = 0 THEN length(authority_source) + 1 ELSE instr(authority_source, '/') END,
          CASE WHEN instr(authority_source, '?') = 0 THEN length(authority_source) + 1 ELSE instr(authority_source, '?') END
        ) - 1
      )
    ELSE without_fragment
  END AS canonical_url,
  title,
  language,
  last_scanned_at,
  created_at
FROM authority_parts;

CREATE TEMP TABLE canonical_pages AS
SELECT
  canonical_url,
  MIN(old_page_id) AS canonical_page_id,
  MIN(created_at) AS created_at,
  MAX(last_scanned_at) AS last_scanned_at
FROM normalized_pages
GROUP BY canonical_url;

CREATE TEMP TABLE page_id_map AS
SELECT
  np.old_page_id,
  cp.canonical_page_id,
  cp.canonical_url
FROM normalized_pages np
JOIN canonical_pages cp ON cp.canonical_url = np.canonical_url;

CREATE TEMP TABLE normalized_work_units AS
WITH fragmentless AS (
  SELECT
    CASE
      WHEN instr(url, '#') > 0 THEN substr(url, 1, instr(url, '#') - 1)
      ELSE url
    END AS without_fragment,
    status,
    retry_count,
    next_attempt_at,
    last_attempt_at,
    last_error,
    created_at
  FROM work_unit
),
authority_parts AS (
  SELECT
    without_fragment,
    status,
    retry_count,
    next_attempt_at,
    last_attempt_at,
    last_error,
    created_at,
    CASE
      WHEN instr(without_fragment, '://') > 0 THEN substr(without_fragment, 1, instr(without_fragment, '://') + 2)
      ELSE ''
    END AS scheme_prefix,
    CASE
      WHEN instr(without_fragment, '://') > 0 THEN substr(without_fragment, instr(without_fragment, '://') + 3)
      ELSE ''
    END AS authority_source
  FROM fragmentless
)
SELECT
  CASE
    WHEN scheme_prefix != '' THEN
      scheme_prefix || substr(
        authority_source,
        1,
        min(
          CASE WHEN instr(authority_source, '/') = 0 THEN length(authority_source) + 1 ELSE instr(authority_source, '/') END,
          CASE WHEN instr(authority_source, '?') = 0 THEN length(authority_source) + 1 ELSE instr(authority_source, '?') END
        ) - 1
      )
    ELSE without_fragment
  END AS canonical_url,
  status,
  retry_count,
  next_attempt_at,
  last_attempt_at,
  last_error,
  created_at
FROM authority_parts;

CREATE TEMP TABLE normalized_page_links AS
WITH fragmentless AS (
  SELECT
    pim.canonical_page_id AS source_page_id,
    pl.created_at,
    CASE
      WHEN instr(pl.target_url, '#') > 0 THEN substr(pl.target_url, 1, instr(pl.target_url, '#') - 1)
      ELSE pl.target_url
    END AS without_fragment
  FROM page_link pl
  JOIN page_id_map pim ON pim.old_page_id = pl.source_page_id
),
authority_parts AS (
  SELECT
    source_page_id,
    created_at,
    without_fragment,
    CASE
      WHEN instr(without_fragment, '://') > 0 THEN substr(without_fragment, 1, instr(without_fragment, '://') + 2)
      ELSE ''
    END AS scheme_prefix,
    CASE
      WHEN instr(without_fragment, '://') > 0 THEN substr(without_fragment, instr(without_fragment, '://') + 3)
      ELSE ''
    END AS authority_source
  FROM fragmentless
),
canonical_links AS (
  SELECT
    source_page_id,
    created_at,
    CASE
      WHEN scheme_prefix != '' THEN
        scheme_prefix || substr(
          authority_source,
          1,
          min(
            CASE WHEN instr(authority_source, '/') = 0 THEN length(authority_source) + 1 ELSE instr(authority_source, '/') END,
            CASE WHEN instr(authority_source, '?') = 0 THEN length(authority_source) + 1 ELSE instr(authority_source, '?') END
          ) - 1
        )
      ELSE without_fragment
    END AS target_url
  FROM authority_parts
),
host_parts AS (
  SELECT
    source_page_id,
    created_at,
    target_url,
    CASE
      WHEN instr(target_url, '://') > 0 THEN substr(target_url, instr(target_url, '://') + 3)
      ELSE ''
    END AS authority_source
  FROM canonical_links
)
SELECT
  source_page_id,
  target_url,
  CASE
    WHEN authority_source = '' THEN ''
    WHEN instr(authority_source, '@') > 0 THEN
      CASE
        WHEN instr(substr(authority_source, instr(authority_source, '@') + 1), ':') > 0 THEN
          substr(
            substr(authority_source, instr(authority_source, '@') + 1),
            1,
            instr(substr(authority_source, instr(authority_source, '@') + 1), ':') - 1
          )
        ELSE substr(authority_source, instr(authority_source, '@') + 1)
      END
    WHEN instr(authority_source, ':') > 0 THEN substr(authority_source, 1, instr(authority_source, ':') - 1)
    ELSE authority_source
  END AS target_host,
  created_at
FROM host_parts;

CREATE TEMP TABLE normalized_page_scan_links AS
WITH fragmentless AS (
  SELECT
    scan_id,
    CASE
      WHEN instr(target_url, '#') > 0 THEN substr(target_url, 1, instr(target_url, '#') - 1)
      ELSE target_url
    END AS without_fragment
  FROM page_scan_link
),
authority_parts AS (
  SELECT
    scan_id,
    without_fragment,
    CASE
      WHEN instr(without_fragment, '://') > 0 THEN substr(without_fragment, 1, instr(without_fragment, '://') + 2)
      ELSE ''
    END AS scheme_prefix,
    CASE
      WHEN instr(without_fragment, '://') > 0 THEN substr(without_fragment, instr(without_fragment, '://') + 3)
      ELSE ''
    END AS authority_source
  FROM fragmentless
),
canonical_links AS (
  SELECT
    scan_id,
    CASE
      WHEN scheme_prefix != '' THEN
        scheme_prefix || substr(
          authority_source,
          1,
          min(
            CASE WHEN instr(authority_source, '/') = 0 THEN length(authority_source) + 1 ELSE instr(authority_source, '/') END,
            CASE WHEN instr(authority_source, '?') = 0 THEN length(authority_source) + 1 ELSE instr(authority_source, '?') END
          ) - 1
        )
      ELSE without_fragment
    END AS target_url
  FROM authority_parts
),
host_parts AS (
  SELECT
    scan_id,
    target_url,
    CASE
      WHEN instr(target_url, '://') > 0 THEN substr(target_url, instr(target_url, '://') + 3)
      ELSE ''
    END AS authority_source
  FROM canonical_links
)
SELECT
  scan_id,
  target_url,
  CASE
    WHEN authority_source = '' THEN ''
    WHEN instr(authority_source, '@') > 0 THEN
      CASE
        WHEN instr(substr(authority_source, instr(authority_source, '@') + 1), ':') > 0 THEN
          substr(
            substr(authority_source, instr(authority_source, '@') + 1),
            1,
            instr(substr(authority_source, instr(authority_source, '@') + 1), ':') - 1
          )
        ELSE substr(authority_source, instr(authority_source, '@') + 1)
      END
    WHEN instr(authority_source, ':') > 0 THEN substr(authority_source, 1, instr(authority_source, ':') - 1)
    ELSE authority_source
  END AS target_host
FROM host_parts;

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

CREATE TABLE page_classification_new(
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

CREATE TABLE page_scan_new(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  title VARCHAR NOT NULL,
  language VARCHAR NOT NULL DEFAULT '',
  scanned_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(page_id) REFERENCES page(id) ON DELETE CASCADE
);

CREATE TABLE page_scan_link_new(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  scan_id INTEGER NOT NULL,
  target_url VARCHAR NOT NULL,
  target_host VARCHAR NOT NULL DEFAULT '',
  FOREIGN KEY(scan_id) REFERENCES page_scan(id) ON DELETE CASCADE,
  UNIQUE(scan_id, target_url)
);

CREATE TABLE page_scan_email_new(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  scan_id INTEGER NOT NULL,
  email VARCHAR NOT NULL,
  FOREIGN KEY(scan_id) REFERENCES page_scan(id) ON DELETE CASCADE,
  UNIQUE(scan_id, email)
);

CREATE TABLE page_scan_crypto_new(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  scan_id INTEGER NOT NULL,
  asset_type VARCHAR NOT NULL,
  reference VARCHAR NOT NULL,
  FOREIGN KEY(scan_id) REFERENCES page_scan(id) ON DELETE CASCADE,
  UNIQUE(scan_id, asset_type, reference)
);

CREATE TABLE page_link_new(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  source_page_id INTEGER NOT NULL,
  target_url VARCHAR NOT NULL,
  target_host VARCHAR NOT NULL DEFAULT '',
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(source_page_id) REFERENCES page(id) ON DELETE CASCADE,
  UNIQUE(source_page_id, target_url)
);

CREATE TABLE page_email_new(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  email VARCHAR NOT NULL,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(page_id) REFERENCES page(id) ON DELETE CASCADE,
  UNIQUE(page_id, email)
);

CREATE TABLE page_crypto_new(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  asset_type VARCHAR NOT NULL,
  reference VARCHAR NOT NULL,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(page_id) REFERENCES page(id) ON DELETE CASCADE,
  UNIQUE(page_id, asset_type, reference)
);

CREATE TABLE site_profile_new(
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

INSERT INTO work_unit_new (
  url,
  status,
  retry_count,
  next_attempt_at,
  last_attempt_at,
  last_error,
  created_at
)
SELECT
  canonical_url,
  CASE
    WHEN SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) > 0 THEN 'done'
    WHEN SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) > 0 THEN 'pending'
    ELSE 'failed'
  END AS status,
  MAX(retry_count) AS retry_count,
  COALESCE(MIN(next_attempt_at), MIN(created_at)) AS next_attempt_at,
  MAX(last_attempt_at) AS last_attempt_at,
  MAX(last_error) AS last_error,
  MIN(created_at) AS created_at
FROM normalized_work_units
GROUP BY canonical_url;

INSERT INTO page_new (
  id,
  title,
  url,
  links,
  emails,
  coins,
  language,
  last_scanned_at,
  created_at
)
SELECT
  cp.canonical_page_id,
  COALESCE((
    SELECT np.title
    FROM normalized_pages np
    WHERE np.canonical_url = cp.canonical_url
    ORDER BY np.last_scanned_at DESC, np.old_page_id DESC
    LIMIT 1
  ), 'no title'),
  cp.canonical_url,
  '',
  '',
  '',
  COALESCE((
    SELECT np.language
    FROM normalized_pages np
    WHERE np.canonical_url = cp.canonical_url
    ORDER BY np.last_scanned_at DESC, np.old_page_id DESC
    LIMIT 1
  ), ''),
  cp.last_scanned_at,
  cp.created_at
FROM canonical_pages cp;

INSERT INTO page_scan_new (id, page_id, title, language, scanned_at)
SELECT
  ps.id,
  pim.canonical_page_id,
  ps.title,
  ps.language,
  ps.scanned_at
FROM page_scan ps
JOIN page_id_map pim ON pim.old_page_id = ps.page_id;

INSERT INTO page_scan_link_new (scan_id, target_url, target_host)
SELECT DISTINCT
  scan_id,
  target_url,
  target_host
FROM normalized_page_scan_links;

INSERT OR IGNORE INTO page_scan_email_new (scan_id, email)
SELECT scan_id, email
FROM page_scan_email;

INSERT OR IGNORE INTO page_scan_crypto_new (scan_id, asset_type, reference)
SELECT scan_id, asset_type, reference
FROM page_scan_crypto;

INSERT INTO page_link_new (source_page_id, target_url, target_host, created_at)
SELECT
  source_page_id,
  target_url,
  target_host,
  MIN(created_at) AS created_at
FROM normalized_page_links
GROUP BY source_page_id, target_url, target_host;

INSERT INTO page_email_new (page_id, email, created_at)
SELECT
  pim.canonical_page_id,
  lower(pe.email) AS email,
  MIN(pe.created_at) AS created_at
FROM page_email pe
JOIN page_id_map pim ON pim.old_page_id = pe.page_id
GROUP BY pim.canonical_page_id, lower(pe.email);

INSERT INTO page_crypto_new (page_id, asset_type, reference, created_at)
SELECT
  pim.canonical_page_id,
  pc.asset_type,
  pc.reference,
  MIN(pc.created_at) AS created_at
FROM page_crypto pc
JOIN page_id_map pim ON pim.old_page_id = pc.page_id
GROUP BY pim.canonical_page_id, pc.asset_type, pc.reference;

INSERT INTO page_classification_new (
  page_id,
  host,
  category,
  confidence,
  score,
  evidence,
  last_classified_at
)
WITH ranked_classifications AS (
  SELECT
    pim.canonical_page_id AS page_id,
    pc.host,
    pc.category,
    pc.confidence,
    pc.score,
    pc.evidence,
    pc.last_classified_at,
    ROW_NUMBER() OVER (
      PARTITION BY pim.canonical_page_id
      ORDER BY
        CASE
          WHEN EXISTS (
            SELECT 1
            FROM site_profile sp
            WHERE sp.host = pc.host
              AND sp.source_page_id = pc.page_id
          ) THEN 0
          ELSE 1
        END,
        pc.score DESC,
        pc.last_classified_at DESC,
        pc.id DESC
    ) AS rank_order
  FROM page_classification pc
  JOIN page_id_map pim ON pim.old_page_id = pc.page_id
)
SELECT
  page_id,
  host,
  category,
  confidence,
  score,
  evidence,
  last_classified_at
FROM ranked_classifications
WHERE rank_order = 1;

INSERT INTO site_profile_new (
  host,
  category,
  confidence,
  score,
  page_count,
  evidence,
  source_page_id,
  last_classified_at,
  created_at
)
SELECT
  sp.host,
  sp.category,
  sp.confidence,
  sp.score,
  COALESCE((
    SELECT COUNT(*)
    FROM page_classification_new pcn
    WHERE pcn.host = sp.host
  ), 0) AS page_count,
  sp.evidence,
  (
    SELECT pim.canonical_page_id
    FROM page_id_map pim
    WHERE pim.old_page_id = sp.source_page_id
    LIMIT 1
  ) AS source_page_id,
  sp.last_classified_at,
  sp.created_at
FROM site_profile sp;

INSERT OR IGNORE INTO site_profile_new (
  host,
  category,
  confidence,
  score,
  page_count,
  evidence,
  source_page_id,
  last_classified_at,
  created_at
)
SELECT
  pcn.host,
  pcn.category,
  pcn.confidence,
  pcn.score,
  (
    SELECT COUNT(*)
    FROM page_classification_new pcn2
    WHERE pcn2.host = pcn.host
  ) AS page_count,
  pcn.evidence,
  pcn.page_id,
  pcn.last_classified_at,
  COALESCE((
    SELECT p.created_at
    FROM page_new p
    WHERE p.id = pcn.page_id
  ), CURRENT_TIMESTAMP)
FROM page_classification_new pcn;

DROP TABLE page_scan_link;
DROP TABLE page_scan_email;
DROP TABLE page_scan_crypto;
DROP TABLE page_scan;
DROP TABLE page_link;
DROP TABLE page_email;
DROP TABLE page_crypto;
DROP TABLE page_classification;
DROP TABLE site_profile;
DROP TABLE page;
DROP TABLE work_unit;

ALTER TABLE work_unit_new RENAME TO work_unit;
ALTER TABLE page_new RENAME TO page;
ALTER TABLE page_classification_new RENAME TO page_classification;
ALTER TABLE page_scan_new RENAME TO page_scan;
ALTER TABLE page_scan_link_new RENAME TO page_scan_link;
ALTER TABLE page_scan_email_new RENAME TO page_scan_email;
ALTER TABLE page_scan_crypto_new RENAME TO page_scan_crypto;
ALTER TABLE page_link_new RENAME TO page_link;
ALTER TABLE page_email_new RENAME TO page_email;
ALTER TABLE page_crypto_new RENAME TO page_crypto;
ALTER TABLE site_profile_new RENAME TO site_profile;

CREATE INDEX idx_work_unit_status_next_attempt_at ON work_unit(status, next_attempt_at);
CREATE INDEX idx_page_classification_host ON page_classification(host);
CREATE INDEX idx_page_classification_category ON page_classification(category);
CREATE INDEX idx_page_scan_page_id_scanned_at ON page_scan(page_id, scanned_at, id);
CREATE INDEX idx_page_scan_link_scan_id ON page_scan_link(scan_id);
CREATE INDEX idx_page_scan_link_target_url ON page_scan_link(target_url);
CREATE INDEX idx_page_scan_email_scan_id ON page_scan_email(scan_id);
CREATE INDEX idx_page_scan_email_email ON page_scan_email(email);
CREATE INDEX idx_page_scan_crypto_scan_id ON page_scan_crypto(scan_id);
CREATE INDEX idx_page_scan_crypto_reference ON page_scan_crypto(reference);
CREATE INDEX idx_page_link_source_page_id ON page_link(source_page_id);
CREATE INDEX idx_page_link_target_url ON page_link(target_url);
CREATE INDEX idx_page_link_target_host ON page_link(target_host);
CREATE INDEX idx_page_email_page_id ON page_email(page_id);
CREATE INDEX idx_page_email_email ON page_email(email);
CREATE INDEX idx_page_crypto_page_id ON page_crypto(page_id);
CREATE INDEX idx_page_crypto_reference ON page_crypto(reference);
CREATE INDEX idx_page_crypto_asset_type ON page_crypto(asset_type);
CREATE INDEX idx_site_profile_category ON site_profile(category);
CREATE INDEX idx_site_profile_host ON site_profile(host);

UPDATE page
SET
  links = COALESCE((
    SELECT group_concat(target_url, ',')
    FROM (
      SELECT target_url
      FROM page_link
      WHERE source_page_id = page.id
      ORDER BY target_url
    ) ordered_links
  ), ''),
  emails = COALESCE((
    SELECT group_concat(email, ',')
    FROM (
      SELECT email
      FROM page_email
      WHERE page_id = page.id
      ORDER BY email
    ) ordered_emails
  ), ''),
  coins = COALESCE((
    SELECT group_concat(asset_type || ':' || reference, ',')
    FROM (
      SELECT asset_type, reference
      FROM page_crypto
      WHERE page_id = page.id
      ORDER BY asset_type, reference
    ) ordered_crypto
  ), '');

PRAGMA foreign_keys = ON;
