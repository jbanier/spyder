-- Create a materialized view for the relationship overview to avoid
-- expensive GROUP BY queries on the full page_link table.
--
-- STRATEGY: Create empty view first, then populate incrementally
-- This allows the migration to complete quickly and avoids long-running locks.
-- The view should be populated via a background job or manual refresh.

-- Step 1: Create the materialized view with no data
CREATE MATERIALIZED VIEW site_relationship_overview AS
SELECT
    pl.source_host,
    lower(pl.target_host) AS target_host,
    COUNT(*) AS reference_count
FROM page_link pl
WHERE pl.target_host != ''
  AND pl.source_host != ''
  AND pl.source_host != lower(pl.target_host)
GROUP BY pl.source_host, lower(pl.target_host)
WITH NO DATA;  -- Critical: don't populate during migration

-- Step 2: Create indexes on the empty view (fast since no data)
-- Unique index required for REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX idx_site_relationship_overview_unique
    ON site_relationship_overview(source_host, target_host);

-- Index for fast ORDER BY reference_count DESC queries
CREATE INDEX idx_site_relationship_overview_count
    ON site_relationship_overview(reference_count DESC, source_host, target_host);

-- Step 3: Add a comment with instructions
COMMENT ON MATERIALIZED VIEW site_relationship_overview IS
'Relationship overview materialized view. Populate with: REFRESH MATERIALIZED VIEW CONCURRENTLY site_relationship_overview;
This may take several hours on a large page_link table (576GB+). Run during low-traffic periods.
Recommended: Use pg_cron or a background job to refresh periodically.';

-- MANUAL STEP REQUIRED AFTER MIGRATION:
-- Run this command outside of the migration (in a separate session):
-- REFRESH MATERIALIZED VIEW CONCURRENTLY site_relationship_overview;
--
-- This will take several hours but won't block other operations due to CONCURRENTLY.
