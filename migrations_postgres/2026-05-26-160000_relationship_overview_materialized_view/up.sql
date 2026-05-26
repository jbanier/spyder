-- Create a materialized view for the relationship overview to avoid
-- expensive GROUP BY queries on the full page_link table.
-- This view can be refreshed periodically or after crawl operations.

CREATE MATERIALIZED VIEW site_relationship_overview AS
SELECT
    pl.source_host,
    lower(pl.target_host) AS target_host,
    COUNT(*) AS reference_count
FROM page_link pl
WHERE pl.target_host != ''
  AND pl.source_host != ''
  AND pl.source_host != lower(pl.target_host)
GROUP BY pl.source_host, lower(pl.target_host);

-- Unique index required for REFRESH MATERIALIZED VIEW CONCURRENTLY
-- This allows non-blocking refreshes
CREATE UNIQUE INDEX idx_site_relationship_overview_unique
    ON site_relationship_overview(source_host, target_host);

-- Index for fast ORDER BY reference_count DESC queries
CREATE INDEX idx_site_relationship_overview_count
    ON site_relationship_overview(reference_count DESC, source_host, target_host);
