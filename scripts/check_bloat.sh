#!/bin/bash
# Check database tables for bloat
# Run weekly to monitor bloat levels

set -euo pipefail

DB_URL="${DATABASE_URL:-postgresql://spyder:spyder@localhost/spyder}"

# Color output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║           SPYDER DATABASE BLOAT CHECK                     ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Date: $(date)"
echo ""

# Top 20 tables by bloat ratio (bytes per row)
echo -e "${GREEN}=== Tables Ordered by Bloat (Bytes per Live Row) ===${NC}"
echo ""

psql "$DB_URL" -c "
SELECT
    relname as table_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||relname)) as table_size,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows,
    round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_pct,
    pg_size_pretty(
        CASE WHEN n_live_tup > 0
        THEN (pg_relation_size(schemaname||'.'||relname) / n_live_tup)::bigint
        ELSE 0 END
    ) as bytes_per_row,
    n_tup_upd as total_updates,
    CASE
        WHEN n_live_tup > 0
        THEN round(100.0 * n_tup_upd / n_live_tup, 1)
        ELSE 0
    END as update_churn_pct
FROM pg_stat_user_tables
WHERE n_live_tup > 100
ORDER BY
    CASE WHEN n_live_tup > 0
    THEN pg_relation_size(schemaname||'.'||relname)::numeric / n_live_tup
    ELSE 0 END DESC
LIMIT 20;
"

echo ""
echo -e "${GREEN}=== High Update Churn Tables ===${NC}"
echo ""

psql "$DB_URL" -c "
SELECT
    relname as table_name,
    n_live_tup as live_rows,
    n_tup_upd as total_updates,
    CASE
        WHEN n_live_tup > 0
        THEN round(100.0 * n_tup_upd / n_live_tup, 1)
        ELSE 0
    END as update_churn_pct,
    pg_size_pretty(pg_relation_size(schemaname||'.'||relname)) as table_size,
    last_autovacuum,
    autovacuum_count
FROM pg_stat_user_tables
WHERE n_live_tup > 100
    AND n_tup_upd::numeric / NULLIF(n_live_tup, 0) > 0.5  -- More than 50% churn
ORDER BY n_tup_upd::numeric / NULLIF(n_live_tup, 0) DESC
LIMIT 15;
"

echo ""
echo -e "${GREEN}=== Critical Tables Health Check ===${NC}"
echo ""

# Check specific tables that were bloated
psql "$DB_URL" -c "
WITH bloat_check AS (
    SELECT
        relname,
        pg_size_pretty(pg_relation_size(schemaname||'.'||relname)) as size,
        n_live_tup as rows,
        CASE WHEN n_live_tup > 0
        THEN pg_relation_size(schemaname||'.'||relname) / n_live_tup
        ELSE 0 END as bytes_per_row,
        pg_size_pretty(
            CASE WHEN n_live_tup > 0
            THEN (pg_relation_size(schemaname||'.'||relname) / n_live_tup)::bigint
            ELSE 0 END
        ) as bytes_per_row_pretty
    FROM pg_stat_user_tables
    WHERE relname IN (
        'page_link',
        'page_scan_link',
        'intel_lead_evidence',
        'host_http_observation',
        'page',
        'auto_blacklist_event'
    )
),
thresholds AS (
    SELECT
        'page_link' as table_name, 10240 as warn_threshold, 'Expected: ~2.5 KB per row' as note
    UNION ALL SELECT 'page_scan_link', 10240, 'Expected: ~2.5 KB per row'
    UNION ALL SELECT 'intel_lead_evidence', 5120, 'Expected: ~400 bytes per row'
    UNION ALL SELECT 'host_http_observation', 102400, 'Expected: ~60 KB per row (has many fields)'
    UNION ALL SELECT 'page', 102400, 'Expected: ~50 KB per row'
    UNION ALL SELECT 'auto_blacklist_event', 20480, 'Expected: ~10 KB per row'
)
SELECT
    bc.relname as table_name,
    bc.size,
    bc.rows,
    bc.bytes_per_row_pretty as bytes_per_row,
    CASE
        WHEN bc.bytes_per_row > t.warn_threshold THEN '⚠️  BLOATED'
        WHEN bc.bytes_per_row > t.warn_threshold * 0.7 THEN '⚡ WARNING'
        ELSE '✓ OK'
    END as status,
    t.note
FROM bloat_check bc
LEFT JOIN thresholds t ON bc.relname = t.table_name
ORDER BY bc.bytes_per_row DESC;
"

echo ""
echo -e "${GREEN}=== Database Size Summary ===${NC}"
echo ""

psql "$DB_URL" -c "
SELECT
    pg_size_pretty(pg_database_size('spyder')) as database_size,
    (SELECT pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||relname)))
     FROM pg_stat_user_tables) as total_tables_size,
    (SELECT pg_size_pretty(SUM(pg_relation_size(indexrelid)))
     FROM pg_stat_user_indexes) as total_indexes_size;
"

echo ""
echo -e "${YELLOW}=== Recommendations ===${NC}"
echo ""

# Check if any tables are severely bloated
BLOATED=$(psql "$DB_URL" -t -c "
SELECT COUNT(*)
FROM pg_stat_user_tables
WHERE n_live_tup > 100
    AND pg_relation_size(schemaname||'.'||relname)::numeric / NULLIF(n_live_tup, 0) > 10240;
")

if [ "$BLOATED" -gt 0 ]; then
    echo -e "${RED}⚠️  Found $BLOATED table(s) with bloat > 10 KB per row${NC}"
    echo ""
    echo "Consider running VACUUM FULL on bloated tables:"
    echo "  ./scripts/vacuum_full_database.sh"
    echo ""
else
    echo -e "${GREEN}✓ No severe bloat detected${NC}"
    echo ""
fi

# Check if autovacuum is running
echo "Last autovacuum times for critical tables:"
psql "$DB_URL" -c "
SELECT
    relname,
    last_autovacuum,
    autovacuum_count,
    CASE
        WHEN last_autovacuum IS NULL THEN '⚠️  Never vacuumed'
        WHEN now() - last_autovacuum > interval '7 days' THEN '⚠️  > 7 days ago'
        ELSE '✓ Recent'
    END as status
FROM pg_stat_user_tables
WHERE relname IN ('page_link', 'intel_lead_evidence', 'host_http_observation')
ORDER BY last_autovacuum DESC NULLS LAST;
"

echo ""
echo "Bloat check complete."
