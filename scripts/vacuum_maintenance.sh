#!/bin/bash
# Maintenance script to reclaim space from bloated page_link table
#
# WARNING: This will lock the page_link table during VACUUM FULL
# Estimated downtime: 30-60 minutes for 183 GB table
#
# Run this during a maintenance window when crawler and frontend are stopped

set -euo pipefail

DB_URL="${DATABASE_URL:-postgresql://spyder:spyder@localhost/spyder}"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Page Link Table Maintenance ===${NC}"
echo ""
echo "This script will:"
echo "  1. Show current table bloat"
echo "  2. Run VACUUM FULL on page_link (locks table!)"
echo "  3. Reindex bloated indexes"
echo "  4. Show space reclaimed"
echo ""
echo -e "${RED}WARNING: This requires stopping the crawler and frontend!${NC}"
echo ""

# Check if services are running
if pgrep -f "target/release/frontend" > /dev/null; then
    echo -e "${RED}ERROR: Frontend is still running!${NC}"
    echo "Stop it with: pkill -f 'target/release/frontend'"
    exit 1
fi

if pgrep -f "target/release/spyder" | grep -v "$$" > /dev/null; then
    echo -e "${YELLOW}WARNING: Spyder processes are still running${NC}"
    pgrep -af "target/release/spyder"
    echo ""
    read -p "Continue anyway? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        exit 1
    fi
fi

# Show before stats
echo ""
echo -e "${GREEN}=== Before Maintenance ===${NC}"
psql "$DB_URL" -c "
SELECT
    'page_link' as table_name,
    pg_size_pretty(pg_total_relation_size('page_link')) as total_size,
    pg_size_pretty(pg_relation_size('page_link')) as table_size,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows,
    round(100.0 * pg_relation_size('page_link') / NULLIF(n_live_tup, 0), 2) as bytes_per_row
FROM pg_stat_user_tables
WHERE relname = 'page_link';
"

echo ""
psql "$DB_URL" -c "
SELECT
    indexrelname as index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes
WHERE relname = 'page_link'
ORDER BY pg_relation_size(indexrelid) DESC;
"

echo ""
read -p "Proceed with VACUUM FULL? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

# Run VACUUM FULL
echo ""
echo -e "${GREEN}=== Running VACUUM FULL (this will take a while...) ===${NC}"
START_TIME=$(date +%s)
psql "$DB_URL" -c "VACUUM FULL VERBOSE page_link;"
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo ""
echo -e "${GREEN}VACUUM FULL completed in ${DURATION} seconds ($((DURATION / 60)) minutes)${NC}"

# Reindex bloated indexes
echo ""
echo -e "${GREEN}=== Reindexing bloated indexes ===${NC}"
echo "This will rebuild indexes concurrently to avoid locking..."

# The relationship indexes are huge and barely used - consider dropping them
echo ""
echo -e "${YELLOW}Note: The relationship indexes are 44 GB each but rarely used.${NC}"
echo "Consider dropping them if you don't need the relationship graph feature:"
echo "  DROP INDEX IF EXISTS idx_page_link_relationship_source_target;"
echo "  DROP INDEX IF EXISTS idx_page_link_relationship_target_source;"
echo ""

read -p "Reindex all indexes? (yes/no): " confirm
if [ "$confirm" = "yes" ]; then
    psql "$DB_URL" -c "REINDEX TABLE CONCURRENTLY page_link;"
    echo -e "${GREEN}Reindex complete${NC}"
fi

# Show after stats
echo ""
echo -e "${GREEN}=== After Maintenance ===${NC}"
psql "$DB_URL" -c "
SELECT
    'page_link' as table_name,
    pg_size_pretty(pg_total_relation_size('page_link')) as total_size,
    pg_size_pretty(pg_relation_size('page_link')) as table_size,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows,
    round(100.0 * pg_relation_size('page_link') / NULLIF(n_live_tup, 0), 2) as bytes_per_row
FROM pg_stat_user_tables
WHERE relname = 'page_link';
"

echo ""
psql "$DB_URL" -c "
SELECT
    indexrelname as index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes
WHERE relname = 'page_link'
ORDER BY pg_relation_size(indexrelid) DESC;
"

echo ""
echo -e "${GREEN}=== Maintenance Complete ===${NC}"
echo "You can now restart the crawler and frontend."
