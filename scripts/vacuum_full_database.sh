#!/bin/bash
# Full database VACUUM to reclaim ~285 GB of bloated space
#
# WARNING: This will take 2-4 hours and locks tables during VACUUM FULL
# Only run during a scheduled maintenance window with all services stopped

set -euo pipefail

DB_URL="${DATABASE_URL:-postgresql://spyder:spyder@localhost/spyder}"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         SPYDER DATABASE FULL VACUUM MAINTENANCE           ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Show what will happen
echo "This script will:"
echo "  1. Check services are stopped"
echo "  2. Show current database bloat (~285 GB wasted)"
echo "  3. Run VACUUM FULL on entire database (2-4 hours)"
echo "  4. Reindex all tables"
echo "  5. Show space reclaimed"
echo ""
echo -e "${RED}ESTIMATED DOWNTIME: 2-4 hours${NC}"
echo -e "${RED}EXPECTED SPACE RECLAIMED: ~285 GB${NC}"
echo ""

# Safety check - ensure services are stopped
echo -e "${YELLOW}=== Checking Services ===${NC}"
SERVICES_RUNNING=0

if pgrep -f "target/release/frontend" > /dev/null; then
    echo -e "${RED}✗ Frontend is running (PID: $(pgrep -f 'target/release/frontend'))${NC}"
    SERVICES_RUNNING=1
fi

if pgrep -f "target/release/spyder" | grep -v "$$" | grep -v "grep" > /dev/null; then
    echo -e "${RED}✗ Spyder processes are running:${NC}"
    pgrep -af "target/release/spyder" | grep -v "$$" | grep -v "grep"
    SERVICES_RUNNING=1
fi

if [ $SERVICES_RUNNING -eq 1 ]; then
    echo ""
    echo -e "${RED}ERROR: Services are still running!${NC}"
    echo "Stop all services before running VACUUM FULL:"
    echo "  pkill -f 'target/release/frontend'"
    echo "  pkill -f 'target/release/spyder'"
    echo ""
    read -p "Force continue anyway? (type 'FORCE' to proceed): " confirm
    if [ "$confirm" != "FORCE" ]; then
        exit 1
    fi
    echo -e "${YELLOW}WARNING: Proceeding with services running - expect conflicts!${NC}"
else
    echo -e "${GREEN}✓ All services stopped${NC}"
fi

# Show before stats
echo ""
echo -e "${GREEN}=== Database State Before Maintenance ===${NC}"
echo ""

echo "Top 15 bloated tables:"
psql "$DB_URL" -c "
SELECT
    schemaname || '.' || relname as table_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||relname)) as table_size,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows,
    pg_size_pretty(
        CASE
            WHEN n_live_tup > 0
            THEN (pg_relation_size(schemaname||'.'||relname)::numeric / n_live_tup)::bigint
            ELSE 0
        END
    ) as size_per_row
FROM pg_stat_user_tables
WHERE n_live_tup > 100
ORDER BY
    CASE
        WHEN n_live_tup > 0
        THEN pg_relation_size(schemaname||'.'||relname)::numeric / n_live_tup
        ELSE 0
    END DESC
LIMIT 15;
"

echo ""
echo "Database size summary:"
psql "$DB_URL" -c "
SELECT
    pg_size_pretty(pg_database_size('spyder')) as total_database_size;
"

echo ""
echo -e "${YELLOW}Expected results after VACUUM FULL:${NC}"
echo "  • Database size: ~285 GB → ~4-5 GB (98% reduction)"
echo "  • Query performance: 100-1000x faster"
echo "  • Disk space freed: ~280+ GB"
echo ""

read -p "Proceed with VACUUM FULL? (type 'yes' to confirm): " confirm
if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

# Record start time
OVERALL_START=$(date +%s)

# Run VACUUM FULL on entire database
echo ""
echo -e "${GREEN}=== Running VACUUM FULL on entire database ===${NC}"
echo "Started at: $(date)"
echo "This will take 2-4 hours..."
echo ""

START_TIME=$(date +%s)
psql "$DB_URL" -c "VACUUM FULL VERBOSE;" 2>&1 | tee /tmp/spyder-vacuum-full.log
END_TIME=$(date +%s)
VACUUM_DURATION=$((END_TIME - START_TIME))

echo ""
echo -e "${GREEN}✓ VACUUM FULL completed in ${VACUUM_DURATION} seconds ($((VACUUM_DURATION / 60)) minutes)${NC}"
echo "Full log saved to: /tmp/spyder-vacuum-full.log"

# Reindex database
echo ""
echo -e "${GREEN}=== Reindexing database ===${NC}"
echo "This will rebuild all indexes to remove bloat..."
echo ""

START_TIME=$(date +%s)
psql "$DB_URL" -c "REINDEX DATABASE spyder;" 2>&1 | tee /tmp/spyder-reindex.log
END_TIME=$(date +%s)
REINDEX_DURATION=$((END_TIME - START_TIME))

echo ""
echo -e "${GREEN}✓ REINDEX completed in ${REINDEX_DURATION} seconds ($((REINDEX_DURATION / 60)) minutes)${NC}"
echo "Full log saved to: /tmp/spyder-reindex.log"

# Analyze database for fresh statistics
echo ""
echo -e "${GREEN}=== Analyzing database ===${NC}"
psql "$DB_URL" -c "ANALYZE;"
echo -e "${GREEN}✓ ANALYZE complete${NC}"

# Show after stats
echo ""
echo -e "${GREEN}=== Database State After Maintenance ===${NC}"
echo ""

echo "Top 15 tables by size:"
psql "$DB_URL" -c "
SELECT
    schemaname || '.' || relname as table_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||relname)) as table_size,
    n_live_tup as live_rows,
    pg_size_pretty(
        CASE
            WHEN n_live_tup > 0
            THEN (pg_relation_size(schemaname||'.'||relname)::numeric / n_live_tup)::bigint
            ELSE 0
        END
    ) as size_per_row
FROM pg_stat_user_tables
WHERE n_live_tup > 100
ORDER BY
    CASE
        WHEN n_live_tup > 0
        THEN pg_relation_size(schemaname||'.'||relname)::numeric / n_live_tup
        ELSE 0
    END DESC
LIMIT 15;
"

echo ""
echo "Database size summary:"
psql "$DB_URL" -c "
SELECT
    pg_size_pretty(pg_database_size('spyder')) as total_database_size;
"

# Calculate total time
OVERALL_END=$(date +%s)
TOTAL_DURATION=$((OVERALL_END - OVERALL_START))

echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║              MAINTENANCE COMPLETE!                         ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Total time: ${TOTAL_DURATION} seconds ($((TOTAL_DURATION / 60)) minutes)"
echo "  • VACUUM FULL: $((VACUUM_DURATION / 60)) minutes"
echo "  • REINDEX: $((REINDEX_DURATION / 60)) minutes"
echo ""
echo -e "${GREEN}You can now restart the services:${NC}"
echo "  ./scripts/start_spyder_stack.sh"
echo ""
echo "Logs saved to:"
echo "  /tmp/spyder-vacuum-full.log"
echo "  /tmp/spyder-reindex.log"
