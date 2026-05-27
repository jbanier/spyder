#!/bin/bash
# Refresh the site_relationship_overview materialized view
# This script should be run AFTER the migration completes
#
# WARNING: This may take several hours on a large database (576GB+ page_link table)
# The CONCURRENTLY option prevents blocking other operations but requires more resources
#
# Usage:
#   ./scripts/refresh_relationship_view.sh         # Full refresh (hours)
#   ./scripts/refresh_relationship_view.sh check   # Check current status

set -euo pipefail

# Database connection from .env or default
DATABASE_URL="${DATABASE_URL:-postgres://spyder:spyder@localhost/spyder}"

# Parse connection string
DB_USER=$(echo "$DATABASE_URL" | sed -n 's#.*://\([^:]*\):.*#\1#p')
DB_PASS=$(echo "$DATABASE_URL" | sed -n 's#.*://[^:]*:\([^@]*\)@.*#\1#p')
DB_HOST=$(echo "$DATABASE_URL" | sed -n 's#.*@\([^/]*\)/.*#\1#p')
DB_NAME=$(echo "$DATABASE_URL" | sed -n 's#.*/\([^?]*\).*#\1#p')

export PGPASSWORD="$DB_PASS"

check_status() {
    echo "Checking materialized view status..."
    psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" << 'SQL'
-- Check if view exists and has data
SELECT
    schemaname,
    matviewname,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size,
    CASE
        WHEN pg_total_relation_size(schemaname||'.'||matviewname) = 0
        THEN 'EMPTY - needs initial refresh'
        ELSE 'POPULATED'
    END as status
FROM pg_matviews
WHERE matviewname = 'site_relationship_overview';

-- Check page_link table size for context
SELECT
    'page_link' as table_name,
    pg_size_pretty(pg_total_relation_size('page_link')) as size,
    COUNT(*) as estimated_rows
FROM page_link;

-- Check for any running refresh operations
SELECT
    pid,
    now() - query_start as duration,
    state,
    wait_event_type,
    wait_event,
    LEFT(query, 80) as query
FROM pg_stat_activity
WHERE query LIKE '%site_relationship_overview%'
  AND state != 'idle'
  AND pid != pg_backend_pid();
SQL
}

refresh_view() {
    echo "========================================="
    echo "Starting materialized view refresh"
    echo "========================================="
    echo "Started at: $(date)"
    echo ""
    echo "This will take several hours on a large database."
    echo "Using CONCURRENTLY to avoid blocking other operations."
    echo ""
    echo "You can monitor progress with:"
    echo "  watch -n 30 './scripts/refresh_relationship_view.sh check'"
    echo ""
    echo "Or check PostgreSQL logs for progress."
    echo ""

    # Start the refresh
    psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" << 'SQL'
-- This will take hours but won't block reads/writes to other tables
REFRESH MATERIALIZED VIEW CONCURRENTLY site_relationship_overview;
SQL

    echo ""
    echo "========================================="
    echo "Refresh completed successfully!"
    echo "Completed at: $(date)"
    echo "========================================="

    # Show final stats
    check_status
}

case "${1:-refresh}" in
    check)
        check_status
        ;;
    refresh)
        check_status
        echo ""
        read -p "Start refresh? This will take several hours. (yes/no): " confirm
        if [[ "$confirm" == "yes" ]]; then
            refresh_view
        else
            echo "Refresh cancelled."
            exit 1
        fi
        ;;
    force)
        refresh_view
        ;;
    *)
        echo "Usage: $0 {refresh|check|force}"
        echo "  refresh - Interactive refresh with confirmation"
        echo "  check   - Check current status"
        echo "  force   - Force refresh without confirmation"
        exit 1
        ;;
esac
