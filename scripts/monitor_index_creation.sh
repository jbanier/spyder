#!/bin/bash
# Monitor CREATE INDEX CONCURRENTLY progress on page_link table

set -euo pipefail

DB_URL="${DATABASE_URL:-postgresql://spyder:spyder@localhost/spyder}"

echo "Monitoring index creation on page_link table..."
echo "Press Ctrl+C to stop monitoring"
echo ""

while true; do
    clear
    date
    echo ""

    # Check if index creation is still running
    psql "$DB_URL" -c "
    SELECT
        pid,
        now() - query_start as duration,
        state,
        wait_event_type,
        wait_event
    FROM pg_stat_activity
    WHERE query LIKE '%CREATE INDEX%page_link%target_url%'
        AND pid != pg_backend_pid();
    " 2>/dev/null || true

    echo ""

    # Show progress
    psql "$DB_URL" -c "
    SELECT
        phase,
        round(100.0 * blocks_done / NULLIF(blocks_total, 0), 2) as pct_complete,
        blocks_done,
        blocks_total,
        tuples_done,
        tuples_total,
        current_locker_pid
    FROM pg_stat_progress_create_index
    WHERE relid = 'page_link'::regclass;
    " 2>/dev/null

    RESULT=$?
    if [ $RESULT -ne 0 ]; then
        echo ""
        echo "Index creation query not found - checking if index exists..."
        psql "$DB_URL" -c "
        SELECT
            indexname,
            pg_size_pretty(pg_relation_size(indexname::regclass)) as size
        FROM pg_indexes
        WHERE tablename = 'page_link' AND indexname = 'idx_page_link_target_url';
        "

        if [ $? -eq 0 ]; then
            echo ""
            echo "✓ Index creation complete!"
            break
        fi
    fi

    sleep 5
done
