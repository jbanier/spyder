#!/usr/bin/env bash
# Real-time monitoring dashboard for Spyder

set -euo pipefail

API_URL="http://localhost:8000/api/metrics"
REFRESH_SECONDS=2

clear
echo "=== Spyder Performance Dashboard ==="
echo "Press Ctrl+C to exit"
echo ""

while true; do
    # Move cursor to top
    tput cup 3 0

    # Fetch metrics
    METRICS=$(curl -s "$API_URL" 2>/dev/null || echo '{}')

    if [ "$METRICS" = "{}" ]; then
        echo "⚠️  Frontend not responding at $API_URL"
        echo "   Start with: ./scripts/start_spyder_stack.sh"
    else
        # Parse metrics
        UPTIME=$(echo "$METRICS" | jq -r '.uptime_seconds // 0')
        PAGES=$(echo "$METRICS" | jq -r '.pages_crawled // 0')
        FAILED=$(echo "$METRICS" | jq -r '.pages_failed // 0')
        LINKS=$(echo "$METRICS" | jq -r '.links_discovered // 0')

        DB_QUERIES=$(echo "$METRICS" | jq -r '.db_queries_executed // 0')
        AVG_QUERY=$(echo "$METRICS" | jq -r '.avg_db_query_ms // 0')

        CACHE_HITS=$(echo "$METRICS" | jq -r '.cache_hits // 0')
        CACHE_MISSES=$(echo "$METRICS" | jq -r '.cache_misses // 0')
        CACHE_RATE=$(echo "$METRICS" | jq -r '.cache_hit_rate // 0')

        WORK_PROC=$(echo "$METRICS" | jq -r '.work_units_processed // 0')
        WORK_SKIP=$(echo "$METRICS" | jq -r '.work_units_skipped // 0')

        # Display
        echo "┌─────────────────────────────────────────────────────────┐"
        echo "│ 📊 CRAWL STATISTICS                                     │"
        echo "├─────────────────────────────────────────────────────────┤"
        printf "│ Pages Crawled:       %-30s │\n" "$PAGES"
        printf "│ Pages Failed:        %-30s │\n" "$FAILED"
        printf "│ Links Discovered:    %-30s │\n" "$LINKS"
        printf "│ Work Units:          %-30s │\n" "$WORK_PROC processed, $WORK_SKIP skipped"
        echo "└─────────────────────────────────────────────────────────┘"
        echo ""
        echo "┌─────────────────────────────────────────────────────────┐"
        echo "│ 🗄️  DATABASE PERFORMANCE                                │"
        echo "├─────────────────────────────────────────────────────────┤"
        printf "│ Queries Executed:    %-30s │\n" "$DB_QUERIES"
        printf "│ Avg Query Time:      %-30s │\n" "${AVG_QUERY}ms"
        echo "└─────────────────────────────────────────────────────────┘"
        echo ""
        echo "┌─────────────────────────────────────────────────────────┐"
        echo "│ ⚡ CACHE PERFORMANCE                                     │"
        echo "├─────────────────────────────────────────────────────────┤"
        printf "│ Cache Hits:          %-30s │\n" "$CACHE_HITS"
        printf "│ Cache Misses:        %-30s │\n" "$CACHE_MISSES"
        printf "│ Hit Rate:            %-30s │\n" "${CACHE_RATE}%"
        echo "└─────────────────────────────────────────────────────────┘"
        echo ""
        printf "Uptime: %ds | Last update: %s\n" "$UPTIME" "$(date '+%H:%M:%S')"
    fi

    sleep "$REFRESH_SECONDS"
done
