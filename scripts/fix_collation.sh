#!/usr/bin/env bash
# Fix PostgreSQL collation version mismatch
# Run with: sudo -u postgres ./scripts/fix_collation.sh

set -euo pipefail

echo "Fixing collation version mismatch..."
echo ""

# Refresh collation version for template databases
psql -d template1 -c "ALTER DATABASE template1 REFRESH COLLATION VERSION;"
psql -d postgres -c "ALTER DATABASE postgres REFRESH COLLATION VERSION;" 2>/dev/null || true

# Also refresh for the main spyder database
psql -d spyder -c "ALTER DATABASE spyder REFRESH COLLATION VERSION;" 2>/dev/null || true

echo ""
echo "✅ Collation versions refreshed"
echo ""
echo "You can now run: sudo -u postgres ./scripts/create_test_db.sh"
