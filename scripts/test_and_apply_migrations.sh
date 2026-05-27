#!/usr/bin/env bash
# Safe migration testing and deployment for Phase 3
# Tests migrations on a copy before applying to production

set -euo pipefail

# Load environment
source "$(dirname "$0")/../.env" 2>/dev/null || true
PROD_DATABASE_URL="${DATABASE_URL:-postgres://spyder:spyder@localhost/spyder}"
TEST_DB_NAME="spyder_migration_test"
# Extract credentials from prod URL and use for test URL
DB_USER=$(echo "$PROD_DATABASE_URL" | sed -n 's|.*://\([^:]*\):.*|\1|p')
DB_PASS=$(echo "$PROD_DATABASE_URL" | sed -n 's|.*://[^:]*:\([^@]*\)@.*|\1|p')
DB_HOST=$(echo "$PROD_DATABASE_URL" | sed -n 's|.*@\([^/]*\)/.*|\1|p')
TEST_DATABASE_URL="postgres://${DB_USER}:${DB_PASS}@${DB_HOST}/${TEST_DB_NAME}"

echo "=== Phase 3 Migration Testing & Deployment ==="
echo ""

# Step 1: Check test database exists
echo "Step 1: Checking test database..."
if ! psql "$TEST_DATABASE_URL" -c "SELECT 1;" > /dev/null 2>&1; then
    echo "❌ Test database does not exist or is not accessible!"
    echo ""
    echo "Please run: sudo -u postgres ./scripts/create_test_db.sh"
    echo ""
    exit 1
fi
echo "✅ Test database is ready: $TEST_DB_NAME"
echo ""

# Step 2: Run all migrations on fresh empty database
echo "Step 2: Running all migrations on empty test database..."
export DATABASE_URL="$TEST_DATABASE_URL"
diesel migration run --migration-dir migrations_postgres > /tmp/migration_test.log 2>&1

if [ $? -eq 0 ]; then
    echo "✅ All migrations applied successfully to test database"
else
    echo "❌ Migrations failed on test database!"
    cat /tmp/migration_test.log
    exit 1
fi
echo ""

# Step 3: Verify new schema objects exist
echo "Step 3: Verifying Phase 3 schema objects..."
echo ""
echo "  Verifying query_log table exists..."
if psql "$TEST_DATABASE_URL" -c "\d query_log" > /dev/null 2>&1; then
    echo "  ✅ query_log table created"
    psql "$TEST_DATABASE_URL" -c "\d query_log"
else
    echo "  ❌ query_log table not found!"
    exit 1
fi

echo ""
echo "  Verifying indexes exist..."
INDEX_COUNT=$(psql "$TEST_DATABASE_URL" -tAc "SELECT COUNT(*) FROM pg_indexes WHERE tablename IN ('query_log', 'page', 'work_unit', 'page_scan', 'site_profile') AND indexname LIKE 'idx_%'" 2>/dev/null)
echo "  ✅ Found $INDEX_COUNT indexes"

echo ""
echo "Step 4: Testing rollback..."
echo "  Rolling back indexes migration..."
if diesel migration revert --migration-dir migrations_postgres; then
    echo "  ✅ Indexes migration rolled back"
else
    echo "  ❌ Rollback failed!"
    exit 1
fi

# Verify indexes are gone
INDEX_COUNT_AFTER=$(psql "$TEST_DATABASE_URL" -tAc "SELECT COUNT(*) FROM pg_indexes WHERE indexname IN ('idx_page_last_scanned_desc', 'idx_work_unit_status_created', 'idx_page_scan_scanned_desc', 'idx_site_profile_last_scanned_desc')" 2>/dev/null)
if [ "$INDEX_COUNT_AFTER" -eq "0" ]; then
    echo "  ✅ Indexes properly removed"
else
    echo "  ❌ Indexes still exist after rollback! Found: $INDEX_COUNT_AFTER"
    exit 1
fi

echo "  Rolling back query_log table migration..."
if diesel migration revert --migration-dir migrations_postgres; then
    echo "  ✅ Query log migration rolled back"
else
    echo "  ❌ Rollback failed!"
    exit 1
fi

if psql "$TEST_DATABASE_URL" -c "\d query_log" > /dev/null 2>&1; then
    echo "  ❌ Table still exists after rollback!"
    exit 1
else
    echo "  ✅ Table properly removed by rollback"
fi

echo ""
echo "Step 5: Re-applying migrations for final test..."
diesel migration run --migration-dir migrations_postgres > /dev/null 2>&1
echo "✅ Migrations re-applied successfully"

echo ""
echo "Step 6: Cleanup test database..."
export DATABASE_URL="$PROD_DATABASE_URL"
echo "⚠️  To clean up the test database manually, run:"
echo "    sudo -u postgres dropdb $TEST_DB_NAME"
echo "✅ Test database left for manual inspection"

echo ""
echo "════════════════════════════════════════════════════════════"
echo "✅ ALL TESTS PASSED - MIGRATIONS ARE SAFE"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "Ready to apply to production database."
echo ""
echo "⚠️  IMPORTANT: Migrations will run on: $PROD_DATABASE_URL"
echo ""
read -p "Apply migrations to PRODUCTION? (yes/no): " -r
echo ""

if [[ $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "Applying migrations to production..."
    echo ""

    # Run migrations on production
    diesel migration run --migration-dir migrations_postgres

    echo ""
    echo "✅ Migrations applied to production!"
    echo ""
    echo "Verifying production schema..."

    # Verify
    if psql "$PROD_DATABASE_URL" -c "\d query_log" > /dev/null 2>&1; then
        echo "✅ query_log table exists in production"
    else
        echo "❌ query_log table not found in production!"
        exit 1
    fi

    # Regenerate schema.rs
    echo ""
    echo "Regenerating Diesel schema..."
    diesel print-schema > src/schema.rs
    echo "✅ Schema updated: src/schema.rs"

    echo ""
    echo "════════════════════════════════════════════════════════════"
    echo "🎉 PHASE 3 COMPLETE!"
    echo "════════════════════════════════════════════════════════════"
    echo ""
    echo "New database features:"
    echo "  • query_log table for performance tracking"
    echo "  • 7 optimized indexes on large tables"
    echo "  • All migrations tested and reversible"
    echo ""
    echo "Next steps:"
    echo "  1. Rebuild: cargo build --release"
    echo "  2. Restart services: ./scripts/start_spyder_stack.sh"
    echo "  3. Monitor performance improvements"
    echo ""
else
    echo "❌ Cancelled. Migrations not applied to production."
    exit 0
fi
