#!/usr/bin/env bash
# Create test database for migration testing
# Run with: sudo -u postgres ./scripts/create_test_db.sh

set -euo pipefail

TEST_DB_NAME="spyder_migration_test"

echo "Creating test database: $TEST_DB_NAME"

# Drop if exists
dropdb --if-exists "$TEST_DB_NAME"

# Create database
createdb "$TEST_DB_NAME"

# Grant all privileges to spyder user
psql -c "GRANT ALL PRIVILEGES ON DATABASE $TEST_DB_NAME TO spyder;"
psql -d "$TEST_DB_NAME" -c "GRANT ALL ON SCHEMA public TO spyder;"
psql -d "$TEST_DB_NAME" -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO spyder;"
psql -d "$TEST_DB_NAME" -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO spyder;"

echo "✅ Test database created and permissions granted to spyder user"
echo ""
echo "You can now run: ./scripts/test_and_apply_migrations.sh"
