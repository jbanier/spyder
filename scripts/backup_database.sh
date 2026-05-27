#!/usr/bin/env bash
# Safe database backup script for Phase 3 preparation
# Creates a timestamped backup of the PostgreSQL database

set -euo pipefail

# Load database URL
source "$(dirname "$0")/../.env" 2>/dev/null || true
DATABASE_URL="${DATABASE_URL:-postgres://localhost/spyder}"

# Extract database name from URL
DB_NAME=$(echo "$DATABASE_URL" | sed -n 's|.*/\([^?]*\).*|\1|p')

# Backup directory
BACKUP_DIR="$(dirname "$0")/../backups"
mkdir -p "$BACKUP_DIR"

# Timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/spyder_backup_${TIMESTAMP}.dump"

echo "=== Spyder Database Backup ==="
echo ""
echo "Database: $DB_NAME"
echo "Backup file: $BACKUP_FILE"
echo ""
echo "Starting backup..."

# Create backup (compressed format)
if pg_dump -Fc "$DATABASE_URL" > "$BACKUP_FILE"; then
    BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
    echo "✅ Backup complete: $BACKUP_SIZE"
    echo ""

    # Verify backup
    echo "Verifying backup..."
    if pg_restore --list "$BACKUP_FILE" | head -20 > /dev/null; then
        echo "✅ Backup verified and valid"
        echo ""
        echo "Backup details:"
        echo "  • Location: $BACKUP_FILE"
        echo "  • Size: $BACKUP_SIZE"
        echo "  • Created: $(date)"
        echo ""
        echo "To restore:"
        echo "  pg_restore -d $DB_NAME -c \"$BACKUP_FILE\""
        echo ""
        echo "✅ Ready for Phase 3 database migrations"
    else
        echo "❌ Backup verification failed!"
        exit 1
    fi
else
    echo "❌ Backup failed!"
    exit 1
fi
