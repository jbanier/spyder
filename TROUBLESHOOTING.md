# Troubleshooting Guide

## Discovery Explorer Issues

### HTTP 500 Error on /discovery

**Symptom:** Discovery page returns HTTP 500 error

**Common Causes:**

1. **Migrations not run**
   ```bash
   # Check if tables exist
   psql $DATABASE_URL -c "\dt url_discovery"
   
   # Run migrations if missing
   diesel migration run
   ```

2. **No records yet (FIXED)**
   - Empty tables are fine - should show empty state
   - Previously caused template errors with Handlebars helpers
   - Fixed in commit bee3893

3. **Template rendering errors**
   - Check frontend logs: `cargo run --bin frontend`
   - Look for Handlebars errors about missing helpers
   - Our templates now use only basic conditionals (no custom helpers)

### Discovery Page Shows "No discoveries found"

**This is normal if:**
- No URLs have been imported yet
- Filters are excluding all records

**To populate:**
```bash
# Import some URLs
echo "http://test.onion" > test.txt
cargo run --bin spyder -- import test.txt --source-type custom --source-name "Test"

# Check they were created
psql $DATABASE_URL -c "SELECT COUNT(*) FROM url_discovery;"
```

## Failures Dashboard Issues

### HTTP 500 Error on /queue/failures

**Symptom:** Failures page returns HTTP 500 error

**Common Causes:**

1. **Materialized view not created**
   ```bash
   # Check if view exists
   psql $DATABASE_URL -c "\d work_unit_failure_summary"
   
   # Run migrations if missing
   diesel migration run
   ```

2. **View needs refresh**
   ```bash
   # Refresh the materialized view
   psql $DATABASE_URL -c "REFRESH MATERIALIZED VIEW CONCURRENTLY work_unit_failure_summary;"
   ```

### Failures Dashboard Shows "No failures"

**This is good!** It means all work units are either:
- Pending (not yet crawled)
- Completed successfully
- Not in a failed state

**To test with failures:**
```bash
# Manually create a test failure
psql $DATABASE_URL -c "
  INSERT INTO work_unit (url, status, failure_category, last_error)
  VALUES ('http://test-fail.onion', 'failed', 'http_404_not_found', 'Not Found');
"

# Refresh view
psql $DATABASE_URL -c "REFRESH MATERIALIZED VIEW CONCURRENTLY work_unit_failure_summary;"
```

## API Endpoint Issues

### GET /api/failures/summary Returns Empty

**Check:**
```bash
# Query the view directly
psql $DATABASE_URL -c "SELECT * FROM work_unit_failure_summary;"

# If empty, check for failed work units
psql $DATABASE_URL -c "SELECT COUNT(*) FROM work_unit WHERE status = 'failed';"
```

### POST /api/queue/bulk-retry Returns 400

**Possible causes:**
- Invalid action (must be "retry" or "abandon")
- Missing category field
- Invalid JSON

**Test with curl:**
```bash
curl -X POST http://localhost:8000/api/queue/bulk-retry \
  -H "Content-Type: application/json" \
  -d '{"category": "http_404_not_found", "action": "retry"}'
```

## Import Command Issues

### Import Shows 0 Queued URLs

**Common causes:**

1. **URLs already exist**
   ```bash
   # Check if URL already in work_unit
   psql $DATABASE_URL -c "SELECT url FROM work_unit WHERE url = 'http://example.onion';"
   ```

2. **URLs are blacklisted**
   ```bash
   # Check blacklist
   psql $DATABASE_URL -c "SELECT * FROM domain_blacklist WHERE domain = 'example.onion';"
   ```

3. **File format not recognized**
   - Check file extension (.txt, .json, .csv)
   - Verify file content format matches extension
   - Try with plain text (one URL per line)

### Import Fails with "error creating import_source"

**Check:**
```bash
# Verify import_source table exists
psql $DATABASE_URL -c "\d import_source"

# Run migrations if missing
diesel migration run
```

## Database Connection Issues

### "Connection refused" or "Database does not exist"

**Check .env file:**
```bash
grep DATABASE_URL .env
```

**Expected format:**
```
DATABASE_URL=postgres://spyder:spyder@localhost/spyder
```

**Verify database exists:**
```bash
psql -l | grep spyder
```

**Create if missing:**
```bash
createdb -U spyder spyder
diesel setup
diesel migration run
```

## General Debugging

### Check Frontend Logs
```bash
cargo run --bin frontend 2>&1 | tee frontend.log
```

### Check Database Schema
```bash
# List all tables
psql $DATABASE_URL -c "\dt"

# Check specific table structure
psql $DATABASE_URL -c "\d url_discovery"
psql $DATABASE_URL -c "\d work_unit"
psql $DATABASE_URL -c "\d import_source"
```

### Verify Migrations
```bash
# List applied migrations
psql $DATABASE_URL -c "SELECT * FROM __diesel_schema_migrations ORDER BY version;"

# Rerun if needed
diesel migration redo
```

### Test API Endpoints
```bash
# Failures summary
curl http://localhost:8000/api/failures/summary | jq

# Bulk retry
curl -X POST http://localhost:8000/api/queue/bulk-retry \
  -H "Content-Type: application/json" \
  -d '{"category": "timeout", "action": "retry"}' | jq
```

## Performance Issues

### Discovery Page Slow with Many Records

**Optimize:**
1. Use filters (depth, import_source)
2. Reduce page size: `?limit=50`
3. Add indexes if needed:
   ```sql
   CREATE INDEX IF NOT EXISTS idx_url_discovery_depth_id 
   ON url_discovery(discovery_depth, id DESC);
   ```

### Materialized View Refresh Slow

**This is normal for large datasets.**

**To refresh in background:**
```bash
psql $DATABASE_URL -c "REFRESH MATERIALIZED VIEW CONCURRENTLY work_unit_failure_summary;" &
```

**Schedule periodic refresh:**
```bash
# Add to cron
0 * * * * psql $DATABASE_URL -c "REFRESH MATERIALIZED VIEW CONCURRENTLY work_unit_failure_summary;"
```

## Getting Help

If you encounter an issue not covered here:

1. **Check logs:** `cargo run --bin frontend 2>&1 | grep -i error`
2. **Check database:** `psql $DATABASE_URL -c "SELECT version()"`
3. **Verify migrations:** `diesel migration run`
4. **Check IMPORT_GUIDE.md** for usage examples
5. **Check HANDOFF.md** for implementation details

## Common SQL Queries

### View Discovery Statistics
```sql
-- Depth distribution
SELECT discovery_depth, COUNT(*) 
FROM url_discovery 
GROUP BY discovery_depth 
ORDER BY discovery_depth;

-- Import source breakdown
SELECT s.source_name, COUNT(d.id) as url_count
FROM import_source s
LEFT JOIN url_discovery d ON d.import_source_id = s.id
GROUP BY s.id, s.source_name
ORDER BY url_count DESC;
```

### View Failure Statistics
```sql
-- Current failures
SELECT failure_category, COUNT(*) 
FROM work_unit 
WHERE status = 'failed' 
GROUP BY failure_category 
ORDER BY COUNT(*) DESC;

-- Refreshed view
REFRESH MATERIALIZED VIEW CONCURRENTLY work_unit_failure_summary;
SELECT * FROM work_unit_failure_summary ORDER BY count DESC;
```

### Check Provenance
```sql
-- Find how a URL was discovered
SELECT 
  d.url,
  d.discovery_depth,
  d.discovery_chain,
  p.url as discovered_from_page,
  s.source_name as import_source
FROM url_discovery d
LEFT JOIN page p ON p.id = d.discovered_from_page_id
LEFT JOIN import_source s ON s.id = d.import_source_id
WHERE d.url = 'http://example.onion';
```
