# Relationship Overview Materialized View Migration

## Problem

The original migration attempted to create and populate a materialized view from the `page_link` table (576GB+) in a single transaction. This caused:
- Migration running for 24+ hours without completion
- Database locks blocking other operations
- Multiple processes stuck waiting on WAL writes and transaction locks

## Solution

The migration now uses a **two-phase approach**:

### Phase 1: Migration (Fast - seconds)
- Creates the materialized view structure with `WITH NO DATA`
- Creates indexes on the empty view
- No data population during migration
- **Migration completes in <1 second**

### Phase 2: Population (Slow - hours, run separately)
- Run manually after migration: `./scripts/refresh_relationship_view.sh`
- Uses `REFRESH MATERIALIZED VIEW CONCURRENTLY`
- Won't block other database operations
- Takes several hours but is interruptible

## Usage

### 1. Run the migration
```bash
diesel migration run
# or your migration tool
```

The migration will complete immediately, creating an empty view.

### 2. Populate the view (after migration)
```bash
# Check current status
./scripts/refresh_relationship_view.sh check

# Start population (interactive with confirmation)
./scripts/refresh_relationship_view.sh

# Or force without confirmation
./scripts/refresh_relationship_view.sh force
```

### 3. Monitor progress
```bash
# In another terminal
watch -n 30 './scripts/refresh_relationship_view.sh check'
```

## Why This Approach?

1. **Fast migrations**: Database migrations should be fast and predictable
2. **No blocking**: Using `CONCURRENTLY` prevents blocking other operations
3. **Interruptible**: Can be stopped and restarted without corrupting the migration
4. **Observable**: Can monitor progress separately from migration
5. **Safe**: Won't hold locks for hours/days

## Performance Expectations

- **Migration**: <1 second
- **Initial population**: 2-8 hours (depends on hardware and table size)
- **Future refreshes**: Similar time, but can be scheduled during low-traffic periods

## Scheduling Refreshes

For production, consider using `pg_cron`:

```sql
-- Refresh every night at 2 AM
SELECT cron.schedule(
    'refresh-site-relationships',
    '0 2 * * *',
    'REFRESH MATERIALIZED VIEW CONCURRENTLY site_relationship_overview'
);
```

Or use a cron job:
```cron
0 2 * * * cd /home/jbanier/Documents/work/spyder && ./scripts/refresh_relationship_view.sh force
```

## Troubleshooting

### Check if view exists but is empty
```sql
SELECT pg_size_pretty(pg_total_relation_size('site_relationship_overview'));
```

### Check for running refresh
```sql
SELECT pid, now() - query_start as duration, state
FROM pg_stat_activity
WHERE query LIKE '%site_relationship_overview%';
```

### Cancel a running refresh (safe)
```sql
SELECT pg_cancel_backend(pid)
FROM pg_stat_activity
WHERE query LIKE '%REFRESH MATERIALIZED VIEW%site_relationship_overview%';
```

## Recovery from Failed Original Migration

If the old migration was stuck, we terminated these processes:
- Process 440612 (stuck for 1 day)
- Process 797595 (stuck for 3+ hours)  
- Process 526119 (stuck for 19+ hours)

The new migration creates the view properly without these issues.
