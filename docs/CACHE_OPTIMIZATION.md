# Cache Optimization - Background Caching Implementation

## Problem

The application experienced request queuing issues where page loads to `/analytics` and `/relationships` would block other requests. This was caused by:

1. **Synchronous cache refreshes**: When cache expired, request handler threads would block while performing expensive database queries
2. **Cold cache busy-waiting**: Even with background refresh, cold caches caused 250ms busy-wait loops in request handlers
3. **Worker thread exhaustion**: Rocket's limited worker pool (typically # of CPU cores) would be exhausted by blocked threads

## Solution Implemented

Converted all routes from **synchronous caching** to **background caching** with zero-wait fallback templates.

### Changes Made

1. **All 13 routes converted to background caching:**
   - `/` (dashboard) - already had background caching
   - `/pages` - CONVERTED
   - `/work` - CONVERTED
   - `/blacklist` - CONVERTED
   - `/top` - CONVERTED
   - `/analytics` - already had background caching
   - `/sites` - CONVERTED
   - `/watchlists` - CONVERTED
   - `/leads` - CONVERTED
   - `/relationships` - CONVERTED
   - `/entities/emails` - CONVERTED
   - `/entities/crypto` - CONVERTED
   - `/entities/ssh` - CONVERTED
   - `/entities/http` - CONVERTED
   - `/entities/services` - CONVERTED

2. **Created warming context functions:**
   - Each route now has a `build_*_warming_context()` function that returns a skeleton template
   - Warming contexts display "Refreshing" placeholders instead of blocking

3. **Eliminated busy-waiting:**
   - Changed `DEFAULT_FRONTEND_CACHE_COLD_WAIT_MS` from `250` to `0`
   - Request handlers now immediately return warming template instead of polling

4. **Removed dead code:**
   - Removed unused `render_cached_context()` helper function
   - Removed unused `anyhow::Context` import

## How It Works

### Before (Synchronous Caching)
```
User requests /pages
  → Cache expired?
    → YES: Block thread, run database query (could take seconds)
    → Return result
  → NO: Return cached result
```

### After (Background Caching)
```
User requests /pages
  → Cache fresh?
    → YES: Return cached result immediately
  → Cache stale?
    → Spawn background thread to refresh
    → Return stale cached result immediately
  → Cache cold (no data)?
    → Spawn background thread to refresh
    → Return warming template immediately (no wait)
```

## Performance Impact

**Benefits:**
- **No request handler blocking**: All workers remain available to handle requests
- **Instant responses**: Users get immediate feedback, even with cold cache
- **Better UX**: Loading states instead of hanging requests
- **Scales better**: Worker pool doesn't get exhausted under load
- **Graceful degradation**: Stale data served during refresh is better than blocking

**Tradeoffs:**
- Users see "Refreshing" placeholders during cold starts
- First load after cache expiry shows stale data (acceptable tradeoff)

## Configuration

### Environment Variables

- `SPYDER_FRONTEND_CACHE_TTL_SECONDS` (default: 30)
  - How long cached data remains fresh
  
- `SPYDER_FRONTEND_CACHE_COLD_WAIT_MS` (default: 0)
  - How long to wait for background refresh on cold cache
  - Set to 0 for instant warming template (recommended)
  - Set higher (e.g., 250) if you want to attempt waiting for data

- `SPYDER_FRONTEND_CACHE_WARM_ROUTES` (default: "/,/analytics")
  - Comma-separated list of routes to pre-warm on startup
  - Prevents cold cache on commonly accessed pages

### Warming More Routes

To pre-warm additional routes on startup, set:
```bash
export SPYDER_FRONTEND_CACHE_WARM_ROUTES="/,/analytics,/pages,/relationships"
```

## Monitoring

The server logs slow cache refreshes. Look for:
```
WARN route=/analytics duration_secs=8.5 "Slow cache refresh"
```

To adjust the threshold, set:
```bash
export SPYDER_FRONTEND_CACHE_SLOW_ROUTE_MS=5000  # Log if refresh takes >5s
```

## Testing

To verify the changes:

1. **Test cold cache behavior:**
   ```bash
   # Restart server (clears in-memory cache)
   pkill frontend && ./target/release/frontend
   
   # Immediately visit a page - should see "Refreshing" briefly
   curl http://localhost:8000/pages
   ```

2. **Test warm cache behavior:**
   ```bash
   # Visit page, wait, visit again - should be instant
   curl http://localhost:8000/pages
   sleep 1
   curl http://localhost:8000/pages  # Should be cached
   ```

3. **Test concurrent requests:**
   ```bash
   # Hit multiple endpoints simultaneously
   for i in {1..10}; do
     curl -s http://localhost:8000/pages &
     curl -s http://localhost:8000/analytics &
     curl -s http://localhost:8000/relationships &
   done
   wait
   ```
   All requests should complete quickly without blocking each other.

## Future Improvements

1. **Database query optimization**: Some routes still perform expensive queries
   - Consider adding database indexes
   - Consider materialized views for aggregations

2. **Longer cache TTLs**: If data doesn't change frequently, increase TTL
   ```bash
   export SPYDER_FRONTEND_CACHE_TTL_SECONDS=300  # 5 minutes
   ```

3. **Smart cache invalidation**: Currently caches expire based on time
   - Could invalidate specific caches when data changes
   - Already implemented for blacklist, watchlist, and leads

4. **Persistent cache**: Currently in-memory only
   - Could use Redis for shared cache across instances
   - Would survive restarts

## Rollback

If you need to revert these changes:

1. The old synchronous caching code is still present but unused:
   - `CacheRead` enum
   - `get_or_refresh()` and `get_or_refresh_at()` methods
   - `context()` method

2. To rollback:
   ```bash
   git revert <this-commit>
   cargo build --release --bin frontend
   ```

## Related Files

- `src/bin/frontend.rs` - Main changes (route handlers, warming contexts)
- `src/config.rs` - Default cache cold wait changed to 0ms
- `docs/CACHE_OPTIMIZATION.md` - This file
