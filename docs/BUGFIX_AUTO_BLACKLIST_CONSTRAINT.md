# Bug Fix: Auto Blacklist Event Unique Constraint Violation

## Issue

The crawler was crashing with the following error when handling blacklisted sites:

```
ERROR spyder: 3404: error deleting blacklisted page
Caused by:
    duplicate key value violates unique constraint "idx_auto_blacklist_event_unique_page"
```

## Root Cause

The unique constraint on `auto_blacklist_event` was defined as:

```sql
CREATE UNIQUE INDEX idx_auto_blacklist_event_unique_page
  ON auto_blacklist_event(domain, rule_id, COALESCE(source_page_id, 0));
```

The use of `COALESCE(source_page_id, 0)` meant that all NULL values were treated as 0 in the index.

When multiple pages from the same domain matched the same auto-blacklist rule:
1. Each page created a separate `auto_blacklist_event` with `(domain, rule_id, page_id)`
2. When the first page was deleted, the foreign key `ON DELETE SET NULL` set its event's `source_page_id` to NULL (→ 0 in index)
3. When the second page was deleted, the cascade tried to UPDATE its event to NULL (→ 0 in index)
4. This created a duplicate `(domain, rule_id, 0)` tuple → **UNIQUE CONSTRAINT VIOLATION**

## Solution

Changed the unique index to a **partial unique index** that only applies when `source_page_id IS NOT NULL`:

```sql
CREATE UNIQUE INDEX idx_auto_blacklist_event_unique_page
  ON auto_blacklist_event(domain, rule_id, source_page_id)
  WHERE source_page_id IS NOT NULL;
```

This allows:
- ✓ Only one event per `(domain, rule_id, specific_page_id)` while pages exist
- ✓ Multiple NULL `source_page_id` values after pages are deleted (preserves audit trail)
- ✓ No constraint violations during page deletion cascades

## Changes Made

1. **Migration**: `migrations/2026-06-03-125634_fix_auto_blacklist_event_unique_constraint/`
   - Drops the old COALESCE-based index
   - Creates new partial unique index

2. **Schema Updates**:
   - `src/lib.rs:9229-9231` - Updated embedded SQLite schema
   - `src/bin/frontend.rs:3688-3690` - Updated embedded SQLite schema

## Verification

Tested the fix with the following scenario:
1. Created two pages from the same domain matching the same rule
2. Inserted two `auto_blacklist_event` records (one per page)
3. Deleted first page → `source_page_id` set to NULL ✓
4. Deleted second page → Previously failed, now succeeds ✓
5. Both events preserved with NULL `source_page_id` ✓

The fix prevents the crash while maintaining audit history of blacklist triggers.
