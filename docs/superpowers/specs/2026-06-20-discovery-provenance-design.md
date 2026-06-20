# Discovery Provenance and Queue Recovery Design

**Date:** 2026-06-20  
**Status:** Approved

## Problem Statement

Spyder currently lacks visibility into two critical areas:

1. **Discovery provenance** - No way to answer "how did the crawler find this site?" or trace the path from seed to discovered page
2. **Queue stagnation** - Crawler has work_units that are failing (blacklisted, timeouts, errors) with no bulk recovery tools, making it appear "stuck"

This makes it difficult to:
- Understand site relationships and crawler reach
- Debug why certain sites were discovered
- Recover from bulk failures efficiently
- Inject new seeds from external sources systematically

## Goals

1. Track complete discovery chains from seed → page A → page B → target for every URL
2. Enable bulk operations on failed work_units grouped by failure category
3. Provide systematic import of external URL sources (Ahmia, OnionTree, dumps, lists)
4. Create UI for exploring discovery provenance and managing queue health

## Architecture Approach

**Discovery-First Architecture** - Make discovery provenance the central organizing principle with first-class schema support and UI.

## Database Schema

### New Tables

#### `url_discovery`
Central provenance tracking for all discovered URLs.

```sql
CREATE TABLE url_discovery (
  id SERIAL PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,
  discovered_from_page_id INTEGER REFERENCES page(id),  -- NULL for seeds/imports
  discovery_chain INTEGER[] NOT NULL DEFAULT '{}',      -- Array of page IDs showing full path
  discovery_depth INTEGER NOT NULL DEFAULT 0,           -- Length of chain
  import_source_id INTEGER REFERENCES import_source(id), -- NULL for organic discovery
  discovered_at TIMESTAMP NOT NULL DEFAULT NOW(),
  first_queued_at TIMESTAMP NOT NULL DEFAULT NOW(),
  
  -- Indexes
  CONSTRAINT url_discovery_url_unique UNIQUE (url)
);

CREATE INDEX idx_url_discovery_depth ON url_discovery(discovery_depth);
CREATE INDEX idx_url_discovery_import_source ON url_discovery(import_source_id);
CREATE INDEX idx_url_discovery_discovered_from ON url_discovery(discovered_from_page_id);
```

#### `import_source`
Tracks external seed sources for bulk imports.

```sql
CREATE TABLE import_source (
  id SERIAL PRIMARY KEY,
  source_type TEXT NOT NULL,  -- 'manual', 'ahmia', 'oniontree', 'paste', 'custom'
  source_name TEXT NOT NULL,  -- e.g., "Ahmia 2026-06-15 dump"
  source_url TEXT,            -- Where it came from (optional)
  imported_at TIMESTAMP NOT NULL DEFAULT NOW(),
  imported_by TEXT NOT NULL,  -- User/system identifier
  total_urls INTEGER NOT NULL,
  metadata JSONB,             -- Flexible storage for source-specific data
  
  -- Example metadata:
  -- {
  --   "filename": "ahmia-2026-06-15.json",
  --   "file_hash": "sha256:...",
  --   "format": "json",
  --   "parsed_fields": ["url", "title", "description"]
  -- }
);
```

#### `work_unit_failure_summary` (Materialized View)
Analytics view for failure categorization.

```sql
CREATE MATERIALIZED VIEW work_unit_failure_summary AS
SELECT 
  failure_category,
  COUNT(*) as count,
  ARRAY_AGG(url ORDER BY last_attempt_at DESC LIMIT 10) as sample_urls,
  MAX(last_attempt_at) as last_failure_at
FROM work_unit
WHERE status IN ('failed', 'pending') 
  AND failure_category IS NOT NULL
GROUP BY failure_category;

CREATE INDEX idx_work_failure_summary_category ON work_unit_failure_summary(failure_category);
```

### Modified Tables

#### `work_unit`
Add discovery link and failure categorization.

```sql
ALTER TABLE work_unit 
  ADD COLUMN url_discovery_id INTEGER REFERENCES url_discovery(id),
  ADD COLUMN failure_category TEXT;

CREATE INDEX idx_work_unit_discovery ON work_unit(url_discovery_id);
CREATE INDEX idx_work_unit_failure_category ON work_unit(failure_category);
```

No changes needed to `page_link` - it already tracks source_page_id → target_url relationships that we'll use to build discovery chains.

## Discovery Chain Building Logic

### When a URL is Discovered

**From page scanning:**

1. Extract links from scanned page (existing `extract_page_snapshot()`)
2. For each discovered link:
   - Normalize URL (existing `normalize_crawl_url()`)
   - Check if URL exists in `url_discovery`
     - If exists: skip (first discovery wins, preserves shortest path)
     - If new: continue to step 3
3. Build discovery chain:
   - Look up discovering page's `url_discovery` entry
   - Copy its `discovery_chain` array
   - Append discovering page's ID to chain
   - Set `discovery_depth = length(new_chain)`
4. Create `url_discovery` entry with chain data
5. Create `work_unit` entry linked via `url_discovery_id`

**From import/seed:**

1. Parse import file (see Import Pipeline section)
2. For each URL:
   - Normalize URL
   - Check blacklist (skip if blacklisted)
   - Check if exists in `url_discovery` (skip if exists)
3. Create `url_discovery` entry:
   - `discovery_chain = []` (empty array)
   - `discovery_depth = 0`
   - `import_source_id = new import source ID`
   - `discovered_from_page_id = NULL`
4. Create `work_unit` entry linked to discovery

### Discovery Chain Examples

```
Seed URL (imported):
  discovery_chain: []
  discovery_depth: 0
  import_source_id: 42
  discovered_from_page_id: NULL
  
Page discovered from seed (page_id: 100):
  discovery_chain: [100]
  discovery_depth: 1
  import_source_id: NULL
  discovered_from_page_id: 100
  
Page discovered from depth-1 page (page_id: 200):
  discovery_chain: [100, 200]
  discovery_depth: 2
  import_source_id: NULL
  discovered_from_page_id: 200
```

### Depth Limiting

Add crawler configuration option:
- `max_discovery_depth` (default: 10)

When enqueueing discovered links, skip URLs where:
```sql
WHERE discovery_depth >= max_discovery_depth
```

This prevents infinite crawling into link farms and deep rabbit holes.

### Re-discovery Handling

If a URL is discovered multiple times from different pages:
- **First discovery wins** - don't update `url_discovery`
- Preserves shortest/earliest discovery path
- Alternative (not implemented initially): track "also discovered from" list in metadata

## Failure Categorization

### Categorization Logic

When `record_work_unit_failure()` is called, categorize the error:

```rust
fn categorize_failure(error: &str, http_status: Option<u16>) -> String {
    match http_status {
        Some(403) => "http_403_forbidden",
        Some(404) => "http_404_not_found",
        Some(500..=599) => "http_5xx_server_error",
        Some(429) => "http_429_rate_limit",
        _ => {
            if error.contains("blacklist") {
                "blacklisted"
            } else if error.contains("timeout") {
                "timeout"
            } else if error.contains("connection refused") {
                "connection_refused"
            } else if error.contains("dns") {
                "dns_failure"
            } else if error.contains("certificate") || error.contains("tls") {
                "tls_error"
            } else {
                "other"
            }
        }
    }
}
```

Store categorized value in `work_unit.failure_category` for fast filtering.

### Bulk Operations API

New functions in `src/lib.rs`:

```rust
/// Get failure summary grouped by category
pub fn get_failure_summary(conn: &mut PgConnection) -> Result<Vec<FailureCategorySummary>>

pub struct FailureCategorySummary {
    pub category: String,
    pub count: i64,
    pub sample_urls: Vec<String>,
    pub last_failure_at: String,
}

/// Bulk retry all work_units in a failure category
pub fn bulk_retry_by_category(
    conn: &mut PgConnection, 
    category: &str, 
    limit: Option<i64>
) -> Result<i64>
// Returns: count of items updated
// Updates: status='pending', retry_count=0, next_attempt_at=NOW()

/// Bulk remove from blacklist and retry
pub fn bulk_whitelist_and_retry(
    conn: &mut PgConnection,
    urls: Vec<String>
) -> Result<(i64, i64)>
// Returns: (blacklist_entries_removed, work_units_retried)
// - Removes from domain_blacklist
// - Resets work_units to pending

/// Bulk abandon work_units by category
pub fn bulk_abandon_by_category(
    conn: &mut PgConnection,
    category: &str
) -> Result<i64>
// Returns: count of items abandoned
// Sets: status='abandoned' (new status value)
```

## Import Pipeline

### CLI Command

```bash
cargo run --bin spyder -- import <file_path> \
  --source-type <type> \
  --source-name <name> \
  [--source-url <url>]

# Examples:
cargo run --bin spyder -- import ahmia-2026-06-15.json \
  --source-type ahmia \
  --source-name "Ahmia June 2026" \
  --source-url "https://ahmia.fi/dumps/2026-06-15"

cargo run --bin spyder -- import onions.txt \
  --source-type custom \
  --source-name "Manual seed list"
```

### Supported Formats

Auto-detected based on file extension and content:

**Plain text** (one URL per line):
```
http://example.onion
http://market.onion/products
```

**JSON array**:
```json
["http://example.onion", "http://market.onion"]
```

**JSON objects** (structured dumps like Ahmia):
```json
[
  {
    "url": "http://example.onion",
    "title": "Example Site",
    "metadata": {"category": "forum"}
  },
  {
    "url": "http://market.onion",
    "title": "Marketplace"
  }
]
```

**CSV**:
```csv
url,title,description
http://example.onion,Example,Some description
http://market.onion,Market,Another description
```

### Import Process

1. **Parse file** based on format detection
2. **Create `import_source` record** with metadata:
   ```json
   {
     "filename": "ahmia-2026-06-15.json",
     "file_hash": "sha256:abc123...",
     "format": "json",
     "parsed_fields": ["url", "title", "metadata"]
   }
   ```
3. **For each URL in file:**
   - Normalize URL (existing `normalize_crawl_url()`)
   - Check blacklist → skip if blacklisted, increment `skipped_count`
   - Check if exists in `url_discovery` → skip if exists, increment `duplicate_count`
   - Create `url_discovery` entry:
     - `url = normalized_url`
     - `discovered_from_page_id = NULL`
     - `discovery_chain = []`
     - `discovery_depth = 0`
     - `import_source_id = new import source ID`
   - Create `work_unit` entry linked to discovery
   - Increment `queued_count`
4. **Update `import_source.total_urls`** with final queued count
5. **Print summary report:**
   ```
   Import complete: ahmia-2026-06-15.json
   - Total URLs in file: 1,250
   - New URLs queued: 843
   - Already known (skipped): 302
   - Blacklisted (skipped): 105
   - Import source ID: 42
   ```

### Future Enhancements (Not in Initial Implementation)

- URL validation/sanitization beyond normalization
- Deduplication within file before DB checks
- Scheduled imports from URLs (poll Ahmia endpoint daily)
- Import source expiry/archival for old imports

## Frontend UI

### Discovery Explorer - `/discovery`

**Purpose:** Explore discovery provenance and understand how sites were found.

**Main components:**

**Search/Filter Bar:**
- URL/host input field
- Import source dropdown (all sources + "Organic discovery")
- Discovery depth range slider (0-10+)
- Date range picker

**Discovery Chain Visualization:**

When viewing a specific URL's discovery path:

```
┌──────────────────────────────────────────────────────┐
│ Discovery Chain for: market.onion/products           │
├──────────────────────────────────────────────────────┤
│                                                      │
│  🌱 Seed (Ahmia June 2026)                          │
│   └─→ 📄 directory.onion/links                      │
│        └─→ 📄 hub.onion/marketplace                 │
│             └─→ 🎯 market.onion/products            │
│                                                      │
│  Discovered: 2026-06-15 14:32:18                    │
│  Depth: 3                                            │
│  First queued: 2026-06-15 14:35:02                  │
│  Current status: Scanned successfully                │
│                                                      │
│  [View Full Page Details]                            │
└──────────────────────────────────────────────────────┘
```

Each node in the chain is clickable to navigate to that page's details.

**Table View - Discoveries by Import Source:**

| Import Source | URLs Imported | Successfully Crawled | Pending | Failed | Max Depth Reached |
|---------------|---------------|---------------------|---------|--------|-------------------|
| Ahmia June 2026 | 843 | 621 | 112 | 110 | 5 |
| Manual seeds | 25 | 25 | 0 | 0 | 8 |
| OnionTree May | 1,205 | 892 | 203 | 110 | 4 |

Click a source → drill down to see all URLs from that import.

**Reverse Lookup:**

From any page detail view (e.g., `/pages/{id}`), show:
- "Discovered from: [page link]" with breadcrumb trail
- "Full discovery chain: Seed → A → B → this page"
- "Also linked from: [list of other pages that link here]"

**Stats Dashboard:**

- Total unique URLs discovered: 15,432
- Discovery depth distribution (bar chart: depth 0-10+)
- Discovery rate over time (line chart, last 30 days)
- Top import sources by contribution (table)
- Organic vs imported ratio (pie chart)

### Failure Dashboard - `/queue/failures`

**Purpose:** Manage work queue health and enable bulk failure recovery.

**Summary Cards:**

Grid of category cards showing counts and quick actions:

```
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│  Blacklisted     │ │  Timeout         │ │  Connection      │
│      150         │ │      45          │ │  Refused: 32     │
│  [Whitelist All] │ │  [Retry All]     │ │  [Retry All]     │
└──────────────────┘ └──────────────────┘ └──────────────────┘

┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│  HTTP 404        │ │  HTTP 403        │ │  DNS Failure     │
│      89          │ │      23          │ │      12          │
│  [Abandon]       │ │  [Retry All]     │ │  [Retry All]     │
└──────────────────┘ └──────────────────┘ └──────────────────┘
```

**Expandable Category Details:**

Click a card → expands below to show:

```
┌──────────────────────────────────────────────────────────────┐
│ ▼ Blacklisted (150 URLs)                                     │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│ Sample URLs (showing 10 of 150):                            │
│  • market-spam.onion/ads                                    │
│  • phishing-site.onion                                      │
│  • link-farm.onion/directory                                │
│  ...                                                         │
│                                                              │
│ Last failure: 2026-06-15 18:42:10                           │
│                                                              │
│ Actions:                                                     │
│  [Whitelist & Retry All]  [Download Full List (.txt)]       │
│  [Whitelist Selected]     [View Blacklist Rules]            │
│                                                              │
│ ☐ Select all  ☑ market-spam.onion/ads                      │
│               ☐ phishing-site.onion                         │
│               ☑ link-farm.onion/directory                   │
└──────────────────────────────────────────────────────────────┘
```

**Bulk Action Flow:**

1. User clicks "Retry All" on "Timeout (45)" card
2. Confirmation modal appears:
   ```
   ┌───────────────────────────────────────────────┐
   │ Confirm Bulk Retry                            │
   ├───────────────────────────────────────────────┤
   │ About to retry 45 URLs that failed due to     │
   │ timeout errors.                               │
   │                                               │
   │ These will be reset to 'pending' status and   │
   │ re-queued for crawling.                       │
   │                                               │
   │           [Cancel]  [Retry All]               │
   └───────────────────────────────────────────────┘
   ```
3. On confirm: POST to `/api/queue/bulk-retry`
   ```json
   {
     "category": "timeout",
     "action": "retry"
   }
   ```
4. Backend returns: `{"updated": 45}`
5. Success toast: "45 URLs reset to pending status"
6. Cards refresh with updated counts

**Additional Queue Health Metrics:**

Top banner showing:
- **Queue size:** "234 pending, 45 in progress"
- **Stagnation alert:** "⚠️ No new discoveries in last 6 hours"
- **Discovery rate:** "Discovering 12 new URLs/hour (avg last 24h)"
- **Call to action:** "Need more seeds? [Import from file →]"

**Integration with Existing Queue View:**

If `/queue` page exists, add navigation tabs:
- **All Items** - existing full queue view
- **Failures** - new failure dashboard (this design)
- **Discovery Stats** - link to `/discovery` page

## Data Flow Examples

### Example 1: Import → Discovery → Crawl

1. **User imports Ahmia dump:**
   ```bash
   cargo run --bin spyder -- import ahmia.json --source-type ahmia --source-name "Ahmia June 2026"
   ```

2. **Import pipeline creates:**
   - 1 `import_source` record (id: 42)
   - 843 `url_discovery` records (depth: 0, import_source_id: 42)
   - 843 `work_unit` records (status: pending)

3. **Crawler processes first work_unit:**
   - Fetches page successfully
   - Extracts 15 links via `extract_page_snapshot()`
   - For each link:
     - Check `url_discovery` → 3 already exist, skip
     - Create 12 new `url_discovery` records:
       - `discovery_chain = [scanned_page_id]`
       - `discovery_depth = 1`
       - `discovered_from_page_id = scanned_page_id`
     - Create 12 new `work_unit` records

4. **User explores discovery:**
   - Visits `/discovery`
   - Filters by "Ahmia June 2026"
   - Sees 843 imported + 12 discovered (depth 1) = 855 total
   - Clicks a depth-1 page
   - Sees chain: "Ahmia seed → discovered page"

### Example 2: Bulk Failure Recovery

1. **Crawler hits blacklist wall:**
   - 150 URLs fail with "blacklist" error
   - `record_work_unit_failure()` sets `failure_category = "blacklisted"`

2. **User visits `/queue/failures`:**
   - Sees "Blacklisted: 150" card
   - Clicks to expand
   - Reviews sample URLs
   - Realizes some are legitimate sites mis-categorized

3. **User performs bulk whitelist:**
   - Selects 50 URLs from the list
   - Clicks "Whitelist Selected"
   - Confirms modal
   - Backend:
     - Removes 50 entries from `domain_blacklist`
     - Updates 50 `work_unit` records: status='pending', retry_count=0

4. **Crawler retries:**
   - Next work cycle picks up the 50 now-pending URLs
   - Successfully crawls 45 of them
   - 5 still fail (different error)

## Testing Strategy

### Database Schema
- Migration runs cleanly (up and down)
- Indexes created correctly
- Foreign key constraints work
- Materialized view refreshes correctly

### Discovery Chain Building
- Seed URLs have depth 0, empty chain
- First-hop discovery creates depth 1 with single-element chain
- Multi-hop discovery correctly appends to chain
- Re-discovery preserves first discovery (doesn't update)
- Depth limiting prevents queueing beyond max depth

### Failure Categorization
- HTTP status codes categorize correctly
- Error message patterns match expected categories
- "other" category catches uncategorized errors
- Materialized view aggregates correctly

### Import Pipeline
- Plain text format parses correctly
- JSON array format parses correctly
- JSON object format extracts URLs
- CSV format parses correctly
- Blacklisted URLs are skipped
- Duplicate URLs are skipped
- Summary report shows accurate counts

### Bulk Operations
- Retry by category updates correct records
- Whitelist removes from blacklist and retries
- Abandon sets status correctly
- Operations respect optional limit parameter

### Frontend
- Discovery chain renders correctly
- Import source table shows accurate stats
- Failure cards display correct counts
- Bulk actions trigger confirmation modals
- API calls update UI on success

## Migration Plan

1. **Create migration for new tables** (`url_discovery`, `import_source`)
2. **Create migration to alter `work_unit`** (add columns)
3. **Backfill existing data:**
   - For existing `work_unit` entries:
     - Create `url_discovery` with `discovery_chain = []`, `depth = 0`
     - Link via `url_discovery_id`
   - For existing `page_link` entries:
     - Optionally reconstruct discovery chains (complex, may skip for initial version)
4. **Deploy code changes** (lib, CLI, frontend)
5. **Refresh materialized view** for failure summary
6. **Document import pipeline** for users

## Performance Considerations

### Storage Growth
- `url_discovery` grows linearly with unique URLs discovered (acceptable)
- `discovery_chain` array size = depth (typically < 10 elements)
- PostgreSQL array storage is efficient for small arrays

### Query Performance
- Depth limiting prevents unbounded growth
- Indexes on `discovery_depth`, `import_source_id` enable fast filtering
- Materialized view pre-aggregates failure stats (refresh periodically)

### Import Performance
- Batch insert `url_discovery` records (100-1000 at a time)
- Use transaction for atomic import
- Large files (>10k URLs) may take 10-30 seconds

### UI Performance
- Discovery chain limited to reasonable depth (< 100 hops in practice)
- Failure summary cards load from materialized view (fast)
- Sample URLs limited to 10 per category (prevents huge payloads)

## Security Considerations

- **Import sources:** Validate file paths, prevent directory traversal
- **URL normalization:** Use existing `normalize_crawl_url()` to prevent injection
- **Bulk operations:** Require confirmation modals to prevent accidental mass changes
- **Blacklist bypass:** Whitelist operations should log who removed what for audit trail

## Future Enhancements

Not in initial implementation, but possible later:

1. **Discovery graph visualization** - Interactive D3.js graph showing site relationships
2. **Multi-path tracking** - Record all discovery paths, not just first
3. **Discovery scoring** - Rank import sources by quality (success rate, depth reached)
4. **Automated import scheduling** - Periodic imports from Ahmia/OnionTree APIs
5. **Discovery alerts** - Notify when interesting sites are discovered (by keyword/topic)
6. **Export discovery data** - Generate reports for research/analysis
7. **Discovery-based prioritization** - Crawl high-value discovery paths first

## Success Metrics

After implementation, we should see:

1. **Provenance visibility:** Can trace any URL back to its source
2. **Queue recovery:** Bulk operations reduce failed queue size by >80% in one action
3. **Discovery growth:** Import pipeline enables adding 1000+ seeds in < 1 minute
4. **User adoption:** Failure dashboard used weekly to unblock crawler
5. **Coverage expansion:** Discovery depth reaches 5+ hops from seeds

## Open Questions

None - all design questions resolved during brainstorming.
