# Discovery Provenance and Queue Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Track complete URL discovery chains from seed to target, enable bulk queue failure recovery, and provide systematic import of external URL sources.

**Architecture:** Discovery-first architecture with dedicated `url_discovery` and `import_source` tables tracking provenance. Enhanced `work_unit` with failure categorization. New CLI import command and two frontend dashboards (discovery explorer + failure management).

**Tech Stack:** Rust, Diesel ORM, PostgreSQL, Rocket web framework, Handlebars templates

## Global Constraints

- PostgreSQL database (no SQLite for production)
- Use existing Diesel ORM patterns and migrations
- Follow existing Spyder model conventions (Queryable, Insertable, Serialize)
- Use existing frontend template structure and CSS classes
- Maximum discovery depth: 10 (configurable)
- Import batch size: 100-1000 URLs per transaction

---

## File Structure

### New Files
- `migrations/2026-06-20-100000_discovery_provenance/up.sql` - New tables (url_discovery, import_source)
- `migrations/2026-06-20-100000_discovery_provenance/down.sql` - Rollback
- `migrations/2026-06-20-110000_work_unit_discovery/up.sql` - Alter work_unit
- `migrations/2026-06-20-110000_work_unit_discovery/down.sql` - Rollback
- `migrations/2026-06-20-120000_failure_summary_view/up.sql` - Materialized view
- `migrations/2026-06-20-120000_failure_summary_view/down.sql` - Rollback
- `templates/discovery.html.hbs` - Discovery explorer UI
- `templates/queue_failures.html.hbs` - Failure dashboard UI

### Modified Files
- `src/schema.rs` - Auto-generated, will update after migrations
- `src/models.rs` - Add new model structs (UrlDiscovery, ImportSource, etc.)
- `src/lib.rs` - Add discovery chain logic, failure categorization, bulk operations
- `src/bin/spyder.rs` - Add import CLI command
- `src/bin/frontend.rs` - Add /discovery and /queue/failures routes
- `templates/pages.html.hbs` - Add discovery chain breadcrumb
- `templates/queue.html.hbs` - Add link to failures dashboard

---

### Task 1: Database Schema - Create url_discovery and import_source Tables

**Files:**
- Create: `migrations/2026-06-20-100000_discovery_provenance/up.sql`
- Create: `migrations/2026-06-20-100000_discovery_provenance/down.sql`

**Interfaces:**
- Consumes: None (foundation task)
- Produces: Tables `url_discovery`, `import_source` with indexes

- [ ] **Step 1: Create migration directory**

```bash
mkdir -p migrations/2026-06-20-100000_discovery_provenance
```

- [ ] **Step 2: Write up migration with table creation**

Create `migrations/2026-06-20-100000_discovery_provenance/up.sql`:

```sql
-- Create import_source table first (no dependencies)
CREATE TABLE import_source (
  id SERIAL PRIMARY KEY,
  source_type TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMP NOT NULL DEFAULT NOW(),
  imported_by TEXT NOT NULL,
  total_urls INTEGER NOT NULL DEFAULT 0,
  metadata JSONB
);

-- Create url_discovery table
CREATE TABLE url_discovery (
  id SERIAL PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,
  discovered_from_page_id INTEGER REFERENCES page(id),
  discovery_chain INTEGER[] NOT NULL DEFAULT '{}',
  discovery_depth INTEGER NOT NULL DEFAULT 0,
  import_source_id INTEGER REFERENCES import_source(id),
  discovered_at TIMESTAMP NOT NULL DEFAULT NOW(),
  first_queued_at TIMESTAMP NOT NULL DEFAULT NOW(),
  
  CONSTRAINT url_discovery_url_unique UNIQUE (url)
);

-- Indexes for url_discovery
CREATE INDEX idx_url_discovery_depth ON url_discovery(discovery_depth);
CREATE INDEX idx_url_discovery_import_source ON url_discovery(import_source_id);
CREATE INDEX idx_url_discovery_discovered_from ON url_discovery(discovered_from_page_id);
```

- [ ] **Step 3: Write down migration**

Create `migrations/2026-06-20-100000_discovery_provenance/down.sql`:

```sql
DROP TABLE IF EXISTS url_discovery CASCADE;
DROP TABLE IF EXISTS import_source CASCADE;
```

- [ ] **Step 4: Test migration up**

```bash
diesel migration run
```

Expected: "Running migration 2026-06-20-100000_discovery_provenance"

- [ ] **Step 5: Test migration down**

```bash
diesel migration revert
```

Expected: "Rolling back migration 2026-06-20-100000_discovery_provenance"

- [ ] **Step 6: Re-run migration for development**

```bash
diesel migration run
```

- [ ] **Step 7: Commit**

```bash
git add migrations/2026-06-20-100000_discovery_provenance/
git commit -m "migration: add url_discovery and import_source tables

- url_discovery tracks discovery provenance with chain array
- import_source tracks external seed sources
- Includes indexes for depth, source, and page lookups"
```

---

### Task 2: Database Schema - Alter work_unit Table

**Files:**
- Create: `migrations/2026-06-20-110000_work_unit_discovery/up.sql`
- Create: `migrations/2026-06-20-110000_work_unit_discovery/down.sql`

**Interfaces:**
- Consumes: `url_discovery` table from Task 1
- Produces: `work_unit.url_discovery_id`, `work_unit.failure_category` columns

- [ ] **Step 1: Create migration directory**

```bash
mkdir -p migrations/2026-06-20-110000_work_unit_discovery
```

- [ ] **Step 2: Write up migration**

Create `migrations/2026-06-20-110000_work_unit_discovery/up.sql`:

```sql
ALTER TABLE work_unit 
  ADD COLUMN url_discovery_id INTEGER REFERENCES url_discovery(id),
  ADD COLUMN failure_category TEXT;

CREATE INDEX idx_work_unit_discovery ON work_unit(url_discovery_id);
CREATE INDEX idx_work_unit_failure_category ON work_unit(failure_category);
```

- [ ] **Step 3: Write down migration**

Create `migrations/2026-06-20-110000_work_unit_discovery/down.sql`:

```sql
DROP INDEX IF EXISTS idx_work_unit_failure_category;
DROP INDEX IF EXISTS idx_work_unit_discovery;

ALTER TABLE work_unit 
  DROP COLUMN IF EXISTS failure_category,
  DROP COLUMN IF EXISTS url_discovery_id;
```

- [ ] **Step 4: Test migration up**

```bash
diesel migration run
```

Expected: "Running migration 2026-06-20-110000_work_unit_discovery"

- [ ] **Step 5: Verify columns exist**

```bash
psql $DATABASE_URL -c "\d work_unit" | grep -E "url_discovery_id|failure_category"
```

Expected: Both columns shown in table description

- [ ] **Step 6: Test migration down and up**

```bash
diesel migration revert
diesel migration run
```

- [ ] **Step 7: Commit**

```bash
git add migrations/2026-06-20-110000_work_unit_discovery/
git commit -m "migration: add discovery and failure columns to work_unit

- url_discovery_id links work to provenance data
- failure_category enables bulk operations by error type
- Includes indexes for fast filtering"
```

---

### Task 3: Database Schema - Create Failure Summary Materialized View

**Files:**
- Create: `migrations/2026-06-20-120000_failure_summary_view/up.sql`
- Create: `migrations/2026-06-20-120000_failure_summary_view/down.sql`

**Interfaces:**
- Consumes: `work_unit.failure_category` from Task 2
- Produces: `work_unit_failure_summary` materialized view

- [ ] **Step 1: Create migration directory**

```bash
mkdir -p migrations/2026-06-20-120000_failure_summary_view
```

- [ ] **Step 2: Write up migration**

Create `migrations/2026-06-20-120000_failure_summary_view/up.sql`:

```sql
CREATE MATERIALIZED VIEW work_unit_failure_summary AS
SELECT 
  failure_category,
  COUNT(*) as count,
  ARRAY(
    SELECT url 
    FROM work_unit w2 
    WHERE w2.failure_category = w1.failure_category
    ORDER BY w2.last_attempt_at DESC 
    LIMIT 10
  ) as sample_urls,
  MAX(last_attempt_at) as last_failure_at
FROM work_unit w1
WHERE status IN ('failed', 'pending') 
  AND failure_category IS NOT NULL
GROUP BY failure_category;

CREATE INDEX idx_work_failure_summary_category 
  ON work_unit_failure_summary(failure_category);
```

- [ ] **Step 3: Write down migration**

Create `migrations/2026-06-20-120000_failure_summary_view/down.sql`:

```sql
DROP MATERIALIZED VIEW IF EXISTS work_unit_failure_summary CASCADE;
```

- [ ] **Step 4: Test migration up**

```bash
diesel migration run
```

Expected: "Running migration 2026-06-20-120000_failure_summary_view"

- [ ] **Step 5: Verify view exists**

```bash
psql $DATABASE_URL -c "SELECT COUNT(*) FROM work_unit_failure_summary;"
```

Expected: Returns count (0 if no failures yet)

- [ ] **Step 6: Test refresh**

```bash
psql $DATABASE_URL -c "REFRESH MATERIALIZED VIEW work_unit_failure_summary;"
```

Expected: "REFRESH MATERIALIZED VIEW"

- [ ] **Step 7: Commit**

```bash
git add migrations/2026-06-20-120000_failure_summary_view/
git commit -m "migration: add work_unit_failure_summary materialized view

Aggregates failure counts and samples by category for fast dashboard queries.
Includes index on failure_category for filtering."
```

---

### Task 4: Update Schema and Models

**Files:**
- Modify: `src/schema.rs` (auto-generated)
- Modify: `src/models.rs`

**Interfaces:**
- Consumes: Database tables from Tasks 1-3
- Produces: Rust model structs `UrlDiscovery`, `ImportSource`, `FailureCategorySummary`, `WorkUnit` (updated)

- [ ] **Step 1: Regenerate schema**

```bash
diesel print-schema > src/schema.rs
```

Expected: File updated with new tables

- [ ] **Step 2: Add UrlDiscovery models to src/models.rs**

Add after existing models:

```rust
#[derive(Selectable, Queryable, Serialize, Clone, Debug)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::url_discovery)]
pub struct UrlDiscovery {
    pub id: i32,
    pub url: String,
    pub discovered_from_page_id: Option<i32>,
    pub discovery_chain: Vec<i32>,
    pub discovery_depth: i32,
    pub import_source_id: Option<i32>,
    pub discovered_at: String,
    pub first_queued_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::url_discovery)]
pub struct NewUrlDiscovery<'a> {
    pub url: &'a str,
    pub discovered_from_page_id: Option<i32>,
    pub discovery_chain: Vec<i32>,
    pub discovery_depth: i32,
    pub import_source_id: Option<i32>,
}
```

- [ ] **Step 3: Add ImportSource models**

```rust
#[derive(Selectable, Queryable, Serialize, Clone, Debug)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::import_source)]
pub struct ImportSource {
    pub id: i32,
    pub source_type: String,
    pub source_name: String,
    pub source_url: Option<String>,
    pub imported_at: String,
    pub imported_by: String,
    pub total_urls: i32,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::import_source)]
pub struct NewImportSource<'a> {
    pub source_type: &'a str,
    pub source_name: &'a str,
    pub source_url: Option<&'a str>,
    pub imported_by: &'a str,
    pub total_urls: i32,
    pub metadata: Option<serde_json::Value>,
}
```

- [ ] **Step 4: Add FailureCategorySummary struct**

```rust
#[derive(QueryableByName, Serialize, Clone, Debug)]
#[serde(crate = "rocket::serde")]
pub struct FailureCategorySummary {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub category: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub count: i64,
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Text>)]
    pub sample_urls: Vec<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub last_failure_at: Option<String>,
}
```

- [ ] **Step 5: Build to check compilation**

```bash
cargo build --lib 2>&1 | head -20
```

Expected: Compiles successfully (warnings OK)

- [ ] **Step 6: Commit**

```bash
git add src/schema.rs src/models.rs
git commit -m "models: add url_discovery, import_source, failure summary

- UrlDiscovery tracks discovery chains with page ID arrays
- ImportSource tracks bulk import metadata
- FailureCategorySummary for materialized view queries
- Auto-regenerated schema from migrations"
```

---

### Task 5: Implement Failure Categorization Logic

**Files:**
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: None (pure function)
- Produces: `fn categorize_failure(error: &str, http_status: Option<u16>) -> String`

- [ ] **Step 1: Write test for categorize_failure**

Add to `src/lib.rs` test module:

```rust
#[test]
fn test_categorize_failure_http_status() {
    assert_eq!(categorize_failure("some error", Some(403)), "http_403_forbidden");
    assert_eq!(categorize_failure("some error", Some(404)), "http_404_not_found");
    assert_eq!(categorize_failure("some error", Some(500)), "http_5xx_server_error");
    assert_eq!(categorize_failure("some error", Some(503)), "http_5xx_server_error");
    assert_eq!(categorize_failure("some error", Some(429)), "http_429_rate_limit");
}

#[test]
fn test_categorize_failure_error_patterns() {
    assert_eq!(categorize_failure("blacklist check failed", None), "blacklisted");
    assert_eq!(categorize_failure("connection timeout", None), "timeout");
    assert_eq!(categorize_failure("connection refused", None), "connection_refused");
    assert_eq!(categorize_failure("dns lookup failed", None), "dns_failure");
    assert_eq!(categorize_failure("certificate invalid", None), "tls_error");
    assert_eq!(categorize_failure("tls handshake error", None), "tls_error");
    assert_eq!(categorize_failure("unknown error", None), "other");
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test test_categorize_failure
```

Expected: FAIL - function not defined

- [ ] **Step 3: Implement categorize_failure function**

Add to `src/lib.rs` (near other utility functions):

```rust
pub fn categorize_failure(error: &str, http_status: Option<u16>) -> String {
    match http_status {
        Some(403) => "http_403_forbidden".to_string(),
        Some(404) => "http_404_not_found".to_string(),
        Some(500..=599) => "http_5xx_server_error".to_string(),
        Some(429) => "http_429_rate_limit".to_string(),
        _ => {
            let error_lower = error.to_lowercase();
            if error_lower.contains("blacklist") {
                "blacklisted".to_string()
            } else if error_lower.contains("timeout") {
                "timeout".to_string()
            } else if error_lower.contains("connection refused") {
                "connection_refused".to_string()
            } else if error_lower.contains("dns") {
                "dns_failure".to_string()
            } else if error_lower.contains("certificate") || error_lower.contains("tls") {
                "tls_error".to_string()
            } else {
                "other".to_string()
            }
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test test_categorize_failure
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add failure categorization logic

Categorizes work_unit failures by HTTP status and error patterns.
Supports blacklist, timeout, connection, DNS, TLS, and HTTP errors."
```

---

### Task 6: Update record_work_unit_failure to Categorize Failures

**Files:**
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `categorize_failure()` from Task 5
- Produces: Updated `record_work_unit_failure()` that sets failure_category

- [ ] **Step 1: Find record_work_unit_failure function**

```bash
grep -n "pub fn record_work_unit_failure" src/lib.rs
```

Expected: Shows line number of function

- [ ] **Step 2: Read current implementation**

```bash
sed -n '<line_number>,+40p' src/lib.rs
```

- [ ] **Step 3: Update function signature to accept http_status**

Modify existing function to add `http_status: Option<u16>` parameter:

```rust
pub fn record_work_unit_failure(
    conn: &mut PgConnection,
    work_unit_id: i32,
    error: &str,
    http_status: Option<u16>,
) -> Result<()> {
```

- [ ] **Step 4: Add categorization before database update**

Add before the diesel::update call:

```rust
    let failure_category = categorize_failure(error, http_status);
```

- [ ] **Step 5: Update the diesel update to include failure_category**

Modify the `.set()` call to include:

```rust
    .set((
        status.eq("failed"),
        last_error.eq(error),
        last_attempt_at.eq(diesel::dsl::now),
        failure_category.eq(&failure_category),
    ))
```

- [ ] **Step 6: Build to check compilation**

```bash
cargo build --lib 2>&1 | grep -E "error|warning" | head -10
```

Expected: Compiles (may have warnings about unused http_status in callers)

- [ ] **Step 7: Commit**

```bash
git add src/lib.rs
git commit -m "feat: categorize failures in record_work_unit_failure

Automatically categorizes failures when recording them.
Adds http_status parameter for HTTP error categorization."
```

---

### Task 7: Implement Discovery Chain Creation for Imported URLs

**Files:**
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `url_discovery` table, `import_source` table
- Produces: `fn create_url_discovery_for_import(conn, url, import_source_id) -> Result<i32>`

- [ ] **Step 1: Write test for create_url_discovery_for_import**

Add to test module:

```rust
#[cfg(test)]
mod test_discovery {
    use super::*;
    
    #[test]
    fn test_create_url_discovery_for_import() {
        let mut conn = establish_test_connection();
        
        // Create test import source
        let import_id = diesel::insert_into(crate::schema::import_source::table)
            .values(NewImportSource {
                source_type: "test",
                source_name: "Test Import",
                source_url: None,
                imported_by: "test_suite",
                total_urls: 1,
                metadata: None,
            })
            .returning(crate::schema::import_source::id)
            .get_result::<i32>(&mut conn)
            .unwrap();
        
        let url = "http://example.onion/test";
        let discovery_id = create_url_discovery_for_import(&mut conn, url, import_id).unwrap();
        
        let discovery = crate::schema::url_discovery::table
            .find(discovery_id)
            .first::<UrlDiscovery>(&mut conn)
            .unwrap();
        
        assert_eq!(discovery.url, url);
        assert_eq!(discovery.discovery_depth, 0);
        assert_eq!(discovery.discovery_chain.len(), 0);
        assert_eq!(discovery.import_source_id, Some(import_id));
        assert_eq!(discovery.discovered_from_page_id, None);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test test_create_url_discovery_for_import
```

Expected: FAIL - function not defined

- [ ] **Step 3: Implement create_url_discovery_for_import**

Add to src/lib.rs:

```rust
pub fn create_url_discovery_for_import(
    conn: &mut PgConnection,
    url: &str,
    import_source_id: i32,
) -> Result<i32> {
    use crate::schema::url_discovery;
    
    let discovery_id = diesel::insert_into(url_discovery::table)
        .values(NewUrlDiscovery {
            url,
            discovered_from_page_id: None,
            discovery_chain: vec![],
            discovery_depth: 0,
            import_source_id: Some(import_source_id),
        })
        .on_conflict(url_discovery::url)
        .do_nothing()
        .returning(url_discovery::id)
        .get_result::<i32>(conn)
        .optional()
        .context("error creating url_discovery for import")?;
    
    match discovery_id {
        Some(id) => Ok(id),
        None => {
            // URL already exists, get existing ID
            url_discovery::table
                .filter(url_discovery::url.eq(url))
                .select(url_discovery::id)
                .first::<i32>(conn)
                .context("error fetching existing url_discovery")
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test test_create_url_discovery_for_import
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: create url_discovery entries for imported URLs

Creates discovery records with depth 0 and empty chain for seeds.
Handles duplicates gracefully with on_conflict."
```

---

### Task 8: Implement Discovery Chain Creation for Discovered Links

**Files:**
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `url_discovery` table, `page` table
- Produces: `fn create_url_discovery_for_link(conn, url, discovering_page_id) -> Result<Option<i32>>`

- [ ] **Step 1: Write test for create_url_discovery_for_link**

Add to test module:

```rust
#[test]
fn test_create_url_discovery_for_link() {
    let mut conn = establish_test_connection();
    
    // Create seed discovery (depth 0)
    let seed_page_id = 100; // Assume page exists
    let seed_discovery_id = diesel::insert_into(crate::schema::url_discovery::table)
        .values(NewUrlDiscovery {
            url: "http://seed.onion",
            discovered_from_page_id: None,
            discovery_chain: vec![],
            discovery_depth: 0,
            import_source_id: None,
        })
        .returning(crate::schema::url_discovery::id)
        .get_result::<i32>(&mut conn)
        .unwrap();
    
    // Discover new URL from seed page
    let new_url = "http://discovered.onion";
    let discovery_id = create_url_discovery_for_link(&mut conn, new_url, seed_page_id)
        .unwrap()
        .expect("Should create new discovery");
    
    let discovery = crate::schema::url_discovery::table
        .find(discovery_id)
        .first::<UrlDiscovery>(&mut conn)
        .unwrap();
    
    assert_eq!(discovery.url, new_url);
    assert_eq!(discovery.discovery_depth, 1);
    assert_eq!(discovery.discovery_chain, vec![seed_page_id]);
    assert_eq!(discovery.discovered_from_page_id, Some(seed_page_id));
    assert_eq!(discovery.import_source_id, None);
}

#[test]
fn test_create_url_discovery_for_link_already_exists() {
    let mut conn = establish_test_connection();
    
    let url = "http://existing.onion";
    
    // Create existing discovery
    diesel::insert_into(crate::schema::url_discovery::table)
        .values(NewUrlDiscovery {
            url,
            discovered_from_page_id: None,
            discovery_chain: vec![],
            discovery_depth: 0,
            import_source_id: None,
        })
        .execute(&mut conn)
        .unwrap();
    
    // Try to create again from different page
    let result = create_url_discovery_for_link(&mut conn, url, 200).unwrap();
    
    assert_eq!(result, None); // Should return None for existing URLs
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test test_create_url_discovery_for_link
```

Expected: FAIL - function not defined

- [ ] **Step 3: Implement create_url_discovery_for_link**

Add to src/lib.rs:

```rust
pub fn create_url_discovery_for_link(
    conn: &mut PgConnection,
    url: &str,
    discovering_page_id: i32,
) -> Result<Option<i32>> {
    use crate::schema::url_discovery;
    
    // Check if URL already discovered (first discovery wins)
    let existing = url_discovery::table
        .filter(url_discovery::url.eq(url))
        .select(url_discovery::id)
        .first::<i32>(conn)
        .optional()
        .context("error checking for existing url_discovery")?;
    
    if existing.is_some() {
        return Ok(None); // Already discovered, skip
    }
    
    // Get discovering page's discovery record to build chain
    let page_discovery = url_discovery::table
        .filter(url_discovery::discovered_from_page_id.eq(discovering_page_id)
            .or(url_discovery::url.eq(
                crate::schema::page::table
                    .find(discovering_page_id)
                    .select(crate::schema::page::url)
                    .single_value()
            )))
        .first::<UrlDiscovery>(conn)
        .optional()
        .context("error loading discovering page's url_discovery")?;
    
    let (mut new_chain, new_depth) = match page_discovery {
        Some(parent) => {
            let mut chain = parent.discovery_chain.clone();
            chain.push(discovering_page_id);
            let depth = chain.len() as i32;
            (chain, depth)
        }
        None => {
            // Discovering page has no discovery record (legacy data)
            // Create minimal chain
            (vec![discovering_page_id], 1)
        }
    };
    
    let discovery_id = diesel::insert_into(url_discovery::table)
        .values(NewUrlDiscovery {
            url,
            discovered_from_page_id: Some(discovering_page_id),
            discovery_chain: new_chain,
            discovery_depth: new_depth,
            import_source_id: None,
        })
        .returning(url_discovery::id)
        .get_result::<i32>(conn)
        .context("error creating url_discovery for link")?;
    
    Ok(Some(discovery_id))
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test test_create_url_discovery_for_link
```

Expected: PASS (both tests)

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: create url_discovery for organically discovered links

Builds discovery chain by appending to parent page's chain.
First discovery wins - duplicates return None."
```

---

### Task 9: Implement Bulk Failure Operations

**Files:**
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `work_unit` table, `work_unit_failure_summary` view
- Produces: `get_failure_summary()`, `bulk_retry_by_category()`, `bulk_whitelist_and_retry()`, `bulk_abandon_by_category()`

- [ ] **Step 1: Implement get_failure_summary**

Add to src/lib.rs:

```rust
pub fn get_failure_summary(conn: &mut PgConnection) -> Result<Vec<FailureCategorySummary>> {
    diesel::sql_query("SELECT * FROM work_unit_failure_summary ORDER BY count DESC")
        .load::<FailureCategorySummary>(conn)
        .context("error loading failure summary")
}
```

- [ ] **Step 2: Implement bulk_retry_by_category**

```rust
pub fn bulk_retry_by_category(
    conn: &mut PgConnection,
    category: &str,
    limit: Option<i64>,
) -> Result<i64> {
    use crate::schema::work_unit;
    
    let mut query = diesel::update(work_unit::table)
        .filter(work_unit::failure_category.eq(category))
        .filter(work_unit::status.eq("failed"))
        .into_boxed();
    
    if let Some(lim) = limit {
        // PostgreSQL doesn't support LIMIT in UPDATE directly
        // Use subquery with LIMIT
        let limited_ids: Vec<i32> = work_unit::table
            .filter(work_unit::failure_category.eq(category))
            .filter(work_unit::status.eq("failed"))
            .select(work_unit::id)
            .limit(lim)
            .load(conn)
            .context("error fetching limited work_unit ids")?;
        
        let updated = diesel::update(work_unit::table)
            .filter(work_unit::id.eq_any(limited_ids))
            .set((
                work_unit::status.eq("pending"),
                work_unit::retry_count.eq(0),
                work_unit::next_attempt_at.eq(diesel::dsl::now),
            ))
            .execute(conn)
            .context("error bulk retrying work_units")?;
        
        return Ok(updated as i64);
    }
    
    let updated = query
        .set((
            work_unit::status.eq("pending"),
            work_unit::retry_count.eq(0),
            work_unit::next_attempt_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .context("error bulk retrying work_units")?;
    
    Ok(updated as i64)
}
```

- [ ] **Step 3: Implement bulk_whitelist_and_retry**

```rust
pub fn bulk_whitelist_and_retry(
    conn: &mut PgConnection,
    urls: Vec<String>,
) -> Result<(i64, i64)> {
    use crate::schema::{domain_blacklist, work_unit};
    
    // Extract unique domains from URLs
    let mut domains = HashSet::new();
    for url_str in &urls {
        if let Ok(url) = url::Url::parse(url_str) {
            if let Some(host) = url.host_str() {
                domains.insert(host.to_string());
            }
        }
    }
    
    // Remove from blacklist
    let blacklist_removed = diesel::delete(domain_blacklist::table)
        .filter(domain_blacklist::domain.eq_any(domains))
        .execute(conn)
        .context("error removing domains from blacklist")?;
    
    // Retry work_units for these URLs
    let work_units_retried = diesel::update(work_unit::table)
        .filter(work_unit::url.eq_any(&urls))
        .filter(work_unit::status.eq("failed"))
        .set((
            work_unit::status.eq("pending"),
            work_unit::retry_count.eq(0),
            work_unit::next_attempt_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .context("error retrying whitelisted work_units")?;
    
    Ok((blacklist_removed as i64, work_units_retried as i64))
}
```

- [ ] **Step 4: Implement bulk_abandon_by_category**

```rust
pub fn bulk_abandon_by_category(
    conn: &mut PgConnection,
    category: &str,
) -> Result<i64> {
    use crate::schema::work_unit;
    
    let updated = diesel::update(work_unit::table)
        .filter(work_unit::failure_category.eq(category))
        .filter(work_unit::status.eq("failed"))
        .set(work_unit::status.eq("abandoned"))
        .execute(conn)
        .context("error abandoning work_units")?;
    
    Ok(updated as i64)
}
```

- [ ] **Step 5: Add necessary imports at top of file**

```rust
use std::collections::HashSet;
```

- [ ] **Step 6: Build to check compilation**

```bash
cargo build --lib
```

Expected: Compiles successfully

- [ ] **Step 7: Commit**

```bash
git add src/lib.rs
git commit -m "feat: implement bulk failure operations

- get_failure_summary: queries materialized view
- bulk_retry_by_category: reset failed items to pending
- bulk_whitelist_and_retry: remove from blacklist and retry
- bulk_abandon_by_category: mark as abandoned"
```

---

### Task 10: Implement Import Pipeline - File Parsing

**Files:**
- Modify: `src/bin/spyder.rs`

**Interfaces:**
- Consumes: None (reads files)
- Produces: `fn parse_import_file(path: &Path) -> Result<Vec<String>>`

- [ ] **Step 1: Add import-related structs**

Add near top of `src/bin/spyder.rs`:

```rust
struct ImportOptions {
    file_path: String,
    source_type: String,
    source_name: String,
    source_url: Option<String>,
}

struct ImportResult {
    total_in_file: usize,
    queued_count: usize,
    duplicate_count: usize,
    blacklisted_count: usize,
    import_source_id: i32,
}
```

- [ ] **Step 2: Implement parse_import_file function**

Add to `src/bin/spyder.rs`:

```rust
fn parse_import_file(path: &Path) -> Result<Vec<String>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read file: {}", path.display()))?;
    
    // Try JSON first
    if path.extension().and_then(|s| s.to_str()) == Some("json") {
        // Try JSON array of strings
        if let Ok(urls) = serde_json::from_str::<Vec<String>>(&content) {
            return Ok(urls);
        }
        
        // Try JSON array of objects with "url" field
        if let Ok(objects) = serde_json::from_str::<Vec<serde_json::Value>>(&content) {
            let urls = objects
                .iter()
                .filter_map(|obj| obj.get("url")?.as_str().map(String::from))
                .collect();
            return Ok(urls);
        }
        
        bail!("JSON file must be array of URLs or objects with 'url' field");
    }
    
    // Try CSV
    if path.extension().and_then(|s| s.to_str()) == Some("csv") {
        let mut reader = csv::Reader::from_reader(content.as_bytes());
        let mut urls = Vec::new();
        
        for result in reader.records() {
            let record = result.context("error parsing CSV record")?;
            if let Some(url) = record.get(0) {
                if !url.is_empty() {
                    urls.push(url.to_string());
                }
            }
        }
        
        return Ok(urls);
    }
    
    // Default: plain text (one URL per line)
    let urls: Vec<String> = content
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(String::from)
        .collect();
    
    Ok(urls)
}
```

- [ ] **Step 3: Add CSV dependency to Cargo.toml**

```toml
csv = "1.3"
```

- [ ] **Step 4: Build to check compilation**

```bash
cargo build --bin spyder 2>&1 | head -20
```

Expected: Compiles successfully

- [ ] **Step 5: Create test files for manual verification**

```bash
# Plain text
echo -e "http://test1.onion\nhttp://test2.onion" > /tmp/test_plain.txt

# JSON array
echo '["http://test3.onion", "http://test4.onion"]' > /tmp/test_json.json

# CSV
echo -e "url,title\nhttp://test5.onion,Test5\nhttp://test6.onion,Test6" > /tmp/test_csv.csv
```

- [ ] **Step 6: Commit**

```bash
git add src/bin/spyder.rs Cargo.toml
git commit -m "feat: implement import file parsing

Supports plain text, JSON array, JSON objects, and CSV formats.
Auto-detects based on file extension."
```

---

### Task 11: Implement Import Pipeline - CLI Command

**Files:**
- Modify: `src/bin/spyder.rs`

**Interfaces:**
- Consumes: `parse_import_file()` from Task 10, `create_url_discovery_for_import()` from Task 7
- Produces: CLI `import` subcommand

- [ ] **Step 1: Find main CLI argument parsing**

```bash
grep -n "fn main" src/bin/spyder.rs
```

- [ ] **Step 2: Add import subcommand to argument parser**

Locate the match statement on subcommands and add:

```rust
("import", Some(matches)) => {
    let file_path = matches.get_one::<String>("file").unwrap();
    let source_type = matches.get_one::<String>("source-type").unwrap();
    let source_name = matches.get_one::<String>("source-name").unwrap();
    let source_url = matches.get_one::<String>("source-url").map(|s| s.as_str());
    
    let options = ImportOptions {
        file_path: file_path.clone(),
        source_type: source_type.clone(),
        source_name: source_name.clone(),
        source_url: source_url.map(String::from),
    };
    
    let result = run_import(&mut connection, &options)?;
    
    println!("Import complete: {}", file_path);
    println!("- Total URLs in file: {}", result.total_in_file);
    println!("- New URLs queued: {}", result.queued_count);
    println!("- Already known (skipped): {}", result.duplicate_count);
    println!("- Blacklisted (skipped): {}", result.blacklisted_count);
    println!("- Import source ID: {}", result.import_source_id);
}
```

- [ ] **Step 3: Add import command definition to CLI args**

Find where Commands are defined and add:

```rust
.subcommand(
    Command::new("import")
        .about("Import URLs from external file (Ahmia, OnionTree, etc.)")
        .arg(Arg::new("file")
            .required(true)
            .help("Path to import file"))
        .arg(Arg::new("source-type")
            .long("source-type")
            .required(true)
            .help("Source type (manual, ahmia, oniontree, paste, custom)"))
        .arg(Arg::new("source-name")
            .long("source-name")
            .required(true)
            .help("Human-readable source name"))
        .arg(Arg::new("source-url")
            .long("source-url")
            .help("Optional URL where file was obtained"))
)
```

- [ ] **Step 4: Implement run_import function**

```rust
fn run_import(
    connection: &mut PgConnection,
    options: &ImportOptions,
) -> Result<ImportResult> {
    use spyder::{create_url_discovery_for_import, create_work_unit, normalize_crawl_url, 
                 list_domain_blacklist_rules};
    
    let path = Path::new(&options.file_path);
    let urls = parse_import_file(path)?;
    let total_in_file = urls.len();
    
    // Calculate file hash
    let file_content = std::fs::read(path)?;
    let file_hash = format!("sha256:{:x}", sha2::Sha256::digest(&file_content));
    
    // Create import_source record
    let metadata = serde_json::json!({
        "filename": path.file_name().and_then(|s| s.to_str()).unwrap_or("unknown"),
        "file_hash": file_hash,
        "format": path.extension().and_then(|s| s.to_str()).unwrap_or("txt"),
    });
    
    let import_source_id = diesel::insert_into(crate::schema::import_source::table)
        .values(spyder::models::NewImportSource {
            source_type: &options.source_type,
            source_name: &options.source_name,
            source_url: options.source_url.as_deref(),
            imported_by: "cli",
            total_urls: 0, // Will update after processing
            metadata: Some(metadata),
        })
        .returning(crate::schema::import_source::id)
        .get_result::<i32>(connection)
        .context("error creating import_source record")?;
    
    // Load blacklist
    let blacklist_domains: Vec<String> = list_domain_blacklist_rules(connection)?
        .into_iter()
        .map(|rule| rule.domain)
        .collect();
    
    let mut queued_count = 0;
    let mut duplicate_count = 0;
    let mut blacklisted_count = 0;
    
    // Process URLs in batches
    for url in urls {
        let normalized = normalize_crawl_url(&url);
        
        // Check blacklist
        if spyder::url_matches_blacklist(&normalized, &blacklist_domains) {
            blacklisted_count += 1;
            continue;
        }
        
        // Create discovery record
        match create_url_discovery_for_import(connection, &normalized, import_source_id) {
            Ok(discovery_id) => {
                // Create work_unit linked to discovery
                // Note: work_unit creation will need updating to link discovery_id
                match create_work_unit(connection, &normalized) {
                    Ok(_) => queued_count += 1,
                    Err(_) => duplicate_count += 1,
                }
            }
            Err(_) => duplicate_count += 1,
        }
    }
    
    // Update import_source with final count
    diesel::update(crate::schema::import_source::table.find(import_source_id))
        .set(crate::schema::import_source::total_urls.eq(queued_count as i32))
        .execute(connection)
        .context("error updating import_source total_urls")?;
    
    Ok(ImportResult {
        total_in_file,
        queued_count,
        duplicate_count,
        blacklisted_count,
        import_source_id,
    })
}
```

- [ ] **Step 5: Add sha2 use statement**

```rust
use sha2::Digest;
```

- [ ] **Step 6: Build to check compilation**

```bash
cargo build --bin spyder
```

Expected: Compiles successfully

- [ ] **Step 7: Test import command with test file**

```bash
cargo run --bin spyder -- import /tmp/test_plain.txt --source-type custom --source-name "Test Import"
```

Expected: Shows import summary with 2 URLs queued

- [ ] **Step 8: Commit**

```bash
git add src/bin/spyder.rs
git commit -m "feat: add import CLI command

Imports URLs from files with automatic format detection.
Creates import_source records and url_discovery entries.
Reports summary of queued/skipped URLs."
```

---

### Task 12: Update create_work_unit to Link url_discovery

**Files:**
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `url_discovery` table
- Produces: Updated `create_work_unit()` that links url_discovery_id

- [ ] **Step 1: Find create_work_unit function**

```bash
grep -n "pub fn create_work_unit" src/lib.rs
```

- [ ] **Step 2: Read current implementation**

```bash
sed -n '<line_number>,+20p' src/lib.rs
```

- [ ] **Step 3: Update function to look up url_discovery_id**

Modify the function to query url_discovery before inserting:

```rust
pub fn create_work_unit(conn: &mut PgConnection, url: &str) -> Result<()> {
    use crate::schema::{work_unit, url_discovery};
    
    // Look up url_discovery_id for this URL
    let discovery_id = url_discovery::table
        .filter(url_discovery::url.eq(url))
        .select(url_discovery::id)
        .first::<i32>(conn)
        .optional()
        .context("error looking up url_discovery")?;
    
    diesel::insert_into(work_unit::table)
        .values((
            work_unit::url.eq(url),
            work_unit::url_discovery_id.eq(discovery_id),
        ))
        .on_conflict(work_unit::url)
        .do_nothing()
        .execute(conn)
        .context("error creating work unit")?;
    Ok(())
}
```

- [ ] **Step 4: Build to check compilation**

```bash
cargo build --lib
```

Expected: Compiles successfully

- [ ] **Step 5: Test that existing work_unit creation still works**

```bash
cargo test create_work_unit
```

Expected: Tests pass (or skip if no tests exist)

- [ ] **Step 6: Commit**

```bash
git add src/lib.rs
git commit -m "feat: link work_unit to url_discovery on creation

Looks up url_discovery_id when creating work_units.
Maintains backward compatibility with NULL discovery_id for legacy entries."
```

---

## Remaining Implementation Tasks

The plan above covers the core backend (database schema, models, discovery logic, failure operations, and import pipeline). The remaining frontend work follows established Spyder patterns:

### Tasks 13-16: Frontend Implementation

**Task 13:** Add `/api/failures/summary` endpoint in `src/bin/frontend.rs`
- Route handler calling `get_failure_summary()`
- Returns JSON for failure dashboard cards

**Task 14:** Add `/api/queue/bulk-retry` POST endpoint
- Accepts `{category, action}` JSON
- Calls `bulk_retry_by_category()` or `bulk_abandon_by_category()`
- Returns `{updated: count}`

**Task 15:** Create `/discovery` route and template
- Route handler in `src/bin/frontend.rs` fetching `url_discovery` records with filters
- Template `templates/discovery.html.hbs` showing discovery chains and stats
- Follow existing template patterns from `templates/relationships.html.hbs`

**Task 16:** Create `/queue/failures` route and template
- Route handler calling `get_failure_summary()`
- Template `templates/queue_failures.html.hbs` with failure cards
- JavaScript for expand/collapse and bulk action modals
- Follow existing patterns from `templates/sites.html.hbs`

**Task 17:** Update `/pages/{id}` detail view
- Modify `src/bin/frontend.rs` page detail handler to include url_discovery lookup
- Update `templates/page_details.html.hbs` to show discovery breadcrumb
- Add "Discovered from:" section with chain visualization

**Task 18:** Add navigation links
- Update `templates/base.html.hbs` or navigation component
- Add "Discovery Explorer" and "Queue Health" links to main nav

### Task 19: Integration Testing

**Manual test plan:**

1. **Import test:**
   ```bash
   echo "http://test1.onion" > /tmp/seeds.txt
   cargo run --bin spyder -- import /tmp/seeds.txt --source-type custom --source-name "Test Seeds"
   ```
   Verify: Import succeeds, url_discovery and work_unit created

2. **Crawler discovery chain test:**
   - Run crawler on imported seed
   - Check that discovered links create url_discovery with depth=1
   - Verify discovery_chain contains seed page ID

3. **Failure categorization test:**
   - Trigger failures (blacklist, timeout)
   - Check work_unit.failure_category populated correctly
   - Refresh materialized view and check summary

4. **Bulk operations test:**
   - Visit `/queue/failures`
   - Verify cards show correct counts
   - Test bulk retry and verify status changes

5. **Discovery explorer test:**
   - Visit `/discovery`
   - Search for imported URL
   - Verify chain visualization shows correctly

### Task 20: Documentation

**User documentation:**

Create `docs/IMPORT_GUIDE.md`:
```markdown
# Importing External URL Sources

## Overview
The import command allows bulk-adding URLs from external sources like Ahmia dumps, OnionTree exports, or custom lists.

## Usage
cargo run --bin spyder -- import <file> --source-type <type> --source-name <name>

## Supported Formats
- Plain text (one URL per line)
- JSON array: ["url1", "url2"]
- JSON objects: [{"url": "...", ...}]
- CSV with URL in first column

## Example
cargo run --bin spyder -- import ahmia-2026-06-20.json \
  --source-type ahmia \
  --source-name "Ahmia June 2026" \
  --source-url "https://ahmia.fi/dumps/2026-06-20"
```

**Update README.md:**
- Add "Discovery Provenance" section
- Document import command
- Link to `/discovery` and `/queue/failures` pages

---

## Self-Review Checklist

✅ **Spec coverage:**
- Database schema: Tasks 1-3 (url_discovery, import_source, work_unit updates, materialized view)
- Discovery chain logic: Tasks 7-8 (import creation, link creation)
- Failure categorization: Tasks 5-6 (categorize function, integration)
- Bulk operations: Task 9 (all four bulk operation functions)
- Import pipeline: Tasks 10-11 (file parsing, CLI command)
- Frontend: Tasks 13-18 outlined (routes, templates, integration)
- Testing: Task 19 (manual test plan)
- Documentation: Task 20 (guides and README updates)

✅ **Placeholder scan:**
- No "TBD" or "TODO" in core tasks 1-12
- Frontend tasks 13-18 are outlined with clear structure to follow existing patterns
- All code blocks complete and executable

✅ **Type consistency:**
- `UrlDiscovery` model defined in Task 4, used in Tasks 7-8
- `ImportSource` model defined in Task 4, used in Task 11
- `FailureCategorySummary` defined in Task 4, used in Task 9
- `categorize_failure()` defined in Task 5, used in Task 6
- Function signatures match across all tasks

✅ **Interfaces clearly defined:**
- Each task specifies what it consumes and produces
- Dependencies between tasks explicit
- Model structs and function signatures complete

---

## Plan Summary

This plan implements discovery provenance and queue recovery in 20 tasks:

**Foundation (Tasks 1-4):** Database schema and Rust models
**Core Logic (Tasks 5-8):** Failure categorization and discovery chain creation
**Bulk Operations (Task 9):** Failure recovery functions
**Import Pipeline (Tasks 10-12):** CLI command and integration
**Frontend (Tasks 13-18):** API routes and UI templates  
**Polish (Tasks 19-20):** Testing and documentation

**Estimated completion time:** 2-3 days for experienced developer
**Testing approach:** TDD for core logic (Tasks 5-8), manual verification for CLI/UI
**Commit frequency:** One commit per task (20 commits total)