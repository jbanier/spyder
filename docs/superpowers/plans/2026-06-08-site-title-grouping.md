# Site Title Grouping and Page Counting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add title-based grouping to sites view, tracking unique page counts per host and allowing operators to see which sites have multiple domains with the same title.

**Architecture:** Add `title` column to `site_profile` table, modify `save_page_info()` to track unique URLs and set titles from first pages, create new `/sites/grouped` route with aggregated queries, keep existing `/sites` view unchanged.

**Tech Stack:** Rust, Diesel ORM, PostgreSQL, Rocket web framework, Handlebars templates

---

## File Structure

**New files:**
- `migrations_postgres/2026-06-08-add-site-profile-title/up.sql` - Add title column and indexes
- `migrations_postgres/2026-06-08-add-site-profile-title/down.sql` - Rollback migration
- `templates/sites_grouped.html.hbs` - Grouped sites view template

**Modified files:**
- `src/schema.rs` - Add title field to site_profile table definition
- `src/models.rs` - Add title field to SiteProfileRecord and NewSiteProfile structs, add new group structs
- `src/lib.rs:1740-2100` - Modify save_page_info() for page counting and title tracking
- `src/lib.rs:1380-1430` - Add list_site_profiles_grouped() function
- `src/bin/frontend.rs` - Add /sites/grouped route handler
- `templates/sites.html.hbs` - Add link to grouped view

---

### Task 1: Database Migration - Add Title Column

**Files:**
- Create: `migrations_postgres/2026-06-08-add-site-profile-title/up.sql`
- Create: `migrations_postgres/2026-06-08-add-site-profile-title/down.sql`

- [ ] **Step 1: Create migration directory**

```bash
mkdir -p migrations_postgres/2026-06-08-add-site-profile-title
```

- [ ] **Step 2: Write up migration**

Create `migrations_postgres/2026-06-08-add-site-profile-title/up.sql`:

```sql
-- Add title column to site_profile table
ALTER TABLE site_profile ADD COLUMN title TEXT;

-- Create indexes for performance
CREATE INDEX idx_site_profile_title ON site_profile(title);
CREATE INDEX idx_site_profile_title_notnull ON site_profile(title) 
    WHERE title IS NOT NULL;
CREATE INDEX idx_site_profile_title_pages ON site_profile(title, page_count DESC) 
    WHERE title IS NOT NULL;
```

- [ ] **Step 3: Write down migration**

Create `migrations_postgres/2026-06-08-add-site-profile-title/down.sql`:

```sql
-- Drop indexes
DROP INDEX IF EXISTS idx_site_profile_title_pages;
DROP INDEX IF EXISTS idx_site_profile_title_notnull;
DROP INDEX IF EXISTS idx_site_profile_title;

-- Drop title column
ALTER TABLE site_profile DROP COLUMN IF EXISTS title;
```

- [ ] **Step 4: Run migration**

Run: `diesel migration run`
Expected: Migration succeeds, title column and indexes created

- [ ] **Step 5: Verify migration**

Run: `psql $DATABASE_URL -c "\d site_profile"`
Expected: title column visible in schema with TEXT type

- [ ] **Step 6: Commit**

```bash
git add migrations_postgres/2026-06-08-add-site-profile-title/
git commit -m "feat: add title column to site_profile table with indexes"
```

---

### Task 2: Update Schema and Models

**Files:**
- Modify: `src/schema.rs:292-307`
- Modify: `src/models.rs:371-398`

- [ ] **Step 1: Regenerate Diesel schema**

Run: `diesel print-schema > src/schema.rs`
Expected: Schema updated with title field

- [ ] **Step 2: Add title to SiteProfileRecord**

Update `src/models.rs` line 371-384:

```rust
pub struct SiteProfileRecord {
    pub id: i32,
    pub host: String,
    pub category: String,
    pub confidence: String,
    pub score: i32,
    pub page_count: i32,
    pub title: Option<String>,  // ADD THIS LINE
    pub first_found_at: String,
    pub last_scanned_at: String,
    pub evidence: String,
    pub source_page_id: Option<i32>,
    pub last_classified_at: String,
    pub created_at: String,
}
```

- [ ] **Step 3: Add title to NewSiteProfile**

Update `src/models.rs` line 386-398:

```rust
#[derive(Insertable)]
#[diesel(table_name = crate::schema::site_profile)]
pub struct NewSiteProfile {
    pub host: String,
    pub category: String,
    pub confidence: String,
    pub score: i32,
    pub page_count: i32,
    pub title: Option<String>,  // ADD THIS LINE
    pub first_found_at: String,
    pub last_scanned_at: String,
    pub evidence: String,
    pub source_page_id: Option<i32>,
}
```

- [ ] **Step 4: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully (may have warnings about unused fields)

- [ ] **Step 5: Commit**

```bash
git add src/schema.rs src/models.rs
git commit -m "feat: add title field to site profile models"
```

---

### Task 3: Add Group Data Structures

**Files:**
- Modify: `src/models.rs:910-950` (after existing structs)

- [ ] **Step 1: Add SiteProfileGroupHost struct**

Add to `src/models.rs` after line 897 (after SiteProfileSummary):

```rust
#[derive(Serialize, Clone, Debug)]
#[serde(crate = "rocket::serde")]
pub struct SiteProfileGroupHost {
    pub host: String,
    pub category: String,
    pub confidence: String,
    pub page_count: i32,
    pub last_scanned_at: String,
    pub last_classified_at: String,
}
```

- [ ] **Step 2: Add SiteProfileGroupSummary struct**

Add to `src/models.rs` after the previous struct:

```rust
#[derive(Serialize, Clone, Debug)]
#[serde(crate = "rocket::serde")]
pub struct SiteProfileGroupSummary {
    pub title: String,
    pub host_count: usize,
    pub total_pages: i32,
    pub most_recent_scan: String,
    pub hosts: Vec<SiteProfileGroupHost>,
}
```

- [ ] **Step 3: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully

- [ ] **Step 4: Commit**

```bash
git add src/models.rs
git commit -m "feat: add site profile group data structures"
```

---

### Task 4: Modify save_page_info - Check for Existing URL

**Files:**
- Modify: `src/lib.rs:1740-1800`

- [ ] **Step 1: Add URL existence check**

Modify `save_page_info()` in `src/lib.rs` after line 1751 (after `let mut snapshot = normalize_page_snapshot(snapshot);`):

```rust
    let mut snapshot = normalize_page_snapshot(snapshot);
    let page_host = host_from_url(&snapshot.url);
    
    // Check if this URL already exists (to track new vs rescans)
    let existing_page_id = crate::schema::page::table
        .filter(crate::schema::page::url.eq(&snapshot.url))
        .select(crate::schema::page::id)
        .first::<i32>(conn)
        .optional()
        .context("error checking existing page")?;
    
    let is_new_url = existing_page_id.is_none();
    
    let blacklist_domains = load_blacklist_domains(conn)?;
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully

- [ ] **Step 3: Test with existing code**

Run: `cargo test --lib` (will test existing functionality still works)
Expected: Tests pass

- [ ] **Step 4: Commit**

```bash
git add src/lib.rs
git commit -m "feat: check for existing URL in save_page_info"
```

---

### Task 5: Modify save_page_info - Update Page Count Logic

**Files:**
- Modify: `src/lib.rs:2020-2100`

- [ ] **Step 1: Add page count increment logic**

After the site_profile computation and upsert code (around line 2030), add:

```rust
        // After site profile upsert...
        
        // Update page count if this is a new URL
        if is_new_url {
            // Increment page_count for this host
            diesel::update(site_profile::table.filter(site_profile::host.eq(&page_host)))
                .set(site_profile::page_count.eq(site_profile::page_count + 1))
                .execute(conn)
                .context("error incrementing page count")?;
        }
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully

- [ ] **Step 3: Commit**

```bash
git add src/lib.rs
git commit -m "feat: increment page_count for new URLs in save_page_info"
```

---

### Task 6: Modify save_page_info - Set Title from First Page

**Files:**
- Modify: `src/lib.rs:2030-2100`

- [ ] **Step 1: Add title tracking logic**

After the page count increment, add:

```rust
        // Set title from first page
        if is_new_url {
            // Check current page_count (after increment)
            let current_profile = site_profile::table
                .filter(site_profile::host.eq(&page_host))
                .first::<SiteProfileRecord>(conn)
                .optional()
                .context("error loading site profile")?;
            
            if let Some(profile) = current_profile {
                // If this is the first page (page_count == 1), set title
                if profile.page_count == 1 && profile.title.is_none() {
                    diesel::update(site_profile::table.filter(site_profile::host.eq(&page_host)))
                        .set(site_profile::title.eq(&snapshot.title))
                        .execute(conn)
                        .context("error setting site title")?;
                }
            }
        }
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully

- [ ] **Step 3: Commit**

```bash
git add src/lib.rs
git commit -m "feat: set site title from first page in save_page_info"
```

---

### Task 7: Add list_site_profiles_grouped Function

**Files:**
- Modify: `src/lib.rs` (add after line 1430, after list_site_profiles)

- [ ] **Step 1: Add helper struct for query results**

Add before the function:

```rust
#[derive(QueryableByName)]
struct SiteProfileGroupRow {
    #[diesel(sql_type = Text)]
    title: String,
    #[diesel(sql_type = BigInt)]
    host_count: i64,
    #[diesel(sql_type = BigInt)]
    total_pages: i64,
    #[diesel(sql_type = Text)]
    most_recent_scan: String,
    #[diesel(sql_type = Text)]
    hosts_json: String,
}
```

- [ ] **Step 2: Add list_site_profiles_grouped function**

```rust
pub fn list_site_profiles_grouped(
    conn: &mut PgConnection,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<SiteProfileGroupSummary>> {
    let limit = limit.unwrap_or(50).min(200);
    let offset = offset.unwrap_or(0);
    
    let query = format!(
        r#"
        SELECT 
            title,
            COUNT(DISTINCT host)::bigint as host_count,
            SUM(page_count)::bigint as total_pages,
            MAX(last_scanned_at) as most_recent_scan,
            json_agg(json_build_object(
                'host', host,
                'category', category,
                'confidence', confidence,
                'page_count', page_count,
                'last_scanned_at', last_scanned_at,
                'last_classified_at', last_classified_at
            ) ORDER BY page_count DESC)::text as hosts_json
        FROM site_profile
        WHERE title IS NOT NULL
        GROUP BY title
        ORDER BY total_pages DESC
        LIMIT {} OFFSET {}
        "#,
        limit, offset
    );
    
    let rows = sql_query(&query)
        .load::<SiteProfileGroupRow>(conn)
        .context("error loading grouped site profiles")?;
    
    let mut results = Vec::new();
    for row in rows {
        let hosts: Vec<SiteProfileGroupHost> = serde_json::from_str(&row.hosts_json)
            .context("error parsing hosts JSON")?;
        
        results.push(SiteProfileGroupSummary {
            title: row.title,
            host_count: row.host_count as usize,
            total_pages: row.total_pages as i32,
            most_recent_scan: row.most_recent_scan,
            hosts,
        });
    }
    
    Ok(results)
}
```

- [ ] **Step 3: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully

- [ ] **Step 4: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add list_site_profiles_grouped query function"
```

---

### Task 8: Add Backfill Function

**Files:**
- Modify: `src/lib.rs` (add after list_site_profiles_grouped)

- [ ] **Step 1: Add backfill_site_titles function**

```rust
pub fn backfill_site_titles(conn: &mut PgConnection) -> Result<usize> {
    use crate::schema::{page, site_profile};
    
    let query = r#"
        UPDATE site_profile sp
        SET title = (
            SELECT p.title
            FROM page p
            WHERE lower(p.url) LIKE '%' || lower(sp.host) || '%'
            ORDER BY p.created_at ASC
            LIMIT 1
        )
        WHERE sp.title IS NULL
          AND EXISTS (
              SELECT 1 FROM page p
              WHERE lower(p.url) LIKE '%' || lower(sp.host) || '%'
          )
    "#;
    
    let updated = sql_query(query)
        .execute(conn)
        .context("error backfilling site titles")?;
    
    Ok(updated)
}
```

- [ ] **Step 2: Add CLI command to spyder binary**

Add to `src/bin/spyder.rs` in the command match statement (around line 100):

```rust
        "backfill-titles" => {
            let mut conn = establish_connection(&database_url)?;
            let updated = spyder::backfill_site_titles(&mut conn)?;
            println!("Backfilled {} site titles", updated);
        }
```

- [ ] **Step 3: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully

- [ ] **Step 4: Test backfill**

Run: `cargo run --bin spyder -- backfill-titles`
Expected: Backfills titles for existing site_profiles

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs src/bin/spyder.rs
git commit -m "feat: add backfill_site_titles function and CLI command"
```

---

### Task 9: Add /sites/grouped Route

**Files:**
- Modify: `src/bin/frontend.rs` (add after /sites route, around line 800)

- [ ] **Step 1: Add route handler**

```rust
#[get("/sites/grouped?<limit>&<offset>")]
fn sites_grouped(
    limit: Option<i64>,
    offset: Option<i64>,
    state: &State<FrontendState>,
) -> Result<Template, FrontendError> {
    let mut conn = state.pool.get()?;
    let groups = spyder::list_site_profiles_grouped(&mut conn, limit, offset)?;
    let limit = limit.unwrap_or(50);
    let offset = offset.unwrap_or(0);
    
    let context = json!({
        "groups": groups,
        "limit": limit,
        "offset": offset,
        "next_offset": offset + limit,
        "prev_offset": if offset >= limit { offset - limit } else { 0 },
        "has_next": groups.len() >= limit as usize,
        "has_prev": offset > 0,
    });
    
    Ok(Template::render("sites_grouped", context))
}
```

- [ ] **Step 2: Register route**

Add to the routes list in `src/bin/frontend.rs` `rocket()` function:

```rust
        .mount(
            "/",
            routes![
                // ... existing routes ...
                sites_grouped,
            ],
        )
```

- [ ] **Step 3: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully

- [ ] **Step 4: Commit**

```bash
git add src/bin/frontend.rs
git commit -m "feat: add /sites/grouped route handler"
```

---

### Task 10: Create Grouped Sites Template

**Files:**
- Create: `templates/sites_grouped.html.hbs`

- [ ] **Step 1: Create template file**

Create `templates/sites_grouped.html.hbs`:

```handlebars
<!DOCTYPE html>
<html>
<head>
    <title>Sites Grouped by Title - Spyder</title>
    <style>
        body { font-family: sans-serif; margin: 20px; }
        .header { margin-bottom: 20px; }
        .site-group { border: 1px solid #ddd; margin-bottom: 10px; border-radius: 4px; }
        .group-header {
            padding: 12px;
            background: #f5f5f5;
            cursor: pointer;
            user-select: none;
        }
        .group-header:hover { background: #e8e8e8; }
        .group-hosts { padding: 8px; background: #fff; display: none; }
        .group-hosts.expanded { display: block; }
        .host-row { padding: 8px; border-top: 1px solid #eee; }
        .expand-icon {
            display: inline-block;
            width: 20px;
            font-family: monospace;
        }
        .badge {
            display: inline-block;
            padding: 2px 8px;
            background: #007bff;
            color: white;
            border-radius: 3px;
            font-size: 12px;
            margin-left: 8px;
        }
        .category-badge {
            padding: 2px 6px;
            background: #6c757d;
            color: white;
            border-radius: 3px;
            font-size: 11px;
        }
        .meta { color: #666; font-size: 12px; margin-left: 12px; }
        .pagination { margin-top: 20px; }
        .pagination a { margin-right: 10px; padding: 5px 10px; border: 1px solid #ddd; text-decoration: none; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Sites Grouped by Title</h1>
        <p>Showing sites grouped by their page titles. <a href="/sites">View ungrouped list</a></p>
    </div>

    <div class="site-groups">
        {{#each groups}}
        <div class="site-group">
            <div class="group-header" onclick="toggleGroup(this)">
                <span class="expand-icon">▶</span>
                <strong>{{title}}</strong>
                <span class="badge">{{host_count}} hosts</span>
                <span class="badge">{{total_pages}} pages</span>
                <span class="meta">Most recent: {{most_recent_scan}}</span>
            </div>
            <div class="group-hosts">
                {{#each hosts}}
                <div class="host-row">
                    <span class="host">{{host}}</span>
                    <span class="category-badge">{{category}}</span>
                    <span>{{page_count}} pages</span>
                    <span class="meta">Last scanned: {{last_scanned_at}}</span>
                </div>
                {{/each}}
            </div>
        </div>
        {{/each}}
    </div>

    <div class="pagination">
        {{#if has_prev}}
        <a href="/sites/grouped?limit={{limit}}&offset={{prev_offset}}">Previous</a>
        {{/if}}
        {{#if has_next}}
        <a href="/sites/grouped?limit={{limit}}&offset={{next_offset}}">Next</a>
        {{/if}}
    </div>

    <script>
        function toggleGroup(header) {
            const hostsDiv = header.nextElementSibling;
            const icon = header.querySelector('.expand-icon');
            
            if (hostsDiv.classList.contains('expanded')) {
                hostsDiv.classList.remove('expanded');
                icon.textContent = '▶';
            } else {
                hostsDiv.classList.add('expanded');
                icon.textContent = '▼';
            }
        }
    </script>
</body>
</html>
```

- [ ] **Step 2: Test rendering**

Run: `cargo run --bin frontend`
Navigate to: `http://localhost:8000/sites/grouped`
Expected: Page loads, groups display

- [ ] **Step 3: Commit**

```bash
git add templates/sites_grouped.html.hbs
git commit -m "feat: add sites grouped by title template"
```

---

### Task 11: Add Link to Grouped View

**Files:**
- Modify: `templates/sites.html.hbs`

- [ ] **Step 1: Find sites template**

Run: `grep -n "<h1>" templates/sites.html.hbs | head -1`
Expected: Find header location

- [ ] **Step 2: Add link to grouped view**

Add after the `<h1>` tag in `templates/sites.html.hbs`:

```handlebars
<h1>Sites</h1>
<p><a href="/sites/grouped">View Sites Grouped by Title</a></p>
```

- [ ] **Step 3: Test navigation**

Run: `cargo run --bin frontend`
Navigate to: `http://localhost:8000/sites`
Expected: Link to grouped view visible and works

- [ ] **Step 4: Commit**

```bash
git add templates/sites.html.hbs
git commit -m "feat: add link to grouped view from sites page"
```

---

### Task 12: End-to-End Testing

**Files:**
- No file changes, testing only

- [ ] **Step 1: Test new URL tracking**

```bash
# Add a test site
cargo run --bin spyder -- add http://testsite.example.com/page1
cargo run --bin spyder -- work --limit 1

# Check page_count increased
psql $DATABASE_URL -c "SELECT host, page_count, title FROM site_profile WHERE host LIKE '%testsite%';"
```
Expected: page_count = 1, title set from page

- [ ] **Step 2: Test rescan doesn't increment**

```bash
# Rescan same URL
cargo run --bin spyder -- add http://testsite.example.com/page1
cargo run --bin spyder -- work --limit 1

# Check page_count unchanged
psql $DATABASE_URL -c "SELECT host, page_count FROM site_profile WHERE host LIKE '%testsite%';"
```
Expected: page_count still = 1

- [ ] **Step 3: Test grouped view query**

```bash
# Create sites with same title
psql $DATABASE_URL -c "UPDATE site_profile SET title = 'Test Site' WHERE host IN (SELECT host FROM site_profile LIMIT 3);"

# Query grouped view
psql $DATABASE_URL -c "
    SELECT title, COUNT(DISTINCT host) as hosts, SUM(page_count) as pages
    FROM site_profile
    WHERE title IS NOT NULL
    GROUP BY title
    ORDER BY pages DESC
    LIMIT 5;
"
```
Expected: Grouped results with aggregated stats

- [ ] **Step 4: Test frontend**

Navigate to `http://localhost:8000/sites/grouped`
Test:
- Groups display correctly
- Expand/collapse works
- Pagination works
- Link back to `/sites` works

- [ ] **Step 5: Document test results**

Create a test summary in commit message for documentation

---

## Self-Review Checklist

**Spec Coverage:**
- ✅ Database migration (Task 1)
- ✅ Schema and model updates (Tasks 2-3)
- ✅ Page counting logic (Tasks 4-6)
- ✅ Grouped query function (Task 7)
- ✅ Backfill function (Task 8)
- ✅ Frontend route (Task 9)
- ✅ Template (Task 10)
- ✅ Navigation (Task 11)
- ✅ Testing (Task 12)

**Placeholders:** None - all code is complete

**Type Consistency:**
- SiteProfileRecord.title: Option<String> ✓
- SiteProfileGroupSummary fields match query results ✓
- Function signatures consistent ✓
