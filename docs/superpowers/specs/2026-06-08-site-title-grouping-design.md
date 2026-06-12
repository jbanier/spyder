# Site Title Grouping and Page Counting Design

## Overview

Add the ability to group sites by their title in the `/sites` view, showing all hosts that share the same title together with aggregated page counts. Track unique page counts per host to show how many distinct pages have been crawled for each site.

## Requirements

- Add a `title` field to site_profile table (populated from first page discovered for each host)
- Track unique URL counts per host in the page_count field
- Create a new `/sites/grouped` view that groups sites by title
- Show aggregated stats: number of hosts per title, total pages across all hosts
- Allow expanding each title group to see individual hosts
- Keep the existing `/sites` view unchanged for backward compatibility
- Only show hosts with at least one crawled page in the grouped view

## User Stories

**As a crawler operator, I want to:**
- See which sites have multiple mirrors/domains (same title, different hosts)
- Understand how many pages have been crawled for each site
- Identify sites with many pages vs. those with few pages
- Group related hosts (mirrors, load balancers) that serve the same content

## Database Schema Changes

### Site Profile Table

Add a `title` column to store the title from the first page discovered for each host.

**Migration:**
```sql
-- Add title column (nullable for backward compatibility)
ALTER TABLE site_profile ADD COLUMN title TEXT;

-- Create indexes for performance
CREATE INDEX idx_site_profile_title ON site_profile(title);
CREATE INDEX idx_site_profile_title_notnull ON site_profile(title) 
    WHERE title IS NOT NULL;
CREATE INDEX idx_site_profile_title_pages ON site_profile(title, page_count DESC) 
    WHERE title IS NOT NULL;
```

**Updated schema:**
```
site_profile:
  - id: INT (primary key)
  - host: TEXT (unique)
  - title: TEXT (NEW - title from first page)
  - category: TEXT
  - confidence: TEXT
  - score: INT
  - page_count: INT (counts unique URLs)
  - evidence: TEXT
  - source_page_id: INT
  - last_classified_at: TEXT
  - created_at: TEXT
  - first_found_at: TEXT
  - last_scanned_at: TEXT
```

### Backfill Strategy

For existing site_profiles without a title:

```sql
-- Find the first page for each host and set its title
UPDATE site_profile sp
SET title = (
    SELECT p.title
    FROM page p
    WHERE lower(p.url) LIKE '%' || lower(sp.host) || '%'
    ORDER BY p.created_at ASC
    LIMIT 1
)
WHERE sp.title IS NULL;
```

This should be run as a one-time migration after adding the column.

## Page Counting Logic

### Current State

The `page_count` field in site_profile exists but may not accurately track unique URLs per host.

### New Behavior

**When saving a page:**

1. Extract host from page URL
2. Check if a page record with this exact URL already exists in the `page` table
3. If this is a new URL (not seen before):
   - Insert/update page record
   - Increment site_profile.page_count for this host
   - If this is the first page for the host (page_count was 0):
     - Set site_profile.title to this page's title
4. If this is an existing URL (rescan):
   - Update page record (title, links, content, etc.)
   - Keep site_profile.page_count unchanged
   - If this is the source_page for site_profile (first page):
     - Update site_profile.title if the page title changed

**Implementation in `save_page_info()`:**

```rust
pub fn save_page_info(conn: &mut PgConnection, snapshot: &PageSnapshot) -> Result<PageSaveOutcome> {
    let host = extract_host(&snapshot.url)?;
    
    // Check if this URL already exists
    let existing_page = page::table
        .filter(page::url.eq(&snapshot.url))
        .first::<Page>(conn)
        .optional()?;
    
    let is_new_url = existing_page.is_none();
    
    // Save/update page record
    let page_id = upsert_page(conn, snapshot)?;
    
    // Update site profile
    if is_new_url {
        // First page for this host?
        let profile_exists = site_profile::table
            .filter(site_profile::host.eq(&host))
            .first::<SiteProfileRecord>(conn)
            .optional()?;
        
        if let Some(profile) = profile_exists {
            // Increment page count
            diesel::update(site_profile::table.filter(site_profile::host.eq(&host)))
                .set(site_profile::page_count.eq(site_profile::page_count + 1))
                .execute(conn)?;
            
            // If this is the first page (page_count was 0), set title
            if profile.page_count == 0 {
                diesel::update(site_profile::table.filter(site_profile::host.eq(&host)))
                    .set(site_profile::title.eq(&snapshot.title))
                    .execute(conn)?;
            }
        } else {
            // Create new site profile with title from first page
            let new_profile = NewSiteProfile {
                host: host.clone(),
                title: Some(snapshot.title.clone()),
                page_count: 1,
                // ... other fields
            };
            diesel::insert_into(site_profile::table)
                .values(&new_profile)
                .execute(conn)?;
        }
    } else {
        // Existing URL - check if we need to update title
        let profile = site_profile::table
            .filter(site_profile::host.eq(&host))
            .first::<SiteProfileRecord>(conn)
            .optional()?;
        
        if let Some(p) = profile {
            if p.source_page_id == Some(page_id) {
                // This is the source page, update title if it changed
                diesel::update(site_profile::table.filter(site_profile::host.eq(&host)))
                    .set(site_profile::title.eq(&snapshot.title))
                    .execute(conn)?;
            }
        }
    }
    
    Ok(PageSaveOutcome::Stored)
}
```

### Edge Cases

**What if the first page is deleted?**
- Keep the title as-is (it represents historical data)
- Alternative: Update title to the next earliest page's title (more complex)

**What if a page URL changes?**
- The old URL is counted, the new URL is also counted (they're different URLs)
- This is correct behavior - we're tracking unique URLs scanned

**What if page_count gets out of sync?**
- Provide a maintenance command to recount: `spyder recount-pages`
- Query: `SELECT host, COUNT(DISTINCT url) FROM page GROUP BY host`

## Backend Implementation

### New Data Structures

```rust
#[derive(Serialize, Clone)]
pub struct SiteProfileGroupSummary {
    pub title: String,
    pub host_count: usize,
    pub total_pages: i32,
    pub most_recent_scan: String,
    pub hosts: Vec<SiteProfileGroupHost>,
}

#[derive(Serialize, Clone)]
pub struct SiteProfileGroupHost {
    pub host: String,
    pub category: String,
    pub confidence: String,
    pub page_count: i32,
    pub last_scanned_at: String,
    pub last_classified_at: String,
}
```

### New Query Functions

**`list_site_profiles_grouped(conn, limit, offset)`**

Returns title-grouped site profiles with aggregated stats.

```sql
SELECT 
    title,
    COUNT(DISTINCT host) as host_count,
    SUM(page_count) as total_pages,
    MAX(last_scanned_at) as most_recent_scan,
    json_agg(json_build_object(
        'host', host,
        'category', category,
        'confidence', confidence,
        'page_count', page_count,
        'last_scanned_at', last_scanned_at,
        'last_classified_at', last_classified_at
    ) ORDER BY page_count DESC) as hosts
FROM site_profile
WHERE title IS NOT NULL
GROUP BY title
ORDER BY total_pages DESC
LIMIT ? OFFSET ?
```

**`get_site_title_group(conn, title)`**

Returns detailed information for a specific title group (all hosts with that title).

```sql
SELECT *
FROM site_profile
WHERE title = ?
ORDER BY page_count DESC
```

**`backfill_site_titles(conn)`**

One-time migration function to populate titles for existing site_profiles.

```rust
pub fn backfill_site_titles(conn: &mut PgConnection) -> Result<usize> {
    // For each site_profile without a title:
    // 1. Find the earliest page for that host
    // 2. Set site_profile.title to that page's title
    
    let profiles_without_title = site_profile::table
        .filter(site_profile::title.is_null())
        .load::<SiteProfileRecord>(conn)?;
    
    let mut updated = 0;
    for profile in profiles_without_title {
        let first_page = page::table
            .filter(page::url.like(format!("%{}%", profile.host)))
            .order(page::created_at.asc())
            .first::<Page>(conn)
            .optional()?;
        
        if let Some(page) = first_page {
            diesel::update(site_profile::table.filter(site_profile::id.eq(profile.id)))
                .set(site_profile::title.eq(&page.title))
                .execute(conn)?;
            updated += 1;
        }
    }
    
    Ok(updated)
}
```

### Modified Functions

**`save_page_info()`** - Update to track unique URLs and set titles (detailed in Page Counting Logic section)

## Frontend Implementation

### New Route: `/sites/grouped`

A new view that displays sites grouped by title with expandable sections.

**URL Parameters:**
- `limit` - Number of groups to display per page (default: 50)
- `offset` - Pagination offset
- `sort` - Sort order: `pages` (default), `hosts`, `title`, `recent`

**Template Structure:**

```html
<h1>Sites Grouped by Title</h1>
<p>Showing sites grouped by their page titles. <a href="/sites">View ungrouped list</a></p>

<div class="site-groups">
  {{#each groups}}
  <div class="site-group">
    <div class="group-header" onclick="toggleGroup(this)">
      <span class="expand-icon">▼</span>
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
        <span class="pages">{{page_count}} pages</span>
        <span class="meta">Last scanned: {{last_scanned_at}}</span>
      </div>
      {{/each}}
    </div>
  </div>
  {{/each}}
</div>

<div class="pagination">
  <!-- Standard pagination controls -->
</div>
```

**JavaScript:**
```javascript
function toggleGroup(header) {
    const hostsDiv = header.nextElementSibling;
    const icon = header.querySelector('.expand-icon');
    
    if (hostsDiv.style.display === 'none') {
        hostsDiv.style.display = 'block';
        icon.textContent = '▼';
    } else {
        hostsDiv.style.display = 'none';
        icon.textContent = '▶';
    }
}
```

**CSS:**
```css
.site-group {
    border: 1px solid #ddd;
    margin-bottom: 10px;
    border-radius: 4px;
}

.group-header {
    padding: 12px;
    background: #f5f5f5;
    cursor: pointer;
    user-select: none;
}

.group-header:hover {
    background: #e8e8e8;
}

.group-hosts {
    padding: 8px;
    background: #fff;
}

.host-row {
    padding: 8px;
    border-top: 1px solid #eee;
}

.expand-icon {
    display: inline-block;
    width: 20px;
    font-family: monospace;
}
```

### Modified Route: `/sites`

Add a link to the new grouped view:

```html
<div class="view-options">
    <a href="/sites/grouped" class="button">View Grouped by Title</a>
</div>
```

Keep all existing functionality unchanged.

## Testing Strategy

### Unit Tests

1. **Page counting logic:**
   - Test that page_count increments for new URLs
   - Test that page_count stays the same for rescans
   - Test that title is set from first page
   - Test that title updates when first page is rescanned

2. **Query functions:**
   - Test `list_site_profiles_grouped()` returns correct aggregates
   - Test that NULL titles are excluded
   - Test sorting and pagination

3. **Backfill function:**
   - Test that existing profiles get correct titles
   - Test handling of profiles with no pages

### Integration Tests

1. **Full workflow:**
   - Add a new site, scan multiple pages
   - Verify page_count increases correctly
   - Verify title is set from first page
   - Rescan first page with different title, verify title updates
   - Rescan other pages, verify page_count doesn't change

2. **Grouped view:**
   - Create multiple hosts with same title
   - Verify they appear in the same group
   - Verify host counts and page counts are correct
   - Test expanding/collapsing groups

### Manual Testing

1. **Database migration:**
   - Run migration on production-like database
   - Verify backfill populates titles correctly
   - Check query performance with indexes

2. **Frontend:**
   - Navigate to `/sites/grouped`
   - Verify groups display correctly
   - Test expand/collapse functionality
   - Verify sorting and pagination
   - Check mobile responsiveness

## Performance Considerations

### Database

**Query performance:**
- The GROUP BY query on title will be fast with the indexes
- Partial index `idx_site_profile_title_notnull` optimizes the WHERE clause
- Composite index `idx_site_profile_title_pages` optimizes both grouping and sorting

**Expected performance:**
- Grouped query: <100ms for 10,000 site_profiles
- Backfill: ~1s per 1,000 profiles (one-time operation)

**Scaling:**
- Current design scales well to 100k+ site_profiles
- If needed, can add materialized view for very large datasets

### Frontend

**Page load:**
- Default limit of 50 groups should render quickly
- Expand/collapse uses JavaScript (no page reload)
- Each group's hosts are pre-loaded in the JSON (no additional queries)

**Memory:**
- Each group with 10 hosts ≈ 2KB
- 50 groups ≈ 100KB page size (acceptable)

## Migration Plan

### Phase 1: Database Changes

1. Create and run migration to add `title` column and indexes
2. Run backfill to populate titles for existing site_profiles
3. Verify data integrity (spot-check titles match first pages)

### Phase 2: Backend Changes

1. Implement page counting logic in `save_page_info()`
2. Add `list_site_profiles_grouped()` query function
3. Add `/sites/grouped` route handler
4. Test with existing database

### Phase 3: Frontend Changes

1. Create `/sites/grouped` template
2. Add navigation link from `/sites` to `/sites/grouped`
3. Add CSS and JavaScript for expand/collapse
4. Test in browser

### Phase 4: Deployment

1. Run database migration in production
2. Run backfill script
3. Deploy backend and frontend changes
4. Monitor query performance
5. Announce new feature to users

## Future Enhancements

Potential improvements for future iterations:

1. **Fuzzy title matching** - Group titles that are similar but not identical (e.g., "Dark Market" and "Dark Market Forum")
2. **Custom title override** - Allow manually setting a site title instead of using the first page's title
3. **Title history** - Track how titles change over time
4. **Export grouped data** - CSV/JSON export of title groups
5. **Search by title** - Filter groups by title keyword
6. **Category-based grouping** - Group by category AND title
7. **Visual indicators** - Show which hosts are likely mirrors (same content fingerprint)

## Summary

This design adds title-based grouping to the sites view while preserving existing functionality. It tracks unique page counts per host and allows operators to see which sites have multiple domains/mirrors. The implementation is straightforward, performant, and follows the existing codebase patterns.
