# Detail Pages Design Specification

**Date:** 2026-06-22  
**Feature:** Site Detail Pages + Enhanced Page Detail Views  
**Goal:** Add comprehensive detail page links throughout the application for sites and pages

---

## Overview

Add detail page buttons/links to all list views (sites, pages, discoveries, leads, search results) that navigate to comprehensive detail pages showing all available information. Implement two types of detail pages:

1. **Site Detail Pages** - New `/sites/{host}` route showing domain-level information (threat-intel focused)
2. **Enhanced Page Details** - Extend existing `/pages/{id}` with discovery provenance, service fingerprints, and work queue history

---

## Motivation

Users need quick access to comprehensive information about sites and pages from any list view. Currently:
- Site/host names in lists are not clickable
- Page detail exists but lacks discovery provenance, service fingerprints, and queue history
- No unified way to see all information about a domain (leads, services, pages, relationships)
- Threat intelligence context is scattered across multiple views

This feature provides one-click navigation from any list to full detail pages with all "mileage" data (stats, last scanned, services, links, graphs, headers, everything).

---

## Architecture

### Route Structure

**New Routes:**
- `GET /sites/{host}` - Site detail page (HTML)
- `GET /api/sites/{host}` - Site detail data (JSON)

**Enhanced Routes:**
- `GET /pages/{id}` - Enhanced page detail (existing route, expanded context)

**URL Encoding:**
- Host names URL-encoded in path: `example.onion` → `/sites/example.onion`
- Backend URL-decodes and validates before database queries
- Return 400 Bad Request for invalid host encoding

### Separation of Concerns

**Site-level (`/sites/{host}`):**
- Domain/host aggregated data
- All pages from this host
- Host-level service fingerprints
- Relationships with other domains
- Discovery productivity stats

**Page-level (`/pages/{id}`):**
- Individual page content and metadata
- Discovery provenance (how this specific URL was found)
- Site context (inherited from host)
- Page-specific entities (emails, crypto, links)

---

## Data Flow

### Site Detail Context

Backend handler: `site_detail(host: String)`

**Data Sources:**

1. **Site Profile** (`site_profile` table)
   - Category, confidence score, keyword tags
   - Page count, evidence, last classified timestamp

2. **Intelligence Leads** (`intel_lead` table)
   - Query where page URL contains host
   - Group by severity (critical, high, medium, low)
   - Include recent unresolved leads

3. **Pages** (`page` table)
   - All pages where `host` column matches
   - Include: id, title, URL, last_scanned_at, entity counts
   - Paginate: default 50 per page, max 500

4. **Service Fingerprints**:
   - **HTTP** (`host_http_observation` table) - Latest status, headers, server version
   - **TLS** (`host_tls_observation` table) - Cert fingerprint, validity, issuer, subject
   - **SSH** (`ssh_observation` table) - Banner, key fingerprint, algorithms

5. **Relationships** (`page_link` table)
   - Inbound: links from other domains to this host
   - Outbound: links from this host to other domains
   - Aggregate by target/source domain, count occurrences

6. **Discovery Stats** (`url_discovery` table)
   - Count URLs discovered from pages on this host (where `discovered_from_page_id` in pages from this host)
   - Discovery productivity metric

7. **Work Queue Stats** (`work_unit` table)
   - Success rate: `status='done'` / total work units for this host
   - Failure breakdown: count by `failure_category`
   - Average retry count

**Query Strategy:**
- Single transaction to ensure consistency
- Use JOINs where appropriate to reduce round-trips
- Paginate large result sets (pages, leads)
- Handle missing data gracefully (no leads, no services, etc.)

### Enhanced Page Detail Context

Backend handler: `page_detail(page_id: i32)` (existing, enhanced)

**Additional Data Sources:**

1. **Discovery Provenance** (`url_discovery` table)
   - Query by page URL: `SELECT * FROM url_discovery WHERE url = page.url`
   - Parse `discovery_chain` array to build breadcrumb
   - Fetch page titles for each page_id in chain
   - Include: depth, import_source, discovered_at timestamp

2. **Service Fingerprints** (same as site detail)
   - Query by page's host
   - Latest HTTP/TLS/SSH observations

3. **Work Queue History** (`work_unit` table)
   - Query by page URL: `SELECT * FROM work_unit WHERE url = page.url`
   - Include: status, retry_count, failure_category, last_error, last_attempt_at, next_attempt_at
   - Show full retry history if multiple attempts

**Integration with Existing Context:**
- Keep all existing page detail data (title, language, links, entities, topics, scans)
- Add new sections without removing or restructuring existing ones
- Maintain backward compatibility with existing templates

---

## UI Design

### Site Detail Page Template

**File:** `templates/site_detail.html.hbs`

**Layout (threat-intel focused):**

```
┌─────────────────────────────────────────────┐
│ HERO SECTION                                │
│ Host: example.onion                         │
│ Category Badge | 47 Pages | 12 Leads       │
│ Last Scanned: 2026-06-22                    │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│ INTELLIGENCE SUMMARY (prominent)            │
│ ┌──────┬──────┬────────┬──────┐            │
│ │ CRIT │ HIGH │ MEDIUM │ LOW  │            │
│ │  3   │  5   │   2    │  2   │            │
│ └──────┴──────┴────────┴──────┘            │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│ ACTIVE LEADS                                │
│ Table: Severity | Lead | Confidence | ...  │
│ (Link to /leads/{id})                       │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│ SITE CLASSIFICATION                         │
│ Category: Forum · High Confidence           │
│ Tags: [drug-market] [payment-processor]    │
│ Evidence: mentions bitcoin, checkout flow   │
└─────────────────────────────────────────────┘

┌───────────────┬─────────────────────────────┐
│ HTTP          │ TLS                         │
│ Status: 200   │ Cert: Valid                 │
│ Server: nginx │ Issuer: Let's Encrypt       │
└───────────────┴─────────────────────────────┘
┌─────────────────────────────────────────────┐
│ SSH (if available)                          │
│ Banner: OpenSSH_8.2p1 Ubuntu-4ubuntu0.5    │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│ PAGES FROM THIS SITE (paginated)            │
│ Table: Title | URL | Last Scan | Entities  │
│ (Link to /pages/{id})                       │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│ RELATIONSHIPS                               │
│ Inbound: 12 domains link here              │
│ Outbound: Links to 34 domains              │
│ (Link to /relationships?focus={host})      │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│ DISCOVERY & QUEUE STATS                     │
│ URLs Discovered: 142                        │
│ Crawl Success Rate: 87%                     │
│ Failed: 15 (timeout: 8, http_404: 7)      │
└─────────────────────────────────────────────┘
```

**Visual Style:**
- Threat severity colors:
  - Critical: `#dc2626` (red)
  - High: `#ea580c` (orange)
  - Medium: `#ca8a04` (yellow)
  - Low: `#2563eb` (blue)
- Service badges: Colored pills for HTTP/TLS/SSH
- Card-based sections with consistent padding/spacing
- Responsive grid for service fingerprints (2 columns desktop, 1 column mobile)

### Enhanced Page Detail Template

**File:** `templates/page_detail.html.hbs` (modify existing)

**New Sections to Add:**

1. **Discovery Provenance Section** (insert after hero, before site classification):

```handlebars
{{#if discovery}}
<section class="card">
    <div class="section-heading">
        <h2>Discovery Provenance</h2>
        <span class="depth-badge depth-{{discovery.depth}}">Depth {{discovery.depth}}</span>
    </div>
    
    <div class="discovery-breadcrumb">
        {{#if discovery.import_source}}
        <span class="breadcrumb-item">
            <span class="breadcrumb-label">Import Source:</span>
            <a href="/import-sources/{{discovery.import_source_id}}">{{discovery.import_source_name}}</a>
        </span>
        <span class="breadcrumb-separator">→</span>
        {{/if}}
        
        {{#each discovery.chain}}
        <span class="breadcrumb-item">
            <a class="breadcrumb-link" href="/pages/{{page_id}}">{{page_title}}</a>
        </span>
        {{#unless @last}}
        <span class="breadcrumb-separator">→</span>
        {{/unless}}
        {{/each}}
        
        <span class="breadcrumb-item breadcrumb-current">This Page</span>
    </div>
    
    <div class="meta-strip">
        <span><strong>Discovered:</strong> {{discovery.discovered_at}}</span>
        <span><strong>Depth:</strong> {{discovery.depth}} hops from seed</span>
    </div>
</section>
{{/if}}
```

2. **Service Fingerprints Section** (new card, use partial):

```handlebars
{{#if service_fingerprints}}
<section class="card">
    <div class="section-heading">
        <h2>Service Fingerprints</h2>
        <span class="muted">Host-level observations</span>
    </div>
    {{> service-fingerprints fingerprints=service_fingerprints}}
</section>
{{/if}}
```

3. **Work Queue History Section** (insert near bottom, before scan history):

```handlebars
{{#if work_queue}}
<section class="card">
    <div class="section-heading">
        <h2>Work Queue History</h2>
        <span class="status-badge status-{{work_queue.status}}">{{work_queue.status}}</span>
    </div>
    
    <div class="meta-strip">
        <span><strong>Status:</strong> {{work_queue.status}}</span>
        <span><strong>Retry Count:</strong> {{work_queue.retry_count}}</span>
        {{#if work_queue.failure_category}}
        <span><strong>Failure Category:</strong> {{work_queue.failure_category}}</span>
        {{/if}}
    </div>
    
    {{#if work_queue.last_error}}
    <div class="error-message">
        <strong>Last Error:</strong> {{work_queue.last_error}}
    </div>
    {{/if}}
    
    <div class="meta-strip">
        {{#if work_queue.last_attempt_at}}
        <span><strong>Last Attempt:</strong> {{work_queue.last_attempt_at}}</span>
        {{/if}}
        {{#if work_queue.next_attempt_at}}
        <span><strong>Next Attempt:</strong> {{work_queue.next_attempt_at}}</span>
        {{/if}}
    </div>
</section>
{{/if}}
```

### Reusable Partials

**File:** `templates/partials/service-fingerprints.hbs`

```handlebars
<div class="service-grid">
    {{#if fingerprints.http}}
    <div class="service-card">
        <h3>HTTP</h3>
        <div class="service-details">
            <p><strong>Status:</strong> {{fingerprints.http.status_code}}</p>
            <p><strong>Server:</strong> {{fingerprints.http.server}}</p>
            {{#if fingerprints.http.content_type}}
            <p><strong>Content-Type:</strong> {{fingerprints.http.content_type}}</p>
            {{/if}}
            <p class="muted">Observed: {{fingerprints.http.observed_at}}</p>
        </div>
    </div>
    {{/if}}
    
    {{#if fingerprints.tls}}
    <div class="service-card">
        <h3>TLS</h3>
        <div class="service-details">
            <p><strong>Valid:</strong> {{fingerprints.tls.is_valid}}</p>
            <p><strong>Issuer:</strong> {{fingerprints.tls.issuer}}</p>
            <p><strong>Subject:</strong> {{fingerprints.tls.subject}}</p>
            <p><strong>Expires:</strong> {{fingerprints.tls.not_after}}</p>
            <p class="muted">Fingerprint: {{fingerprints.tls.cert_fingerprint}}</p>
        </div>
    </div>
    {{/if}}
    
    {{#if fingerprints.ssh}}
    <div class="service-card">
        <h3>SSH</h3>
        <div class="service-details">
            <p><strong>Banner:</strong> {{fingerprints.ssh.banner}}</p>
            <p><strong>Key Type:</strong> {{fingerprints.ssh.key_type}}</p>
            <p class="muted">Fingerprint: {{fingerprints.ssh.key_fingerprint}}</p>
            <p class="muted">Observed: {{fingerprints.ssh.observed_at}}</p>
        </div>
    </div>
    {{/if}}
</div>
```

**File:** `templates/partials/intel-summary.hbs`

```handlebars
<div class="intel-summary-grid">
    <div class="intel-severity-card severity-critical">
        <div class="severity-count">{{summary.critical_count}}</div>
        <div class="severity-label">Critical</div>
    </div>
    <div class="intel-severity-card severity-high">
        <div class="severity-count">{{summary.high_count}}</div>
        <div class="severity-label">High</div>
    </div>
    <div class="intel-severity-card severity-medium">
        <div class="severity-count">{{summary.medium_count}}</div>
        <div class="severity-label">Medium</div>
    </div>
    <div class="intel-severity-card severity-low">
        <div class="severity-count">{{summary.low_count}}</div>
        <div class="severity-label">Low</div>
    </div>
</div>
```

---

## Link Updates

Update templates to add detail page links:

### 1. `templates/sites.html.hbs`

**Change:** Make host name clickable
```handlebars
<!-- Before -->
<td>{{host}}</td>

<!-- After -->
<td><a class="row-link" href="/sites/{{host}}">{{host}}</a></td>
```

### 2. `templates/sites_grouped.html.hbs`

**Change:** Make host name clickable in grouped view
```handlebars
<!-- Before -->
<td>{{host}}</td>

<!-- After -->
<td><a class="row-link" href="/sites/{{host}}">{{host}}</a></td>
```

### 3. `templates/discovery.html.hbs`

**Change:** Make URL clickable, link to page detail if crawled
```handlebars
<!-- Before -->
<td>
    <a href="{{url}}" target="_blank" class="row-link">{{url}}</a>
</td>

<!-- After -->
<td>
    {{#if page_id}}
    <a class="row-link" href="/pages/{{page_id}}">{{url}}</a>
    {{else}}
    <a href="{{url}}" target="_blank" class="row-link external-link">{{url}}</a>
    {{/if}}
</td>
```

### 4. `templates/leads.html.hbs`

**Change:** Add link to source page
```handlebars
<!-- Before -->
<td>{{source_page_title}}</td>

<!-- After -->
<td>
    {{#if source_page_id}}
    <a class="row-link" href="/pages/{{source_page_id}}">{{source_page_title}}</a>
    {{else}}
    {{source_page_title}}
    {{/if}}
</td>
```

### 5. `templates/dashboard.html.hbs`

**Change:** Add detail links where sites/pages mentioned (identify specific locations during implementation)

### 6. `templates/pages.html.hbs`

**Change:** Already has page detail links ✓ - verify they work correctly

### 7. `templates/search.html.hbs`

**Change:** Already has page detail links ✓ - verify they work correctly

---

## Backend Implementation

### New Backend Functions (`src/lib.rs`)

```rust
/// Site detail data structure
pub struct SiteDetailData {
    pub profile: SiteProfileRecord,
    pub intel_summary: IntelSummary,
    pub active_leads: Vec<IntelLead>,
    pub pages: Vec<PageSummary>,
    pub service_fingerprints: ServiceFingerprints,
    pub relationships: RelationshipData,
    pub discovery_stats: DiscoveryStats,
    pub queue_stats: QueueStats,
}

pub struct IntelSummary {
    pub critical_count: i64,
    pub high_count: i64,
    pub medium_count: i64,
    pub low_count: i64,
}

pub struct ServiceFingerprints {
    pub http: Option<HostHttpObservation>,
    pub tls: Option<HostTlsObservation>,
    pub ssh: Option<SshObservation>,
}

pub struct RelationshipData {
    pub inbound_count: i64,
    pub outbound_count: i64,
    pub inbound_domains: Vec<String>,
    pub outbound_domains: Vec<String>,
}

pub struct DiscoveryStats {
    pub urls_discovered_from_this_site: i64,
}

pub struct QueueStats {
    pub total_work_units: i64,
    pub success_count: i64,
    pub failure_count: i64,
    pub success_rate: f64,
    pub failure_breakdown: Vec<(String, i64)>, // (category, count)
    pub avg_retry_count: f64,
}

pub struct PageSummary {
    pub id: i32,
    pub title: String,
    pub url: String,
    pub last_scanned_at: String,
    pub email_count: i64,
    pub crypto_count: i64,
    pub link_count: i64,
}

pub struct DiscoveryChain {
    pub depth: i32,
    pub import_source_id: Option<i32>,
    pub import_source_name: Option<String>,
    pub chain: Vec<ChainItem>,
    pub discovered_at: String,
}

pub struct ChainItem {
    pub page_id: i32,
    pub page_title: String,
    pub page_url: String,
}

pub struct WorkQueueHistory {
    pub status: String,
    pub retry_count: i32,
    pub failure_category: Option<String>,
    pub last_error: Option<String>,
    pub last_attempt_at: Option<String>,
    pub next_attempt_at: Option<String>,
}

/// Get comprehensive site detail data
pub fn get_site_detail(
    conn: &mut PgConnection,
    host: &str,
    page_limit: i64,
    page_offset: i64,
) -> Result<SiteDetailData> {
    // Implementation: Query all data sources in single transaction
    // 1. Site profile
    // 2. Intel leads (group by severity, get recent unresolved)
    // 3. Pages from this host (paginated)
    // 4. Service fingerprints (latest observations)
    // 5. Relationships (aggregate by domain)
    // 6. Discovery stats
    // 7. Queue stats
}

/// Get intelligence lead summary for a host
pub fn get_site_intel_summary(
    conn: &mut PgConnection,
    host: &str,
) -> Result<IntelSummary> {
    // Query intel_lead, JOIN page ON page.id = intel_lead.page_id
    // WHERE page.url LIKE '%' || host || '%'
    // GROUP BY severity, COUNT(*)
}

/// Get active (unresolved) leads for a site
pub fn get_site_active_leads(
    conn: &mut PgConnection,
    host: &str,
    limit: i64,
) -> Result<Vec<IntelLead>> {
    // Query intel_lead WHERE status != 'resolved'
    // AND page.url LIKE '%' || host || '%'
    // ORDER BY severity DESC, created_at DESC
    // LIMIT limit
}

/// Get all pages from a host
pub fn get_site_pages(
    conn: &mut PgConnection,
    host: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<PageSummary>> {
    // Query page table WHERE host = host
    // JOIN aggregated entity counts
    // ORDER BY last_scanned_at DESC
    // LIMIT limit OFFSET offset
}

/// Get service fingerprints for a host
pub fn get_host_service_fingerprints(
    conn: &mut PgConnection,
    host: &str,
) -> Result<ServiceFingerprints> {
    // Query latest observations from:
    // - host_http_observation
    // - host_tls_observation
    // - ssh_observation
    // WHERE host = host
    // ORDER BY observed_at DESC
    // LIMIT 1 per service type
}

/// Get relationship data for a host
pub fn get_site_relationships(
    conn: &mut PgConnection,
    host: &str,
) -> Result<RelationshipData> {
    // Inbound: SELECT DISTINCT source_page.host
    //          FROM page_link JOIN page AS source_page
    //          WHERE target_url LIKE '%' || host || '%'
    //          AND source_page.host != host
    // Outbound: SELECT DISTINCT target_page.host
    //           FROM page_link JOIN page AS source_page
    //           WHERE source_page.host = host
    //           AND target_url NOT LIKE '%' || host || '%'
}

/// Get discovery statistics for a site
pub fn get_site_discovery_stats(
    conn: &mut PgConnection,
    host: &str,
) -> Result<DiscoveryStats> {
    // SELECT COUNT(*) FROM url_discovery
    // WHERE discovered_from_page_id IN (
    //   SELECT id FROM page WHERE host = host
    // )
}

/// Get work queue statistics for a site
pub fn get_site_queue_stats(
    conn: &mut PgConnection,
    host: &str,
) -> Result<QueueStats> {
    // SELECT status, failure_category, COUNT(*), AVG(retry_count)
    // FROM work_unit
    // WHERE url LIKE '%' || host || '%'
    // GROUP BY status, failure_category
}

/// Get discovery chain for a page
pub fn get_page_discovery_chain(
    conn: &mut PgConnection,
    page_url: &str,
) -> Result<Option<DiscoveryChain>> {
    // 1. Query url_discovery WHERE url = page_url
    // 2. If found, parse discovery_chain array
    // 3. For each page_id in chain, fetch page title and URL
    // 4. If import_source_id present, fetch import_source details
    // 5. Return structured chain with depth, source, items
}

/// Get work queue history for a page
pub fn get_page_work_queue_history(
    conn: &mut PgConnection,
    page_url: &str,
) -> Result<Option<WorkQueueHistory>> {
    // Query work_unit WHERE url = page_url
    // Return most recent work unit record
    // Include: status, retry_count, failure_category, errors, timestamps
}
```

### Frontend Route Handlers (`src/bin/frontend.rs`)

```rust
#[get("/sites/<host>")]
fn site_detail(host: String, state: &State<AppState>) -> HtmlResult {
    // 1. URL decode host
    let decoded_host = urlencoding::decode(&host)
        .map_err(|_| FrontendError::bad_request("Invalid host encoding"))?;
    
    // 2. Validate host format (basic validation)
    if decoded_host.is_empty() || decoded_host.len() > 253 {
        return Err(FrontendError::bad_request("Invalid host"));
    }
    
    // 3. Get database connection
    let mut connection = state.db_pool.get()
        .map_err(|e| FrontendError::internal("database connection", e.into()))?;
    
    // 4. Query site detail data
    let site_data = get_site_detail(&mut connection, &decoded_host, 50, 0)
        .map_err(|e| {
            if e.to_string().contains("not found") {
                FrontendError::not_found("Site not found")
            } else {
                FrontendError::internal("loading site detail", e)
            }
        })?;
    
    // 5. Build template context
    let context = json!({
        "title": format!("Site: {}", decoded_host),
        "host": decoded_host,
        "profile": site_data.profile,
        "intel_summary": site_data.intel_summary,
        "active_leads": site_data.active_leads,
        "pages": site_data.pages,
        "service_fingerprints": site_data.service_fingerprints,
        "relationships": site_data.relationships,
        "discovery_stats": site_data.discovery_stats,
        "queue_stats": site_data.queue_stats,
    });
    
    // 6. Render template
    render_template("site_detail", &context, state)
}

#[get("/pages/<page_id>")]
fn page_detail(page_id: i32, state: &State<AppState>) -> HtmlResult {
    // Existing implementation + new queries:
    
    // ... existing page loading code ...
    
    // NEW: Get discovery chain
    let discovery_chain = get_page_discovery_chain(&mut connection, &page.url)
        .ok()
        .flatten();
    
    // NEW: Get service fingerprints
    let service_fingerprints = get_host_service_fingerprints(&mut connection, &page.host)
        .ok();
    
    // NEW: Get work queue history
    let work_queue = get_page_work_queue_history(&mut connection, &page.url)
        .ok()
        .flatten();
    
    // Build context (add new fields to existing)
    let mut context = existing_context; // Keep all existing fields
    context["discovery"] = json!(discovery_chain);
    context["service_fingerprints"] = json!(service_fingerprints);
    context["work_queue"] = json!(work_queue);
    
    render_template("page_detail", &context, state)
}

// API endpoint for programmatic access
#[get("/api/sites/<host>")]
fn api_site_detail(host: String, state: &State<AppState>) -> Result<Json<ApiResponse<SiteDetailData>>> {
    // Similar to HTML handler but return JSON
    let decoded_host = urlencoding::decode(&host)
        .map_err(|_| ApiError::BadRequest("Invalid host encoding".into()))?;
    
    let mut connection = state.db_pool.get()
        .map_err(|e| ApiError::Internal(e.into()))?;
    
    let site_data = get_site_detail(&mut connection, &decoded_host, 50, 0)
        .map_err(|e| ApiError::Internal(e))?;
    
    Ok(Json(ApiResponse::success(site_data)))
}
```

---

## Error Handling

### Expected Errors

1. **Site not found** (404)
   - Site profile doesn't exist in database
   - Response: Render error page "Site not found"

2. **Page not found** (404)
   - Page ID doesn't exist
   - Response: Already handled by existing code

3. **Invalid host encoding** (400)
   - URL decoding fails
   - Response: "Invalid host encoding"

4. **Database errors** (500)
   - Connection pool exhausted
   - Query failures
   - Response: Generic error page with logged details

### Graceful Degradation

Handle missing optional data without failing entire page:

```rust
// If no intel leads exist
if intel_leads.is_empty() {
    context["has_leads"] = json!(false);
} else {
    context["has_leads"] = json!(true);
    context["leads"] = json!(intel_leads);
}

// If no service fingerprints
if service_fingerprints.http.is_none() 
   && service_fingerprints.tls.is_none() 
   && service_fingerprints.ssh.is_none() {
    context["has_services"] = json!(false);
} else {
    context["has_services"] = json!(true);
    context["service_fingerprints"] = json!(service_fingerprints);
}
```

Template conditional rendering:

```handlebars
{{#if has_leads}}
<section class="card">
    <!-- Leads content -->
</section>
{{else}}
<section class="card empty-state">
    <p>No intelligence leads for this site.</p>
</section>
{{/if}}
```

---

## Performance Considerations

### Database Optimization

1. **Indexes**
   - Ensure index on `page.host` (likely already exists)
   - Index on `work_unit.url` for queue history queries
   - Index on `url_discovery.url` for provenance lookups
   - Index on `intel_lead.page_id` for lead queries

2. **Query Efficiency**
   - Use JOINs instead of N+1 queries
   - Aggregate counts in SQL rather than application code
   - Limit result sets (paginate pages, limit leads to 100)

3. **Caching Strategy**
   - Service fingerprints change infrequently → cache for 5 minutes
   - Site profile updates slowly → cache for 1 minute
   - Intel leads update frequently → no caching
   - Consider adding Redis cache layer if needed

### Pagination

**Site pages table:**
- Default: 50 per page
- Max: 500 per page
- Add pagination controls to template
- Query: `LIMIT {limit} OFFSET {offset}`

**Active leads:**
- Limit to 100 most recent
- Link to full `/leads?host={host}` for complete list

### Load Testing Targets

Test with:
- Sites with 1,000+ pages
- Sites with 100+ intelligence leads
- Sites with complete service fingerprints (HTTP + TLS + SSH)
- Sites with no data (new imports)

---

## Testing Strategy

### Unit Tests

**Backend functions (`src/lib.rs`):**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_site_detail_found() {
        // Test successful site detail retrieval
        let mut conn = test_db_connection();
        let result = get_site_detail(&mut conn, "test.onion", 50, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_site_detail_not_found() {
        // Test site that doesn't exist
        let mut conn = test_db_connection();
        let result = get_site_detail(&mut conn, "nonexistent.onion", 50, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_page_discovery_chain_with_import() {
        // Test discovery chain for imported URL (depth 0)
        let mut conn = test_db_connection();
        let result = get_page_discovery_chain(&mut conn, "http://seed.onion");
        assert!(result.is_ok());
        let chain = result.unwrap().unwrap();
        assert_eq!(chain.depth, 0);
        assert!(chain.import_source_id.is_some());
    }

    #[test]
    fn test_get_page_discovery_chain_organic() {
        // Test discovery chain for organically discovered URL (depth > 0)
        let mut conn = test_db_connection();
        let result = get_page_discovery_chain(&mut conn, "http://discovered.onion");
        assert!(result.is_ok());
        let chain = result.unwrap().unwrap();
        assert!(chain.depth > 0);
        assert!(!chain.chain.is_empty());
    }

    #[test]
    fn test_get_site_intel_summary() {
        // Test intel lead aggregation
        let mut conn = test_db_connection();
        let result = get_site_intel_summary(&mut conn, "test.onion");
        assert!(result.is_ok());
    }
}
```

### Integration Tests

**Frontend routes (`src/bin/frontend.rs`):**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_site_detail_route_success() {
        let client = test_client();
        let response = client.get("/sites/test.onion").dispatch();
        assert_eq!(response.status(), Status::Ok);
        assert!(response.into_string().unwrap().contains("test.onion"));
    }

    #[test]
    fn test_site_detail_route_not_found() {
        let client = test_client();
        let response = client.get("/sites/nonexistent.onion").dispatch();
        assert_eq!(response.status(), Status::NotFound);
    }

    #[test]
    fn test_site_detail_invalid_encoding() {
        let client = test_client();
        let response = client.get("/sites/%ZZ%ZZ").dispatch();
        assert_eq!(response.status(), Status::BadRequest);
    }

    #[test]
    fn test_enhanced_page_detail_with_discovery() {
        let client = test_client();
        let response = client.get("/pages/1").dispatch();
        assert_eq!(response.status(), Status::Ok);
        let body = response.into_string().unwrap();
        assert!(body.contains("Discovery Provenance") || body.contains("No discovery"));
    }

    #[test]
    fn test_api_site_detail() {
        let client = test_client();
        let response = client.get("/api/sites/test.onion").dispatch();
        assert_eq!(response.status(), Status::Ok);
        let json: ApiResponse<SiteDetailData> = response.into_json().unwrap();
        assert!(json.success);
    }
}
```

### Manual Testing Checklist

- [ ] Navigate to `/sites` list, click host name, verify detail page loads
- [ ] Navigate to `/sites/grouped`, click host name in each group, verify detail page loads
- [ ] Navigate to `/discovery`, click URL (if crawled), verify page detail loads
- [ ] Navigate to `/pages`, click page title, verify enhanced detail view shows new sections
- [ ] Navigate to `/leads`, click source page link, verify page detail loads
- [ ] Navigate to `/search`, search for page, click result, verify page detail loads
- [ ] Test site with no intel leads - verify empty state displays correctly
- [ ] Test site with no service fingerprints - verify section hidden or shows "No data"
- [ ] Test page with no discovery record - verify section hidden gracefully
- [ ] Test page with no work queue history - verify section hidden gracefully
- [ ] Test URL encoding: site with special characters in host (if applicable)
- [ ] Test pagination on site detail page (site with 100+ pages)
- [ ] Test mobile responsive layout for both detail pages

---

## CSS Requirements

Add to existing `static/styles.css`:

```css
/* Site Detail Page */
.intel-summary-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1rem;
    margin: 1rem 0;
}

.intel-severity-card {
    padding: 1.5rem;
    border-radius: 8px;
    text-align: center;
}

.intel-severity-card.severity-critical {
    background: #fef2f2;
    border: 2px solid #dc2626;
}

.intel-severity-card.severity-high {
    background: #fff7ed;
    border: 2px solid #ea580c;
}

.intel-severity-card.severity-medium {
    background: #fefce8;
    border: 2px solid #ca8a04;
}

.intel-severity-card.severity-low {
    background: #eff6ff;
    border: 2px solid #2563eb;
}

.severity-count {
    font-size: 2rem;
    font-weight: bold;
    margin-bottom: 0.5rem;
}

.severity-label {
    font-size: 0.875rem;
    text-transform: uppercase;
    font-weight: 600;
}

/* Service Fingerprints */
.service-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 1rem;
}

.service-card {
    padding: 1rem;
    border: 1px solid #e5e7eb;
    border-radius: 8px;
    background: #f9fafb;
}

.service-card h3 {
    margin-top: 0;
    margin-bottom: 1rem;
    font-size: 1rem;
    color: #374151;
}

.service-details p {
    margin: 0.5rem 0;
    font-size: 0.875rem;
}

/* Discovery Breadcrumb */
.discovery-breadcrumb {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem;
    padding: 1rem;
    background: #f9fafb;
    border-radius: 8px;
    font-size: 0.875rem;
}

.breadcrumb-item {
    display: inline-flex;
    align-items: center;
}

.breadcrumb-link {
    color: #2563eb;
    text-decoration: none;
    padding: 0.25rem 0.5rem;
    border-radius: 4px;
    background: #eff6ff;
}

.breadcrumb-link:hover {
    background: #dbeafe;
}

.breadcrumb-current {
    font-weight: 600;
    color: #374151;
}

.breadcrumb-separator {
    color: #9ca3af;
    margin: 0 0.25rem;
}

.breadcrumb-label {
    font-weight: 600;
    color: #6b7280;
    margin-right: 0.5rem;
}

.depth-badge {
    padding: 0.25rem 0.75rem;
    border-radius: 12px;
    font-size: 0.75rem;
    font-weight: 600;
}

.depth-badge.depth-0 {
    background: #dbeafe;
    color: #1e40af;
}

.depth-badge.depth-1 {
    background: #ddd6fe;
    color: #5b21b6;
}

.depth-badge.depth-2 {
    background: #fce7f3;
    color: #9f1239;
}

/* Work Queue Status */
.status-badge {
    padding: 0.25rem 0.75rem;
    border-radius: 12px;
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
}

.status-badge.status-done {
    background: #d1fae5;
    color: #065f46;
}

.status-badge.status-pending {
    background: #fef3c7;
    color: #92400e;
}

.status-badge.status-failed {
    background: #fee2e2;
    color: #991b1b;
}

.error-message {
    padding: 1rem;
    background: #fef2f2;
    border-left: 4px solid #dc2626;
    border-radius: 4px;
    margin: 1rem 0;
    font-family: monospace;
    font-size: 0.875rem;
}

/* Responsive adjustments */
@media (max-width: 768px) {
    .intel-summary-grid {
        grid-template-columns: repeat(2, 1fr);
    }
    
    .service-grid {
        grid-template-columns: 1fr;
    }
    
    .discovery-breadcrumb {
        flex-direction: column;
        align-items: flex-start;
    }
}
```

---

## Dependencies

**New Rust crates (if needed):**
- `urlencoding = "2.1"` - For URL encoding/decoding host names

Add to `Cargo.toml`:
```toml
[dependencies]
urlencoding = "2.1"
```

No other new dependencies required - uses existing Diesel, Rocket, Handlebars stack.

---

## Migration Path

No database migrations required - all necessary tables and columns already exist:
- `site_profile`
- `intel_lead`
- `page`
- `host_http_observation`, `host_tls_observation`, `ssh_observation`
- `page_link`
- `url_discovery`
- `work_unit`

---

## Success Criteria

**Functional:**
- ✅ Clicking any host name in any list navigates to `/sites/{host}` detail page
- ✅ Clicking any page title in any list navigates to `/pages/{id}` detail page
- ✅ Site detail page shows comprehensive host information (intel, services, pages, relationships)
- ✅ Page detail page shows discovery provenance, service fingerprints, queue history
- ✅ All sections handle missing data gracefully (empty states, hidden sections)
- ✅ 404 errors for non-existent sites/pages
- ✅ 400 errors for invalid URL encoding

**Performance:**
- ✅ Site detail page loads in <2 seconds for sites with 100+ pages
- ✅ Page detail page loads in <1 second
- ✅ No N+1 query problems
- ✅ Proper pagination prevents memory issues

**User Experience:**
- ✅ Threat-intel information is prominent and easy to understand
- ✅ Navigation between related entities (site → pages → leads) is intuitive
- ✅ Visual hierarchy makes important information stand out
- ✅ Responsive layout works on mobile and desktop
- ✅ Consistent with existing design patterns

**Code Quality:**
- ✅ Reusable partials reduce duplication
- ✅ Unit tests cover all new backend functions
- ✅ Integration tests cover all new routes
- ✅ Error handling is comprehensive
- ✅ Code follows existing patterns and conventions

---

## Out of Scope

**Not included in this design:**
- Real-time updates (WebSockets, SSE)
- Inline editing of site classifications
- Graphical relationship visualization (beyond table/list)
- Export functionality (CSV, JSON downloads)
- Advanced filtering on site detail page
- Timeline/historical view of site changes
- Comparison view (compare two sites side-by-side)

These can be added in future iterations if needed.

---

## Appendix: Example Queries

### Get site intel summary
```sql
SELECT 
  severity,
  COUNT(*) as count
FROM intel_lead
JOIN page ON page.id = intel_lead.page_id
WHERE page.url LIKE '%example.onion%'
GROUP BY severity
ORDER BY 
  CASE severity
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    WHEN 'low' THEN 4
  END;
```

### Get site pages with entity counts
```sql
SELECT 
  p.id,
  p.title,
  p.url,
  p.last_scanned_at,
  COUNT(DISTINCT pe.id) as email_count,
  COUNT(DISTINCT pc.id) as crypto_count,
  COUNT(DISTINCT pl.id) as link_count
FROM page p
LEFT JOIN page_email pe ON pe.page_id = p.id
LEFT JOIN page_crypto pc ON pc.page_id = p.id
LEFT JOIN page_link pl ON pl.source_page_id = p.id
WHERE p.host = 'example.onion'
GROUP BY p.id, p.title, p.url, p.last_scanned_at
ORDER BY p.last_scanned_at DESC
LIMIT 50 OFFSET 0;
```

### Get discovery chain for page
```sql
SELECT 
  d.discovery_depth,
  d.discovery_chain,
  d.import_source_id,
  d.discovered_at,
  s.source_name as import_source_name
FROM url_discovery d
LEFT JOIN import_source s ON s.id = d.import_source_id
WHERE d.url = 'http://example.onion/page';
```

### Get work queue stats for site
```sql
SELECT 
  status,
  failure_category,
  COUNT(*) as count,
  AVG(retry_count) as avg_retries
FROM work_unit
WHERE url LIKE '%example.onion%'
GROUP BY status, failure_category
ORDER BY count DESC;
```

---

**End of Design Specification**
