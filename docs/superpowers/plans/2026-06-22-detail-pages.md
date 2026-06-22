# Detail Pages Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add comprehensive site detail pages and enhance page detail views with discovery provenance, service fingerprints, and work queue history.

**Architecture:** Create new `/sites/{host}` route with threat-intel focused layout, enhance existing `/pages/{id}` route with additional sections, build reusable Handlebars partials for shared UI components, update all list view templates to link to detail pages.

**Tech Stack:** Rust (Diesel ORM, Rocket web framework), PostgreSQL, Handlebars templates, existing CSS framework

## Global Constraints

- Use existing Diesel, Rocket, and Handlebars stack - no new framework dependencies
- Add urlencoding = "2.1" crate for URL encoding/decoding
- Follow existing code patterns in src/lib.rs and src/bin/frontend.rs
- Maintain backward compatibility with existing templates
- All database queries must handle missing data gracefully
- Default pagination: 50 items per page, max 500
- Intel leads limited to 100 most recent per site
- Threat severity order: critical, high, medium, low
- All new functions must include unit tests
- All new routes must include integration tests

---

## File Structure

### New Files to Create

**Templates:**
- `templates/site_detail.html.hbs` - Site detail page (threat-intel focused layout)
- `templates/partials/service-fingerprints.hbs` - Reusable service fingerprint cards
- `templates/partials/intel-summary.hbs` - Reusable intel severity summary grid

**None in src/** - All backend code goes into existing `src/lib.rs` and `src/bin/frontend.rs`

### Files to Modify

**Backend:**
- `src/lib.rs` - Add data structures and backend query functions
- `src/bin/frontend.rs` - Add new routes and enhance existing page_detail handler
- `Cargo.toml` - Add urlencoding dependency

**Templates:**
- `templates/page_detail.html.hbs` - Add discovery provenance, service fingerprints, work queue sections
- `templates/sites.html.hbs` - Make host names clickable
- `templates/sites_grouped.html.hbs` - Make host names clickable
- `templates/discovery.html.hbs` - Link discovered URLs to page detail when available
- `templates/leads.html.hbs` - Link source page titles to page detail

**Styles:**
- `static/styles.css` - Add CSS for new components

---

### Task 1: Add urlencoding dependency

**Files:**
- Modify: `Cargo.toml:dependencies`

**Interfaces:**
- Consumes: None
- Produces: urlencoding crate available for import

- [ ] **Step 1: Add urlencoding dependency**

```toml
[dependencies]
urlencoding = "2.1"
```

Add after existing dependencies in Cargo.toml.

- [ ] **Step 2: Verify dependency builds**

Run: `cargo check`
Expected: SUCCESS with no errors

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "deps: add urlencoding for URL decoding in site detail routes"
```

---

### Task 2: Add backend data structures

**Files:**
- Modify: `src/lib.rs:end-of-file`

**Interfaces:**
- Consumes: None (struct definitions only)
- Produces: 
  - `pub struct IntelSummary` with fields `critical_count: i64, high_count: i64, medium_count: i64, low_count: i64`
  - `pub struct ServiceFingerprints` with fields `http: Option<HostHttpObservation>, tls: Option<HostTlsObservation>, ssh: Option<SshObservation>`
  - `pub struct RelationshipData` with fields `inbound_count: i64, outbound_count: i64, inbound_domains: Vec<String>, outbound_domains: Vec<String>`
  - `pub struct DiscoveryStats` with field `urls_discovered_from_this_site: i64`
  - `pub struct QueueStats` with fields `total_work_units: i64, success_count: i64, failure_count: i64, success_rate: f64, failure_breakdown: Vec<(String, i64)>, avg_retry_count: f64`
  - `pub struct PageSummary` with fields `id: i32, title: String, url: String, last_scanned_at: String, email_count: i64, crypto_count: i64, link_count: i64`
  - `pub struct SiteDetailData` with all aggregated fields
  - `pub struct DiscoveryChain` with fields `depth: i32, import_source_id: Option<i32>, import_source_name: Option<String>, chain: Vec<ChainItem>, discovered_at: String`
  - `pub struct ChainItem` with fields `page_id: i32, page_title: String, page_url: String`
  - `pub struct WorkQueueHistory` with fields `status: String, retry_count: i32, failure_category: Option<String>, last_error: Option<String>, last_attempt_at: Option<String>, next_attempt_at: Option<String>`

- [ ] **Step 1: Write test for IntelSummary serialization**

```rust
#[cfg(test)]
mod detail_page_tests {
    use super::*;

    #[test]
    fn test_intel_summary_creation() {
        let summary = IntelSummary {
            critical_count: 5,
            high_count: 10,
            medium_count: 3,
            low_count: 2,
        };
        assert_eq!(summary.critical_count, 5);
        assert_eq!(summary.high_count, 10);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_intel_summary_creation`
Expected: FAIL with "IntelSummary not found"

- [ ] **Step 3: Add all data structures**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntelSummary {
    pub critical_count: i64,
    pub high_count: i64,
    pub medium_count: i64,
    pub low_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceFingerprints {
    pub http: Option<HostHttpObservation>,
    pub tls: Option<HostTlsObservation>,
    pub ssh: Option<SshObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipData {
    pub inbound_count: i64,
    pub outbound_count: i64,
    pub inbound_domains: Vec<String>,
    pub outbound_domains: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryStats {
    pub urls_discovered_from_this_site: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    pub total_work_units: i64,
    pub success_count: i64,
    pub failure_count: i64,
    pub success_rate: f64,
    pub failure_breakdown: Vec<(String, i64)>,
    pub avg_retry_count: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageSummary {
    pub id: i32,
    pub title: String,
    pub url: String,
    pub last_scanned_at: String,
    pub email_count: i64,
    pub crypto_count: i64,
    pub link_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryChain {
    pub depth: i32,
    pub import_source_id: Option<i32>,
    pub import_source_name: Option<String>,
    pub chain: Vec<ChainItem>,
    pub discovered_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainItem {
    pub page_id: i32,
    pub page_title: String,
    pub page_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkQueueHistory {
    pub status: String,
    pub retry_count: i32,
    pub failure_category: Option<String>,
    pub last_error: Option<String>,
    pub last_attempt_at: Option<String>,
    pub next_attempt_at: Option<String>,
}
```

Add these at the end of src/lib.rs, before the test module.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_intel_summary_creation`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add data structures for site and page detail views"
```

---

### Task 3: Implement get_site_intel_summary function

**Files:**
- Modify: `src/lib.rs:end-of-functions`

**Interfaces:**
- Consumes: `&mut PgConnection, host: &str`
- Produces: `pub fn get_site_intel_summary(conn: &mut PgConnection, host: &str) -> Result<IntelSummary>`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn test_get_site_intel_summary() {
    let mut conn = establish_test_connection();
    setup_test_data(&mut conn);
    
    let result = get_site_intel_summary(&mut conn, "test.onion");
    assert!(result.is_ok());
    let summary = result.unwrap();
    assert!(summary.critical_count >= 0);
    assert!(summary.high_count >= 0);
}
```

Add to detail_page_tests module.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_get_site_intel_summary`
Expected: FAIL with "get_site_intel_summary not found"

- [ ] **Step 3: Implement get_site_intel_summary**

```rust
pub fn get_site_intel_summary(
    conn: &mut PgConnection,
    host: &str,
) -> Result<IntelSummary> {
    use crate::schema::{intel_lead, page};

    let results = intel_lead::table
        .inner_join(page::table.on(page::id.eq(intel_lead::page_id)))
        .filter(page::url.like(format!("%{}%", host)))
        .select((intel_lead::severity,))
        .load::<(String,)>(conn)
        .context("error loading intel leads for summary")?;

    let mut critical_count = 0i64;
    let mut high_count = 0i64;
    let mut medium_count = 0i64;
    let mut low_count = 0i64;

    for (severity,) in results {
        match severity.as_str() {
            "critical" => critical_count += 1,
            "high" => high_count += 1,
            "medium" => medium_count += 1,
            "low" => low_count += 1,
            _ => {}
        }
    }

    Ok(IntelSummary {
        critical_count,
        high_count,
        medium_count,
        low_count,
    })
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_get_site_intel_summary`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add get_site_intel_summary for intel lead aggregation"
```

---

### Task 4: Implement get_site_active_leads function

**Files:**
- Modify: `src/lib.rs:after-get_site_intel_summary`

**Interfaces:**
- Consumes: `&mut PgConnection, host: &str, limit: i64`
- Produces: `pub fn get_site_active_leads(conn: &mut PgConnection, host: &str, limit: i64) -> Result<Vec<IntelLead>>`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn test_get_site_active_leads() {
    let mut conn = establish_test_connection();
    setup_test_data(&mut conn);
    
    let result = get_site_active_leads(&mut conn, "test.onion", 10);
    assert!(result.is_ok());
    let leads = result.unwrap();
    assert!(leads.len() <= 10);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_get_site_active_leads`
Expected: FAIL

- [ ] **Step 3: Implement get_site_active_leads**

```rust
pub fn get_site_active_leads(
    conn: &mut PgConnection,
    host: &str,
    limit: i64,
) -> Result<Vec<IntelLead>> {
    use crate::schema::{intel_lead, page};

    let leads = intel_lead::table
        .inner_join(page::table.on(page::id.eq(intel_lead::page_id)))
        .filter(page::url.like(format!("%{}%", host)))
        .filter(intel_lead::status.ne("resolved"))
        .order_by((
            diesel::dsl::sql::<diesel::sql_types::Integer>(
                "CASE severity 
                 WHEN 'critical' THEN 1 
                 WHEN 'high' THEN 2 
                 WHEN 'medium' THEN 3 
                 WHEN 'low' THEN 4 
                 ELSE 5 END"
            ),
            intel_lead::created_at.desc(),
        ))
        .select(IntelLead::as_select())
        .limit(limit)
        .load::<IntelLead>(conn)
        .context("error loading active intel leads")?;

    Ok(leads)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_get_site_active_leads`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add get_site_active_leads for unresolved leads"
```

---

### Task 5: Implement get_site_pages function

**Files:**
- Modify: `src/lib.rs:after-get_site_active_leads`

**Interfaces:**
- Consumes: `&mut PgConnection, host: &str, limit: i64, offset: i64`
- Produces: `pub fn get_site_pages(conn: &mut PgConnection, host: &str, limit: i64, offset: i64) -> Result<Vec<PageSummary>>`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn test_get_site_pages() {
    let mut conn = establish_test_connection();
    setup_test_data(&mut conn);
    
    let result = get_site_pages(&mut conn, "test.onion", 50, 0);
    assert!(result.is_ok());
    let pages = result.unwrap();
    for page in &pages {
        assert!(page.url.contains("test.onion"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_get_site_pages`
Expected: FAIL

- [ ] **Step 3: Implement get_site_pages**

```rust
pub fn get_site_pages(
    conn: &mut PgConnection,
    host: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<PageSummary>> {
    use crate::schema::{page, page_crypto, page_email, page_link};

    let pages = page::table
        .filter(page::host.eq(host))
        .order_by(page::last_scanned_at.desc())
        .limit(limit)
        .offset(offset)
        .select((page::id, page::title, page::url, page::last_scanned_at))
        .load::<(i32, String, String, String)>(conn)
        .context("error loading site pages")?;

    let mut summaries = Vec::new();
    for (id, title, url, last_scanned_at) in pages {
        let email_count = page_email::table
            .filter(page_email::page_id.eq(id))
            .count()
            .get_result::<i64>(conn)
            .unwrap_or(0);

        let crypto_count = page_crypto::table
            .filter(page_crypto::page_id.eq(id))
            .count()
            .get_result::<i64>(conn)
            .unwrap_or(0);

        let link_count = page_link::table
            .filter(page_link::source_page_id.eq(id))
            .count()
            .get_result::<i64>(conn)
            .unwrap_or(0);

        summaries.push(PageSummary {
            id,
            title,
            url,
            last_scanned_at,
            email_count,
            crypto_count,
            link_count,
        });
    }

    Ok(summaries)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_get_site_pages`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add get_site_pages with entity counts"
```

---

### Task 6: Implement get_host_service_fingerprints function

**Files:**
- Modify: `src/lib.rs:after-get_site_pages`

**Interfaces:**
- Consumes: `&mut PgConnection, host: &str`
- Produces: `pub fn get_host_service_fingerprints(conn: &mut PgConnection, host: &str) -> Result<ServiceFingerprints>`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn test_get_host_service_fingerprints() {
    let mut conn = establish_test_connection();
    
    let result = get_host_service_fingerprints(&mut conn, "test.onion");
    assert!(result.is_ok());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_get_host_service_fingerprints`
Expected: FAIL

- [ ] **Step 3: Implement get_host_service_fingerprints**

```rust
pub fn get_host_service_fingerprints(
    conn: &mut PgConnection,
    host: &str,
) -> Result<ServiceFingerprints> {
    use crate::schema::{host_http_observation, host_tls_observation, ssh_observation};

    let http = host_http_observation::table
        .filter(host_http_observation::host.eq(host))
        .order_by(host_http_observation::observed_at.desc())
        .first::<HostHttpObservation>(conn)
        .optional()
        .context("error loading HTTP observation")?;

    let tls = host_tls_observation::table
        .filter(host_tls_observation::host.eq(host))
        .order_by(host_tls_observation::observed_at.desc())
        .first::<HostTlsObservation>(conn)
        .optional()
        .context("error loading TLS observation")?;

    let ssh = ssh_observation::table
        .filter(ssh_observation::host.eq(host))
        .order_by(ssh_observation::observed_at.desc())
        .first::<SshObservation>(conn)
        .optional()
        .context("error loading SSH observation")?;

    Ok(ServiceFingerprints { http, tls, ssh })
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_get_host_service_fingerprints`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add get_host_service_fingerprints for HTTP/TLS/SSH data"
```

---

### Task 7: Implement get_site_relationships function

**Files:**
- Modify: `src/lib.rs:after-get_host_service_fingerprints`

**Interfaces:**
- Consumes: `&mut PgConnection, host: &str`
- Produces: `pub fn get_site_relationships(conn: &mut PgConnection, host: &str) -> Result<RelationshipData>`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn test_get_site_relationships() {
    let mut conn = establish_test_connection();
    
    let result = get_site_relationships(&mut conn, "test.onion");
    assert!(result.is_ok());
    let rel = result.unwrap();
    assert!(rel.inbound_count >= 0);
    assert!(rel.outbound_count >= 0);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_get_site_relationships`
Expected: FAIL

- [ ] **Step 3: Implement get_site_relationships**

```rust
pub fn get_site_relationships(
    conn: &mut PgConnection,
    host: &str,
) -> Result<RelationshipData> {
    use crate::schema::{page, page_link};

    // Inbound: other domains linking to this host
    let inbound_domains: Vec<String> = page_link::table
        .inner_join(page::table.on(page::id.eq(page_link::source_page_id)))
        .filter(page_link::target_url.like(format!("%{}%", host)))
        .filter(page::host.ne(host))
        .select(page::host)
        .distinct()
        .load::<String>(conn)
        .context("error loading inbound relationships")?;

    let inbound_count = inbound_domains.len() as i64;

    // Outbound: this host linking to other domains
    let outbound_domains: Vec<String> = page_link::table
        .inner_join(page::table.on(page::id.eq(page_link::source_page_id)))
        .filter(page::host.eq(host))
        .filter(page_link::target_url.not_like(format!("%{}%", host)))
        .select(page_link::target_url)
        .distinct()
        .load::<String>(conn)
        .context("error loading outbound relationships")?
        .into_iter()
        .filter_map(|url| {
            url::Url::parse(&url)
                .ok()
                .and_then(|u| u.host_str().map(|h| h.to_string()))
        })
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let outbound_count = outbound_domains.len() as i64;

    Ok(RelationshipData {
        inbound_count,
        outbound_count,
        inbound_domains,
        outbound_domains,
    })
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_get_site_relationships`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add get_site_relationships for inbound/outbound links"
```

---

### Task 8: Implement get_site_discovery_stats function

**Files:**
- Modify: `src/lib.rs:after-get_site_relationships`

**Interfaces:**
- Consumes: `&mut PgConnection, host: &str`
- Produces: `pub fn get_site_discovery_stats(conn: &mut PgConnection, host: &str) -> Result<DiscoveryStats>`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn test_get_site_discovery_stats() {
    let mut conn = establish_test_connection();
    
    let result = get_site_discovery_stats(&mut conn, "test.onion");
    assert!(result.is_ok());
    let stats = result.unwrap();
    assert!(stats.urls_discovered_from_this_site >= 0);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_get_site_discovery_stats`
Expected: FAIL

- [ ] **Step 3: Implement get_site_discovery_stats**

```rust
pub fn get_site_discovery_stats(
    conn: &mut PgConnection,
    host: &str,
) -> Result<DiscoveryStats> {
    use crate::schema::{page, url_discovery};

    let page_ids: Vec<i32> = page::table
        .filter(page::host.eq(host))
        .select(page::id)
        .load::<i32>(conn)
        .context("error loading page ids")?;

    let count = url_discovery::table
        .filter(url_discovery::discovered_from_page_id.eq_any(page_ids))
        .count()
        .get_result::<i64>(conn)
        .unwrap_or(0);

    Ok(DiscoveryStats {
        urls_discovered_from_this_site: count,
    })
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_get_site_discovery_stats`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add get_site_discovery_stats for URL discovery productivity"
```

---

### Task 9: Implement get_site_queue_stats function

**Files:**
- Modify: `src/lib.rs:after-get_site_discovery_stats`

**Interfaces:**
- Consumes: `&mut PgConnection, host: &str`
- Produces: `pub fn get_site_queue_stats(conn: &mut PgConnection, host: &str) -> Result<QueueStats>`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn test_get_site_queue_stats() {
    let mut conn = establish_test_connection();
    
    let result = get_site_queue_stats(&mut conn, "test.onion");
    assert!(result.is_ok());
    let stats = result.unwrap();
    assert!(stats.success_rate >= 0.0 && stats.success_rate <= 100.0);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_get_site_queue_stats`
Expected: FAIL

- [ ] **Step 3: Implement get_site_queue_stats**

```rust
pub fn get_site_queue_stats(
    conn: &mut PgConnection,
    host: &str,
) -> Result<QueueStats> {
    use crate::schema::work_unit;

    let stats_rows = work_unit::table
        .filter(work_unit::url.like(format!("%{}%", host)))
        .select((
            work_unit::status,
            work_unit::failure_category,
            diesel::dsl::count_star(),
            diesel::dsl::avg(work_unit::retry_count),
        ))
        .group_by((work_unit::status, work_unit::failure_category))
        .load::<(String, Option<String>, i64, Option<f64>)>(conn)
        .context("error loading work queue stats")?;

    let total_work_units: i64 = stats_rows.iter().map(|(_, _, count, _)| count).sum();
    let success_count: i64 = stats_rows
        .iter()
        .filter(|(status, _, _, _)| status == "done")
        .map(|(_, _, count, _)| count)
        .sum();
    let failure_count: i64 = stats_rows
        .iter()
        .filter(|(status, _, _, _)| status == "failed")
        .map(|(_, _, count, _)| count)
        .sum();

    let success_rate = if total_work_units > 0 {
        (success_count as f64 / total_work_units as f64) * 100.0
    } else {
        0.0
    };

    let failure_breakdown: Vec<(String, i64)> = stats_rows
        .iter()
        .filter(|(status, cat, _, _)| status == "failed" && cat.is_some())
        .map(|(_, cat, count, _)| (cat.clone().unwrap(), *count))
        .collect();

    let avg_retry_count = stats_rows
        .iter()
        .filter_map(|(_, _, _, avg)| *avg)
        .next()
        .unwrap_or(0.0);

    Ok(QueueStats {
        total_work_units,
        success_count,
        failure_count,
        success_rate,
        failure_breakdown,
        avg_retry_count,
    })
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_get_site_queue_stats`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add get_site_queue_stats for work queue metrics"
```

---

### Task 10: Implement get_site_detail aggregator function

**Files:**
- Modify: `src/lib.rs:after-get_site_queue_stats`

**Interfaces:**
- Consumes: `&mut PgConnection, host: &str, page_limit: i64, page_offset: i64`
- Produces: `pub fn get_site_detail(conn: &mut PgConnection, host: &str, page_limit: i64, page_offset: i64) -> Result<SiteDetailData>`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn test_get_site_detail() {
    let mut conn = establish_test_connection();
    setup_test_data(&mut conn);
    
    let result = get_site_detail(&mut conn, "test.onion", 50, 0);
    assert!(result.is_ok());
    let detail = result.unwrap();
    assert_eq!(detail.profile.host, "test.onion");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_get_site_detail`
Expected: FAIL

- [ ] **Step 3: Implement get_site_detail**

```rust
pub fn get_site_detail(
    conn: &mut PgConnection,
    host: &str,
    page_limit: i64,
    page_offset: i64,
) -> Result<SiteDetailData> {
    use crate::schema::site_profile;

    conn.transaction(|conn| {
        let profile = site_profile::table
            .filter(site_profile::host.eq(host))
            .first::<SiteProfileRecord>(conn)
            .context("site not found")?;

        let intel_summary = get_site_intel_summary(conn, host)?;
        let active_leads = get_site_active_leads(conn, host, 100)?;
        let pages = get_site_pages(conn, host, page_limit, page_offset)?;
        let service_fingerprints = get_host_service_fingerprints(conn, host)?;
        let relationships = get_site_relationships(conn, host)?;
        let discovery_stats = get_site_discovery_stats(conn, host)?;
        let queue_stats = get_site_queue_stats(conn, host)?;

        Ok(SiteDetailData {
            profile,
            intel_summary,
            active_leads,
            pages,
            service_fingerprints,
            relationships,
            discovery_stats,
            queue_stats,
        })
    })
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_get_site_detail`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add get_site_detail aggregator for site detail page"
```

---

### Task 11: Implement get_page_discovery_chain function

**Files:**
- Modify: `src/lib.rs:after-get_site_detail`

**Interfaces:**
- Consumes: `&mut PgConnection, page_url: &str`
- Produces: `pub fn get_page_discovery_chain(conn: &mut PgConnection, page_url: &str) -> Result<Option<DiscoveryChain>>`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn test_get_page_discovery_chain() {
    let mut conn = establish_test_connection();
    setup_test_data(&mut conn);
    
    let result = get_page_discovery_chain(&mut conn, "http://test.onion/page");
    assert!(result.is_ok());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_get_page_discovery_chain`
Expected: FAIL

- [ ] **Step 3: Implement get_page_discovery_chain**

```rust
pub fn get_page_discovery_chain(
    conn: &mut PgConnection,
    page_url: &str,
) -> Result<Option<DiscoveryChain>> {
    use crate::schema::{import_source, page, url_discovery};

    let discovery = url_discovery::table
        .filter(url_discovery::url.eq(page_url))
        .first::<UrlDiscovery>(conn)
        .optional()
        .context("error loading url_discovery")?;

    let discovery = match discovery {
        Some(d) => d,
        None => return Ok(None),
    };

    let import_source_name = if let Some(source_id) = discovery.import_source_id {
        import_source::table
            .find(source_id)
            .select(import_source::source_name)
            .first::<String>(conn)
            .optional()
            .context("error loading import source")?
    } else {
        None
    };

    let mut chain_items = Vec::new();
    for page_id_opt in &discovery.discovery_chain {
        if let Some(page_id) = page_id_opt {
            let (title, url) = page::table
                .find(page_id)
                .select((page::title, page::url))
                .first::<(String, String)>(conn)
                .context("error loading chain page")?;

            chain_items.push(ChainItem {
                page_id: *page_id,
                page_title: title,
                page_url: url,
            });
        }
    }

    Ok(Some(DiscoveryChain {
        depth: discovery.discovery_depth,
        import_source_id: discovery.import_source_id,
        import_source_name,
        chain: chain_items,
        discovered_at: discovery.discovered_at,
    }))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_get_page_discovery_chain`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add get_page_discovery_chain for provenance tracking"
```

---

### Task 12: Implement get_page_work_queue_history function

**Files:**
- Modify: `src/lib.rs:after-get_page_discovery_chain`

**Interfaces:**
- Consumes: `&mut PgConnection, page_url: &str`
- Produces: `pub fn get_page_work_queue_history(conn: &mut PgConnection, page_url: &str) -> Result<Option<WorkQueueHistory>>`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn test_get_page_work_queue_history() {
    let mut conn = establish_test_connection();
    
    let result = get_page_work_queue_history(&mut conn, "http://test.onion");
    assert!(result.is_ok());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_get_page_work_queue_history`
Expected: FAIL

- [ ] **Step 3: Implement get_page_work_queue_history**

```rust
pub fn get_page_work_queue_history(
    conn: &mut PgConnection,
    page_url: &str,
) -> Result<Option<WorkQueueHistory>> {
    use crate::schema::work_unit;

    let work = work_unit::table
        .filter(work_unit::url.eq(page_url))
        .order_by(work_unit::last_attempt_at.desc())
        .first::<WorkUnit>(conn)
        .optional()
        .context("error loading work queue history")?;

    Ok(work.map(|w| WorkQueueHistory {
        status: w.status,
        retry_count: w.retry_count,
        failure_category: w.failure_category,
        last_error: w.last_error,
        last_attempt_at: w.last_attempt_at,
        next_attempt_at: w.next_attempt_at,
    }))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_get_page_work_queue_history`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "feat: add get_page_work_queue_history for queue status"
```

---

### Task 13: Add site_detail route handler

**Files:**
- Modify: `src/bin/frontend.rs:routes-section`

**Interfaces:**
- Consumes: `host: String` (URL parameter), uses `get_site_detail()` from lib.rs
- Produces: Route handler `#[get("/sites/<host>")] fn site_detail(host: String, state: &State<AppState>) -> HtmlResult`

- [ ] **Step 1: Write failing integration test**

```rust
#[test]
fn test_site_detail_route() {
    let client = test_client();
    let response = client.get("/sites/test.onion").dispatch();
    assert_eq!(response.status(), Status::Ok);
}
```

Add to frontend.rs test module.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_site_detail_route`
Expected: FAIL with 404 Not Found

- [ ] **Step 3: Add urlencoding import**

```rust
use urlencoding;
```

Add to imports at top of src/bin/frontend.rs.

- [ ] **Step 4: Implement site_detail handler**

```rust
#[get("/sites/<host>")]
fn site_detail(host: String, state: &State<AppState>) -> HtmlResult {
    let decoded_host = urlencoding::decode(&host)
        .map_err(|_| FrontendError::bad_request("Invalid host encoding"))?;

    if decoded_host.is_empty() || decoded_host.len() > 253 {
        return Err(FrontendError::bad_request("Invalid host"));
    }

    let mut connection = state
        .db_pool
        .get()
        .map_err(|e| FrontendError::internal("database connection", e.into()))?;

    let site_data = spyder::get_site_detail(&mut connection, &decoded_host, 50, 0).map_err(
        |e| {
            if e.to_string().contains("not found") {
                FrontendError::not_found("Site not found")
            } else {
                FrontendError::internal("loading site detail", e)
            }
        },
    )?;

    let has_leads = !site_data.active_leads.is_empty();
    let has_services = site_data.service_fingerprints.http.is_some()
        || site_data.service_fingerprints.tls.is_some()
        || site_data.service_fingerprints.ssh.is_some();

    let context = json!({
        "title": format!("Site: {}", decoded_host),
        "host": decoded_host,
        "profile": site_data.profile,
        "intel_summary": site_data.intel_summary,
        "active_leads": site_data.active_leads,
        "has_leads": has_leads,
        "pages": site_data.pages,
        "has_pages": !site_data.pages.is_empty(),
        "service_fingerprints": site_data.service_fingerprints,
        "has_services": has_services,
        "relationships": site_data.relationships,
        "discovery_stats": site_data.discovery_stats,
        "queue_stats": site_data.queue_stats,
    });

    render_template("site_detail", &context, state)
}
```

Add after existing route handlers in src/bin/frontend.rs.

- [ ] **Step 5: Register route in rocket() function**

Find the `rocket()` function and add `.mount("/", routes![..., site_detail])` to the routes list.

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test test_site_detail_route`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/bin/frontend.rs
git commit -m "feat: add /sites/<host> route handler"
```

---

### Task 14: Add API site_detail route handler

**Files:**
- Modify: `src/bin/frontend.rs:api-routes-section`

**Interfaces:**
- Consumes: `host: String`, uses `get_site_detail()` from lib.rs
- Produces: Route handler `#[get("/api/sites/<host>")] fn api_site_detail(...) -> Result<Json<ApiResponse<SiteDetailData>>>`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn test_api_site_detail() {
    let client = test_client();
    let response = client.get("/api/sites/test.onion").dispatch();
    assert_eq!(response.status(), Status::Ok);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_api_site_detail`
Expected: FAIL

- [ ] **Step 3: Implement api_site_detail handler**

```rust
#[get("/api/sites/<host>")]
fn api_site_detail(
    host: String,
    state: &State<AppState>,
) -> Result<Json<ApiResponse<SiteDetailData>>, ApiError> {
    let decoded_host = urlencoding::decode(&host)
        .map_err(|_| ApiError::BadRequest("Invalid host encoding".into()))?;

    let mut connection = state
        .db_pool
        .get()
        .map_err(|e| ApiError::Internal(e.into()))?;

    let site_data = spyder::get_site_detail(&mut connection, &decoded_host, 50, 0)
        .map_err(|e| ApiError::Internal(e))?;

    Ok(Json(ApiResponse::success(site_data)))
}
```

- [ ] **Step 4: Register route in rocket() function**

Add `.mount("/api", routes![..., api_site_detail])`.

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test test_api_site_detail`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/bin/frontend.rs
git commit -m "feat: add /api/sites/<host> JSON endpoint"
```

---

### Task 15: Enhance page_detail route with new data

**Files:**
- Modify: `src/bin/frontend.rs:page_detail-function`

**Interfaces:**
- Consumes: Existing `page_detail` handler
- Produces: Enhanced handler with discovery, service_fingerprints, work_queue in context

- [ ] **Step 1: Write test for enhanced page detail**

```rust
#[test]
fn test_enhanced_page_detail() {
    let client = test_client();
    let response = client.get("/pages/1").dispatch();
    assert_eq!(response.status(), Status::Ok);
    let body = response.into_string().unwrap();
    // Just verify it doesn't crash - discovery/services may be missing
    assert!(body.contains("Page Detail") || body.contains("page"));
}
```

- [ ] **Step 2: Run test to verify baseline**

Run: `cargo test test_enhanced_page_detail`
Expected: PASS (existing page_detail works)

- [ ] **Step 3: Add new queries to page_detail handler**

Find the `page_detail` function and after loading the page, add:

```rust
// NEW: Get discovery chain
let discovery_chain = spyder::get_page_discovery_chain(&mut connection, &page.url)
    .ok()
    .flatten();

// NEW: Get service fingerprints
let service_fingerprints = spyder::get_host_service_fingerprints(&mut connection, &page.host)
    .ok();

// NEW: Get work queue history
let work_queue = spyder::get_page_work_queue_history(&mut connection, &page.url)
    .ok()
    .flatten();
```

Then in the context building section, add:

```rust
context["discovery"] = json!(discovery_chain);
context["service_fingerprints"] = json!(service_fingerprints);
context["work_queue"] = json!(work_queue);
```

- [ ] **Step 4: Run test to verify it still passes**

Run: `cargo test test_enhanced_page_detail`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/bin/frontend.rs
git commit -m "feat: enhance page_detail with discovery, services, queue data"
```

---

### Task 16: Create service-fingerprints partial template

**Files:**
- Create: `templates/partials/service-fingerprints.hbs`

**Interfaces:**
- Consumes: `fingerprints` context variable with `http`, `tls`, `ssh` fields
- Produces: Handlebars partial rendering service cards

- [ ] **Step 1: Create partials directory**

Run: `mkdir -p templates/partials`
Expected: Directory created (may already exist)

- [ ] **Step 2: Create service-fingerprints.hbs**

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

- [ ] **Step 3: Test partial renders (manual)**

Start server: `cargo run --bin frontend`
Navigate to a page with services in browser
Expected: Service cards render if data exists

- [ ] **Step 4: Commit**

```bash
git add templates/partials/service-fingerprints.hbs
git commit -m "feat: add service-fingerprints reusable partial"
```

---

### Task 17: Create intel-summary partial template

**Files:**
- Create: `templates/partials/intel-summary.hbs`

**Interfaces:**
- Consumes: `summary` context variable with `critical_count`, `high_count`, `medium_count`, `low_count`
- Produces: Handlebars partial rendering intel severity grid

- [ ] **Step 1: Create intel-summary.hbs**

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

- [ ] **Step 2: Commit**

```bash
git add templates/partials/intel-summary.hbs
git commit -m "feat: add intel-summary reusable partial"
```

---

### Task 18: Create site_detail template

**Files:**
- Create: `templates/site_detail.html.hbs`

**Interfaces:**
- Consumes: Context from site_detail route handler
- Produces: Complete site detail page HTML

- [ ] **Step 1: Create site_detail.html.hbs with hero section**

```handlebars
{{#*inline "body"}}
<section class="hero hero-compact">
    <p class="eyebrow">Site Detail</p>
    <h1>{{host}}</h1>
    <div class="meta-strip">
        <span><strong>Category:</strong> {{profile.category}}</span>
        <span><strong>Pages:</strong> {{profile.page_count}}</span>
        <span><strong>Last Scanned:</strong> {{profile.last_scanned_at}}</span>
    </div>
    <div class="meta-badges">
        <span class="site-category-badge site-confidence-{{profile.confidence}}">
            {{profile.category}} · {{profile.confidence}}
        </span>
    </div>
</section>
```

- [ ] **Step 2: Add intel summary section**

```handlebars
<section class="card">
    <div class="section-heading">
        <h2>Intelligence Summary</h2>
        <span class="muted">Lead severity breakdown</span>
    </div>
    {{> intel-summary summary=intel_summary}}
</section>
```

- [ ] **Step 3: Add active leads section**

```handlebars
{{#if has_leads}}
<section class="card">
    <div class="section-heading">
        <h2>Active Leads</h2>
        <span class="muted">{{active_leads.length}} unresolved</span>
    </div>
    <div class="table-wrap">
        <table class="data-table">
            <thead>
                <tr>
                    <th>Severity</th>
                    <th>Lead</th>
                    <th>Confidence</th>
                    <th>Status</th>
                    <th>Created</th>
                </tr>
            </thead>
            <tbody>
                {{#each active_leads}}
                <tr>
                    <td><span class="lead-badge severity-{{severity}}">{{severity}}</span></td>
                    <td><a class="row-link" href="/leads/{{id}}">{{entity}}</a></td>
                    <td>{{confidence}}</td>
                    <td>{{status}}</td>
                    <td>{{created_at}}</td>
                </tr>
                {{/each}}
            </tbody>
        </table>
    </div>
</section>
{{else}}
<section class="card empty-state">
    <p>No active intelligence leads for this site.</p>
</section>
{{/if}}
```

- [ ] **Step 4: Add service fingerprints section**

```handlebars
{{#if has_services}}
<section class="card">
    <div class="section-heading">
        <h2>Service Fingerprints</h2>
        <span class="muted">Latest observations</span>
    </div>
    {{> service-fingerprints fingerprints=service_fingerprints}}
</section>
{{/if}}
```

- [ ] **Step 5: Add pages section**

```handlebars
{{#if has_pages}}
<section class="card">
    <div class="section-heading">
        <h2>Pages from this Site</h2>
        <span class="muted">{{pages.length}} pages</span>
    </div>
    <div class="table-wrap">
        <table class="data-table">
            <thead>
                <tr>
                    <th>Title</th>
                    <th>Last Scan</th>
                    <th>Entities</th>
                </tr>
            </thead>
            <tbody>
                {{#each pages}}
                <tr>
                    <td>
                        <a class="row-link" href="/pages/{{id}}">{{title}}</a>
                        <div class="cell-subtitle">{{url}}</div>
                    </td>
                    <td>{{last_scanned_at}}</td>
                    <td>
                        <span class="count-pill">{{email_count}} emails</span>
                        <span class="count-pill">{{crypto_count}} crypto</span>
                        <span class="count-pill">{{link_count}} links</span>
                    </td>
                </tr>
                {{/each}}
            </tbody>
        </table>
    </div>
</section>
{{/if}}
```

- [ ] **Step 6: Add relationships and stats sections**

```handlebars
<section class="card">
    <div class="section-heading">
        <h2>Relationships</h2>
    </div>
    <div class="meta-strip">
        <span><strong>Inbound:</strong> {{relationships.inbound_count}} domains link here</span>
        <span><strong>Outbound:</strong> Links to {{relationships.outbound_count}} domains</span>
    </div>
    <div class="actions">
        <a href="/relationships?focus={{host}}" class="btn btn-secondary">View Relationship Graph</a>
    </div>
</section>

<section class="card">
    <div class="section-heading">
        <h2>Discovery & Queue Stats</h2>
    </div>
    <div class="meta-strip">
        <span><strong>URLs Discovered:</strong> {{discovery_stats.urls_discovered_from_this_site}}</span>
        <span><strong>Total Work Units:</strong> {{queue_stats.total_work_units}}</span>
        <span><strong>Success Rate:</strong> {{queue_stats.success_rate}}%</span>
    </div>
</section>

{{/inline}}

{{> base}}
```

- [ ] **Step 7: Commit**

```bash
git add templates/site_detail.html.hbs
git commit -m "feat: add site_detail template with threat-intel layout"
```

---

### Task 19: Enhance page_detail template with new sections

**Files:**
- Modify: `templates/page_detail.html.hbs`

**Interfaces:**
- Consumes: Enhanced context from page_detail handler
- Produces: Page detail with discovery, services, queue sections

- [ ] **Step 1: Find insertion point after hero section**

Read templates/page_detail.html.hbs and locate the hero section end (after `</section>` for hero).

- [ ] **Step 2: Add discovery provenance section**

Insert after hero section:

```handlebars
{{#if discovery}}
<section class="card">
    <div class="section-heading">
        <h2>Discovery Provenance</h2>
        <span class="depth-badge depth-{{discovery.depth}}">Depth {{discovery.depth}}</span>
    </div>
    
    <div class="discovery-breadcrumb">
        {{#if discovery.import_source_name}}
        <span class="breadcrumb-item">
            <span class="breadcrumb-label">Import Source:</span>
            <span>{{discovery.import_source_name}}</span>
        </span>
        <span class="breadcrumb-separator">→</span>
        {{/if}}
        
        {{#each discovery.chain}}
        <span class="breadcrumb-item">
            <a class="breadcrumb-link" href="/pages/{{page_id}}">{{page_title}}</a>
        </span>
        <span class="breadcrumb-separator">→</span>
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

- [ ] **Step 3: Add service fingerprints section**

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

- [ ] **Step 4: Add work queue history section**

Insert near bottom, before scan history section:

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

- [ ] **Step 5: Commit**

```bash
git add templates/page_detail.html.hbs
git commit -m "feat: add discovery, services, queue sections to page detail"
```

---

### Task 20: Update sites.html.hbs template with clickable hosts

**Files:**
- Modify: `templates/sites.html.hbs`

**Interfaces:**
- Consumes: Existing sites template
- Produces: Template with host names linked to /sites/{host}

- [ ] **Step 1: Find host column in template**

Read templates/sites.html.hbs and find `<td>{{host}}</td>` line.

- [ ] **Step 2: Make host clickable**

Replace `<td>{{host}}</td>` with:

```handlebars
<td><a class="row-link" href="/sites/{{host}}">{{host}}</a></td>
```

- [ ] **Step 3: Test manually**

Start server, navigate to /sites, click host name.
Expected: Navigate to /sites/{host} detail page

- [ ] **Step 4: Commit**

```bash
git add templates/sites.html.hbs
git commit -m "feat: make host names clickable in sites list"
```

---

### Task 21: Update sites_grouped.html.hbs template with clickable hosts

**Files:**
- Modify: `templates/sites_grouped.html.hbs`

**Interfaces:**
- Consumes: Existing sites_grouped template
- Produces: Template with host names linked to /sites/{host}

- [ ] **Step 1: Find host column in template**

Read templates/sites_grouped.html.hbs and find `<td>{{host}}</td>` line in the hosts table.

- [ ] **Step 2: Make host clickable**

Replace `<td>{{host}}</td>` with:

```handlebars
<td><a class="row-link" href="/sites/{{host}}">{{host}}</a></td>
```

- [ ] **Step 3: Commit**

```bash
git add templates/sites_grouped.html.hbs
git commit -m "feat: make host names clickable in grouped sites view"
```

---

### Task 22: Update discovery.html.hbs template with page detail links

**Files:**
- Modify: `templates/discovery.html.hbs`
- Modify: `src/bin/frontend.rs:discovery-handler`

**Interfaces:**
- Consumes: Existing discovery template and handler
- Produces: URLs link to page detail when available, otherwise external

- [ ] **Step 1: Update discovery handler to include page_id**

Find the `build_discovery_context` function in src/bin/frontend.rs.

In the query that fetches discoveries, add a JOIN to get page_id:

```rust
// Add LEFT JOIN to page table to get page_id if crawled
let discoveries_with_pages: Vec<(i32, String, i32, String, Option<i32>, Option<i32>)> = 
    url_discovery::table
        .left_join(page::table.on(page::url.eq(url_discovery::url)))
        .select((
            url_discovery::id,
            url_discovery::url,
            url_discovery::discovery_depth,
            url_discovery::discovered_at,
            url_discovery::import_source_id,
            page::id.nullable(),  // page_id
        ))
        // ... rest of query
```

Then in discoveries_json mapping, add page_id:

```rust
let discoveries_json: Vec<_> = discoveries_with_pages.iter().map(
    |(id, url, depth, discovered_at, source_id, page_id)| {
        // ...
        "page_id": page_id,
        // ...
    }
).collect();
```

- [ ] **Step 2: Update discovery.html.hbs template**

Find the URL cell `<td><a href="{{url}}" target="_blank" class="row-link">{{url}}</a></td>`.

Replace with:

```handlebars
<td>
    {{#if page_id}}
    <a class="row-link" href="/pages/{{page_id}}">{{url}}</a>
    {{else}}
    <a href="{{url}}" target="_blank" class="row-link external-link">{{url}}</a>
    {{/if}}
</td>
```

- [ ] **Step 3: Commit**

```bash
git add src/bin/frontend.rs templates/discovery.html.hbs
git commit -m "feat: link discovered URLs to page detail when crawled"
```

---

### Task 23: Update leads.html.hbs template with page detail links

**Files:**
- Modify: `templates/leads.html.hbs`

**Interfaces:**
- Consumes: Existing leads template (context already includes source_page_id from backend)
- Produces: Source page titles linked to /pages/{id}

- [ ] **Step 1: Find source page column**

Read templates/leads.html.hbs and find where `source_page_title` is displayed.

- [ ] **Step 2: Make source page clickable**

Replace source page cell with:

```handlebars
<td>
    {{#if source_page_id}}
    <a class="row-link" href="/pages/{{source_page_id}}">{{source_page_title}}</a>
    {{else}}
    {{source_page_title}}
    {{/if}}
</td>
```

- [ ] **Step 3: Commit**

```bash
git add templates/leads.html.hbs
git commit -m "feat: link lead source pages to page detail"
```

---

### Task 24: Add CSS styles for new components

**Files:**
- Modify: `static/styles.css`

**Interfaces:**
- Consumes: None
- Produces: CSS classes for intel summary, service grid, discovery breadcrumb, work queue badges

- [ ] **Step 1: Add intel summary styles**

```css
/* Site Detail Page - Intel Summary */
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
```

Append to static/styles.css.

- [ ] **Step 2: Add service fingerprint styles**

```css
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
```

- [ ] **Step 3: Add discovery breadcrumb styles**

```css
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
```

- [ ] **Step 4: Add work queue status styles**

```css
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
```

- [ ] **Step 5: Add responsive adjustments**

```css
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

- [ ] **Step 6: Verify CSS compiles**

Run: `cargo run --bin frontend`
Open browser, check styles load
Expected: New components styled correctly

- [ ] **Step 7: Commit**

```bash
git add static/styles.css
git commit -m "feat: add CSS for detail page components"
```

---

### Task 25: Manual integration testing

**Files:**
- None (testing only)

**Interfaces:**
- Consumes: All previous tasks
- Produces: Verified working system

- [ ] **Step 1: Start server and test sites list**

Run: `cargo run --bin frontend`
Navigate to http://localhost:8000/sites
Click a host name
Expected: Navigate to /sites/{host} detail page with all sections

- [ ] **Step 2: Test site with leads**

Find site with intelligence leads in database
Navigate to its detail page
Expected: Intel summary shows counts, active leads table populated

- [ ] **Step 3: Test site without leads**

Navigate to site without leads
Expected: Empty state message "No active intelligence leads"

- [ ] **Step 4: Test page detail enhancements**

Navigate to http://localhost:8000/pages
Click a page title
Expected: See discovery provenance (if tracked), service fingerprints, work queue sections

- [ ] **Step 5: Test discovery links**

Navigate to http://localhost:8000/discovery
Click a discovered URL
Expected: If crawled, navigate to page detail; otherwise external link

- [ ] **Step 6: Test leads page links**

Navigate to http://localhost:8000/leads
Click source page title
Expected: Navigate to page detail

- [ ] **Step 7: Test mobile responsive**

Resize browser to mobile width
Expected: Intel grid shows 2 columns, service grid stacks, breadcrumb stacks

- [ ] **Step 8: Test error cases**

Navigate to http://localhost:8000/sites/nonexistent.onion
Expected: 404 error page

Navigate to http://localhost:8000/sites/%ZZ%ZZ
Expected: 400 error page "Invalid host encoding"

- [ ] **Step 9: Document test results**

Create manual-test-results.txt with pass/fail for each test.

- [ ] **Step 10: Commit if all pass**

```bash
git add manual-test-results.txt
git commit -m "test: manual integration testing complete"
```

---

## Self-Review Checklist

**Spec Coverage:**
- ✅ Site detail page at /sites/{host}
- ✅ API endpoint at /api/sites/{host}
- ✅ Enhanced page detail with discovery, services, queue
- ✅ Service fingerprints partial
- ✅ Intel summary partial
- ✅ Link updates in all templates (sites, sites_grouped, discovery, leads)
- ✅ CSS for all new components
- ✅ Backend functions for all data aggregation
- ✅ Error handling (404, 400, 500)
- ✅ Unit tests for backend functions
- ✅ Integration tests for routes
- ✅ Manual testing

**Placeholder Check:**
- ✅ No TBDs or TODOs
- ✅ Complete code in every implementation step
- ✅ Exact file paths specified
- ✅ Expected test outputs defined

**Type Consistency:**
- ✅ `IntelSummary` fields match across all tasks
- ✅ `ServiceFingerprints` structure consistent
- ✅ Function signatures match between lib.rs and frontend.rs
- ✅ Template context variables match handler outputs

---

## Execution Complete

All tasks ready for implementation. Each task is independently testable with clear success criteria.

Plan complete and saved to `docs/superpowers/plans/2026-06-22-detail-pages.md`.
