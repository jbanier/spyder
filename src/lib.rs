pub mod extraction;
pub mod models;
pub mod schema;

use anyhow::{Context, Result};
use diesel::deserialize::QueryableByName;
use diesel::dsl::{count_star, sql};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Bool, Nullable, Text};
use diesel::upsert::excluded;
use dotenvy::dotenv;
use models::*;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use url::form_urlencoded;
use url::Url;

pub const STATUS_PENDING: &str = "pending";
pub const STATUS_DONE: &str = "done";
pub const STATUS_FAILED: &str = "failed";
pub const MAX_RETRY_ATTEMPTS: i32 = 5;
const DEFAULT_PAGE_LIMIT: i64 = 50;
const MAX_PAGE_LIMIT: i64 = 200;
const CATEGORY_SEARCH_ENGINE: &str = "search-engine";
const CATEGORY_FORUM: &str = "forum";
const CATEGORY_MARKET: &str = "market";
const CATEGORY_DIRECTORY: &str = "directory";
const CATEGORY_WIKI: &str = "wiki";
const CATEGORY_BLOG: &str = "blog";
const CATEGORY_ESCROW: &str = "escrow";
const CATEGORY_SHOP: &str = "shop";
const CATEGORY_VENDOR_PAGE: &str = "vendor-page";
const CATEGORY_DOCS: &str = "docs";
const CATEGORY_INDEXER: &str = "indexer";
const CATEGORY_CONTENT: &str = "content";
const CATEGORY_UNKNOWN: &str = "unknown";
const CONFIDENCE_HIGH: &str = "high";
const CONFIDENCE_MEDIUM: &str = "medium";
const CONFIDENCE_LOW: &str = "low";

#[derive(Clone)]
struct ClassificationOutcome {
    host: String,
    category: String,
    confidence: String,
    score: i32,
    evidence: Vec<String>,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(QueryableByName)]
struct NullableTextRow {
    #[diesel(sql_type = Nullable<Text>)]
    value: Option<String>,
}

#[derive(QueryableByName)]
struct PageSummaryRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = Text)]
    title: String,
    #[diesel(sql_type = Text)]
    url: String,
    #[diesel(sql_type = Text)]
    host: String,
    #[diesel(sql_type = Text)]
    language: String,
    #[diesel(sql_type = Text)]
    last_scanned_at: String,
    #[diesel(sql_type = BigInt)]
    outbound_link_count: i64,
    #[diesel(sql_type = BigInt)]
    email_count: i64,
    #[diesel(sql_type = BigInt)]
    crypto_count: i64,
}

#[derive(QueryableByName)]
struct SearchResultRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    page_id: i32,
    #[diesel(sql_type = Text)]
    title: String,
    #[diesel(sql_type = Text)]
    url: String,
    #[diesel(sql_type = Text)]
    host: String,
    #[diesel(sql_type = Text)]
    language: String,
    #[diesel(sql_type = Text)]
    scraped_at: String,
}

#[derive(QueryableByName)]
struct PageScanSummaryRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    page_id: i32,
    #[diesel(sql_type = Text)]
    title: String,
    #[diesel(sql_type = Text)]
    language: String,
    #[diesel(sql_type = Text)]
    scanned_at: String,
    #[diesel(sql_type = BigInt)]
    outbound_link_count: i64,
    #[diesel(sql_type = BigInt)]
    email_count: i64,
    #[diesel(sql_type = BigInt)]
    crypto_count: i64,
}

#[derive(QueryableByName)]
struct EmailEntitySummaryRow {
    #[diesel(sql_type = Text)]
    value: String,
    #[diesel(sql_type = BigInt)]
    page_count: i64,
}

#[derive(QueryableByName)]
struct CryptoEntitySummaryRow {
    #[diesel(sql_type = Text)]
    asset_type: String,
    #[diesel(sql_type = Text)]
    reference: String,
    #[diesel(sql_type = BigInt)]
    page_count: i64,
}

#[derive(QueryableByName)]
struct SiteRelationshipRow {
    #[diesel(sql_type = Text)]
    source_host: String,
    #[diesel(sql_type = Text)]
    target_host: String,
    #[diesel(sql_type = BigInt)]
    reference_count: i64,
}

#[derive(QueryableByName)]
struct TargetHostCountRow {
    #[diesel(sql_type = Text)]
    target_host: String,
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(Clone, Copy)]
struct PaginationInput {
    limit: i64,
    offset: i64,
}

#[derive(Default)]
struct ScanObservationSet {
    links: BTreeSet<(String, String)>,
    emails: BTreeSet<String>,
    crypto_refs: BTreeSet<(String, String)>,
}

pub fn establish_connection() -> Result<SqliteConnection> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    SqliteConnection::establish(&database_url)
        .with_context(|| format!("error connecting to {database_url}"))
}

pub fn normalize_blacklist_domain(raw_domain: &str) -> Result<String> {
    let trimmed = raw_domain.trim().trim_end_matches('.');
    anyhow::ensure!(!trimmed.is_empty(), "blacklist domain must not be empty");
    anyhow::ensure!(
        !trimmed.contains("://"),
        "blacklist domain must not include a scheme"
    );

    let candidate = trimmed.to_ascii_lowercase();
    let parsed = Url::parse(&format!("http://{candidate}"))
        .with_context(|| format!("invalid blacklist domain: {raw_domain}"))?;
    anyhow::ensure!(
        parsed.username().is_empty() && parsed.password().is_none(),
        "blacklist domain must not include credentials"
    );
    anyhow::ensure!(
        parsed.port().is_none(),
        "blacklist domain must not include a port"
    );
    anyhow::ensure!(
        parsed.path() == "/" && parsed.query().is_none() && parsed.fragment().is_none(),
        "blacklist domain must not include a path, query, or fragment"
    );

    let host = parsed
        .host_str()
        .map(|value| value.to_ascii_lowercase())
        .context("blacklist domain must contain a valid host")?;
    anyhow::ensure!(
        !host.is_empty(),
        "blacklist domain must contain a valid host"
    );
    Ok(host)
}

pub fn find_matching_blacklist_domain(host: &str, blacklist_domains: &[String]) -> Option<String> {
    let normalized_host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if normalized_host.is_empty() {
        return None;
    }

    blacklist_domains
        .iter()
        .filter(|domain| host_matches_blacklist_domain(&normalized_host, domain))
        .max_by_key(|domain| domain.len())
        .cloned()
}

pub fn list_domain_blacklist_rules(
    conn: &mut SqliteConnection,
) -> Result<Vec<DomainBlacklistRule>> {
    use crate::schema::domain_blacklist::dsl as blacklist_dsl;

    blacklist_dsl::domain_blacklist
        .order(blacklist_dsl::domain.asc())
        .then_order_by(blacklist_dsl::id.asc())
        .select(DomainBlacklistRule::as_select())
        .load::<DomainBlacklistRule>(conn)
        .context("error loading blacklist domains")
}

pub fn add_domain_blacklist_entry(
    conn: &mut SqliteConnection,
    raw_domain: &str,
) -> Result<DomainBlacklistRule> {
    use crate::schema::domain_blacklist::dsl as blacklist_dsl;

    let normalized_domain = normalize_blacklist_domain(raw_domain)?;
    diesel::insert_into(crate::schema::domain_blacklist::table)
        .values(NewDomainBlacklist {
            domain: &normalized_domain,
        })
        .on_conflict(blacklist_dsl::domain)
        .do_nothing()
        .execute(conn)
        .context("error saving blacklist domain")?;

    blacklist_dsl::domain_blacklist
        .filter(blacklist_dsl::domain.eq(&normalized_domain))
        .select(DomainBlacklistRule::as_select())
        .first::<DomainBlacklistRule>(conn)
        .context("error loading saved blacklist domain")
}

pub fn remove_domain_blacklist_entry(
    conn: &mut SqliteConnection,
    raw_domain: &str,
) -> Result<String> {
    use crate::schema::domain_blacklist::dsl as blacklist_dsl;

    let normalized_domain = normalize_blacklist_domain(raw_domain)?;
    diesel::delete(
        crate::schema::domain_blacklist::table.filter(blacklist_dsl::domain.eq(&normalized_domain)),
    )
    .execute(conn)
    .context("error removing blacklist domain")?;

    Ok(normalized_domain)
}

pub fn list_domain_blacklist_summaries(
    conn: &mut SqliteConnection,
) -> Result<Vec<DomainBlacklistSummary>> {
    let rules = list_domain_blacklist_rules(conn)?;
    let blacklist_domains = rules
        .iter()
        .map(|rule| rule.domain.clone())
        .collect::<Vec<_>>();
    let page_link_counts =
        load_grouped_target_host_counts(conn, "SELECT target_host, COUNT(*) AS count FROM page_link WHERE target_host != '' GROUP BY target_host")?;
    let page_scan_link_counts =
        load_grouped_target_host_counts(conn, "SELECT target_host, COUNT(*) AS count FROM page_scan_link WHERE target_host != '' GROUP BY target_host")?;

    let mut page_link_count_by_domain = rules
        .iter()
        .map(|rule| (rule.domain.clone(), 0usize))
        .collect::<HashMap<_, _>>();
    let mut page_scan_link_count_by_domain = rules
        .iter()
        .map(|rule| (rule.domain.clone(), 0usize))
        .collect::<HashMap<_, _>>();

    for row in page_link_counts {
        if let Some(domain) = find_matching_blacklist_domain(&row.target_host, &blacklist_domains) {
            *page_link_count_by_domain.entry(domain).or_default() += row.count.max(0) as usize;
        }
    }
    for row in page_scan_link_counts {
        if let Some(domain) = find_matching_blacklist_domain(&row.target_host, &blacklist_domains) {
            *page_scan_link_count_by_domain.entry(domain).or_default() += row.count.max(0) as usize;
        }
    }

    Ok(rules
        .into_iter()
        .map(|rule| DomainBlacklistSummary {
            id: rule.id,
            domain: rule.domain.clone(),
            created_at: rule.created_at,
            page_link_count: *page_link_count_by_domain.get(&rule.domain).unwrap_or(&0),
            page_scan_link_count: *page_scan_link_count_by_domain
                .get(&rule.domain)
                .unwrap_or(&0),
        })
        .collect())
}

pub fn list_site_profiles(
    conn: &mut SqliteConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<SiteProfileSummary>> {
    use crate::schema::site_profile::dsl as site_profile_dsl;

    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = crate::schema::site_profile::table
        .select(count_star())
        .first(conn)
        .context("error counting site profiles")?;
    let records = site_profile_dsl::site_profile
        .order(site_profile_dsl::score.desc())
        .then_order_by(site_profile_dsl::page_count.desc())
        .then_order_by(site_profile_dsl::host.asc())
        .limit(pagination.limit)
        .offset(pagination.offset)
        .select(SiteProfileRecord::as_select())
        .load::<SiteProfileRecord>(conn)
        .context("error loading site profiles")?;
    let items = build_site_profile_summaries(conn, &records)?;

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn create_work_unit(conn: &mut SqliteConnection, url: &str) -> Result<()> {
    let work_unit = NewUnit {
        url,
        status: STATUS_PENDING,
    };

    diesel::insert_into(crate::schema::work_unit::table)
        .values(work_unit)
        .on_conflict(crate::schema::work_unit::url)
        .do_nothing()
        .execute(conn)
        .context("error saving work unit")?;

    Ok(())
}

pub fn save_page_info(conn: &mut SqliteConnection, snapshot: &PageSnapshot) -> Result<()> {
    use crate::schema::page::dsl::{
        coins as page_coins, emails as page_emails, language as page_language,
        last_scanned_at as page_last_scanned_at, links as page_links, title as page_title,
        url as page_url,
    };
    use crate::schema::{
        page_classification, page_crypto, page_email, page_link, page_scan, page_scan_crypto,
        page_scan_email, page_scan_link, site_profile,
    };

    let new_page = NewPage {
        title: snapshot.title.clone(),
        url: snapshot.url.clone(),
        links: snapshot
            .links
            .iter()
            .map(|item| item.target_url.clone())
            .collect::<Vec<_>>()
            .join(","),
        emails: snapshot.emails.join(","),
        coins: snapshot
            .crypto_refs
            .iter()
            .map(|item| format!("{}:{}", item.asset_type, item.reference))
            .collect::<Vec<_>>()
            .join(","),
        language: snapshot.language.clone(),
    };

    conn.transaction::<_, anyhow::Error, _>(|conn| {
        diesel::insert_into(crate::schema::page::table)
            .values(&new_page)
            .on_conflict(page_url)
            .do_update()
            .set((
                page_title.eq(excluded(page_title)),
                page_links.eq(excluded(page_links)),
                page_emails.eq(excluded(page_emails)),
                page_coins.eq(excluded(page_coins)),
                page_language.eq(excluded(page_language)),
                page_last_scanned_at.eq(sql::<Text>("CURRENT_TIMESTAMP")),
            ))
            .execute(conn)
            .context("error saving page")?;

        let stored_page_id = crate::schema::page::table
            .filter(page_url.eq(&snapshot.url))
            .select(crate::schema::page::id)
            .first::<i32>(conn)
            .context("error loading saved page id")?;

        let stored_scan_id = diesel::insert_into(page_scan::table)
            .values(NewPageScan {
                page_id: stored_page_id,
                title: snapshot.title.clone(),
                language: snapshot.language.clone(),
            })
            .returning(page_scan::id)
            .get_result::<i32>(conn)
            .context("error saving page scan")?;

        let scan_link_rows = snapshot
            .links
            .iter()
            .map(|item| NewPageScanLink {
                scan_id: stored_scan_id,
                target_url: item.target_url.clone(),
                target_host: item.target_host.clone(),
            })
            .collect::<Vec<_>>();
        if !scan_link_rows.is_empty() {
            diesel::insert_into(page_scan_link::table)
                .values(&scan_link_rows)
                .execute(conn)
                .context("error saving page scan links")?;
        }

        let scan_email_rows = snapshot
            .emails
            .iter()
            .map(|email| NewPageScanEmail {
                scan_id: stored_scan_id,
                email: email.clone(),
            })
            .collect::<Vec<_>>();
        if !scan_email_rows.is_empty() {
            diesel::insert_into(page_scan_email::table)
                .values(&scan_email_rows)
                .execute(conn)
                .context("error saving page scan emails")?;
        }

        let scan_crypto_rows = snapshot
            .crypto_refs
            .iter()
            .map(|item| NewPageScanCrypto {
                scan_id: stored_scan_id,
                asset_type: item.asset_type.clone(),
                reference: item.reference.clone(),
            })
            .collect::<Vec<_>>();
        if !scan_crypto_rows.is_empty() {
            diesel::insert_into(page_scan_crypto::table)
                .values(&scan_crypto_rows)
                .execute(conn)
                .context("error saving page scan crypto references")?;
        }

        diesel::delete(page_link::table.filter(page_link::source_page_id.eq(stored_page_id)))
            .execute(conn)
            .context("error clearing saved page links")?;
        diesel::delete(page_email::table.filter(page_email::page_id.eq(stored_page_id)))
            .execute(conn)
            .context("error clearing saved page emails")?;
        diesel::delete(page_crypto::table.filter(page_crypto::page_id.eq(stored_page_id)))
            .execute(conn)
            .context("error clearing saved page crypto refs")?;

        let link_rows = snapshot
            .links
            .iter()
            .map(|item| NewPageLink {
                source_page_id: stored_page_id,
                target_url: item.target_url.clone(),
                target_host: item.target_host.clone(),
            })
            .collect::<Vec<_>>();
        if !link_rows.is_empty() {
            diesel::insert_into(page_link::table)
                .values(&link_rows)
                .execute(conn)
                .context("error saving page links")?;
        }

        let email_rows = snapshot
            .emails
            .iter()
            .map(|email| NewPageEmail {
                page_id: stored_page_id,
                email: email.clone(),
            })
            .collect::<Vec<_>>();
        if !email_rows.is_empty() {
            diesel::insert_into(page_email::table)
                .values(&email_rows)
                .execute(conn)
                .context("error saving page emails")?;
        }

        let crypto_rows = snapshot
            .crypto_refs
            .iter()
            .map(|item| NewPageCrypto {
                page_id: stored_page_id,
                asset_type: item.asset_type.clone(),
                reference: item.reference.clone(),
            })
            .collect::<Vec<_>>();
        if !crypto_rows.is_empty() {
            diesel::insert_into(page_crypto::table)
                .values(&crypto_rows)
                .execute(conn)
                .context("error saving page crypto references")?;
        }

        let classification = classify_page_snapshot(snapshot);
        if !classification.host.is_empty() {
            diesel::insert_into(page_classification::table)
                .values(NewPageClassification {
                    page_id: stored_page_id,
                    host: classification.host.clone(),
                    category: classification.category.clone(),
                    confidence: classification.confidence.clone(),
                    score: classification.score,
                    evidence: serialize_evidence(&classification.evidence),
                })
                .on_conflict(page_classification::page_id)
                .do_update()
                .set((
                    page_classification::host.eq(excluded(page_classification::host)),
                    page_classification::category.eq(excluded(page_classification::category)),
                    page_classification::confidence.eq(excluded(page_classification::confidence)),
                    page_classification::score.eq(excluded(page_classification::score)),
                    page_classification::evidence.eq(excluded(page_classification::evidence)),
                    page_classification::last_classified_at.eq(sql::<Text>("CURRENT_TIMESTAMP")),
                ))
                .execute(conn)
                .context("error saving page classification")?;

            let site_profile_record = recompute_site_profile_record(conn, &classification.host)?;
            diesel::insert_into(site_profile::table)
                .values(NewSiteProfile {
                    host: site_profile_record.host.clone(),
                    category: site_profile_record.category.clone(),
                    confidence: site_profile_record.confidence.clone(),
                    score: site_profile_record.score,
                    page_count: site_profile_record.page_count,
                    evidence: site_profile_record.evidence.clone(),
                    source_page_id: site_profile_record.source_page_id,
                })
                .on_conflict(site_profile::host)
                .do_update()
                .set((
                    site_profile::category.eq(excluded(site_profile::category)),
                    site_profile::confidence.eq(excluded(site_profile::confidence)),
                    site_profile::score.eq(excluded(site_profile::score)),
                    site_profile::page_count.eq(excluded(site_profile::page_count)),
                    site_profile::evidence.eq(excluded(site_profile::evidence)),
                    site_profile::source_page_id.eq(excluded(site_profile::source_page_id)),
                    site_profile::last_classified_at.eq(sql::<Text>("CURRENT_TIMESTAMP")),
                ))
                .execute(conn)
                .context("error saving site profile")?;
        }

        Ok(())
    })?;

    Ok(())
}

pub fn mark_work_unit_as_done(conn: &mut SqliteConnection, work_unit_id: i32) -> Result<()> {
    use crate::schema::work_unit::dsl::*;

    diesel::update(crate::schema::work_unit::table.filter(id.eq(work_unit_id)))
        .set((
            status.eq(STATUS_DONE),
            next_attempt_at.eq(sql::<Text>("CURRENT_TIMESTAMP")),
            last_attempt_at.eq(sql::<Nullable<Text>>("CURRENT_TIMESTAMP")),
            last_error.eq::<Option<String>>(None),
        ))
        .execute(conn)
        .context("error updating work unit status")?;

    Ok(())
}

pub fn record_work_unit_failure(
    conn: &mut SqliteConnection,
    work_unit_id: i32,
    error_message: &str,
    retriable: bool,
) -> Result<()> {
    use crate::schema::work_unit::dsl::*;

    let existing_work_unit = crate::schema::work_unit::table
        .filter(id.eq(work_unit_id))
        .select(WorkUnit::as_select())
        .first::<WorkUnit>(conn)
        .context("error loading work unit before retry update")?;
    let next_retry_count = existing_work_unit.retry_count + 1;
    let bounded_error = truncate(error_message, 500);
    let should_retry = retriable && next_retry_count < MAX_RETRY_ATTEMPTS;

    if should_retry {
        let backoff_minutes = retry_backoff_minutes(next_retry_count);
        diesel::update(crate::schema::work_unit::table.filter(id.eq(work_unit_id)))
            .set((
                status.eq(STATUS_PENDING),
                retry_count.eq(next_retry_count),
                last_error.eq(Some(bounded_error)),
                last_attempt_at.eq(sql::<Nullable<Text>>("CURRENT_TIMESTAMP")),
                next_attempt_at.eq(sql::<Text>(&format!(
                    "datetime(CURRENT_TIMESTAMP, '+{} minutes')",
                    backoff_minutes
                ))),
            ))
            .execute(conn)
            .context("error scheduling work unit retry")?;
    } else {
        diesel::update(crate::schema::work_unit::table.filter(id.eq(work_unit_id)))
            .set((
                status.eq(STATUS_FAILED),
                retry_count.eq(next_retry_count),
                last_error.eq(Some(bounded_error)),
                last_attempt_at.eq(sql::<Nullable<Text>>("CURRENT_TIMESTAMP")),
            ))
            .execute(conn)
            .context("error marking work unit as failed")?;
    }

    Ok(())
}

pub fn get_pending_work_units(conn: &mut SqliteConnection) -> Result<Vec<WorkUnit>> {
    use crate::schema::work_unit::dsl::*;

    crate::schema::work_unit::table
        .filter(status.eq(STATUS_PENDING))
        .filter(sql::<Bool>("next_attempt_at <= CURRENT_TIMESTAMP"))
        .order(next_attempt_at.asc())
        .then_order_by(id.asc())
        .select(WorkUnit::as_select())
        .load(conn)
        .context("error querying pending work units")
}

pub fn list_work_units(
    conn: &mut SqliteConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<WorkUnit>> {
    use crate::schema::work_unit::dsl::*;

    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = crate::schema::work_unit::table
        .select(count_star())
        .first(conn)
        .context("error counting work units")?;
    let items = crate::schema::work_unit::table
        .order(created_at.desc())
        .then_order_by(id.desc())
        .limit(pagination.limit)
        .offset(pagination.offset)
        .select(WorkUnit::as_select())
        .load(conn)
        .context("error querying work units")?;

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn list_page_summaries(
    conn: &mut SqliteConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<PageSummary>> {
    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = scalar_count(conn, "SELECT COUNT(*) AS count FROM page")
        .context("error counting pages for summary")?;
    let host_expr = sql_host_expr("p.url");
    let query = format!(
        "
        SELECT
            p.id,
            p.title,
            p.url,
            {host_expr} AS host,
            p.language,
            p.last_scanned_at,
            COALESCE(pl.link_count, 0) AS outbound_link_count,
            COALESCE(pe.email_count, 0) AS email_count,
            COALESCE(pc.crypto_count, 0) AS crypto_count
        FROM page p
        LEFT JOIN (
            SELECT source_page_id, COUNT(*) AS link_count
            FROM page_link
            GROUP BY source_page_id
        ) pl ON pl.source_page_id = p.id
        LEFT JOIN (
            SELECT page_id, COUNT(*) AS email_count
            FROM page_email
            GROUP BY page_id
        ) pe ON pe.page_id = p.id
        LEFT JOIN (
            SELECT page_id, COUNT(*) AS crypto_count
            FROM page_crypto
            GROUP BY page_id
        ) pc ON pc.page_id = p.id
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT ? OFFSET ?
        "
    );
    let rows = sql_query(query)
        .bind::<BigInt, _>(pagination.limit)
        .bind::<BigInt, _>(pagination.offset)
        .load::<PageSummaryRow>(conn)
        .context("error querying page summaries")?;
    let site_profiles = load_site_profile_badges_by_hosts(
        conn,
        &rows.iter().map(|row| row.host.clone()).collect::<Vec<_>>(),
    )?;
    let items = rows
        .into_iter()
        .map(|row| PageSummary {
            id: row.id,
            title: row.title,
            url: row.url,
            site_category: site_profiles.get(&row.host).cloned(),
            host: row.host,
            language: row.language,
            last_scanned_at: row.last_scanned_at,
            outbound_link_count: row.outbound_link_count.max(0) as usize,
            email_count: row.email_count.max(0) as usize,
            crypto_count: row.crypto_count.max(0) as usize,
        })
        .collect();

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn get_page_detail(conn: &mut SqliteConnection, page_id: i32) -> Result<Option<PageDetail>> {
    use crate::schema::page::dsl as page_dsl;
    use crate::schema::page_crypto::dsl as crypto_dsl;
    use crate::schema::page_email::dsl as email_dsl;
    use crate::schema::page_link::dsl as link_dsl;

    let page = page_dsl::page
        .filter(page_dsl::id.eq(page_id))
        .select(Page::as_select())
        .first::<Page>(conn)
        .optional()
        .context("error loading page detail")?;

    let Some(page) = page else {
        return Ok(None);
    };
    let page_host = host_from_url(&page.url);
    let site_profile = load_site_profile_by_host(conn, &page_host)?;

    let outgoing_rows = link_dsl::page_link
        .filter(link_dsl::source_page_id.eq(page.id))
        .select(PageLink::as_select())
        .load::<PageLink>(conn)
        .context("error loading outgoing links")?;
    let blacklist_domains = load_blacklist_domains(conn)?;
    let outgoing_links = build_link_references(
        conn,
        outgoing_rows
            .iter()
            .map(scan_link_like_page_link_to_observation)
            .collect::<Vec<_>>(),
        &blacklist_domains,
    )?;

    let inbound_rows = link_dsl::page_link
        .filter(link_dsl::target_url.eq(&page.url))
        .select(PageLink::as_select())
        .load::<PageLink>(conn)
        .context("error loading inbound links")?;
    let source_ids = inbound_rows
        .iter()
        .map(|row| row.source_page_id)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let source_pages = load_pages_by_ids(conn, &source_ids)?
        .into_iter()
        .map(|item| (item.id, item))
        .collect::<std::collections::HashMap<_, _>>();

    let mut incoming_links = inbound_rows
        .into_iter()
        .filter_map(|row| source_pages.get(&row.source_page_id))
        .map(|source_page| IncomingReference {
            source_page_id: source_page.id,
            source_title: source_page.title.clone(),
            source_url: source_page.url.clone(),
            source_host: host_from_url(&source_page.url),
        })
        .collect::<Vec<_>>();
    incoming_links.sort_by(|left, right| {
        left.source_host
            .cmp(&right.source_host)
            .then_with(|| left.source_url.cmp(&right.source_url))
    });

    let mut emails = email_dsl::page_email
        .filter(email_dsl::page_id.eq(page.id))
        .select(PageEmail::as_select())
        .load::<PageEmail>(conn)
        .context("error loading page emails")?
        .into_iter()
        .map(|row| EmailObservation {
            detail_url: build_query_url("/entities/emails", &[("value", &row.email)]),
            value: row.email,
        })
        .collect::<Vec<_>>();
    emails.sort_by(|left, right| left.value.cmp(&right.value));

    let mut crypto_refs = crypto_dsl::page_crypto
        .filter(crypto_dsl::page_id.eq(page.id))
        .select(PageCrypto::as_select())
        .load::<PageCrypto>(conn)
        .context("error loading page crypto references")?
        .into_iter()
        .map(|row| CryptoObservation {
            detail_url: build_query_url(
                "/entities/crypto",
                &[
                    ("asset_type", &row.asset_type),
                    ("reference", &row.reference),
                ],
            ),
            asset_type: row.asset_type,
            reference: row.reference,
        })
        .collect::<Vec<_>>();
    crypto_refs.sort_by(|left, right| {
        left.asset_type
            .cmp(&right.asset_type)
            .then_with(|| left.reference.cmp(&right.reference))
    });

    Ok(Some(PageDetail {
        id: page.id,
        title: page.title,
        url: page.url.clone(),
        host: page_host,
        language: page.language,
        created_at: page.created_at,
        last_scanned_at: page.last_scanned_at,
        outgoing_links,
        incoming_links,
        emails,
        crypto_refs,
        site_profile,
    }))
}

pub fn list_page_scan_summaries(
    conn: &mut SqliteConnection,
    page_id: i32,
) -> Result<Vec<PageScanSummary>> {
    let rows = sql_query(
        "
        SELECT
            ps.id,
            ps.page_id,
            ps.title,
            ps.language,
            ps.scanned_at,
            COALESCE(psl.link_count, 0) AS outbound_link_count,
            COALESCE(pse.email_count, 0) AS email_count,
            COALESCE(psc.crypto_count, 0) AS crypto_count
        FROM page_scan ps
        LEFT JOIN (
            SELECT scan_id, COUNT(*) AS link_count
            FROM page_scan_link
            GROUP BY scan_id
        ) psl ON psl.scan_id = ps.id
        LEFT JOIN (
            SELECT scan_id, COUNT(*) AS email_count
            FROM page_scan_email
            GROUP BY scan_id
        ) pse ON pse.scan_id = ps.id
        LEFT JOIN (
            SELECT scan_id, COUNT(*) AS crypto_count
            FROM page_scan_crypto
            GROUP BY scan_id
        ) psc ON psc.scan_id = ps.id
        WHERE ps.page_id = ?
        ORDER BY ps.scanned_at DESC, ps.id DESC
        ",
    )
    .bind::<diesel::sql_types::Integer, _>(page_id)
    .load::<PageScanSummaryRow>(conn)
    .context("error querying page scan summaries")?;

    let mut scans = rows
        .into_iter()
        .map(|row| PageScanSummary {
            id: row.id,
            page_id: row.page_id,
            title: row.title,
            language: row.language,
            scanned_at: row.scanned_at,
            outbound_link_count: row.outbound_link_count.max(0) as usize,
            email_count: row.email_count.max(0) as usize,
            crypto_count: row.crypto_count.max(0) as usize,
            change_summary: None,
            detail_url: build_page_scan_detail_url(row.page_id, row.id),
        })
        .collect::<Vec<_>>();

    let scan_ids = scans.iter().map(|scan| scan.id).collect::<Vec<_>>();
    let observation_sets = load_scan_observation_sets(conn, &scan_ids)?;
    let empty = ScanObservationSet::default();

    for index in 0..scans.len().saturating_sub(1) {
        let current = scans[index].clone();
        let previous = scans[index + 1].clone();
        let current_observations = observation_sets.get(&current.id).unwrap_or(&empty);
        let previous_observations = observation_sets.get(&previous.id).unwrap_or(&empty);
        scans[index].change_summary = Some(build_change_summary(
            current_observations,
            previous_observations,
            current.title != previous.title,
            current.language != previous.language,
        ));
    }

    Ok(scans)
}

pub fn get_page_scan_detail(
    conn: &mut SqliteConnection,
    page_id: i32,
    scan_id: i32,
) -> Result<Option<PageScanDetail>> {
    let page = load_page_by_id(conn, page_id)?;
    let Some(page) = page else {
        return Ok(None);
    };

    let scans = list_page_scan_summaries(conn, page_id)?;
    let Some(selected_index) = scans.iter().position(|scan| scan.id == scan_id) else {
        return Ok(None);
    };
    let scan = scans[selected_index].clone();
    let previous_scan = scans.get(selected_index + 1).cloned();
    let mut requested_scan_ids = vec![scan.id];
    if let Some(previous_scan) = previous_scan.as_ref() {
        requested_scan_ids.push(previous_scan.id);
    }

    let scan_links = load_scan_link_rows(conn, &requested_scan_ids)?;
    let scan_emails = load_scan_email_rows(conn, &requested_scan_ids)?;
    let scan_crypto_refs = load_scan_crypto_rows(conn, &requested_scan_ids)?;

    let current_link_rows = scan_links.get(&scan.id).cloned().unwrap_or_default();
    let current_email_rows = scan_emails.get(&scan.id).cloned().unwrap_or_default();
    let current_crypto_rows = scan_crypto_refs.get(&scan.id).cloned().unwrap_or_default();
    let blacklist_domains = load_blacklist_domains(conn)?;
    let page_host = host_from_url(&page.url);
    let site_profile = load_site_profile_by_host(conn, &page_host)?;

    let outgoing_links = build_link_references(
        conn,
        current_link_rows
            .iter()
            .map(scan_link_to_observation)
            .collect::<Vec<_>>(),
        &blacklist_domains,
    )?;
    let emails = build_email_observations(
        current_email_rows
            .iter()
            .map(|row| row.email.clone())
            .collect::<Vec<_>>(),
    );
    let crypto_refs = build_crypto_observations(
        current_crypto_rows
            .iter()
            .map(|row| (row.asset_type.clone(), row.reference.clone()))
            .collect::<Vec<_>>(),
    );

    let diff = if let Some(previous_scan) = previous_scan.as_ref() {
        let previous_link_rows = scan_links
            .get(&previous_scan.id)
            .cloned()
            .unwrap_or_default();
        let previous_email_rows = scan_emails
            .get(&previous_scan.id)
            .cloned()
            .unwrap_or_default();
        let previous_crypto_rows = scan_crypto_refs
            .get(&previous_scan.id)
            .cloned()
            .unwrap_or_default();

        let current_link_set = link_set_from_scan_rows(&current_link_rows);
        let previous_link_set = link_set_from_scan_rows(&previous_link_rows);
        let current_email_set = email_set_from_scan_rows(&current_email_rows);
        let previous_email_set = email_set_from_scan_rows(&previous_email_rows);
        let current_crypto_set = crypto_set_from_scan_rows(&current_crypto_rows);
        let previous_crypto_set = crypto_set_from_scan_rows(&previous_crypto_rows);

        let change_summary = build_change_summary(
            &ScanObservationSet {
                links: current_link_set.clone(),
                emails: current_email_set.clone(),
                crypto_refs: current_crypto_set.clone(),
            },
            &ScanObservationSet {
                links: previous_link_set.clone(),
                emails: previous_email_set.clone(),
                crypto_refs: previous_crypto_set.clone(),
            },
            scan.title != previous_scan.title,
            scan.language != previous_scan.language,
        );

        PageScanDiff {
            has_previous_scan: true,
            previous_scan_id: Some(previous_scan.id),
            previous_scanned_at: Some(previous_scan.scanned_at.clone()),
            title_before: Some(previous_scan.title.clone()),
            title_after: scan.title.clone(),
            language_before: Some(previous_scan.language.clone()),
            language_after: scan.language.clone(),
            change_summary,
            added_links: build_link_references(
                conn,
                current_link_set
                    .difference(&previous_link_set)
                    .map(|(target_url, target_host)| LinkObservation {
                        target_url: target_url.clone(),
                        target_host: target_host.clone(),
                    })
                    .collect::<Vec<_>>(),
                &blacklist_domains,
            )?,
            removed_links: build_link_references(
                conn,
                previous_link_set
                    .difference(&current_link_set)
                    .map(|(target_url, target_host)| LinkObservation {
                        target_url: target_url.clone(),
                        target_host: target_host.clone(),
                    })
                    .collect::<Vec<_>>(),
                &blacklist_domains,
            )?,
            added_emails: build_email_observations(
                current_email_set
                    .difference(&previous_email_set)
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
            removed_emails: build_email_observations(
                previous_email_set
                    .difference(&current_email_set)
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
            added_crypto_refs: build_crypto_observations(
                current_crypto_set
                    .difference(&previous_crypto_set)
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
            removed_crypto_refs: build_crypto_observations(
                previous_crypto_set
                    .difference(&current_crypto_set)
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
        }
    } else {
        PageScanDiff {
            has_previous_scan: false,
            previous_scan_id: None,
            previous_scanned_at: None,
            title_before: None,
            title_after: scan.title.clone(),
            language_before: None,
            language_after: scan.language.clone(),
            change_summary: PageScanChangeSummary::default(),
            added_links: Vec::new(),
            removed_links: Vec::new(),
            added_emails: Vec::new(),
            removed_emails: Vec::new(),
            added_crypto_refs: Vec::new(),
            removed_crypto_refs: Vec::new(),
        }
    };

    Ok(Some(PageScanDetail {
        page_id: page.id,
        page_title: page.title,
        page_url: page.url.clone(),
        page_host,
        scan,
        previous_scan,
        outgoing_links,
        emails,
        crypto_refs,
        diff,
        site_profile,
    }))
}

pub fn collect_stats(conn: &mut SqliteConnection) -> Result<Stats> {
    use crate::schema::work_unit::dsl as work_dsl;

    let total_pages =
        scalar_count(conn, "SELECT COUNT(*) AS count FROM page").context("error counting pages")?;
    let pending_work_units = work_dsl::work_unit
        .filter(work_dsl::status.eq(STATUS_PENDING))
        .select(count_star())
        .first(conn)
        .context("error counting pending work units")?;
    let failed_work_units = work_dsl::work_unit
        .filter(work_dsl::status.eq(STATUS_FAILED))
        .select(count_star())
        .first(conn)
        .context("error counting failed work units")?;

    let host_expr = sql_host_expr("url");
    let total_domains = scalar_count(
        conn,
        &format!(
            "SELECT COUNT(*) AS count FROM (SELECT DISTINCT {host_expr} AS host FROM page WHERE {host_expr} != '')"
        ),
    )
    .context("error counting distinct domains")?;
    let last_scrape = scalar_nullable_text(conn, "SELECT MAX(last_scanned_at) AS value FROM page")
        .context("error loading last scrape")?
        .unwrap_or_else(|| "Never".to_string());

    Ok(Stats {
        total_pages,
        total_domains,
        pending_work_units,
        failed_work_units,
        last_scrape,
    })
}

pub fn search_pages(
    conn: &mut SqliteConnection,
    query: &str,
    requested_limit: Option<i64>,
) -> Result<Vec<SearchResult>> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let limit = requested_limit.unwrap_or(10).clamp(1, 50);
    let pattern = format!("%{}%", escape_like(trimmed));
    let host_expr = sql_host_expr("p.url");
    let sql = format!(
        "
        SELECT
            p.id AS page_id,
            p.title,
            p.url,
            {host_expr} AS host,
            p.language,
            p.last_scanned_at AS scraped_at
        FROM page p
        WHERE p.title LIKE ? ESCAPE '\\' COLLATE NOCASE
            OR p.url LIKE ? ESCAPE '\\' COLLATE NOCASE
            OR p.language LIKE ? ESCAPE '\\' COLLATE NOCASE
            OR EXISTS (
                SELECT 1
                FROM page_email pe
                WHERE pe.page_id = p.id
                    AND pe.email LIKE ? ESCAPE '\\' COLLATE NOCASE
            )
            OR EXISTS (
                SELECT 1
                FROM page_crypto pc
                WHERE pc.page_id = p.id
                    AND (pc.asset_type || ':' || pc.reference) LIKE ? ESCAPE '\\' COLLATE NOCASE
            )
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT ?
    "
    );
    let rows = sql_query(sql)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<BigInt, _>(limit)
        .load::<SearchResultRow>(conn)
        .context("error searching pages")?;
    let site_profiles = load_site_profile_badges_by_hosts(
        conn,
        &rows.iter().map(|row| row.host.clone()).collect::<Vec<_>>(),
    )?;

    Ok(rows
        .into_iter()
        .map(|row| SearchResult {
            page_id: row.page_id,
            title: row.title,
            url: row.url,
            host: row.host.clone(),
            language: row.language,
            scraped_at: row.scraped_at,
            site_category: site_profiles.get(&row.host).cloned(),
        })
        .collect())
}

pub fn list_email_entities(
    conn: &mut SqliteConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<EmailEntitySummary>> {
    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = scalar_count(
        conn,
        "SELECT COUNT(*) AS count FROM (SELECT email FROM page_email GROUP BY email)",
    )
    .context("error counting email entities")?;
    let rows = sql_query(
        "
        SELECT
            email AS value,
            COUNT(*) AS page_count
        FROM page_email
        GROUP BY email
        ORDER BY page_count DESC, value ASC
        LIMIT ? OFFSET ?
        ",
    )
    .bind::<BigInt, _>(pagination.limit)
    .bind::<BigInt, _>(pagination.offset)
    .load::<EmailEntitySummaryRow>(conn)
    .context("error loading email entities")?;
    let items = rows
        .into_iter()
        .map(|row| EmailEntitySummary {
            detail_url: build_query_url("/entities/emails", &[("value", &row.value)]),
            value: row.value,
            page_count: row.page_count.max(0) as usize,
        })
        .collect();

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn get_email_entity_detail(
    conn: &mut SqliteConnection,
    value: &str,
) -> Result<Option<EmailEntityDetail>> {
    use crate::schema::page_email::dsl as email_dsl;

    let page_ids = email_dsl::page_email
        .filter(email_dsl::email.eq(value))
        .select(email_dsl::page_id)
        .load::<i32>(conn)
        .context("error loading pages for email entity")?;
    if page_ids.is_empty() {
        return Ok(None);
    }

    let mut pages = load_pages_by_ids(conn, &page_ids)?
        .into_iter()
        .map(page_reference_from_page)
        .collect::<Vec<_>>();
    pages.sort_by(|left, right| {
        right
            .last_scanned_at
            .cmp(&left.last_scanned_at)
            .then_with(|| left.url.cmp(&right.url))
    });

    Ok(Some(EmailEntityDetail {
        value: value.to_string(),
        pages,
    }))
}

pub fn list_crypto_entities(
    conn: &mut SqliteConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<CryptoEntitySummary>> {
    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = scalar_count(
        conn,
        "SELECT COUNT(*) AS count FROM (SELECT asset_type, reference FROM page_crypto GROUP BY asset_type, reference)",
    )
    .context("error counting crypto entities")?;
    let rows = sql_query(
        "
        SELECT
            asset_type,
            reference,
            COUNT(*) AS page_count
        FROM page_crypto
        GROUP BY asset_type, reference
        ORDER BY page_count DESC, asset_type ASC, reference ASC
        LIMIT ? OFFSET ?
        ",
    )
    .bind::<BigInt, _>(pagination.limit)
    .bind::<BigInt, _>(pagination.offset)
    .load::<CryptoEntitySummaryRow>(conn)
    .context("error loading crypto entities")?;
    let items = rows
        .into_iter()
        .map(|row| CryptoEntitySummary {
            detail_url: build_query_url(
                "/entities/crypto",
                &[
                    ("asset_type", &row.asset_type),
                    ("reference", &row.reference),
                ],
            ),
            asset_type: row.asset_type,
            reference: row.reference,
            page_count: row.page_count.max(0) as usize,
        })
        .collect();

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn get_crypto_entity_detail(
    conn: &mut SqliteConnection,
    asset_type: &str,
    reference: &str,
) -> Result<Option<CryptoEntityDetail>> {
    use crate::schema::page_crypto::dsl as crypto_dsl;

    let page_ids = crypto_dsl::page_crypto
        .filter(crypto_dsl::asset_type.eq(asset_type))
        .filter(crypto_dsl::reference.eq(reference))
        .select(crypto_dsl::page_id)
        .load::<i32>(conn)
        .context("error loading pages for crypto entity")?;
    if page_ids.is_empty() {
        return Ok(None);
    }

    let mut pages = load_pages_by_ids(conn, &page_ids)?
        .into_iter()
        .map(page_reference_from_page)
        .collect::<Vec<_>>();
    pages.sort_by(|left, right| {
        right
            .last_scanned_at
            .cmp(&left.last_scanned_at)
            .then_with(|| left.url.cmp(&right.url))
    });

    Ok(Some(CryptoEntityDetail {
        asset_type: asset_type.to_string(),
        reference: reference.to_string(),
        pages,
    }))
}

pub fn list_site_relationships(
    conn: &mut SqliteConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<SiteRelationship>> {
    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let source_host_expr = sql_host_expr("p.url");
    let total_count = scalar_count(
        conn,
        &format!(
            "
            SELECT COUNT(*) AS count
            FROM (
                SELECT 1
                FROM page_link pl
                JOIN page p ON p.id = pl.source_page_id
                WHERE pl.target_host != ''
                    AND {source_host_expr} != ''
                    AND {source_host_expr} != pl.target_host
                GROUP BY {source_host_expr}, pl.target_host
            )
            "
        ),
    )
    .context("error counting site relationships")?;
    let query = format!(
        "
        SELECT
            {source_host_expr} AS source_host,
            pl.target_host,
            COUNT(*) AS reference_count
        FROM page_link pl
        JOIN page p ON p.id = pl.source_page_id
        WHERE pl.target_host != ''
            AND {source_host_expr} != ''
            AND {source_host_expr} != pl.target_host
        GROUP BY {source_host_expr}, pl.target_host
        ORDER BY reference_count DESC, source_host ASC, pl.target_host ASC
        LIMIT ? OFFSET ?
        "
    );
    let rows = sql_query(query)
        .bind::<BigInt, _>(pagination.limit)
        .bind::<BigInt, _>(pagination.offset)
        .load::<SiteRelationshipRow>(conn)
        .context("error loading site relationships")?;
    let blacklist_domains = load_blacklist_domains(conn)?;
    let site_profiles = load_site_profile_badges_by_hosts(
        conn,
        &rows
            .iter()
            .flat_map(|row| [row.source_host.clone(), row.target_host.clone()])
            .collect::<Vec<_>>(),
    )?;
    let items = rows
        .into_iter()
        .map(|row| {
            let blacklist_match_domain =
                find_matching_blacklist_domain(&row.target_host, &blacklist_domains);
            SiteRelationship {
                source_site_category: site_profiles.get(&row.source_host).cloned(),
                target_site_category: site_profiles.get(&row.target_host).cloned(),
                source_host: row.source_host,
                target_host: row.target_host,
                reference_count: row.reference_count.max(0) as usize,
                is_blacklisted: blacklist_match_domain.is_some(),
                blacklist_match_domain,
            }
        })
        .collect();

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

fn classify_page_snapshot(snapshot: &PageSnapshot) -> ClassificationOutcome {
    let host = host_from_url(&snapshot.url);
    let mut scores = HashMap::<String, i32>::new();
    let mut evidence_by_category = HashMap::<String, Vec<(i32, String)>>::new();

    for hint in &snapshot.classification_signals.hints {
        if !is_known_site_category(&hint.category) {
            continue;
        }
        *scores.entry(hint.category.clone()).or_default() += hint.weight.max(0);
        evidence_by_category
            .entry(hint.category.clone())
            .or_default()
            .push((hint.weight, hint.evidence.clone()));
    }

    let mut top = top_category_and_score(&scores);
    if top.as_ref().map(|(_, score)| *score).unwrap_or_default() < 4 {
        top = None;
    }

    let (category, score, mut evidence) = if let Some((category, score)) = top {
        let mut evidence = evidence_by_category.remove(&category).unwrap_or_default();
        evidence.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
        (
            category,
            score,
            evidence
                .into_iter()
                .map(|(_, evidence)| evidence)
                .take(6)
                .collect::<Vec<_>>(),
        )
    } else if snapshot.classification_signals.word_count >= 120 {
        (
            CATEGORY_CONTENT.to_string(),
            (snapshot.classification_signals.word_count / 80).clamp(2, 6) as i32,
            vec!["text:substantial-content".to_string()],
        )
    } else {
        (
            CATEGORY_UNKNOWN.to_string(),
            0,
            vec!["signals:insufficient".to_string()],
        )
    };

    if category == CATEGORY_CONTENT && snapshot.classification_signals.word_count >= 250 {
        push_unique(&mut evidence, "text:deep-page".to_string());
    }

    let runner_up_score = second_best_score(&scores, &category);
    let confidence = if category == CATEGORY_UNKNOWN {
        CONFIDENCE_LOW.to_string()
    } else if category == CATEGORY_CONTENT {
        if snapshot.classification_signals.word_count >= 250 {
            CONFIDENCE_MEDIUM.to_string()
        } else {
            CONFIDENCE_LOW.to_string()
        }
    } else if score >= 12 && score - runner_up_score >= 4 {
        CONFIDENCE_HIGH.to_string()
    } else if score >= 7 && score - runner_up_score >= 2 {
        CONFIDENCE_MEDIUM.to_string()
    } else {
        CONFIDENCE_LOW.to_string()
    };

    ClassificationOutcome {
        host,
        category,
        confidence,
        score,
        evidence,
    }
}

fn recompute_site_profile_record(
    conn: &mut SqliteConnection,
    host_value: &str,
) -> Result<NewSiteProfile> {
    use crate::schema::page_classification::dsl as page_classification_dsl;

    let rows = page_classification_dsl::page_classification
        .filter(page_classification_dsl::host.eq(host_value))
        .select(PageClassificationRecord::as_select())
        .load::<PageClassificationRecord>(conn)
        .context("error loading page classifications for host")?;
    anyhow::ensure!(
        !rows.is_empty(),
        "cannot build site profile without page classifications for host"
    );

    let mut category_scores = HashMap::<String, i32>::new();
    let mut support_counts = HashMap::<String, i32>::new();
    for row in &rows {
        *category_scores.entry(row.category.clone()).or_default() += row.score.max(0);
        *support_counts.entry(row.category.clone()).or_default() += 1;
    }
    let (category, score) = top_category_and_score(&category_scores)
        .unwrap_or_else(|| (CATEGORY_UNKNOWN.to_string(), 0));
    let runner_up_score = second_best_score(&category_scores, &category);
    let supporting_pages = support_counts.get(&category).copied().unwrap_or_default();
    let source_row = rows
        .iter()
        .filter(|row| row.category == category)
        .max_by(|left, right| {
            left.score
                .cmp(&right.score)
                .then_with(|| left.last_classified_at.cmp(&right.last_classified_at))
                .then_with(|| right.page_id.cmp(&left.page_id))
        });
    let mut evidence = source_row
        .map(|row| deserialize_evidence(&row.evidence))
        .unwrap_or_default();
    push_unique(&mut evidence, format!("pages:{}", rows.len()));
    if supporting_pages > 1 {
        push_unique(
            &mut evidence,
            format!("supporting-pages:{supporting_pages}"),
        );
    }
    evidence.truncate(6);

    let confidence = if category == CATEGORY_UNKNOWN {
        CONFIDENCE_LOW.to_string()
    } else if score >= 18 && supporting_pages >= 2 && score - runner_up_score >= 4 {
        CONFIDENCE_HIGH.to_string()
    } else if score >= 8 && score - runner_up_score >= 2 {
        CONFIDENCE_MEDIUM.to_string()
    } else {
        CONFIDENCE_LOW.to_string()
    };

    Ok(NewSiteProfile {
        host: host_value.to_string(),
        category,
        confidence,
        score,
        page_count: rows.len() as i32,
        evidence: serialize_evidence(&evidence),
        source_page_id: source_row.map(|row| row.page_id),
    })
}

fn load_site_profile_by_host(
    conn: &mut SqliteConnection,
    host: &str,
) -> Result<Option<SiteProfileSummary>> {
    Ok(load_site_profiles_by_hosts(conn, &[host.to_string()])?.remove(host))
}

fn load_site_profile_badges_by_hosts(
    conn: &mut SqliteConnection,
    hosts: &[String],
) -> Result<HashMap<String, SiteCategoryBadge>> {
    Ok(load_site_profile_records_by_hosts(conn, hosts)?
        .into_iter()
        .map(|(host, record)| {
            (
                host,
                site_category_badge(&record.category, &record.confidence),
            )
        })
        .collect())
}

fn load_site_profiles_by_hosts(
    conn: &mut SqliteConnection,
    hosts: &[String],
) -> Result<HashMap<String, SiteProfileSummary>> {
    let records_by_host = load_site_profile_records_by_hosts(conn, hosts)?;
    let records = records_by_host.values().cloned().collect::<Vec<_>>();
    let summaries = build_site_profile_summaries(conn, &records)?;
    Ok(summaries
        .into_iter()
        .map(|summary| (summary.host.clone(), summary))
        .collect())
}

fn load_site_profile_records_by_hosts(
    conn: &mut SqliteConnection,
    hosts: &[String],
) -> Result<HashMap<String, SiteProfileRecord>> {
    use crate::schema::site_profile::dsl as site_profile_dsl;

    let unique_hosts = hosts
        .iter()
        .map(|host| host.trim().to_ascii_lowercase())
        .filter(|host| !host.is_empty())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if unique_hosts.is_empty() {
        return Ok(HashMap::new());
    }

    Ok(site_profile_dsl::site_profile
        .filter(site_profile_dsl::host.eq_any(&unique_hosts))
        .select(SiteProfileRecord::as_select())
        .load::<SiteProfileRecord>(conn)
        .context("error loading site profiles by host")?
        .into_iter()
        .map(|record| (record.host.clone(), record))
        .collect())
}

fn build_site_profile_summaries(
    conn: &mut SqliteConnection,
    records: &[SiteProfileRecord],
) -> Result<Vec<SiteProfileSummary>> {
    let source_page_ids = records
        .iter()
        .filter_map(|record| record.source_page_id)
        .collect::<Vec<_>>();
    let source_pages = load_pages_by_ids(conn, &source_page_ids)?
        .into_iter()
        .map(|page| (page.id, page))
        .collect::<HashMap<_, _>>();

    Ok(records
        .iter()
        .map(|record| {
            let source_page = record
                .source_page_id
                .and_then(|page_id| source_pages.get(&page_id));
            SiteProfileSummary {
                host: record.host.clone(),
                category: record.category.clone(),
                label: site_category_label(&record.category).to_string(),
                confidence: record.confidence.clone(),
                evidence: deserialize_evidence(&record.evidence),
                page_count: record.page_count.max(0) as usize,
                source_page_id: record.source_page_id,
                source_page_title: source_page.map(|page| page.title.clone()),
                source_page_url: source_page.map(|page| page.url.clone()),
                last_classified_at: record.last_classified_at.clone(),
            }
        })
        .collect())
}

fn load_page_by_id(conn: &mut SqliteConnection, page_id: i32) -> Result<Option<Page>> {
    use crate::schema::page::dsl as page_dsl;

    page_dsl::page
        .filter(page_dsl::id.eq(page_id))
        .select(Page::as_select())
        .first::<Page>(conn)
        .optional()
        .context("error loading page by id")
}

fn load_scan_observation_sets(
    conn: &mut SqliteConnection,
    scan_ids: &[i32],
) -> Result<HashMap<i32, ScanObservationSet>> {
    let scan_links = load_scan_link_rows(conn, scan_ids)?;
    let scan_emails = load_scan_email_rows(conn, scan_ids)?;
    let scan_crypto_refs = load_scan_crypto_rows(conn, scan_ids)?;
    let mut sets = scan_ids
        .iter()
        .copied()
        .map(|scan_id| (scan_id, ScanObservationSet::default()))
        .collect::<HashMap<_, _>>();

    for (scan_id, rows) in scan_links {
        let entry = sets.entry(scan_id).or_default();
        entry.links.extend(rows.into_iter().map(|row| {
            let observation = scan_link_to_observation(&row);
            (observation.target_url, observation.target_host)
        }));
    }
    for (scan_id, rows) in scan_emails {
        let entry = sets.entry(scan_id).or_default();
        entry.emails.extend(rows.into_iter().map(|row| row.email));
    }
    for (scan_id, rows) in scan_crypto_refs {
        let entry = sets.entry(scan_id).or_default();
        entry
            .crypto_refs
            .extend(rows.into_iter().map(|row| (row.asset_type, row.reference)));
    }

    Ok(sets)
}

fn load_scan_link_rows(
    conn: &mut SqliteConnection,
    scan_ids: &[i32],
) -> Result<HashMap<i32, Vec<PageScanLink>>> {
    use crate::schema::page_scan_link::dsl as scan_link_dsl;

    if scan_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = scan_link_dsl::page_scan_link
        .filter(scan_link_dsl::scan_id.eq_any(scan_ids))
        .select(PageScanLink::as_select())
        .load::<PageScanLink>(conn)
        .context("error loading page scan links")?;
    let mut grouped = HashMap::<i32, Vec<PageScanLink>>::new();
    for row in rows {
        grouped.entry(row.scan_id).or_default().push(row);
    }
    for values in grouped.values_mut() {
        values.sort_by(|left, right| {
            left.target_host
                .cmp(&right.target_host)
                .then_with(|| left.target_url.cmp(&right.target_url))
        });
    }
    Ok(grouped)
}

fn load_scan_email_rows(
    conn: &mut SqliteConnection,
    scan_ids: &[i32],
) -> Result<HashMap<i32, Vec<PageScanEmail>>> {
    use crate::schema::page_scan_email::dsl as scan_email_dsl;

    if scan_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = scan_email_dsl::page_scan_email
        .filter(scan_email_dsl::scan_id.eq_any(scan_ids))
        .select(PageScanEmail::as_select())
        .load::<PageScanEmail>(conn)
        .context("error loading page scan emails")?;
    let mut grouped = HashMap::<i32, Vec<PageScanEmail>>::new();
    for row in rows {
        grouped.entry(row.scan_id).or_default().push(row);
    }
    for values in grouped.values_mut() {
        values.sort_by(|left, right| left.email.cmp(&right.email));
    }
    Ok(grouped)
}

fn load_scan_crypto_rows(
    conn: &mut SqliteConnection,
    scan_ids: &[i32],
) -> Result<HashMap<i32, Vec<PageScanCrypto>>> {
    use crate::schema::page_scan_crypto::dsl as scan_crypto_dsl;

    if scan_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = scan_crypto_dsl::page_scan_crypto
        .filter(scan_crypto_dsl::scan_id.eq_any(scan_ids))
        .select(PageScanCrypto::as_select())
        .load::<PageScanCrypto>(conn)
        .context("error loading page scan crypto references")?;
    let mut grouped = HashMap::<i32, Vec<PageScanCrypto>>::new();
    for row in rows {
        grouped.entry(row.scan_id).or_default().push(row);
    }
    for values in grouped.values_mut() {
        values.sort_by(|left, right| {
            left.asset_type
                .cmp(&right.asset_type)
                .then_with(|| left.reference.cmp(&right.reference))
        });
    }
    Ok(grouped)
}

fn link_set_from_scan_rows(rows: &[PageScanLink]) -> BTreeSet<(String, String)> {
    rows.iter()
        .map(|row| (row.target_url.clone(), row.target_host.clone()))
        .collect()
}

fn email_set_from_scan_rows(rows: &[PageScanEmail]) -> BTreeSet<String> {
    rows.iter().map(|row| row.email.clone()).collect()
}

fn crypto_set_from_scan_rows(rows: &[PageScanCrypto]) -> BTreeSet<(String, String)> {
    rows.iter()
        .map(|row| (row.asset_type.clone(), row.reference.clone()))
        .collect()
}

fn build_change_summary(
    current: &ScanObservationSet,
    previous: &ScanObservationSet,
    title_changed: bool,
    language_changed: bool,
) -> PageScanChangeSummary {
    let added_links = current.links.difference(&previous.links).count();
    let removed_links = previous.links.difference(&current.links).count();
    let added_emails = current.emails.difference(&previous.emails).count();
    let removed_emails = previous.emails.difference(&current.emails).count();
    let added_crypto_refs = current
        .crypto_refs
        .difference(&previous.crypto_refs)
        .count();
    let removed_crypto_refs = previous
        .crypto_refs
        .difference(&current.crypto_refs)
        .count();
    let has_changes = title_changed
        || language_changed
        || added_links > 0
        || removed_links > 0
        || added_emails > 0
        || removed_emails > 0
        || added_crypto_refs > 0
        || removed_crypto_refs > 0;

    PageScanChangeSummary {
        added_links,
        removed_links,
        added_emails,
        removed_emails,
        added_crypto_refs,
        removed_crypto_refs,
        title_changed,
        language_changed,
        has_changes,
    }
}

fn build_link_references(
    conn: &mut SqliteConnection,
    observations: Vec<LinkObservation>,
    blacklist_domains: &[String],
) -> Result<Vec<LinkReference>> {
    let known_targets = load_known_targets_by_url(
        conn,
        &observations
            .iter()
            .map(|item| item.target_url.clone())
            .collect::<Vec<_>>(),
    )?;
    let mut links = observations
        .into_iter()
        .map(|item| {
            let known_target = known_targets.get(&item.target_url);
            let blacklist_match_domain =
                find_matching_blacklist_domain(&item.target_host, blacklist_domains);
            LinkReference {
                target_url: item.target_url,
                target_host: item.target_host,
                target_page_id: known_target.map(|page| page.id),
                target_page_title: known_target.map(|page| page.title.clone()),
                is_blacklisted: blacklist_match_domain.is_some(),
                blacklist_match_domain,
            }
        })
        .collect::<Vec<_>>();
    links.sort_by(|left, right| {
        left.target_host
            .cmp(&right.target_host)
            .then_with(|| left.target_url.cmp(&right.target_url))
    });
    Ok(links)
}

fn build_email_observations(values: Vec<String>) -> Vec<EmailObservation> {
    let mut observations = values
        .into_iter()
        .map(|value| EmailObservation {
            detail_url: build_query_url("/entities/emails", &[("value", &value)]),
            value,
        })
        .collect::<Vec<_>>();
    observations.sort_by(|left, right| left.value.cmp(&right.value));
    observations
}

fn build_crypto_observations(values: Vec<(String, String)>) -> Vec<CryptoObservation> {
    let mut observations = values
        .into_iter()
        .map(|(asset_type, reference)| CryptoObservation {
            detail_url: build_query_url(
                "/entities/crypto",
                &[("asset_type", &asset_type), ("reference", &reference)],
            ),
            asset_type,
            reference,
        })
        .collect::<Vec<_>>();
    observations.sort_by(|left, right| {
        left.asset_type
            .cmp(&right.asset_type)
            .then_with(|| left.reference.cmp(&right.reference))
    });
    observations
}

fn load_blacklist_domains(conn: &mut SqliteConnection) -> Result<Vec<String>> {
    Ok(list_domain_blacklist_rules(conn)?
        .into_iter()
        .map(|rule| rule.domain)
        .collect())
}

fn load_grouped_target_host_counts(
    conn: &mut SqliteConnection,
    query: &str,
) -> Result<Vec<TargetHostCountRow>> {
    sql_query(query)
        .load::<TargetHostCountRow>(conn)
        .context("error loading grouped target host counts")
}

fn load_known_targets_by_url(
    conn: &mut SqliteConnection,
    target_urls: &[String],
) -> Result<HashMap<String, Page>> {
    use crate::schema::page::dsl as page_dsl;

    let unique_urls = target_urls
        .iter()
        .filter(|value| !value.is_empty())
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if unique_urls.is_empty() {
        return Ok(HashMap::new());
    }

    Ok(page_dsl::page
        .filter(page_dsl::url.eq_any(&unique_urls))
        .select(Page::as_select())
        .load::<Page>(conn)
        .context("error loading known target pages")?
        .into_iter()
        .map(|page| (page.url.clone(), page))
        .collect())
}

fn scan_link_to_observation(row: &PageScanLink) -> LinkObservation {
    LinkObservation {
        target_url: row.target_url.clone(),
        target_host: row.target_host.clone(),
    }
}

fn scan_link_like_page_link_to_observation(row: &PageLink) -> LinkObservation {
    LinkObservation {
        target_url: row.target_url.clone(),
        target_host: row.target_host.clone(),
    }
}

fn load_pages_by_ids(conn: &mut SqliteConnection, page_ids: &[i32]) -> Result<Vec<Page>> {
    use crate::schema::page::dsl as page_dsl;

    if page_ids.is_empty() {
        return Ok(Vec::new());
    }

    page_dsl::page
        .filter(page_dsl::id.eq_any(page_ids))
        .select(Page::as_select())
        .load(conn)
        .context("error loading pages by id")
}

fn page_reference_from_page(page: Page) -> PageReference {
    PageReference {
        id: page.id,
        title: page.title,
        url: page.url,
        language: page.language,
        last_scanned_at: page.last_scanned_at,
    }
}

fn scalar_count(conn: &mut SqliteConnection, query: &str) -> Result<i64> {
    Ok(sql_query(query)
        .get_result::<CountRow>(conn)
        .context("error loading count result")?
        .count)
}

fn scalar_nullable_text(conn: &mut SqliteConnection, query: &str) -> Result<Option<String>> {
    Ok(sql_query(query)
        .get_result::<NullableTextRow>(conn)
        .context("error loading text result")?
        .value)
}

fn host_from_url(value: &str) -> String {
    Url::parse(value)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_ascii_lowercase()))
        .unwrap_or_default()
}

fn build_page_scan_detail_url(page_id: i32, scan_id: i32) -> String {
    format!("/pages/{page_id}/history/{scan_id}")
}

fn build_query_url(base: &str, params: &[(&str, &str)]) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, value);
    }
    format!("{base}?{}", serializer.finish())
}

fn escape_like(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn serialize_evidence(evidence: &[String]) -> String {
    evidence
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn deserialize_evidence(value: &str) -> Vec<String> {
    value
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect()
}

fn normalize_pagination(
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
    default_limit: i64,
    max_limit: i64,
) -> PaginationInput {
    PaginationInput {
        limit: requested_limit.unwrap_or(default_limit).clamp(1, max_limit),
        offset: requested_offset.unwrap_or(0).max(0),
    }
}

fn host_matches_blacklist_domain(host: &str, domain: &str) -> bool {
    host == domain
        || host
            .strip_suffix(domain)
            .map(|prefix| prefix.ends_with('.'))
            .unwrap_or(false)
}

fn retry_backoff_minutes(next_retry_count: i32) -> i32 {
    let exponent = next_retry_count.saturating_sub(1) as u32;
    2_i32.pow(exponent).min(60)
}

fn is_known_site_category(category: &str) -> bool {
    matches!(
        category,
        CATEGORY_SEARCH_ENGINE
            | CATEGORY_FORUM
            | CATEGORY_MARKET
            | CATEGORY_DIRECTORY
            | CATEGORY_WIKI
            | CATEGORY_BLOG
            | CATEGORY_ESCROW
            | CATEGORY_SHOP
            | CATEGORY_VENDOR_PAGE
            | CATEGORY_DOCS
            | CATEGORY_INDEXER
            | CATEGORY_CONTENT
            | CATEGORY_UNKNOWN
    )
}

fn top_category_and_score(scores: &HashMap<String, i32>) -> Option<(String, i32)> {
    scores
        .iter()
        .max_by(|left, right| left.1.cmp(right.1).then_with(|| right.0.cmp(left.0)))
        .map(|(category, score)| (category.clone(), *score))
}

fn second_best_score(scores: &HashMap<String, i32>, winning_category: &str) -> i32 {
    scores
        .iter()
        .filter(|(category, _)| category.as_str() != winning_category)
        .map(|(_, score)| *score)
        .max()
        .unwrap_or_default()
}

fn push_unique(values: &mut Vec<String>, candidate: String) {
    if !values.iter().any(|value| value == &candidate) {
        values.push(candidate);
    }
}

fn site_category_badge(category: &str, confidence: &str) -> SiteCategoryBadge {
    SiteCategoryBadge {
        category: category.to_string(),
        label: site_category_label(category).to_string(),
        confidence: confidence.to_string(),
    }
}

fn site_category_label(category: &str) -> &'static str {
    match category {
        CATEGORY_SEARCH_ENGINE => "Search Engine",
        CATEGORY_FORUM => "Forum",
        CATEGORY_MARKET => "Market",
        CATEGORY_DIRECTORY => "Directory",
        CATEGORY_WIKI => "Wiki",
        CATEGORY_BLOG => "Blog",
        CATEGORY_ESCROW => "Escrow",
        CATEGORY_SHOP => "Shop",
        CATEGORY_VENDOR_PAGE => "Vendor Page",
        CATEGORY_DOCS => "Docs",
        CATEGORY_INDEXER => "Indexer",
        CATEGORY_CONTENT => "Content",
        _ => "Unknown",
    }
}

fn sql_host_expr(column: &str) -> String {
    format!(
        "
        CASE
            WHEN instr({column}, '://') > 0 THEN
                CASE
                    WHEN instr(substr({column}, instr({column}, '://') + 3), '/') > 0 THEN
                        substr(
                            substr({column}, instr({column}, '://') + 3),
                            1,
                            instr(substr({column}, instr({column}, '://') + 3), '/') - 1
                        )
                    ELSE substr({column}, instr({column}, '://') + 3)
                END
            ELSE ''
        END
        "
    )
}

fn truncate(input: &str, max_len: usize) -> String {
    input.chars().take(max_len).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::connection::SimpleConnection;

    fn setup_connection() -> SqliteConnection {
        let mut conn = SqliteConnection::establish(":memory:").expect("in-memory sqlite");
        conn.batch_execute(
            "
            CREATE TABLE work_unit(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              url VARCHAR NOT NULL UNIQUE,
              status VARCHAR NOT NULL DEFAULT 'pending',
              retry_count INTEGER NOT NULL DEFAULT 0,
              next_attempt_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              last_attempt_at VARCHAR,
              last_error VARCHAR,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX idx_work_unit_status_next_attempt_at ON work_unit(status, next_attempt_at);
            CREATE TABLE domain_blacklist(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              domain VARCHAR NOT NULL UNIQUE,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE page(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              title VARCHAR NOT NULL,
              url VARCHAR NOT NULL UNIQUE,
              links VARCHAR NOT NULL,
              emails VARCHAR NOT NULL,
              coins VARCHAR NOT NULL,
              language VARCHAR NOT NULL DEFAULT '',
              last_scanned_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE page_classification(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              page_id INTEGER NOT NULL UNIQUE,
              host VARCHAR NOT NULL,
              category VARCHAR NOT NULL,
              confidence VARCHAR NOT NULL,
              score INTEGER NOT NULL DEFAULT 0,
              evidence VARCHAR NOT NULL DEFAULT '',
              last_classified_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE page_scan(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              page_id INTEGER NOT NULL,
              title VARCHAR NOT NULL,
              language VARCHAR NOT NULL DEFAULT '',
              scanned_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE page_scan_link(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              scan_id INTEGER NOT NULL,
              target_url VARCHAR NOT NULL,
              target_host VARCHAR NOT NULL DEFAULT '',
              UNIQUE(scan_id, target_url)
            );
            CREATE TABLE page_scan_email(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              scan_id INTEGER NOT NULL,
              email VARCHAR NOT NULL,
              UNIQUE(scan_id, email)
            );
            CREATE TABLE page_scan_crypto(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              scan_id INTEGER NOT NULL,
              asset_type VARCHAR NOT NULL,
              reference VARCHAR NOT NULL,
              UNIQUE(scan_id, asset_type, reference)
            );
            CREATE TABLE page_link(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              source_page_id INTEGER NOT NULL,
              target_url VARCHAR NOT NULL,
              target_host VARCHAR NOT NULL DEFAULT '',
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(source_page_id, target_url)
            );
            CREATE TABLE page_email(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              page_id INTEGER NOT NULL,
              email VARCHAR NOT NULL,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(page_id, email)
            );
            CREATE TABLE page_crypto(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              page_id INTEGER NOT NULL,
              asset_type VARCHAR NOT NULL,
              reference VARCHAR NOT NULL,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(page_id, asset_type, reference)
            );
            CREATE TABLE site_profile(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              host VARCHAR NOT NULL UNIQUE,
              category VARCHAR NOT NULL,
              confidence VARCHAR NOT NULL,
              score INTEGER NOT NULL DEFAULT 0,
              page_count INTEGER NOT NULL DEFAULT 0,
              evidence VARCHAR NOT NULL DEFAULT '',
              source_page_id INTEGER,
              last_classified_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            ",
        )
        .expect("schema setup");
        conn
    }

    fn alpha_snapshot() -> PageSnapshot {
        PageSnapshot {
            title: "Alpha Market".to_string(),
            url: "http://alpha.onion".to_string(),
            language: "English".to_string(),
            links: vec![LinkObservation {
                target_url: "http://beta.onion".to_string(),
                target_host: "beta.onion".to_string(),
            }],
            emails: vec!["team@shared.test".to_string()],
            crypto_refs: vec![CryptoReference {
                asset_type: "bitcoin".to_string(),
                reference: "bc1qalpha000000000000000000000000000000000".to_string(),
            }],
            classification_signals: ClassificationSignals {
                word_count: 180,
                hints: vec![
                    CategoryHint {
                        category: CATEGORY_MARKET.to_string(),
                        evidence: "title:market".to_string(),
                        weight: 6,
                    },
                    CategoryHint {
                        category: CATEGORY_SHOP.to_string(),
                        evidence: "text:add-to-cart".to_string(),
                        weight: 4,
                    },
                ],
                ..ClassificationSignals::default()
            },
        }
    }

    fn beta_snapshot() -> PageSnapshot {
        PageSnapshot {
            title: "Beta Forum".to_string(),
            url: "http://beta.onion".to_string(),
            language: "French".to_string(),
            links: vec![LinkObservation {
                target_url: "http://alpha.onion".to_string(),
                target_host: "alpha.onion".to_string(),
            }],
            emails: vec!["team@shared.test".to_string()],
            crypto_refs: vec![
                CryptoReference {
                    asset_type: "bitcoin".to_string(),
                    reference: "bc1qalpha000000000000000000000000000000000".to_string(),
                },
                CryptoReference {
                    asset_type: "ethereum".to_string(),
                    reference: "0x2222222222222222222222222222222222222222".to_string(),
                },
            ],
            classification_signals: ClassificationSignals {
                word_count: 220,
                password_form_count: 1,
                hints: vec![
                    CategoryHint {
                        category: CATEGORY_FORUM.to_string(),
                        evidence: "title:forum".to_string(),
                        weight: 6,
                    },
                    CategoryHint {
                        category: CATEGORY_FORUM.to_string(),
                        evidence: "text:thread".to_string(),
                        weight: 4,
                    },
                ],
                ..ClassificationSignals::default()
            },
        }
    }

    #[test]
    fn work_units_are_inserted_idempotently() {
        let mut conn = setup_connection();

        create_work_unit(&mut conn, "https://example.com").expect("first insert");
        create_work_unit(&mut conn, "https://example.com").expect("duplicate insert");

        let work_units = list_work_units(&mut conn, None, None).expect("load work units");
        assert_eq!(work_units.items.len(), 1);
        assert_eq!(work_units.items[0].status, STATUS_PENDING);
    }

    #[test]
    fn transient_failures_are_rescheduled_then_exhausted() {
        let mut conn = setup_connection();

        create_work_unit(&mut conn, "https://broken.example").expect("insert work unit");
        let work_unit = list_work_units(&mut conn, None, None)
            .expect("load work units")
            .items
            .remove(0);
        record_work_unit_failure(&mut conn, work_unit.id, "network timeout", true)
            .expect("retryable failure");

        let updated = list_work_units(&mut conn, None, None)
            .expect("reload work units")
            .items
            .remove(0);
        assert_eq!(updated.status, STATUS_PENDING);
        assert_eq!(updated.retry_count, 1);
        assert_eq!(updated.last_error.as_deref(), Some("network timeout"));
        assert!(get_pending_work_units(&mut conn)
            .expect("due work units")
            .is_empty());

        for _ in 0..(MAX_RETRY_ATTEMPTS - 1) {
            record_work_unit_failure(&mut conn, work_unit.id, "network timeout", true)
                .expect("subsequent retryable failure");
        }

        let exhausted = list_work_units(&mut conn, None, None)
            .expect("reload exhausted work unit")
            .items
            .remove(0);
        assert_eq!(exhausted.status, STATUS_FAILED);
        assert_eq!(exhausted.retry_count, MAX_RETRY_ATTEMPTS);
    }

    #[test]
    fn permanent_failures_are_terminal() {
        let mut conn = setup_connection();

        create_work_unit(&mut conn, "notaurl").expect("insert work unit");
        let work_unit = list_work_units(&mut conn, None, None)
            .expect("load work units")
            .items
            .remove(0);
        record_work_unit_failure(&mut conn, work_unit.id, "invalid url", false)
            .expect("terminal failure");

        let updated = list_work_units(&mut conn, None, None)
            .expect("reload work units")
            .items
            .remove(0);
        assert_eq!(updated.status, STATUS_FAILED);
        assert_eq!(updated.retry_count, 1);
    }

    #[test]
    fn blacklist_domains_are_normalized_and_match_subdomains() {
        assert_eq!(
            normalize_blacklist_domain(" Example.COM ").expect("normalized domain"),
            "example.com".to_string()
        );
        assert_eq!(
            find_matching_blacklist_domain(
                "www.example.com",
                &["example.com".to_string(), "www.example.com".to_string()]
            ),
            Some("www.example.com".to_string())
        );
        assert_eq!(
            find_matching_blacklist_domain("badexample.com", &["example.com".to_string()]),
            None
        );
        assert!(normalize_blacklist_domain("https://example.com").is_err());
        assert!(normalize_blacklist_domain("example.com/path").is_err());
    }

    #[test]
    fn blacklist_entries_are_idempotent_and_removable() {
        let mut conn = setup_connection();

        add_domain_blacklist_entry(&mut conn, "Example.com").expect("first add");
        add_domain_blacklist_entry(&mut conn, "example.com").expect("second add");
        let rules = list_domain_blacklist_rules(&mut conn).expect("list rules");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].domain, "example.com");

        remove_domain_blacklist_entry(&mut conn, "example.com").expect("remove entry");
        remove_domain_blacklist_entry(&mut conn, "example.com").expect("remove absent entry");
        assert!(list_domain_blacklist_rules(&mut conn)
            .expect("list after remove")
            .is_empty());
    }

    #[test]
    fn page_relations_entities_search_and_pagination_are_available() {
        let mut conn = setup_connection();

        save_page_info(&mut conn, &alpha_snapshot()).expect("save alpha");
        save_page_info(&mut conn, &beta_snapshot()).expect("save beta");
        create_work_unit(&mut conn, "http://pending.onion").expect("insert work unit");

        let summaries = list_page_summaries(&mut conn, Some(1), Some(1)).expect("page summaries");
        assert_eq!(summaries.total_count, 2);
        assert_eq!(summaries.items.len(), 1);

        let all_summaries = list_page_summaries(&mut conn, None, None).expect("full summaries");
        let alpha = all_summaries
            .items
            .iter()
            .find(|item| item.url == "http://alpha.onion")
            .expect("alpha summary");
        assert_eq!(alpha.outbound_link_count, 1);
        assert_eq!(alpha.email_count, 1);
        assert_eq!(alpha.crypto_count, 1);
        assert_eq!(
            alpha
                .site_category
                .as_ref()
                .map(|badge| badge.category.as_str()),
            Some(CATEGORY_MARKET)
        );

        let detail = get_page_detail(&mut conn, alpha.id)
            .expect("page detail")
            .expect("alpha detail");
        assert_eq!(detail.outgoing_links.len(), 1);
        assert_eq!(detail.incoming_links.len(), 1);
        assert_eq!(detail.emails[0].value, "team@shared.test");
        assert_eq!(detail.crypto_refs.len(), 1);
        assert_eq!(
            detail
                .site_profile
                .as_ref()
                .map(|profile| profile.category.as_str()),
            Some(CATEGORY_MARKET)
        );

        let email_entities = list_email_entities(&mut conn, None, None).expect("email entities");
        assert_eq!(email_entities.items[0].page_count, 2);

        let email_detail = get_email_entity_detail(&mut conn, "team@shared.test")
            .expect("email detail")
            .expect("email detail exists");
        assert_eq!(email_detail.pages.len(), 2);

        let crypto_detail = get_crypto_entity_detail(
            &mut conn,
            "bitcoin",
            "bc1qalpha000000000000000000000000000000000",
        )
        .expect("crypto detail")
        .expect("crypto detail exists");
        assert_eq!(crypto_detail.pages.len(), 2);

        let relationships = list_site_relationships(&mut conn, None, None).expect("relationships");
        assert_eq!(relationships.items.len(), 2);

        let stats = collect_stats(&mut conn).expect("collect stats");
        assert_eq!(stats.total_pages, 2);
        assert_eq!(stats.total_domains, 2);
        assert_eq!(stats.pending_work_units, 1);
        assert_eq!(stats.failed_work_units, 0);
        assert_ne!(stats.last_scrape, "Never");

        let search_results = search_pages(
            &mut conn,
            "0x2222222222222222222222222222222222222222",
            Some(5),
        )
        .expect("search pages");
        assert_eq!(search_results.len(), 1);
        assert_eq!(search_results[0].title, "Beta Forum");
        assert_eq!(
            search_results[0]
                .site_category
                .as_ref()
                .map(|badge| badge.category.as_str()),
            Some(CATEGORY_FORUM)
        );
    }

    #[test]
    fn site_profiles_are_aggregated_and_listed() {
        let mut conn = setup_connection();

        let mut search_page = alpha_snapshot();
        search_page.url = "http://gamma.onion/search".to_string();
        search_page.title = "Gamma Search".to_string();
        search_page.links = vec![
            LinkObservation {
                target_url: "http://alpha.onion/forum/thread-1".to_string(),
                target_host: "alpha.onion".to_string(),
            },
            LinkObservation {
                target_url: "http://beta.onion/forum/thread-2".to_string(),
                target_host: "beta.onion".to_string(),
            },
        ];
        search_page.classification_signals = ClassificationSignals {
            word_count: 240,
            search_form_count: 1,
            hints: vec![
                CategoryHint {
                    category: CATEGORY_SEARCH_ENGINE.to_string(),
                    evidence: "form:search".to_string(),
                    weight: 7,
                },
                CategoryHint {
                    category: CATEGORY_INDEXER.to_string(),
                    evidence: "links:many-outbound".to_string(),
                    weight: 3,
                },
            ],
            ..ClassificationSignals::default()
        };

        let mut docs_page = alpha_snapshot();
        docs_page.url = "http://gamma.onion/docs/start".to_string();
        docs_page.title = "Gamma Docs".to_string();
        docs_page.classification_signals = ClassificationSignals {
            word_count: 260,
            hints: vec![
                CategoryHint {
                    category: CATEGORY_DOCS.to_string(),
                    evidence: "title:docs".to_string(),
                    weight: 6,
                },
                CategoryHint {
                    category: CATEGORY_SEARCH_ENGINE.to_string(),
                    evidence: "text:search-results".to_string(),
                    weight: 4,
                },
            ],
            ..ClassificationSignals::default()
        };

        save_page_info(&mut conn, &search_page).expect("save search page");
        save_page_info(&mut conn, &docs_page).expect("save docs page");

        let sites = list_site_profiles(&mut conn, None, None).expect("site profiles");
        let gamma = sites
            .items
            .into_iter()
            .find(|site| site.host == "gamma.onion")
            .expect("gamma site profile");
        assert_eq!(gamma.category, CATEGORY_SEARCH_ENGINE);
        assert_eq!(gamma.page_count, 2);
        assert_eq!(
            gamma.source_page_url.as_deref(),
            Some("http://gamma.onion/search")
        );
        assert!(gamma.evidence.iter().any(|item| item == "pages:2"));
    }

    #[test]
    fn blacklisted_links_are_preserved_and_explicit_in_views() {
        let mut conn = setup_connection();

        add_domain_blacklist_entry(&mut conn, "beta.onion").expect("add blacklist");
        save_page_info(&mut conn, &alpha_snapshot()).expect("save alpha");
        save_page_info(&mut conn, &beta_snapshot()).expect("save beta");

        let alpha = list_page_summaries(&mut conn, None, None)
            .expect("summaries")
            .items
            .into_iter()
            .find(|item| item.url == "http://alpha.onion")
            .expect("alpha summary");
        let detail = get_page_detail(&mut conn, alpha.id)
            .expect("detail")
            .expect("detail exists");
        assert_eq!(detail.outgoing_links.len(), 1);
        assert!(detail.outgoing_links[0].is_blacklisted);
        assert_eq!(
            detail.outgoing_links[0].blacklist_match_domain.as_deref(),
            Some("beta.onion")
        );

        let relationships = list_site_relationships(&mut conn, None, None).expect("relationships");
        let blacklisted_relationship = relationships
            .items
            .into_iter()
            .find(|item| item.target_host == "beta.onion")
            .expect("blacklisted relationship");
        assert!(blacklisted_relationship.is_blacklisted);
        assert_eq!(
            blacklisted_relationship.blacklist_match_domain.as_deref(),
            Some("beta.onion")
        );

        let summaries = list_domain_blacklist_summaries(&mut conn).expect("blacklist summaries");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].page_link_count, 1);
        assert_eq!(summaries[0].page_scan_link_count, 1);
    }

    #[test]
    fn page_detail_links_are_url_encoded() {
        let mut conn = setup_connection();

        let mut snapshot = alpha_snapshot();
        snapshot.emails = vec!["ops+intel@alpha.onion".to_string()];
        save_page_info(&mut conn, &snapshot).expect("save alpha");

        let summary = list_page_summaries(&mut conn, None, None)
            .expect("summaries")
            .items
            .remove(0);
        let detail = get_page_detail(&mut conn, summary.id)
            .expect("detail")
            .expect("detail exists");

        assert_eq!(
            detail.emails[0].detail_url,
            "/entities/emails?value=ops%2Bintel%40alpha.onion"
        );
    }

    #[test]
    fn saving_a_page_creates_initial_scan_history() {
        let mut conn = setup_connection();

        save_page_info(&mut conn, &alpha_snapshot()).expect("save alpha");

        let page = list_page_summaries(&mut conn, None, None)
            .expect("page summaries")
            .items
            .remove(0);
        let history = list_page_scan_summaries(&mut conn, page.id).expect("page history");

        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_scan").expect("scan count"),
            1
        );
        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_scan_link")
                .expect("scan link count"),
            1
        );
        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_scan_email")
                .expect("scan email count"),
            1
        );
        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_scan_crypto")
                .expect("scan crypto count"),
            1
        );
        assert_eq!(history.len(), 1);
        assert!(history[0].change_summary.is_none());
        assert_eq!(
            history[0].detail_url,
            format!("/pages/{}/history/{}", page.id, history[0].id)
        );

        let detail = get_page_scan_detail(&mut conn, page.id, history[0].id)
            .expect("scan detail")
            .expect("scan detail exists");
        assert!(!detail.diff.has_previous_scan);
        assert_eq!(detail.outgoing_links.len(), 1);
        assert_eq!(detail.emails[0].value, "team@shared.test");
    }

    #[test]
    fn rescanning_a_page_replaces_child_observations() {
        let mut conn = setup_connection();

        save_page_info(&mut conn, &alpha_snapshot()).expect("save alpha");

        let mut rescanned = alpha_snapshot();
        rescanned.title = "Alpha Mirror".to_string();
        rescanned.language = "Spanish".to_string();
        rescanned.emails = vec!["ops@alpha.onion".to_string()];
        rescanned.crypto_refs = vec![CryptoReference {
            asset_type: "ethereum".to_string(),
            reference: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        }];
        rescanned.links = vec![LinkObservation {
            target_url: "http://gamma.onion".to_string(),
            target_host: "gamma.onion".to_string(),
        }];
        save_page_info(&mut conn, &rescanned).expect("resave alpha");

        let summary = list_page_summaries(&mut conn, None, None)
            .expect("summaries")
            .items
            .remove(0);
        let detail = get_page_detail(&mut conn, summary.id)
            .expect("detail")
            .expect("detail exists");

        assert_eq!(detail.emails[0].value, "ops@alpha.onion");
        assert_eq!(detail.crypto_refs.len(), 1);
        assert_eq!(detail.crypto_refs[0].asset_type, "ethereum");
        assert_eq!(detail.outgoing_links.len(), 1);

        assert!(get_email_entity_detail(&mut conn, "team@shared.test")
            .expect("old email detail")
            .is_none());
        assert!(get_crypto_entity_detail(
            &mut conn,
            "bitcoin",
            "bc1qalpha000000000000000000000000000000000",
        )
        .expect("old crypto detail")
        .is_none());

        let history = list_page_scan_summaries(&mut conn, summary.id).expect("page history");
        assert_eq!(history.len(), 2);
        let latest_change_summary = history[0]
            .change_summary
            .as_ref()
            .expect("latest change summary");
        assert_eq!(latest_change_summary.added_links, 1);
        assert_eq!(latest_change_summary.removed_links, 1);
        assert_eq!(latest_change_summary.added_emails, 1);
        assert_eq!(latest_change_summary.removed_emails, 1);
        assert_eq!(latest_change_summary.added_crypto_refs, 1);
        assert_eq!(latest_change_summary.removed_crypto_refs, 1);
        assert!(latest_change_summary.title_changed);
        assert!(latest_change_summary.language_changed);

        let scan_detail = get_page_scan_detail(&mut conn, summary.id, history[0].id)
            .expect("scan detail")
            .expect("scan detail exists");
        assert!(scan_detail.diff.has_previous_scan);
        assert_eq!(scan_detail.diff.previous_scan_id, Some(history[1].id));
        assert_eq!(
            scan_detail.diff.added_links[0].target_url,
            "http://gamma.onion"
        );
        assert_eq!(
            scan_detail.diff.removed_links[0].target_url,
            "http://beta.onion"
        );
        assert_eq!(scan_detail.diff.added_emails[0].value, "ops@alpha.onion");
        assert_eq!(scan_detail.diff.removed_emails[0].value, "team@shared.test");
        assert_eq!(scan_detail.diff.added_crypto_refs[0].asset_type, "ethereum");
        assert_eq!(
            scan_detail.diff.removed_crypto_refs[0].asset_type,
            "bitcoin"
        );
    }

    #[test]
    fn page_scan_history_migration_adds_empty_history_tables() {
        let mut conn = SqliteConnection::establish(":memory:").expect("in-memory sqlite");
        conn.batch_execute(
            "
            CREATE TABLE page(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              title VARCHAR NOT NULL,
              url VARCHAR NOT NULL UNIQUE,
              links VARCHAR NOT NULL,
              emails VARCHAR NOT NULL,
              coins VARCHAR NOT NULL,
              language VARCHAR NOT NULL DEFAULT '',
              last_scanned_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO page(title, url, links, emails, coins, language)
            VALUES ('Legacy Page', 'http://legacy.onion', '', '', '', '');
            ",
        )
        .expect("legacy page schema setup");

        conn.batch_execute(include_str!(
            "../migrations/2026-04-20-140000_page_scan_history/up.sql"
        ))
        .expect("page scan history migration");

        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_scan").expect("scan count"),
            0
        );

        conn.batch_execute(
            "
            INSERT INTO page_scan(page_id, title, language)
            VALUES (1, 'Legacy Page', 'English');
            INSERT INTO page_scan_email(scan_id, email)
            VALUES (1, 'legacy@onion.test');
            ",
        )
        .expect("history inserts");

        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_scan_email")
                .expect("scan email count"),
            1
        );
    }

    #[test]
    fn domain_blacklist_migration_adds_blacklist_table() {
        let mut conn = SqliteConnection::establish(":memory:").expect("in-memory sqlite");
        conn.batch_execute(
            "
            CREATE TABLE page(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              title VARCHAR NOT NULL,
              url VARCHAR NOT NULL UNIQUE,
              links VARCHAR NOT NULL,
              emails VARCHAR NOT NULL,
              coins VARCHAR NOT NULL,
              language VARCHAR NOT NULL DEFAULT '',
              last_scanned_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            ",
        )
        .expect("legacy page schema setup");

        conn.batch_execute(include_str!(
            "../migrations/2026-04-20-150000_domain_blacklist/up.sql"
        ))
        .expect("domain blacklist migration");

        add_domain_blacklist_entry(&mut conn, "blocked.onion").expect("insert blacklist entry");
        let rules = list_domain_blacklist_rules(&mut conn).expect("load blacklist rules");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].domain, "blocked.onion");
    }

    #[test]
    fn retry_backfill_migration_populates_relationship_tables() {
        let mut conn = SqliteConnection::establish(":memory:").expect("in-memory sqlite");
        conn.batch_execute(
            "
            CREATE TABLE work_unit(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              url VARCHAR NOT NULL UNIQUE,
              status VARCHAR NOT NULL DEFAULT 'pending',
              retry_count INTEGER NOT NULL DEFAULT 0,
              last_error VARCHAR,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE page(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              title VARCHAR NOT NULL,
              url VARCHAR NOT NULL UNIQUE,
              links VARCHAR NOT NULL,
              emails VARCHAR NOT NULL,
              coins VARCHAR NOT NULL,
              language VARCHAR NOT NULL DEFAULT '',
              last_scanned_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE page_link(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              source_page_id INTEGER NOT NULL,
              target_url VARCHAR NOT NULL,
              target_host VARCHAR NOT NULL DEFAULT '',
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(source_page_id, target_url)
            );
            CREATE TABLE page_email(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              page_id INTEGER NOT NULL,
              email VARCHAR NOT NULL,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(page_id, email)
            );
            CREATE TABLE page_crypto(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              page_id INTEGER NOT NULL,
              asset_type VARCHAR NOT NULL,
              reference VARCHAR NOT NULL,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(page_id, asset_type, reference)
            );
            INSERT INTO work_unit(url, status) VALUES ('http://legacy.onion', 'pending');
            INSERT INTO page(title, url, links, emails, coins, language, last_scanned_at, created_at)
            VALUES (
              'Legacy Page',
              'http://legacy.onion',
              'http://beta.onion/about,http://gamma.onion',
              'intel+ops@legacy.onion,team@legacy.onion',
              'bitcoin:bc1qlegacy00000000000000000000000000000000,ethereum:0x3333333333333333333333333333333333333333',
              '',
              CURRENT_TIMESTAMP,
              CURRENT_TIMESTAMP
            );
            ",
        )
        .expect("legacy schema setup");

        conn.batch_execute(include_str!(
            "../migrations/2026-04-20-130000_retry_queue_and_backfill/up.sql"
        ))
        .expect("retry/backfill migration");

        let link_count =
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_link").expect("link count");
        let email_count = scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_email")
            .expect("email count");
        let crypto_count = scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_crypto")
            .expect("crypto count");

        assert_eq!(link_count, 2);
        assert_eq!(email_count, 2);
        assert_eq!(crypto_count, 2);

        let migrated_work_unit = list_work_units(&mut conn, None, None)
            .expect("work units")
            .items
            .remove(0);
        assert_eq!(
            migrated_work_unit.next_attempt_at,
            migrated_work_unit.created_at
        );
        assert!(migrated_work_unit.last_attempt_at.is_none());
    }
}
