pub mod extraction;
pub mod models;
pub mod schema;

use anyhow::{Context, Result};
use diesel::connection::SimpleConnection;
use diesel::deserialize::QueryableByName;
use diesel::dsl::{count_star, sql};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Bool, Nullable, Text};
use diesel::sqlite::SqliteConnection;
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
pub const SSH_STATUS_SUCCESS: &str = "success";
pub const MAX_RETRY_ATTEMPTS: i32 = 5;
#[cfg(test)]
const SQLITE_BUSY_TIMEOUT_MS: i32 = 5_000;
const DEFAULT_TOP_SITE_LIMIT: i64 = 25;
const DEFAULT_PAGE_LIMIT: i64 = 50;
const MAX_PAGE_LIMIT: i64 = 200;
const DEFAULT_RELATIONSHIP_GRAPH_LIMIT: i64 = 80;
const MIN_RELATIONSHIP_GRAPH_LIMIT: i64 = 10;
const MAX_RELATIONSHIP_GRAPH_DEPTH: i64 = 4;
const DEFAULT_RELATIONSHIP_GRAPH_DEPTH: i64 = 3;
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
const CATEGORY_SEO_SPAM: &str = "seo-spam";
const CATEGORY_UNKNOWN: &str = "unknown";
const CONFIDENCE_HIGH: &str = "high";
const CONFIDENCE_MEDIUM: &str = "medium";
const CONFIDENCE_LOW: &str = "low";
pub const LEAD_STATUS_NEW: &str = "new";
pub const LEAD_STATUS_TRIAGED: &str = "triaged";
pub const LEAD_STATUS_MONITORING: &str = "monitoring";
pub const LEAD_STATUS_SUPPRESSED: &str = "suppressed";
pub const WATCHLIST_TYPE_DOMAIN: &str = "domain";
pub const WATCHLIST_TYPE_URL: &str = "url";
pub const WATCHLIST_TYPE_EMAIL: &str = "email";
pub const WATCHLIST_TYPE_CRYPTO: &str = "crypto";
pub const WATCHLIST_TYPE_KEYWORD: &str = "keyword";
pub const WATCHLIST_TYPE_SSH_FINGERPRINT: &str = "ssh_fingerprint";
pub const WATCHLIST_TYPE_HTTP_FINGERPRINT: &str = "http_fingerprint";
pub const WATCHLIST_TYPE_FAVICON_HASH: &str = "favicon_hash";
pub const AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY: &str = "site_category";
pub const AUTO_BLACKLIST_RULE_TYPE_KEYWORD: &str = "keyword";
const LEAD_SEVERITY_LOW: &str = "low";
const LEAD_SEVERITY_MEDIUM: &str = "medium";
const LEAD_SEVERITY_HIGH: &str = "high";
const LEAD_SEVERITY_CRITICAL: &str = "critical";
const DEFAULT_LEAD_LIMIT: i64 = 50;
const MAX_LEAD_LIMIT: i64 = 200;
const DEFAULT_RECOMPUTE_LIMIT: i64 = 250;
const MAX_RECOMPUTE_LIMIT: i64 = 5_000;
pub const DEFAULT_BLACKLIST_LEAD_LINK_BATCH_SIZE: i64 = 50_000;
const MAX_BLACKLIST_LEAD_LINK_BATCH_SIZE: i64 = 1_000_000;
const MANY_NEW_OUTBOUND_LINK_THRESHOLD: usize = 25;
const HIGH_DEGREE_SOURCE_HOST_THRESHOLD: i64 = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PageSaveOutcome {
    Stored,
    SkippedBlacklisted,
    PurgedAfterAutoBlacklist,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkQueueOutcome {
    Queued,
    SkippedBlacklisted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlDialect {
    Postgres,
    Sqlite,
}

pub trait AppConnection: Connection + SimpleConnection {
    const DIALECT: SqlDialect;

    fn dialect(&self) -> SqlDialect {
        Self::DIALECT
    }
}

impl AppConnection for PgConnection {
    const DIALECT: SqlDialect = SqlDialect::Postgres;
}

impl AppConnection for SqliteConnection {
    const DIALECT: SqlDialect = SqlDialect::Sqlite;
}

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
struct SiteRelationshipGraphEdgeRow {
    #[diesel(sql_type = Text)]
    source_host: String,
    #[diesel(sql_type = Text)]
    target_host: String,
    #[diesel(sql_type = BigInt)]
    reference_count: i64,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    depth: i32,
}

#[derive(QueryableByName)]
struct TargetHostCountRow {
    #[diesel(sql_type = Text)]
    target_host: String,
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(QueryableByName)]
struct RecentHostRow {
    #[diesel(sql_type = Text)]
    host: String,
    #[diesel(sql_type = Text)]
    last_scanned_at: String,
}

#[derive(QueryableByName)]
struct SshHostKeySummaryRow {
    #[diesel(sql_type = Text)]
    algorithm: String,
    #[diesel(sql_type = Text)]
    fingerprint: String,
    #[diesel(sql_type = BigInt)]
    host_count: i64,
    #[diesel(sql_type = BigInt)]
    endpoint_count: i64,
    #[diesel(sql_type = Text)]
    last_success_at: String,
}

#[derive(QueryableByName)]
struct HostTagRow {
    #[diesel(sql_type = Text)]
    host: String,
    #[diesel(sql_type = Text)]
    tag: String,
}

#[derive(QueryableByName)]
struct HostMetricSummaryRow {
    #[diesel(sql_type = Text)]
    host: String,
    #[diesel(sql_type = BigInt)]
    count: i64,
    #[diesel(sql_type = Nullable<Text>)]
    last_scanned_at: Option<String>,
}

#[derive(QueryableByName)]
struct CategoryDistributionRow {
    #[diesel(sql_type = Text)]
    category: String,
    #[diesel(sql_type = BigInt)]
    host_count: i64,
}

#[derive(QueryableByName)]
struct CategoryTimelineRow {
    #[diesel(sql_type = Text)]
    day: String,
    #[diesel(sql_type = Text)]
    category: String,
    #[diesel(sql_type = BigInt)]
    host_count: i64,
}

#[derive(QueryableByName)]
struct HostPageContextRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    page_id: i32,
    #[diesel(sql_type = Text)]
    page_title: String,
    #[diesel(sql_type = Text)]
    page_url: String,
    #[diesel(sql_type = Text)]
    host: String,
    #[diesel(sql_type = Text)]
    last_scanned_at: String,
}

#[derive(QueryableByName)]
struct LeadPageEvidenceRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    page_id: i32,
    #[diesel(sql_type = Text)]
    page_title: String,
    #[diesel(sql_type = Text)]
    page_url: String,
    #[diesel(sql_type = Text)]
    host: String,
    #[diesel(sql_type = Text)]
    observed_at: String,
}

#[derive(QueryableByName)]
struct SharedEmailLeadRow {
    #[diesel(sql_type = Text)]
    email: String,
    #[diesel(sql_type = BigInt)]
    host_count: i64,
    #[diesel(sql_type = BigInt)]
    page_count: i64,
    #[diesel(sql_type = Text)]
    first_seen_at: String,
    #[diesel(sql_type = Text)]
    last_seen_at: String,
}

#[derive(QueryableByName)]
struct SharedCryptoLeadRow {
    #[diesel(sql_type = Text)]
    asset_type: String,
    #[diesel(sql_type = Text)]
    reference: String,
    #[diesel(sql_type = BigInt)]
    host_count: i64,
    #[diesel(sql_type = BigInt)]
    page_count: i64,
    #[diesel(sql_type = Text)]
    first_seen_at: String,
    #[diesel(sql_type = Text)]
    last_seen_at: String,
}

#[derive(QueryableByName)]
struct RecentScanLeadRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    scan_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    page_id: i32,
    #[diesel(sql_type = Text)]
    page_title: String,
    #[diesel(sql_type = Text)]
    page_url: String,
    #[diesel(sql_type = Text)]
    page_host: String,
    #[diesel(sql_type = Text)]
    scanned_at: String,
    #[diesel(sql_type = Nullable<diesel::sql_types::Integer>)]
    previous_scan_id: Option<i32>,
}

#[derive(QueryableByName)]
struct BlacklistedLinkLeadRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    page_id: i32,
    #[diesel(sql_type = Text)]
    page_title: String,
    #[diesel(sql_type = Text)]
    page_url: String,
    #[diesel(sql_type = Text)]
    source_host: String,
    #[diesel(sql_type = Text)]
    target_url: String,
    #[diesel(sql_type = Text)]
    target_host: String,
    #[diesel(sql_type = Text)]
    observed_at: String,
    #[diesel(sql_type = Text)]
    blacklist_domain: String,
}

#[derive(QueryableByName)]
struct SharedFingerprintLeadRow {
    #[diesel(sql_type = Text)]
    fingerprint_value: String,
    #[diesel(sql_type = BigInt)]
    host_count: i64,
    #[diesel(sql_type = BigInt)]
    endpoint_count: i64,
    #[diesel(sql_type = Text)]
    first_seen_at: String,
    #[diesel(sql_type = Text)]
    last_seen_at: String,
}

#[derive(QueryableByName)]
struct FingerprintEndpointEvidenceRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    source_id: i32,
    #[diesel(sql_type = Text)]
    host: String,
    #[diesel(sql_type = Text)]
    endpoint: String,
    #[diesel(sql_type = Text)]
    observed_at: String,
}

#[derive(QueryableByName)]
struct SharedSshLeadRow {
    #[diesel(sql_type = Text)]
    algorithm: String,
    #[diesel(sql_type = Text)]
    fingerprint: String,
    #[diesel(sql_type = BigInt)]
    host_count: i64,
    #[diesel(sql_type = BigInt)]
    endpoint_count: i64,
    #[diesel(sql_type = Text)]
    first_seen_at: String,
    #[diesel(sql_type = Text)]
    last_seen_at: String,
}

#[derive(QueryableByName)]
struct SshEndpointEvidenceRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    source_id: i32,
    #[diesel(sql_type = Text)]
    host: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    port: i32,
    #[diesel(sql_type = Text)]
    observed_at: String,
}

#[derive(QueryableByName)]
struct CategoryChangeLeadRow {
    #[diesel(sql_type = Text)]
    host: String,
    #[diesel(sql_type = Text)]
    current_category: String,
    #[diesel(sql_type = Text)]
    previous_category: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    page_id: i32,
    #[diesel(sql_type = Text)]
    page_title: String,
    #[diesel(sql_type = Text)]
    page_url: String,
    #[diesel(sql_type = Text)]
    observed_at: String,
}

#[derive(QueryableByName)]
struct HighDegreeTargetLeadRow {
    #[diesel(sql_type = Text)]
    target_host: String,
    #[diesel(sql_type = BigInt)]
    source_host_count: i64,
    #[diesel(sql_type = BigInt)]
    reference_count: i64,
    #[diesel(sql_type = Text)]
    first_seen_at: String,
    #[diesel(sql_type = Text)]
    last_seen_at: String,
}

#[derive(QueryableByName)]
struct DuplicateSiteTitleLeadRow {
    #[diesel(sql_type = Text)]
    title: String,
    #[diesel(sql_type = BigInt)]
    host_count: i64,
    #[diesel(sql_type = BigInt)]
    page_count: i64,
    #[diesel(sql_type = Text)]
    first_seen_at: String,
    #[diesel(sql_type = Text)]
    last_seen_at: String,
}

#[derive(QueryableByName)]
struct WatchlistLeadEvidenceRow {
    #[diesel(sql_type = Text)]
    source_type: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    source_id: i32,
    #[diesel(sql_type = Text)]
    source_key: String,
    #[diesel(sql_type = Text)]
    evidence_text: String,
    #[diesel(sql_type = Text)]
    observed_at: String,
    #[diesel(sql_type = Nullable<Text>)]
    site_host: Option<String>,
}

#[derive(QueryableByName)]
struct RelationshipEvidenceRow {
    #[diesel(sql_type = Text)]
    source_host: String,
    #[diesel(sql_type = Text)]
    target_host: String,
    #[diesel(sql_type = BigInt)]
    reference_count: i64,
    #[diesel(sql_type = Text)]
    observed_at: String,
}

#[derive(QueryableByName)]
struct SiteProfileListRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = Text)]
    host: String,
    #[diesel(sql_type = Text)]
    category: String,
    #[diesel(sql_type = Text)]
    confidence: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    score: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    page_count: i32,
    #[diesel(sql_type = Text)]
    first_found_at: String,
    #[diesel(sql_type = Text)]
    last_scanned_at: String,
    #[diesel(sql_type = Text)]
    evidence: String,
    #[diesel(sql_type = Nullable<diesel::sql_types::Integer>)]
    source_page_id: Option<i32>,
    #[diesel(sql_type = Text)]
    last_classified_at: String,
    #[diesel(sql_type = Text)]
    created_at: String,
}

#[derive(QueryableByName)]
struct SiteScanStatsRow {
    #[diesel(sql_type = BigInt)]
    page_count: i64,
    #[diesel(sql_type = Text)]
    first_found_at: String,
    #[diesel(sql_type = Text)]
    last_scanned_at: String,
}

#[derive(Clone)]
struct SiteScanStats {
    page_count: i32,
    first_found_at: String,
    last_scanned_at: String,
}

#[derive(Clone, Copy)]
struct PaginationInput {
    limit: i64,
    offset: i64,
}

#[derive(Clone)]
struct HostPageContext {
    page_id: i32,
    page_title: String,
    page_url: String,
    last_scanned_at: String,
}

#[derive(Clone)]
struct UrlEndpoint {
    host: String,
    scheme: String,
    port: i32,
}

#[derive(Default)]
struct ScanObservationSet {
    links: BTreeSet<(String, String)>,
    emails: BTreeSet<String>,
    crypto_refs: BTreeSet<(String, String)>,
}

#[derive(Clone, Debug)]
pub struct IntelLeadRecomputeOptions {
    pub limit: Option<i64>,
    pub since_scan_id: Option<i32>,
    pub rule_ids: Vec<String>,
    pub blacklist_after_link_id: Option<i32>,
    pub blacklist_link_batch_size: Option<i64>,
}

#[derive(Clone)]
struct IntelLeadCandidate {
    rule_id: String,
    lead_key: String,
    title: String,
    summary: String,
    score: i32,
    confidence: i32,
    primary_entity_type: String,
    primary_entity_value: String,
    related_entity_type: Option<String>,
    related_entity_value: Option<String>,
    first_seen_at: String,
    last_seen_at: String,
    evidence: Vec<IntelLeadEvidenceCandidate>,
}

#[derive(Clone)]
struct IntelLeadEvidenceCandidate {
    source_type: String,
    source_id: i32,
    source_key: String,
    evidence_text: String,
    observed_at: String,
}

type LeadCandidateBuilder =
    fn(&mut PgConnection, Option<i32>, i64) -> Result<Vec<IntelLeadCandidate>>;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct LeadEvidenceSource {
    source_type: String,
    source_id: i32,
    source_key: String,
}

pub fn establish_connection() -> Result<PgConnection> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let mut connection = PgConnection::establish(&database_url)
        .with_context(|| format!("error connecting to {database_url}"))?;
    configure_postgres_connection(&mut connection)?;
    Ok(connection)
}

fn configure_postgres_connection(conn: &mut PgConnection) -> Result<()> {
    conn.batch_execute("SET TIME ZONE 'UTC';")
        .context("error configuring postgres connection")
}

#[cfg(test)]
fn configure_sqlite_connection(conn: &mut SqliteConnection, database_url: &str) -> Result<()> {
    conn.batch_execute(&format!(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS};
        "
    ))
    .context("error configuring sqlite connection")?;

    if is_file_backed_sqlite_database(database_url) {
        conn.batch_execute(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            ",
        )
        .context("error enabling sqlite WAL mode")?;
    }

    Ok(())
}

#[cfg(test)]
fn is_file_backed_sqlite_database(database_url: &str) -> bool {
    let normalized = database_url.trim().to_ascii_lowercase();
    !(normalized == ":memory:"
        || normalized.starts_with("file::memory:")
        || normalized.contains("mode=memory"))
}

pub fn strip_url_fragment(raw_url: &str) -> String {
    match Url::parse(raw_url) {
        Ok(parsed) if parsed.fragment().is_some() => raw_url
            .split_once('#')
            .map(|(without_fragment, _)| without_fragment.to_string())
            .unwrap_or_else(|| raw_url.to_string()),
        _ => raw_url.to_string(),
    }
}

pub fn normalize_crawl_url(raw_url: &str) -> String {
    let without_fragment = strip_url_fragment(raw_url);
    match Url::parse(&without_fragment) {
        Ok(parsed) => {
            let host = match parsed.host_str() {
                Some(host) => host,
                None => return without_fragment,
            };
            let mut normalized = format!("{}://", parsed.scheme());
            if !parsed.username().is_empty() {
                normalized.push_str(parsed.username());
                if let Some(password) = parsed.password() {
                    normalized.push(':');
                    normalized.push_str(password);
                }
                normalized.push('@');
            }
            normalized.push_str(host);
            if let Some(port) = parsed.port() {
                normalized.push(':');
                normalized.push_str(&port.to_string());
            }
            normalized
        }
        Err(_) => without_fragment,
    }
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

pub fn valid_watchlist_item_types() -> [&'static str; 8] {
    [
        WATCHLIST_TYPE_DOMAIN,
        WATCHLIST_TYPE_URL,
        WATCHLIST_TYPE_EMAIL,
        WATCHLIST_TYPE_CRYPTO,
        WATCHLIST_TYPE_KEYWORD,
        WATCHLIST_TYPE_SSH_FINGERPRINT,
        WATCHLIST_TYPE_HTTP_FINGERPRINT,
        WATCHLIST_TYPE_FAVICON_HASH,
    ]
}

pub fn normalize_watchlist_item_type(raw_item_type: &str) -> Result<String> {
    let item_type = raw_item_type.trim().to_ascii_lowercase();
    anyhow::ensure!(
        valid_watchlist_item_types().contains(&item_type.as_str()),
        "invalid watchlist item type: {raw_item_type}"
    );
    Ok(item_type)
}

pub fn normalize_watchlist_value(raw_item_type: &str, raw_value: &str) -> Result<String> {
    let item_type = normalize_watchlist_item_type(raw_item_type)?;
    let trimmed = raw_value.trim();
    anyhow::ensure!(!trimmed.is_empty(), "watchlist value must not be empty");

    match item_type.as_str() {
        WATCHLIST_TYPE_DOMAIN => normalize_blacklist_domain(trimmed),
        WATCHLIST_TYPE_URL => {
            let without_fragment = strip_url_fragment(trimmed);
            let parsed = Url::parse(&without_fragment)
                .with_context(|| format!("invalid watchlist URL: {raw_value}"))?;
            anyhow::ensure!(
                matches!(parsed.scheme(), "http" | "https"),
                "watchlist URL must use http or https"
            );
            anyhow::ensure!(
                parsed.host_str().is_some(),
                "watchlist URL must include a host"
            );
            Ok(without_fragment.to_ascii_lowercase())
        }
        WATCHLIST_TYPE_EMAIL => {
            let value = trimmed.to_ascii_lowercase();
            anyhow::ensure!(
                value.contains('@') && !value.starts_with('@') && !value.ends_with('@'),
                "watchlist email must look like an email address"
            );
            Ok(value)
        }
        WATCHLIST_TYPE_KEYWORD => {
            let value = trimmed
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_ascii_lowercase();
            anyhow::ensure!(!value.is_empty(), "watchlist keyword must not be empty");
            Ok(value)
        }
        WATCHLIST_TYPE_CRYPTO
        | WATCHLIST_TYPE_SSH_FINGERPRINT
        | WATCHLIST_TYPE_HTTP_FINGERPRINT
        | WATCHLIST_TYPE_FAVICON_HASH => Ok(trimmed.to_ascii_lowercase()),
        _ => unreachable!("watchlist type was already validated"),
    }
}

pub fn normalize_watchlist_label(raw_label: Option<&str>) -> String {
    raw_label
        .unwrap_or_default()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn normalize_forum_keyword_label(raw_label: &str) -> Result<String> {
    let normalized = raw_label
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase();
    anyhow::ensure!(
        !normalized.is_empty(),
        "forum keyword label must not be empty"
    );
    Ok(normalized)
}

pub fn normalize_forum_keyword_pattern(raw_pattern: &str) -> Result<String> {
    let normalized = raw_pattern.trim().to_ascii_lowercase();
    anyhow::ensure!(
        !normalized.is_empty(),
        "forum keyword pattern must not be empty"
    );
    Ok(normalized)
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

pub fn host_matches_blacklist(host: &str, blacklist_domains: &[String]) -> bool {
    find_matching_blacklist_domain(host, blacklist_domains).is_some()
}

pub fn url_matches_blacklist(url: &str, blacklist_domains: &[String]) -> bool {
    Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(|host| host.to_ascii_lowercase()))
        .is_some_and(|host| host_matches_blacklist(&host, blacklist_domains))
}

pub fn auto_blacklist_category_options() -> Vec<AutoBlacklistCategoryOption> {
    valid_auto_blacklist_site_categories()
        .iter()
        .map(|category| AutoBlacklistCategoryOption {
            value: (*category).to_string(),
            label: site_category_label(category).to_string(),
        })
        .collect()
}

pub fn normalize_auto_blacklist_rule_type(raw_rule_type: &str) -> Result<String> {
    let rule_type = raw_rule_type.trim().to_ascii_lowercase();
    anyhow::ensure!(
        matches!(
            rule_type.as_str(),
            AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY | AUTO_BLACKLIST_RULE_TYPE_KEYWORD
        ),
        "invalid auto blacklist rule type: {raw_rule_type}"
    );
    Ok(rule_type)
}

pub fn normalize_auto_blacklist_rule_value(raw_rule_type: &str, raw_value: &str) -> Result<String> {
    let rule_type = normalize_auto_blacklist_rule_type(raw_rule_type)?;
    match rule_type.as_str() {
        AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY => normalize_auto_blacklist_site_category(raw_value),
        AUTO_BLACKLIST_RULE_TYPE_KEYWORD => normalize_auto_blacklist_keyword(raw_value),
        _ => unreachable!("auto blacklist rule type was already validated"),
    }
}

pub fn normalize_auto_blacklist_label(raw_label: Option<&str>) -> String {
    raw_label
        .unwrap_or_default()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn list_auto_blacklist_rules(conn: &mut PgConnection) -> Result<Vec<AutoBlacklistRule>> {
    use crate::schema::auto_blacklist_rule::dsl as rule_dsl;

    rule_dsl::auto_blacklist_rule
        .order(rule_dsl::rule_type.asc())
        .then_order_by(rule_dsl::value.asc())
        .then_order_by(rule_dsl::id.asc())
        .select(AutoBlacklistRule::as_select())
        .load::<AutoBlacklistRule>(conn)
        .context("error loading auto blacklist rules")
}

pub fn list_recent_auto_blacklist_events(
    conn: &mut PgConnection,
    requested_limit: Option<i64>,
) -> Result<Vec<AutoBlacklistEvent>> {
    use crate::schema::auto_blacklist_event::dsl as event_dsl;

    let limit = requested_limit.unwrap_or(50).clamp(1, 200);
    event_dsl::auto_blacklist_event
        .order(event_dsl::created_at.desc())
        .then_order_by(event_dsl::id.desc())
        .limit(limit)
        .select(AutoBlacklistEvent::as_select())
        .load::<AutoBlacklistEvent>(conn)
        .context("error loading auto blacklist events")
}

pub fn get_auto_blacklist_config(conn: &mut PgConnection) -> Result<AutoBlacklistConfig> {
    Ok(AutoBlacklistConfig {
        rules: list_auto_blacklist_rules(conn)?,
        events: list_recent_auto_blacklist_events(conn, Some(50))?,
        category_options: auto_blacklist_category_options(),
    })
}

pub fn add_auto_blacklist_rule(
    conn: &mut PgConnection,
    raw_rule_type: &str,
    raw_value: &str,
    raw_label: Option<&str>,
) -> Result<AutoBlacklistRule> {
    use crate::schema::auto_blacklist_rule::dsl as rule_dsl;

    let rule_type = normalize_auto_blacklist_rule_type(raw_rule_type)?;
    let value = normalize_auto_blacklist_rule_value(&rule_type, raw_value)?;
    let label = match normalize_auto_blacklist_label(raw_label) {
        label if !label.is_empty() => label,
        _ if rule_type == AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY => {
            site_category_label(&value).to_string()
        }
        _ => value.clone(),
    };

    diesel::insert_into(rule_dsl::auto_blacklist_rule)
        .values(NewAutoBlacklistRule {
            rule_type: &rule_type,
            value: &value,
            label: &label,
        })
        .on_conflict_do_nothing()
        .execute(conn)
        .context("error saving auto blacklist rule")?;

    rule_dsl::auto_blacklist_rule
        .filter(rule_dsl::rule_type.eq(&rule_type))
        .filter(rule_dsl::value.eq(&value))
        .select(AutoBlacklistRule::as_select())
        .first::<AutoBlacklistRule>(conn)
        .context("error loading saved auto blacklist rule")
}

pub fn set_auto_blacklist_rule_enabled(
    conn: &mut PgConnection,
    rule_id: i32,
    next_enabled: bool,
) -> Result<Option<AutoBlacklistRule>> {
    use crate::schema::auto_blacklist_rule::dsl as rule_dsl;

    let existing = rule_dsl::auto_blacklist_rule
        .filter(rule_dsl::id.eq(rule_id))
        .select(AutoBlacklistRule::as_select())
        .first::<AutoBlacklistRule>(conn)
        .optional()
        .context("error loading auto blacklist rule")?;
    if existing.is_some() {
        diesel::update(rule_dsl::auto_blacklist_rule.filter(rule_dsl::id.eq(rule_id)))
            .set(rule_dsl::enabled.eq(next_enabled))
            .execute(conn)
            .context("error updating auto blacklist rule")?;
    }

    rule_dsl::auto_blacklist_rule
        .filter(rule_dsl::id.eq(rule_id))
        .select(AutoBlacklistRule::as_select())
        .first::<AutoBlacklistRule>(conn)
        .optional()
        .context("error loading updated auto blacklist rule")
}

pub fn remove_auto_blacklist_rule(
    conn: &mut PgConnection,
    rule_id: i32,
) -> Result<Option<AutoBlacklistRule>> {
    use crate::schema::auto_blacklist_rule::dsl as rule_dsl;

    let existing = rule_dsl::auto_blacklist_rule
        .filter(rule_dsl::id.eq(rule_id))
        .select(AutoBlacklistRule::as_select())
        .first::<AutoBlacklistRule>(conn)
        .optional()
        .context("error loading auto blacklist rule")?;
    if existing.is_some() {
        diesel::delete(rule_dsl::auto_blacklist_rule.filter(rule_dsl::id.eq(rule_id)))
            .execute(conn)
            .context("error removing auto blacklist rule")?;
    }
    Ok(existing)
}

pub fn apply_auto_blacklist_rules_to_existing(
    conn: &mut PgConnection,
    dry_run: bool,
    requested_limit: Option<i64>,
) -> Result<AutoBlacklistBackfillResult> {
    let limit = requested_limit.unwrap_or(500).clamp(1, 5_000);
    let rules = list_enabled_auto_blacklist_rules(conn)?;
    let category_rules = rules
        .iter()
        .filter(|rule| rule.rule_type == AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY)
        .cloned()
        .collect::<Vec<_>>();
    let keyword_rules = rules
        .iter()
        .filter(|rule| rule.rule_type == AUTO_BLACKLIST_RULE_TYPE_KEYWORD)
        .cloned()
        .collect::<Vec<_>>();
    let mut scanned_count = 0usize;
    let mut matches = Vec::new();

    if !category_rules.is_empty() {
        let profiles = load_auto_blacklist_profile_backfill_rows(conn, limit)?;
        scanned_count += profiles.len();
        for profile in profiles {
            for rule in &category_rules {
                if profile.category == rule.value {
                    matches.push(AutoBlacklistBackfillMatch {
                        domain: profile.host.clone(),
                        rule_id: rule.id,
                        rule_type: rule.rule_type.clone(),
                        matched_value: rule.value.clone(),
                        evidence: truncate(
                            &format!("site category {}", site_category_label(&profile.category)),
                            240,
                        ),
                        source_page_id: profile.source_page_id,
                    });
                }
            }
        }
    }

    if !keyword_rules.is_empty() {
        let pages = load_auto_blacklist_page_backfill_rows(conn, limit)?;
        scanned_count += pages.len();
        let page_ids = pages.iter().map(|page| page.id).collect::<Vec<_>>();
        let tags_by_page = load_page_keyword_tags_by_page_ids(conn, &page_ids)?;
        for page in pages {
            let domain = host_from_url(&page.url);
            if domain.is_empty() {
                continue;
            }
            let tags = tags_by_page.get(&page.id).cloned().unwrap_or_default();
            let corpus = auto_blacklist_backfill_corpus(&page, &tags);
            for rule in &keyword_rules {
                if auto_blacklist_keyword_matches(&corpus, &rule.value) {
                    matches.push(AutoBlacklistBackfillMatch {
                        domain: domain.clone(),
                        rule_id: rule.id,
                        rule_type: rule.rule_type.clone(),
                        matched_value: rule.value.clone(),
                        evidence: truncate(
                            &format!("keyword phrase '{}' matched", rule.value),
                            240,
                        ),
                        source_page_id: Some(page.id),
                    });
                }
            }
        }
    }

    matches.sort_by(|left, right| {
        left.domain
            .cmp(&right.domain)
            .then_with(|| left.rule_id.cmp(&right.rule_id))
            .then_with(|| left.source_page_id.cmp(&right.source_page_id))
    });
    matches.dedup_by(|left, right| {
        left.domain == right.domain
            && left.rule_id == right.rule_id
            && left.source_page_id == right.source_page_id
    });

    let matched_count = matches.len();
    let mut blacklisted_count = 0usize;
    let mut event_count = 0usize;
    if !dry_run {
        let mut blacklist_domains = load_blacklist_domains(conn)?;
        for matched in &matches {
            if find_matching_blacklist_domain(&matched.domain, &blacklist_domains).is_none() {
                blacklisted_count += 1;
                blacklist_domains.push(matched.domain.clone());
            }
            add_domain_blacklist_entry(conn, &matched.domain)?;
            event_count += insert_auto_blacklist_event(
                conn,
                matched.rule_id,
                &matched.domain,
                matched.source_page_id,
                &matched.rule_type,
                &matched.matched_value,
                &matched.evidence,
            )?;
        }
    }

    Ok(AutoBlacklistBackfillResult {
        dry_run,
        scanned_count,
        matched_count,
        blacklisted_count,
        event_count,
        matches,
    })
}

pub fn list_forum_keyword_rules(conn: &mut PgConnection) -> Result<Vec<ForumKeywordRule>> {
    use crate::schema::forum_keyword_rule::dsl as forum_keyword_rule_dsl;

    forum_keyword_rule_dsl::forum_keyword_rule
        .order(forum_keyword_rule_dsl::label.asc())
        .then_order_by(forum_keyword_rule_dsl::pattern.asc())
        .select(ForumKeywordRule::as_select())
        .load::<ForumKeywordRule>(conn)
        .context("error loading forum keyword rules")
}

pub fn add_forum_keyword_rule(
    conn: &mut PgConnection,
    raw_label: &str,
    raw_pattern: &str,
) -> Result<ForumKeywordRule> {
    use crate::schema::forum_keyword_rule::dsl as forum_keyword_rule_dsl;

    let label = normalize_forum_keyword_label(raw_label)?;
    let pattern = normalize_forum_keyword_pattern(raw_pattern)?;
    diesel::insert_into(forum_keyword_rule_dsl::forum_keyword_rule)
        .values(&NewForumKeywordRule {
            label: &label,
            pattern: &pattern,
        })
        .on_conflict((
            forum_keyword_rule_dsl::label,
            forum_keyword_rule_dsl::pattern,
        ))
        .do_nothing()
        .execute(conn)
        .context("error saving forum keyword rule")?;

    forum_keyword_rule_dsl::forum_keyword_rule
        .filter(forum_keyword_rule_dsl::label.eq(&label))
        .filter(forum_keyword_rule_dsl::pattern.eq(&pattern))
        .select(ForumKeywordRule::as_select())
        .first::<ForumKeywordRule>(conn)
        .context("error loading saved forum keyword rule")
}

pub fn remove_forum_keyword_rule(
    conn: &mut PgConnection,
    raw_label: &str,
    raw_pattern: &str,
) -> Result<Option<(String, String)>> {
    use crate::schema::forum_keyword_rule::dsl as forum_keyword_rule_dsl;

    let label = normalize_forum_keyword_label(raw_label)?;
    let pattern = normalize_forum_keyword_pattern(raw_pattern)?;
    let deleted = diesel::delete(
        forum_keyword_rule_dsl::forum_keyword_rule
            .filter(forum_keyword_rule_dsl::label.eq(&label))
            .filter(forum_keyword_rule_dsl::pattern.eq(&pattern)),
    )
    .execute(conn)
    .context("error removing forum keyword rule")?;

    Ok((deleted > 0).then_some((label, pattern)))
}

pub fn list_watchlist_items(conn: &mut PgConnection) -> Result<Vec<WatchlistItem>> {
    use crate::schema::watchlist_item::dsl as watchlist_dsl;

    watchlist_dsl::watchlist_item
        .order(watchlist_dsl::item_type.asc())
        .then_order_by(watchlist_dsl::value.asc())
        .then_order_by(watchlist_dsl::id.asc())
        .select(WatchlistItem::as_select())
        .load::<WatchlistItem>(conn)
        .context("error loading watchlist items")
}

pub fn add_watchlist_item(
    conn: &mut PgConnection,
    raw_item_type: &str,
    raw_value: &str,
    raw_label: Option<&str>,
) -> Result<WatchlistItem> {
    use crate::schema::watchlist_item::dsl as watchlist_dsl;

    let item_type = normalize_watchlist_item_type(raw_item_type)?;
    let value = normalize_watchlist_value(&item_type, raw_value)?;
    let label = normalize_watchlist_label(raw_label);
    diesel::insert_into(watchlist_dsl::watchlist_item)
        .values(NewWatchlistItem {
            item_type: &item_type,
            value: &value,
            label: &label,
        })
        .on_conflict_do_nothing()
        .execute(conn)
        .context("error saving watchlist item")?;

    watchlist_dsl::watchlist_item
        .filter(watchlist_dsl::item_type.eq(&item_type))
        .filter(watchlist_dsl::value.eq(&value))
        .select(WatchlistItem::as_select())
        .first::<WatchlistItem>(conn)
        .context("error loading saved watchlist item")
}

pub fn remove_watchlist_item(
    conn: &mut PgConnection,
    item_id: i32,
) -> Result<Option<WatchlistItem>> {
    use crate::schema::watchlist_item::dsl as watchlist_dsl;

    let existing = watchlist_dsl::watchlist_item
        .filter(watchlist_dsl::id.eq(item_id))
        .select(WatchlistItem::as_select())
        .first::<WatchlistItem>(conn)
        .optional()
        .context("error loading watchlist item")?;
    if existing.is_some() {
        diesel::delete(watchlist_dsl::watchlist_item.filter(watchlist_dsl::id.eq(item_id)))
            .execute(conn)
            .context("error removing watchlist item")?;
    }
    Ok(existing)
}

pub fn list_domain_blacklist_rules(conn: &mut PgConnection) -> Result<Vec<DomainBlacklistRule>> {
    use crate::schema::domain_blacklist::dsl as blacklist_dsl;

    blacklist_dsl::domain_blacklist
        .order(blacklist_dsl::domain.asc())
        .then_order_by(blacklist_dsl::id.asc())
        .select(DomainBlacklistRule::as_select())
        .load::<DomainBlacklistRule>(conn)
        .context("error loading blacklist domains")
}

pub fn add_domain_blacklist_entry(
    conn: &mut PgConnection,
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

pub fn remove_domain_blacklist_entry(conn: &mut PgConnection, raw_domain: &str) -> Result<String> {
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
    conn: &mut PgConnection,
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
    conn: &mut PgConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<SiteProfileSummary>> {
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
    let query = format!(
        "
        SELECT
            sp.id,
            sp.host,
            sp.category,
            sp.confidence,
            sp.score,
            sp.page_count,
            sp.first_found_at,
            sp.last_scanned_at,
            sp.evidence,
            sp.source_page_id,
            sp.last_classified_at,
            sp.created_at
        FROM site_profile sp
        ORDER BY sp.last_scanned_at DESC, sp.host ASC
        LIMIT $1 OFFSET $2
        "
    );
    let records = sql_query(query)
        .bind::<BigInt, _>(pagination.limit)
        .bind::<BigInt, _>(pagination.offset)
        .load::<SiteProfileListRow>(conn)
        .context("error loading site profiles")?
        .into_iter()
        .map(site_profile_record_from_row)
        .collect::<Vec<_>>();
    let items = build_site_profile_summaries(conn, &records)?;

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn create_work_unit(conn: &mut PgConnection, url: &str) -> Result<()> {
    let normalized_url = normalize_crawl_url(url);
    let work_unit = NewUnit {
        url: &normalized_url,
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

pub fn create_work_unit_unless_blacklisted(
    conn: &mut PgConnection,
    url: &str,
    blacklist_domains: &[String],
) -> Result<WorkQueueOutcome> {
    let normalized_url = normalize_crawl_url(url);
    if url_matches_blacklist(&normalized_url, blacklist_domains) {
        return Ok(WorkQueueOutcome::SkippedBlacklisted);
    }

    create_work_unit(conn, &normalized_url)?;
    Ok(WorkQueueOutcome::Queued)
}

pub fn requeue_work_unit(conn: &mut PgConnection, url: &str) -> Result<()> {
    use crate::schema::work_unit::dsl as work_unit_dsl;

    let normalized_url = normalize_crawl_url(url);
    let work_unit = NewUnit {
        url: &normalized_url,
        status: STATUS_PENDING,
    };

    diesel::insert_into(crate::schema::work_unit::table)
        .values(work_unit)
        .on_conflict(work_unit_dsl::url)
        .do_update()
        .set((
            work_unit_dsl::status.eq(STATUS_PENDING),
            work_unit_dsl::retry_count.eq(0),
            work_unit_dsl::next_attempt_at.eq(sql::<Text>(sql_current_timestamp_expr(conn))),
            work_unit_dsl::last_attempt_at.eq::<Option<String>>(None),
            work_unit_dsl::last_error.eq::<Option<String>>(None),
        ))
        .execute(conn)
        .context("error requeueing work unit")?;

    Ok(())
}

pub fn queue_known_pages_for_rescan(
    conn: &mut PgConnection,
    requested_limit: Option<i64>,
    onion_only: bool,
) -> Result<usize> {
    use crate::schema::page::dsl as page_dsl;

    let limit = requested_limit.map(|value| value.max(0) as usize);
    let urls = page_dsl::page
        .order(page_dsl::last_scanned_at.asc())
        .then_order_by(page_dsl::id.asc())
        .select(page_dsl::url)
        .load::<String>(conn)
        .context("error loading known page URLs for rescan")?;
    let blacklist_domains = load_blacklist_domains(conn)?;

    let mut queued_count = 0usize;
    for page_url in urls {
        let host = host_from_url(&page_url);
        if onion_only && !host.ends_with(".onion") {
            continue;
        }
        if host_matches_blacklist(&host, &blacklist_domains) {
            continue;
        }
        if limit.is_some_and(|max_count| queued_count >= max_count) {
            break;
        }
        requeue_work_unit(conn, &page_url)?;
        queued_count += 1;
    }

    Ok(queued_count)
}

fn normalize_link_observations(links: &[LinkObservation]) -> Vec<LinkObservation> {
    let mut normalized = links
        .iter()
        .map(|link| {
            let target_url = normalize_crawl_url(&link.target_url);
            let target_host = Url::parse(&target_url)
                .ok()
                .and_then(|parsed| parsed.host_str().map(|host| host.to_ascii_lowercase()))
                .unwrap_or_else(|| link.target_host.to_ascii_lowercase());

            LinkObservation {
                target_url,
                target_host,
            }
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    normalized.sort_by(|left, right| {
        left.target_url
            .cmp(&right.target_url)
            .then_with(|| left.target_host.cmp(&right.target_host))
    });
    normalized
}

fn normalize_page_snapshot(snapshot: &PageSnapshot) -> PageSnapshot {
    let mut normalized = snapshot.clone();
    normalized.url = normalize_crawl_url(&normalized.url);
    normalized.links = normalize_link_observations(&normalized.links);
    normalized.language_detection = normalize_language_detection(&normalized);
    if normalized.language.trim().is_empty() {
        normalized.language = normalized.language_detection.name.clone();
    }
    normalized.topic_observations = normalize_topic_observations(&normalized.topic_observations);
    normalized
}

fn normalize_language_detection(snapshot: &PageSnapshot) -> LanguageDetection {
    let mut detection = snapshot.language_detection.clone();
    detection.code = detection.code.trim().to_ascii_lowercase();
    detection.name = detection.name.trim().to_string();
    detection.source = detection.source.trim().to_string();
    detection.evidence = detection.evidence.trim().to_string();
    detection.confidence = detection.confidence.clamp(0, 100);

    if detection.name.is_empty() || detection.name == "Unknown" {
        let legacy_language = snapshot.language.trim();
        if !legacy_language.is_empty() && legacy_language != "Unknown" {
            detection.name = legacy_language.to_string();
            detection.confidence = detection.confidence.max(40);
            detection.source = "legacy-page-language".to_string();
            detection.evidence = format!("legacy-page-language:{legacy_language}");
        }
    }
    if detection.name.is_empty() {
        detection.name = "Unknown".to_string();
    }
    if detection.source.is_empty() {
        detection.source = "none".to_string();
    }
    if detection.evidence.is_empty() {
        detection.evidence = "signals:insufficient".to_string();
    }

    detection
}

fn normalize_topic_observations(topics: &[TopicObservation]) -> Vec<TopicObservation> {
    let mut by_topic = HashMap::<String, TopicObservation>::new();
    for topic in topics {
        let normalized_topic = topic.topic.trim().to_ascii_lowercase();
        if normalized_topic.is_empty() {
            continue;
        }
        let score = topic.score.clamp(0, 100);
        let confidence = match topic.confidence.trim().to_ascii_lowercase().as_str() {
            CONFIDENCE_HIGH => CONFIDENCE_HIGH.to_string(),
            CONFIDENCE_MEDIUM => CONFIDENCE_MEDIUM.to_string(),
            CONFIDENCE_LOW => CONFIDENCE_LOW.to_string(),
            _ if score >= 18 => CONFIDENCE_HIGH.to_string(),
            _ if score >= 9 => CONFIDENCE_MEDIUM.to_string(),
            _ => CONFIDENCE_LOW.to_string(),
        };
        let entry = by_topic
            .entry(normalized_topic.clone())
            .or_insert_with(|| TopicObservation {
                topic: normalized_topic.clone(),
                score: 0,
                confidence: CONFIDENCE_LOW.to_string(),
                evidence: Vec::new(),
            });
        entry.score = entry.score.max(score);
        entry.confidence = topic_confidence_for_score(entry.score, &confidence);
        for evidence in topic.evidence.iter().map(|value| value.trim()) {
            if !evidence.is_empty() && !entry.evidence.iter().any(|value| value == evidence) {
                entry.evidence.push(evidence.to_string());
            }
        }
        entry.evidence.truncate(8);
    }

    let mut normalized = by_topic.into_values().collect::<Vec<_>>();
    normalized.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.topic.cmp(&right.topic))
    });
    normalized.truncate(8);
    normalized
}

fn add_history_classification_signals(
    conn: &mut PgConnection,
    stored_page_id: i32,
    signals: &mut ClassificationSignals,
) -> Result<()> {
    use crate::schema::page_scan::dsl as scan_dsl;

    let recent_titles = scan_dsl::page_scan
        .filter(scan_dsl::page_id.eq(stored_page_id))
        .order(scan_dsl::id.desc())
        .limit(8)
        .select(scan_dsl::title)
        .load::<String>(conn)
        .context("error loading recent scan titles for classification")?;
    let distinct_title_count = recent_titles
        .iter()
        .filter_map(|title| normalized_churn_title(title))
        .collect::<HashSet<_>>()
        .len();

    if distinct_title_count >= 4 {
        push_classification_hint(
            signals,
            CATEGORY_SEO_SPAM,
            format!("title:randomized-across-scans:{distinct_title_count}"),
            3,
        );
    } else if distinct_title_count >= 3 {
        push_classification_hint(
            signals,
            CATEGORY_SEO_SPAM,
            format!("title:varies-across-scans:{distinct_title_count}"),
            2,
        );
    }

    Ok(())
}

fn normalized_churn_title(title: &str) -> Option<String> {
    let normalized = title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase();
    (!normalized.is_empty() && normalized != "no title" && normalized.len() >= 4)
        .then_some(normalized)
}

fn push_classification_hint(
    signals: &mut ClassificationSignals,
    category: &str,
    evidence: String,
    weight: i32,
) {
    if signals
        .hints
        .iter()
        .any(|hint| hint.category == category && hint.evidence == evidence)
    {
        return;
    }

    signals.hints.push(CategoryHint {
        category: category.to_string(),
        evidence,
        weight,
    });
}

fn topic_confidence_for_score(score: i32, fallback: &str) -> String {
    if score >= 18 {
        CONFIDENCE_HIGH.to_string()
    } else if score >= 9 {
        CONFIDENCE_MEDIUM.to_string()
    } else if matches!(
        fallback,
        CONFIDENCE_HIGH | CONFIDENCE_MEDIUM | CONFIDENCE_LOW
    ) {
        fallback.to_string()
    } else {
        CONFIDENCE_LOW.to_string()
    }
}

fn compute_page_keyword_tags(snapshot: &PageSnapshot, rules: &[ForumKeywordRule]) -> Vec<String> {
    let haystack = snapshot.keyword_corpus.to_ascii_lowercase();
    let mut tags = rules
        .iter()
        .filter_map(|rule| {
            haystack
                .contains(&rule.pattern)
                .then(|| format!("keyword:{}", rule.label))
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    tags.sort();
    tags
}

pub fn save_page_info(conn: &mut PgConnection, snapshot: &PageSnapshot) -> Result<PageSaveOutcome> {
    use crate::schema::page::dsl::{
        coins as page_coins, emails as page_emails, language as page_language,
        last_scanned_at as page_last_scanned_at, links as page_links, title as page_title,
        url as page_url,
    };
    use crate::schema::{
        page_classification, page_crypto, page_email, page_keyword_tag, page_language_detection,
        page_link, page_scan, page_scan_crypto, page_scan_email, page_scan_link, page_topic_tag,
        site_profile,
    };
    let mut snapshot = normalize_page_snapshot(snapshot);
    let page_host = host_from_url(&snapshot.url);
    let blacklist_domains = load_blacklist_domains(conn)?;
    if host_matches_blacklist(&page_host, &blacklist_domains) {
        return Ok(PageSaveOutcome::SkippedBlacklisted);
    }

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
                page_last_scanned_at.eq(sql::<Text>(sql_current_timestamp_expr(conn))),
            ))
            .execute(conn)
            .context("error saving page")?;

        let stored_page_id = crate::schema::page::table
            .filter(page_url.eq(&snapshot.url))
            .select(crate::schema::page::id)
            .first::<i32>(conn)
            .context("error loading saved page id")?;

        let forum_keyword_tags =
            compute_page_keyword_tags(&snapshot, &list_forum_keyword_rules(conn)?);
        let existing_keyword_tag_timestamps = page_keyword_tag::table
            .filter(page_keyword_tag::page_id.eq(stored_page_id))
            .select(PageKeywordTag::as_select())
            .load::<PageKeywordTag>(conn)
            .context("error loading existing page keyword tags")?
            .into_iter()
            .map(|row| (row.tag, row.created_at))
            .collect::<HashMap<_, _>>();
        let current_timestamp = current_timestamp_text(conn)?;

        diesel::insert_into(page_language_detection::table)
            .values(NewPageLanguageDetection {
                page_id: stored_page_id,
                language_code: snapshot.language_detection.code.clone(),
                language_name: snapshot.language_detection.name.clone(),
                confidence: snapshot.language_detection.confidence,
                source: snapshot.language_detection.source.clone(),
                evidence: snapshot.language_detection.evidence.clone(),
            })
            .on_conflict(page_language_detection::page_id)
            .do_update()
            .set((
                page_language_detection::language_code
                    .eq(excluded(page_language_detection::language_code)),
                page_language_detection::language_name
                    .eq(excluded(page_language_detection::language_name)),
                page_language_detection::confidence
                    .eq(excluded(page_language_detection::confidence)),
                page_language_detection::source.eq(excluded(page_language_detection::source)),
                page_language_detection::evidence.eq(excluded(page_language_detection::evidence)),
                page_language_detection::updated_at
                    .eq(sql::<Text>(sql_current_timestamp_expr(conn))),
            ))
            .execute(conn)
            .context("error saving page language detection")?;

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
        diesel::delete(
            page_keyword_tag::table.filter(page_keyword_tag::page_id.eq(stored_page_id)),
        )
        .execute(conn)
        .context("error clearing saved page keyword tags")?;
        diesel::delete(page_topic_tag::table.filter(page_topic_tag::page_id.eq(stored_page_id)))
            .execute(conn)
            .context("error clearing saved page topic tags")?;

        let link_rows = snapshot
            .links
            .iter()
            .map(|item| NewPageLink {
                source_page_id: stored_page_id,
                source_host: page_host.clone(),
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

        let keyword_tag_rows = forum_keyword_tags
            .iter()
            .map(|tag| NewPageKeywordTag {
                page_id: stored_page_id,
                tag: tag.clone(),
                created_at: existing_keyword_tag_timestamps
                    .get(tag)
                    .cloned()
                    .unwrap_or_else(|| current_timestamp.clone()),
            })
            .collect::<Vec<_>>();
        if !keyword_tag_rows.is_empty() {
            diesel::insert_into(page_keyword_tag::table)
                .values(&keyword_tag_rows)
                .execute(conn)
                .context("error saving page keyword tags")?;
        }

        let topic_tag_rows = snapshot
            .topic_observations
            .iter()
            .map(|topic| NewPageTopicTag {
                page_id: stored_page_id,
                topic: topic.topic.clone(),
                score: topic.score,
                confidence: topic.confidence.clone(),
                evidence: serialize_evidence(&topic.evidence),
            })
            .collect::<Vec<_>>();
        if !topic_tag_rows.is_empty() {
            diesel::insert_into(page_topic_tag::table)
                .values(&topic_tag_rows)
                .execute(conn)
                .context("error saving page topic tags")?;
        }

        add_history_classification_signals(
            conn,
            stored_page_id,
            &mut snapshot.classification_signals,
        )?;

        let classification = classify_page_snapshot(&snapshot);
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
                    page_classification::last_classified_at
                        .eq(sql::<Text>(sql_current_timestamp_expr(conn))),
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
                    first_found_at: site_profile_record.first_found_at.clone(),
                    last_scanned_at: site_profile_record.last_scanned_at.clone(),
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
                    site_profile::last_scanned_at.eq(excluded(site_profile::last_scanned_at)),
                    site_profile::evidence.eq(excluded(site_profile::evidence)),
                    site_profile::source_page_id.eq(excluded(site_profile::source_page_id)),
                    site_profile::last_classified_at
                        .eq(sql::<Text>(sql_current_timestamp_expr(conn))),
                ))
                .execute(conn)
                .context("error saving site profile")?;
            apply_auto_blacklist_rules_for_page(
                conn,
                &snapshot,
                stored_page_id,
                &site_profile_record,
            )?;
            if host_matches_blacklist(&page_host, &load_blacklist_domains(conn)?) {
                delete_page_and_empty_site_profile(conn, stored_page_id, &page_host)?;
                return Ok(PageSaveOutcome::PurgedAfterAutoBlacklist);
            }
        }

        Ok(PageSaveOutcome::Stored)
    })
}

fn delete_page_and_empty_site_profile(
    conn: &mut PgConnection,
    page_id: i32,
    host: &str,
) -> Result<()> {
    use crate::schema::page::dsl as page_dsl;
    use crate::schema::site_profile::dsl as site_profile_dsl;

    diesel::delete(page_dsl::page.filter(page_dsl::id.eq(page_id)))
        .execute(conn)
        .context("error deleting blacklisted page")?;

    if !host.trim().is_empty() && page_count_for_host(conn, host)? == 0 {
        diesel::delete(site_profile_dsl::site_profile.filter(site_profile_dsl::host.eq(host)))
            .execute(conn)
            .context("error deleting empty blacklisted site profile")?;
    }

    Ok(())
}

fn page_count_for_host(conn: &mut PgConnection, host: &str) -> Result<i64> {
    let host_expr = sql_host_without_port_expr("p.url", conn);
    let query = format!(
        "
        SELECT COUNT(*) AS count
        FROM page p
        WHERE {host_expr} = $1
        "
    );
    Ok(sql_query(query)
        .bind::<Text, _>(host)
        .get_result::<CountRow>(conn)
        .context("error counting pages for host")?
        .count)
}

pub fn mark_work_unit_as_done(conn: &mut PgConnection, work_unit_id: i32) -> Result<()> {
    use crate::schema::work_unit::dsl::*;

    diesel::update(crate::schema::work_unit::table.filter(id.eq(work_unit_id)))
        .set((
            status.eq(STATUS_DONE),
            next_attempt_at.eq(sql::<Text>(sql_current_timestamp_expr(conn))),
            last_attempt_at.eq(sql::<Nullable<Text>>(sql_current_timestamp_expr(conn))),
            last_error.eq::<Option<String>>(None),
        ))
        .execute(conn)
        .context("error updating work unit status")?;

    Ok(())
}

pub fn record_work_unit_failure(
    conn: &mut PgConnection,
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
                last_attempt_at.eq(sql::<Nullable<Text>>(sql_current_timestamp_expr(conn))),
                next_attempt_at.eq(sql::<Text>(&sql_timestamp_plus_minutes_expr(
                    conn,
                    backoff_minutes,
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
                last_attempt_at.eq(sql::<Nullable<Text>>(sql_current_timestamp_expr(conn))),
            ))
            .execute(conn)
            .context("error marking work unit as failed")?;
    }

    Ok(())
}

pub fn get_pending_work_units(conn: &mut PgConnection) -> Result<Vec<WorkUnit>> {
    use crate::schema::work_unit::dsl::*;

    crate::schema::work_unit::table
        .filter(status.eq(STATUS_PENDING))
        .filter(sql::<Bool>(&sql_now_comparison_expr(
            "next_attempt_at",
            conn,
        )))
        .order(next_attempt_at.asc())
        .then_order_by(id.asc())
        .select(WorkUnit::as_select())
        .load(conn)
        .context("error querying pending work units")
}

pub fn list_work_units(
    conn: &mut PgConnection,
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
    conn: &mut PgConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<PageSummary>> {
    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = scalar_count(
        conn,
        "
            SELECT COALESCE(SUM(sp.page_count), 0) AS count
            FROM site_profile sp
            ",
    )
    .context("error counting pages for summary")?;
    let host_expr = sql_host_expr("selected_pages.url", conn);
    let query = format!(
        "
        WITH selected_pages AS (
            SELECT
                p.id,
                p.title,
                p.url,
                p.language,
                p.last_scanned_at
            FROM page p
            ORDER BY p.last_scanned_at DESC, p.id DESC
            LIMIT $1 OFFSET $2
        )
        SELECT
            selected_pages.id,
            selected_pages.title,
            selected_pages.url,
            {host_expr} AS host,
            selected_pages.language,
            selected_pages.last_scanned_at,
            COALESCE(link_counts.outbound_link_count, 0) AS outbound_link_count,
            COALESCE(email_counts.email_count, 0) AS email_count,
            COALESCE(crypto_counts.crypto_count, 0) AS crypto_count
        FROM selected_pages
        LEFT JOIN (
            SELECT
                pl.source_page_id AS page_id,
                COUNT(*) AS outbound_link_count
            FROM page_link pl
            JOIN selected_pages sp ON sp.id = pl.source_page_id
            GROUP BY pl.source_page_id
        ) AS link_counts ON link_counts.page_id = selected_pages.id
        LEFT JOIN (
            SELECT
                pe.page_id,
                COUNT(*) AS email_count
            FROM page_email pe
            JOIN selected_pages sp ON sp.id = pe.page_id
            GROUP BY pe.page_id
        ) AS email_counts ON email_counts.page_id = selected_pages.id
        LEFT JOIN (
            SELECT
                pc.page_id,
                COUNT(*) AS crypto_count
            FROM page_crypto pc
            JOIN selected_pages sp ON sp.id = pc.page_id
            GROUP BY pc.page_id
        ) AS crypto_counts ON crypto_counts.page_id = selected_pages.id
        ORDER BY selected_pages.last_scanned_at DESC, selected_pages.id DESC
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

pub fn get_page_detail(conn: &mut PgConnection, page_id: i32) -> Result<Option<PageDetail>> {
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
    let host_http_observation = match endpoint_from_url(&page.url) {
        Some(endpoint) => {
            get_host_http_observation_detail(conn, &endpoint.host, &endpoint.scheme, endpoint.port)?
        }
        None => None,
    };
    let language_detection = load_page_language_detection_summary(conn, page.id)?;
    let topic_tags = load_page_topic_summaries(conn, page.id)?;
    let intel_leads = load_active_lead_badges_for_sources(
        conn,
        &[
            source_ref("page", page.id, page.url.clone()),
            source_ref("site", 0, page_host.clone()),
        ],
    )?;

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
        language_detection,
        topic_tags,
        site_profile,
        host_http_observation,
        intel_leads,
    }))
}

fn load_page_language_detection_summary(
    conn: &mut PgConnection,
    page_id_value: i32,
) -> Result<Option<LanguageDetectionSummary>> {
    use crate::schema::page_language_detection::dsl as language_dsl;

    language_dsl::page_language_detection
        .filter(language_dsl::page_id.eq(page_id_value))
        .select(PageLanguageDetectionRecord::as_select())
        .first::<PageLanguageDetectionRecord>(conn)
        .optional()
        .context("error loading page language detection")
        .map(|record| {
            record.map(|record| LanguageDetectionSummary {
                language_code: record.language_code,
                language_name: record.language_name,
                confidence: record.confidence,
                source: record.source,
                evidence: record.evidence,
                updated_at: record.updated_at,
            })
        })
}

fn load_page_topic_summaries(
    conn: &mut PgConnection,
    page_id_value: i32,
) -> Result<Vec<PageTopicSummary>> {
    use crate::schema::page_topic_tag::dsl as topic_dsl;

    Ok(topic_dsl::page_topic_tag
        .filter(topic_dsl::page_id.eq(page_id_value))
        .order(topic_dsl::score.desc())
        .then_order_by(topic_dsl::topic.asc())
        .select(PageTopicTagRecord::as_select())
        .load::<PageTopicTagRecord>(conn)
        .context("error loading page topic tags")?
        .into_iter()
        .map(|record| PageTopicSummary {
            label: page_topic_label(&record.topic),
            topic: record.topic,
            score: record.score,
            confidence: record.confidence,
            evidence: deserialize_evidence(&record.evidence),
        })
        .collect())
}

pub fn list_page_scan_summaries(
    conn: &mut PgConnection,
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
        WHERE ps.page_id = $1
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
    conn: &mut PgConnection,
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

pub fn collect_stats(conn: &mut PgConnection) -> Result<Stats> {
    use crate::schema::work_unit::dsl as work_dsl;

    let total_pages = scalar_count(
        conn,
        "SELECT COALESCE(SUM(page_count), 0) AS count FROM site_profile",
    )
    .context("error counting pages from site profiles")?;
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

    let total_domains = scalar_count(conn, "SELECT COUNT(*) AS count FROM site_profile")
        .context("error counting site profiles")?;
    let last_scrape = scalar_nullable_text(
        conn,
        "SELECT MAX(last_scanned_at) AS value FROM site_profile",
    )
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

pub fn severity_for_intel_score(score: i32) -> &'static str {
    match score.clamp(0, 100) {
        90..=100 => LEAD_SEVERITY_CRITICAL,
        70..=89 => LEAD_SEVERITY_HIGH,
        40..=69 => LEAD_SEVERITY_MEDIUM,
        1..=39 => LEAD_SEVERITY_LOW,
        _ => LEAD_SEVERITY_LOW,
    }
}

pub fn valid_intel_lead_statuses() -> [&'static str; 4] {
    [
        LEAD_STATUS_NEW,
        LEAD_STATUS_TRIAGED,
        LEAD_STATUS_MONITORING,
        LEAD_STATUS_SUPPRESSED,
    ]
}

pub fn normalize_intel_lead_status(raw_status: &str) -> Result<String> {
    let status = raw_status.trim().to_ascii_lowercase();
    anyhow::ensure!(
        valid_intel_lead_statuses().contains(&status.as_str()),
        "invalid lead status: {raw_status}"
    );
    Ok(status)
}

pub fn list_intel_leads(
    conn: &mut PgConnection,
    status_filter: Option<&str>,
    severity_filter: Option<&str>,
    rule_filter: Option<&str>,
    entity_filter: Option<&str>,
    sort: Option<&str>,
    direction: Option<&str>,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<IntelLeadSummary>> {
    use crate::schema::intel_lead::dsl as lead_dsl;

    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_LEAD_LIMIT,
        MAX_LEAD_LIMIT,
    );
    let status_filter = normalize_optional_filter(status_filter);
    let severity_filter = normalize_optional_filter(severity_filter);
    let rule_filter = normalize_optional_filter(rule_filter);
    let entity_filter = normalize_optional_filter(entity_filter);

    let mut count_query = lead_dsl::intel_lead.into_boxed::<diesel::pg::Pg>();
    count_query = apply_lead_filters(
        count_query,
        status_filter.as_deref(),
        severity_filter.as_deref(),
        rule_filter.as_deref(),
        entity_filter.as_deref(),
    );
    let total_count = count_query
        .select(count_star())
        .first::<i64>(conn)
        .context("error counting intel leads")?;

    let mut query = lead_dsl::intel_lead.into_boxed::<diesel::pg::Pg>();
    query = apply_lead_filters(
        query,
        status_filter.as_deref(),
        severity_filter.as_deref(),
        rule_filter.as_deref(),
        entity_filter.as_deref(),
    );
    let descending = !matches!(
        direction
            .unwrap_or("desc")
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "asc"
    );
    query = match (sort.unwrap_or("last_seen"), descending) {
        ("severity", true) | ("score", true) => query
            .order(lead_dsl::score.desc())
            .then_order_by(lead_dsl::last_seen_at.desc())
            .then_order_by(lead_dsl::id.desc()),
        ("severity", false) | ("score", false) => query
            .order(lead_dsl::score.asc())
            .then_order_by(lead_dsl::last_seen_at.desc())
            .then_order_by(lead_dsl::id.desc()),
        ("confidence", true) => query
            .order(lead_dsl::confidence.desc())
            .then_order_by(lead_dsl::last_seen_at.desc())
            .then_order_by(lead_dsl::id.desc()),
        ("confidence", false) => query
            .order(lead_dsl::confidence.asc())
            .then_order_by(lead_dsl::last_seen_at.desc())
            .then_order_by(lead_dsl::id.desc()),
        ("status", true) => query
            .order(lead_dsl::status.desc())
            .then_order_by(lead_dsl::score.desc())
            .then_order_by(lead_dsl::last_seen_at.desc()),
        ("status", false) => query
            .order(lead_dsl::status.asc())
            .then_order_by(lead_dsl::score.desc())
            .then_order_by(lead_dsl::last_seen_at.desc()),
        ("rule", true) => query
            .order(lead_dsl::rule_id.desc())
            .then_order_by(lead_dsl::score.desc())
            .then_order_by(lead_dsl::last_seen_at.desc()),
        ("rule", false) => query
            .order(lead_dsl::rule_id.asc())
            .then_order_by(lead_dsl::score.desc())
            .then_order_by(lead_dsl::last_seen_at.desc()),
        ("entity", true) => query
            .order(lead_dsl::primary_entity_value.desc())
            .then_order_by(lead_dsl::score.desc())
            .then_order_by(lead_dsl::last_seen_at.desc()),
        ("entity", false) => query
            .order(lead_dsl::primary_entity_value.asc())
            .then_order_by(lead_dsl::score.desc())
            .then_order_by(lead_dsl::last_seen_at.desc()),
        ("last_seen", false) => query
            .order(lead_dsl::last_seen_at.asc())
            .then_order_by(lead_dsl::id.asc()),
        _ => query
            .order(lead_dsl::last_seen_at.desc())
            .then_order_by(lead_dsl::id.desc()),
    };

    let records = query
        .limit(pagination.limit)
        .offset(pagination.offset)
        .select(IntelLeadRecord::as_select())
        .load::<IntelLeadRecord>(conn)
        .context("error loading intel leads")?;
    let evidence_counts = load_lead_evidence_counts(
        conn,
        &records.iter().map(|record| record.id).collect::<Vec<_>>(),
    )?;
    let items = records
        .into_iter()
        .map(|record| {
            let lead_id = record.id;
            intel_lead_summary_from_record(record, *evidence_counts.get(&lead_id).unwrap_or(&0))
        })
        .collect();

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn get_intel_lead_detail(
    conn: &mut PgConnection,
    lead_id: i32,
) -> Result<Option<IntelLeadDetail>> {
    use crate::schema::intel_lead::dsl as lead_dsl;
    use crate::schema::intel_lead_evidence::dsl as evidence_dsl;

    let record = lead_dsl::intel_lead
        .filter(lead_dsl::id.eq(lead_id))
        .select(IntelLeadRecord::as_select())
        .first::<IntelLeadRecord>(conn)
        .optional()
        .context("error loading intel lead")?;
    let Some(record) = record else {
        return Ok(None);
    };

    let evidence_records = evidence_dsl::intel_lead_evidence
        .filter(evidence_dsl::lead_id.eq(record.id))
        .order(evidence_dsl::observed_at.desc())
        .then_order_by(evidence_dsl::id.desc())
        .select(IntelLeadEvidenceRecord::as_select())
        .load::<IntelLeadEvidenceRecord>(conn)
        .context("error loading intel lead evidence")?;
    let evidence_count = evidence_records.len();
    let mut page_ids = evidence_records
        .iter()
        .filter(|row| row.source_type == "page" && row.source_id > 0)
        .map(|row| row.source_id)
        .collect::<HashSet<_>>();
    let scan_ids = evidence_records
        .iter()
        .filter(|row| row.source_type == "page_scan" && row.source_id > 0)
        .map(|row| row.source_id)
        .collect::<Vec<_>>();
    for page_id in load_page_ids_for_scan_ids(conn, &scan_ids)? {
        page_ids.insert(page_id);
    }
    let mut related_pages = load_pages_by_ids(conn, &page_ids.into_iter().collect::<Vec<_>>())?
        .into_iter()
        .map(page_reference_from_page)
        .collect::<Vec<_>>();
    related_pages.sort_by(|left, right| {
        right
            .last_scanned_at
            .cmp(&left.last_scanned_at)
            .then_with(|| left.url.cmp(&right.url))
    });

    let mut related_sites = evidence_records
        .iter()
        .filter(|row| row.source_type == "site" && !row.source_key.is_empty())
        .map(|row| row.source_key.clone())
        .collect::<BTreeSet<_>>();
    if record.primary_entity_type == "site" || record.primary_entity_type == "host" {
        related_sites.insert(record.primary_entity_value.clone());
    }
    if matches!(
        record.related_entity_type.as_deref(),
        Some("site") | Some("host")
    ) {
        if let Some(value) = record.related_entity_value.as_ref() {
            related_sites.insert(value.clone());
        }
    }

    let mut related_entities = Vec::new();
    push_entity_reference(
        &mut related_entities,
        &record.primary_entity_type,
        &record.primary_entity_value,
    );
    if let (Some(entity_type), Some(entity_value)) = (
        record.related_entity_type.as_ref(),
        record.related_entity_value.as_ref(),
    ) {
        push_entity_reference(&mut related_entities, entity_type, entity_value);
    }
    for row in &evidence_records {
        if matches!(
            row.source_type.as_str(),
            "email"
                | "crypto"
                | "ssh_host_key"
                | "http_fingerprint"
                | "favicon_hash"
                | "service_fingerprint"
        ) {
            push_entity_reference(&mut related_entities, &row.source_type, &row.source_key);
        }
    }
    related_entities.sort_by(|left, right| {
        left.entity_type
            .cmp(&right.entity_type)
            .then_with(|| left.entity_value.cmp(&right.entity_value))
    });
    related_entities.dedup_by(|left, right| {
        left.entity_type == right.entity_type && left.entity_value == right.entity_value
    });

    let evidence = evidence_records
        .into_iter()
        .map(|row| {
            let source_url = intel_evidence_source_url(conn, &row)?;
            Ok(IntelLeadEvidenceView {
                id: row.id,
                source_type: row.source_type,
                source_id: row.source_id,
                source_key: row.source_key,
                evidence_text: row.evidence_text,
                observed_at: row.observed_at,
                source_url,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(IntelLeadDetail {
        lead: intel_lead_summary_from_record(record, evidence_count),
        evidence,
        related_pages,
        related_sites: related_sites.into_iter().collect(),
        related_entities,
    }))
}

pub fn update_intel_lead_status(
    conn: &mut PgConnection,
    lead_id: i32,
    raw_status: &str,
) -> Result<Option<IntelLeadDetail>> {
    use crate::schema::intel_lead::dsl as lead_dsl;

    let status = normalize_intel_lead_status(raw_status)?;
    let changed = diesel::update(lead_dsl::intel_lead.filter(lead_dsl::id.eq(lead_id)))
        .set((
            lead_dsl::status.eq(status),
            lead_dsl::updated_at.eq(sql::<Text>(sql_current_timestamp_expr(conn))),
        ))
        .execute(conn)
        .context("error updating intel lead status")?;
    if changed == 0 {
        return Ok(None);
    }
    get_intel_lead_detail(conn, lead_id)
}

pub fn suppress_intel_lead(
    conn: &mut PgConnection,
    lead_id: i32,
) -> Result<Option<IntelLeadSummary>> {
    Ok(update_intel_lead_status(conn, lead_id, LEAD_STATUS_SUPPRESSED)?.map(|detail| detail.lead))
}

pub fn page_link_batch_upper_bound(
    conn: &mut PgConnection,
    after_link_id: i32,
    batch_size: i64,
) -> Result<Option<i32>> {
    let after_link_id = after_link_id.max(0);
    let batch_size = batch_size.clamp(1, MAX_BLACKLIST_LEAD_LINK_BATCH_SIZE);
    let row = sql_query(
        "
        SELECT COALESCE(MAX(id), $1)::bigint AS count
        FROM (
            SELECT id
            FROM page_link
            WHERE id > $1
            ORDER BY id ASC
            LIMIT $2
        ) AS page_link_batch
        ",
    )
    .bind::<diesel::sql_types::Integer, _>(after_link_id)
    .bind::<BigInt, _>(batch_size)
    .get_result::<CountRow>(conn)
    .context("error loading page_link batch upper bound")?;
    if row.count <= i64::from(after_link_id) {
        Ok(None)
    } else {
        Ok(Some(row.count.min(i64::from(i32::MAX)) as i32))
    }
}

pub fn recompute_intel_leads(
    conn: &mut PgConnection,
    options: IntelLeadRecomputeOptions,
) -> Result<IntelLeadRecomputeSummary> {
    recompute_intel_leads_with_reporter(conn, options, |_| {})
}

pub fn recompute_intel_leads_with_reporter<F>(
    conn: &mut PgConnection,
    options: IntelLeadRecomputeOptions,
    mut reporter: F,
) -> Result<IntelLeadRecomputeSummary>
where
    F: FnMut(String),
{
    let rule_limit = options
        .limit
        .unwrap_or(DEFAULT_RECOMPUTE_LIMIT)
        .clamp(1, MAX_RECOMPUTE_LIMIT);
    let selected_rules = normalize_requested_lead_rules(&options.rule_ids)?;
    let mut candidate_count = 0usize;
    let mut created_count = 0usize;
    let mut updated_count = 0usize;
    let mut evidence_count = 0usize;
    let mut rule_summaries = Vec::new();

    for rule in intel_lead_rule_specs() {
        if !selected_rules.is_empty() && !selected_rules.contains(rule.rule_id) {
            continue;
        }
        reporter(format!("lead rule {}: building candidates", rule.rule_id));
        let candidates = if rule.rule_id == "blacklisted-site-link" {
            let after_link_id = options.blacklist_after_link_id.unwrap_or(0).max(0);
            let link_batch_size = options
                .blacklist_link_batch_size
                .unwrap_or(DEFAULT_BLACKLIST_LEAD_LINK_BATCH_SIZE)
                .clamp(1, MAX_BLACKLIST_LEAD_LINK_BATCH_SIZE);
            reporter(format!(
                "lead rule blacklisted-site-link: scanning page_link id > {after_link_id} limit {link_batch_size}"
            ));
            build_blacklisted_site_link_lead_candidates(
                conn,
                rule_limit,
                after_link_id,
                link_batch_size,
            )
        } else {
            (rule.builder)(conn, options.since_scan_id, rule_limit)
        }
        .with_context(|| format!("error recomputing lead rule {}", rule.rule_id))?;
        reporter(format!(
            "lead rule {}: {} candidates",
            rule.rule_id,
            candidates.len()
        ));
        let mut rule_created_count = 0usize;
        let mut rule_updated_count = 0usize;
        let mut rule_evidence_count = 0usize;
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            for candidate in &candidates {
                let result = upsert_intel_lead_candidate(conn, candidate)?;
                if result.created {
                    rule_created_count += 1;
                } else {
                    rule_updated_count += 1;
                }
                rule_evidence_count += result.evidence_count;
            }
            Ok(())
        })?;
        reporter(format!(
            "lead rule {}: {} created, {} updated, {} evidence rows touched",
            rule.rule_id, rule_created_count, rule_updated_count, rule_evidence_count
        ));
        candidate_count += candidates.len();
        created_count += rule_created_count;
        updated_count += rule_updated_count;
        evidence_count += rule_evidence_count;
        rule_summaries.push(IntelLeadRuleRecomputeSummary {
            rule_id: rule.rule_id.to_string(),
            candidate_count: candidates.len(),
            created_count: rule_created_count,
            updated_count: rule_updated_count,
            evidence_count: rule_evidence_count,
        });
    }

    Ok(IntelLeadRecomputeSummary {
        candidate_count,
        created_count,
        updated_count,
        evidence_count,
        rule_summaries,
    })
}

fn normalize_optional_filter(value: Option<&str>) -> Option<String> {
    value
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
}

struct IntelLeadRuleSpec {
    rule_id: &'static str,
    builder: LeadCandidateBuilder,
}

fn intel_lead_rule_specs() -> Vec<IntelLeadRuleSpec> {
    vec![
        IntelLeadRuleSpec {
            rule_id: "shared-ssh-host-key",
            builder: build_shared_ssh_lead_candidates_for_rule,
        },
        IntelLeadRuleSpec {
            rule_id: "shared-service-banner",
            builder: build_shared_service_fingerprint_lead_candidates_for_rule,
        },
        IntelLeadRuleSpec {
            rule_id: "shared-http-header-fingerprint",
            builder: build_shared_http_header_fingerprint_lead_candidates_for_rule,
        },
        IntelLeadRuleSpec {
            rule_id: "shared-favicon-hash",
            builder: build_shared_favicon_hash_lead_candidates_for_rule,
        },
        IntelLeadRuleSpec {
            rule_id: "duplicate-site-title",
            builder: build_duplicate_site_title_lead_candidates,
        },
        IntelLeadRuleSpec {
            rule_id: "scan-new-observations",
            builder: build_scan_update_lead_candidates,
        },
        IntelLeadRuleSpec {
            rule_id: "blacklisted-site-link",
            builder: build_blacklisted_site_link_lead_candidates_for_rule,
        },
        IntelLeadRuleSpec {
            rule_id: "host-category-change",
            builder: build_category_change_lead_candidates_for_rule,
        },
        IntelLeadRuleSpec {
            rule_id: "high-degree-target",
            builder: build_high_degree_target_lead_candidates,
        },
        IntelLeadRuleSpec {
            rule_id: "watchlist-match",
            builder: build_watchlist_lead_candidates,
        },
        IntelLeadRuleSpec {
            rule_id: "shared-email",
            builder: build_shared_email_lead_candidates,
        },
        IntelLeadRuleSpec {
            rule_id: "shared-crypto",
            builder: build_shared_crypto_lead_candidates,
        },
    ]
}

pub fn intel_lead_rule_ids() -> Vec<&'static str> {
    intel_lead_rule_specs()
        .into_iter()
        .map(|rule| rule.rule_id)
        .collect()
}

fn normalize_requested_lead_rules(raw_rule_ids: &[String]) -> Result<HashSet<String>> {
    let known_rules = intel_lead_rule_specs()
        .into_iter()
        .map(|rule| rule.rule_id)
        .collect::<HashSet<_>>();
    let mut requested = HashSet::new();
    for raw_rule_id in raw_rule_ids {
        let rule_id = raw_rule_id.trim();
        if rule_id.is_empty() {
            continue;
        }
        anyhow::ensure!(
            known_rules.contains(rule_id),
            "unknown lead rule {rule_id}; known rules: {}",
            known_rules.iter().copied().collect::<Vec<_>>().join(", ")
        );
        requested.insert(rule_id.to_string());
    }
    Ok(requested)
}

fn build_shared_ssh_lead_candidates_for_rule(
    conn: &mut PgConnection,
    _since_scan_id: Option<i32>,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    build_shared_ssh_lead_candidates(conn, rule_limit)
}

fn build_shared_service_fingerprint_lead_candidates_for_rule(
    conn: &mut PgConnection,
    _since_scan_id: Option<i32>,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    build_shared_service_fingerprint_lead_candidates(conn, rule_limit)
}

fn build_shared_http_header_fingerprint_lead_candidates_for_rule(
    conn: &mut PgConnection,
    _since_scan_id: Option<i32>,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    build_shared_http_fingerprint_lead_candidates(
        conn,
        "header_fingerprint",
        "shared-http-header-fingerprint",
        "http_fingerprint",
        "HTTP header fingerprint",
        rule_limit,
    )
}

fn build_shared_favicon_hash_lead_candidates_for_rule(
    conn: &mut PgConnection,
    _since_scan_id: Option<i32>,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    build_shared_http_fingerprint_lead_candidates(
        conn,
        "favicon_hash",
        "shared-favicon-hash",
        "favicon_hash",
        "favicon hash",
        rule_limit,
    )
}

fn build_blacklisted_site_link_lead_candidates_for_rule(
    conn: &mut PgConnection,
    _since_scan_id: Option<i32>,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    build_blacklisted_site_link_lead_candidates(
        conn,
        rule_limit,
        0,
        DEFAULT_BLACKLIST_LEAD_LINK_BATCH_SIZE,
    )
}

fn build_category_change_lead_candidates_for_rule(
    conn: &mut PgConnection,
    _since_scan_id: Option<i32>,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    build_category_change_lead_candidates(conn, rule_limit)
}

fn apply_lead_filters<'a>(
    mut query: crate::schema::intel_lead::BoxedQuery<'a, diesel::pg::Pg>,
    status_filter: Option<&str>,
    severity_filter: Option<&str>,
    rule_filter: Option<&str>,
    entity_filter: Option<&str>,
) -> crate::schema::intel_lead::BoxedQuery<'a, diesel::pg::Pg> {
    if let Some(status_filter) = status_filter {
        query = query.filter(sql::<Bool>(&format!(
            "status = {}",
            quote_sql_text_literal(status_filter)
        )));
    }
    if let Some(severity_filter) = severity_filter {
        query = query.filter(sql::<Bool>(&format!(
            "severity = {}",
            quote_sql_text_literal(severity_filter)
        )));
    }
    if let Some(rule_filter) = rule_filter {
        query = query.filter(sql::<Bool>(&format!(
            "rule_id = {}",
            quote_sql_text_literal(rule_filter)
        )));
    }
    if let Some(entity_filter) = entity_filter {
        let pattern = quote_sql_text_literal(&format!("%{}%", escape_like(entity_filter)));
        query = query.filter(sql::<Bool>(&format!(
            "(lead_key LIKE {pattern} ESCAPE '\\' \
              OR title LIKE {pattern} ESCAPE '\\' \
              OR summary LIKE {pattern} ESCAPE '\\' \
              OR primary_entity_value LIKE {pattern} ESCAPE '\\' \
              OR COALESCE(related_entity_value, '') LIKE {pattern} ESCAPE '\\')"
        )));
    }
    query
}

fn load_lead_evidence_counts(
    conn: &mut PgConnection,
    lead_ids: &[i32],
) -> Result<HashMap<i32, usize>> {
    if lead_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = crate::schema::intel_lead_evidence::table
        .filter(crate::schema::intel_lead_evidence::lead_id.eq_any(lead_ids))
        .group_by(crate::schema::intel_lead_evidence::lead_id)
        .select((crate::schema::intel_lead_evidence::lead_id, count_star()))
        .load::<(i32, i64)>(conn)
        .context("error loading intel lead evidence counts")?;
    Ok(rows
        .into_iter()
        .map(|(lead_id, count)| (lead_id, count.max(0) as usize))
        .collect())
}

fn intel_lead_summary_from_record(
    record: IntelLeadRecord,
    evidence_count: usize,
) -> IntelLeadSummary {
    IntelLeadSummary {
        id: record.id,
        rule_id: record.rule_id,
        lead_key: record.lead_key,
        title: record.title,
        summary: record.summary,
        severity: record.severity,
        confidence: record.confidence,
        score: record.score,
        status: record.status,
        primary_entity_type: record.primary_entity_type,
        primary_entity_value: record.primary_entity_value,
        related_entity_type: record.related_entity_type,
        related_entity_value: record.related_entity_value,
        first_seen_at: record.first_seen_at,
        last_seen_at: record.last_seen_at,
        updated_at: record.updated_at,
        evidence_count,
        detail_url: build_intel_lead_detail_url(record.id),
    }
}

fn intel_lead_badge_from_record(record: IntelLeadRecord) -> IntelLeadBadge {
    IntelLeadBadge {
        id: record.id,
        rule_id: record.rule_id,
        title: record.title,
        severity: record.severity,
        confidence: record.confidence,
        score: record.score,
        status: record.status,
        detail_url: build_intel_lead_detail_url(record.id),
    }
}

struct IntelLeadUpsertOutcome {
    created: bool,
    evidence_count: usize,
}

fn upsert_intel_lead_candidate(
    conn: &mut PgConnection,
    candidate: &IntelLeadCandidate,
) -> Result<IntelLeadUpsertOutcome> {
    use crate::schema::intel_lead::dsl as lead_dsl;
    use crate::schema::intel_lead_evidence::dsl as evidence_dsl;

    let existing = lead_dsl::intel_lead
        .filter(lead_dsl::lead_key.eq(&candidate.lead_key))
        .select(IntelLeadRecord::as_select())
        .first::<IntelLeadRecord>(conn)
        .optional()
        .context("error loading existing intel lead")?;
    let severity = severity_for_intel_score(candidate.score).to_string();
    let status = match existing.as_ref().map(|record| record.status.as_str()) {
        Some(LEAD_STATUS_SUPPRESSED) if severity == LEAD_SEVERITY_CRITICAL => {
            LEAD_STATUS_NEW.to_string()
        }
        Some(existing_status) => existing_status.to_string(),
        None => LEAD_STATUS_NEW.to_string(),
    };
    let first_seen_at = existing
        .as_ref()
        .map(|record| record.first_seen_at.clone())
        .unwrap_or_else(|| candidate.first_seen_at.clone());
    let lead = NewIntelLead {
        rule_id: candidate.rule_id.clone(),
        lead_key: candidate.lead_key.clone(),
        title: truncate(&candidate.title, 240),
        summary: truncate(&candidate.summary, 1000),
        severity,
        confidence: candidate.confidence.clamp(0, 100),
        score: candidate.score.clamp(0, 100),
        status,
        primary_entity_type: candidate.primary_entity_type.clone(),
        primary_entity_value: candidate.primary_entity_value.clone(),
        related_entity_type: candidate.related_entity_type.clone(),
        related_entity_value: candidate.related_entity_value.clone(),
        first_seen_at,
        last_seen_at: candidate.last_seen_at.clone(),
    };

    diesel::insert_into(lead_dsl::intel_lead)
        .values(&lead)
        .on_conflict(lead_dsl::lead_key)
        .do_update()
        .set((
            lead_dsl::rule_id.eq(excluded(lead_dsl::rule_id)),
            lead_dsl::title.eq(excluded(lead_dsl::title)),
            lead_dsl::summary.eq(excluded(lead_dsl::summary)),
            lead_dsl::severity.eq(excluded(lead_dsl::severity)),
            lead_dsl::confidence.eq(excluded(lead_dsl::confidence)),
            lead_dsl::score.eq(excluded(lead_dsl::score)),
            lead_dsl::status.eq(excluded(lead_dsl::status)),
            lead_dsl::primary_entity_type.eq(excluded(lead_dsl::primary_entity_type)),
            lead_dsl::primary_entity_value.eq(excluded(lead_dsl::primary_entity_value)),
            lead_dsl::related_entity_type.eq(excluded(lead_dsl::related_entity_type)),
            lead_dsl::related_entity_value.eq(excluded(lead_dsl::related_entity_value)),
            lead_dsl::last_seen_at.eq(excluded(lead_dsl::last_seen_at)),
            lead_dsl::updated_at.eq(sql::<Text>(sql_current_timestamp_expr(conn))),
        ))
        .execute(conn)
        .context("error upserting intel lead")?;

    let stored_lead = lead_dsl::intel_lead
        .filter(lead_dsl::lead_key.eq(&candidate.lead_key))
        .select(IntelLeadRecord::as_select())
        .first::<IntelLeadRecord>(conn)
        .context("error loading upserted intel lead")?;
    let evidence_rows = candidate
        .evidence
        .iter()
        .map(|evidence| NewIntelLeadEvidence {
            lead_id: stored_lead.id,
            source_type: evidence.source_type.clone(),
            source_id: evidence.source_id,
            source_key: truncate(&evidence.source_key, 500),
            evidence_text: truncate(&evidence.evidence_text, 1000),
            observed_at: evidence.observed_at.clone(),
        })
        .collect::<Vec<_>>();
    if !evidence_rows.is_empty() {
        diesel::insert_into(evidence_dsl::intel_lead_evidence)
            .values(&evidence_rows)
            .on_conflict((
                evidence_dsl::lead_id,
                evidence_dsl::source_type,
                evidence_dsl::source_id,
                evidence_dsl::source_key,
                evidence_dsl::evidence_text,
            ))
            .do_update()
            .set(evidence_dsl::observed_at.eq(excluded(evidence_dsl::observed_at)))
            .execute(conn)
            .context("error upserting intel lead evidence")?;
    }

    Ok(IntelLeadUpsertOutcome {
        created: existing.is_none(),
        evidence_count: evidence_rows.len(),
    })
}

fn evidence_candidate(
    source_type: &str,
    source_id: i32,
    source_key: impl Into<String>,
    evidence_text: impl Into<String>,
    observed_at: impl Into<String>,
) -> IntelLeadEvidenceCandidate {
    IntelLeadEvidenceCandidate {
        source_type: source_type.to_string(),
        source_id,
        source_key: source_key.into(),
        evidence_text: evidence_text.into(),
        observed_at: observed_at.into(),
    }
}

fn build_intel_lead_detail_url(lead_id: i32) -> String {
    format!("/leads/{lead_id}")
}

fn load_page_ids_for_scan_ids(conn: &mut PgConnection, scan_ids: &[i32]) -> Result<Vec<i32>> {
    use crate::schema::page_scan::dsl as scan_dsl;

    if scan_ids.is_empty() {
        return Ok(Vec::new());
    }
    scan_dsl::page_scan
        .filter(scan_dsl::id.eq_any(scan_ids))
        .select(scan_dsl::page_id)
        .load::<i32>(conn)
        .context("error loading pages for scan ids")
}

fn push_entity_reference(
    references: &mut Vec<IntelLeadEntityReference>,
    entity_type: &str,
    entity_value: &str,
) {
    let entity_type = entity_type.trim();
    let entity_value = entity_value.trim();
    if entity_type.is_empty() || entity_value.is_empty() {
        return;
    }
    references.push(IntelLeadEntityReference {
        entity_type: entity_type.to_string(),
        entity_value: entity_value.to_string(),
        detail_url: intel_entity_detail_url(entity_type, entity_value),
    });
}

fn intel_entity_detail_url(entity_type: &str, entity_value: &str) -> Option<String> {
    match entity_type {
        "email" => Some(build_query_url(
            "/entities/emails",
            &[("value", entity_value)],
        )),
        "crypto" => {
            let (asset_type, reference) = entity_value.split_once(':')?;
            Some(build_query_url(
                "/entities/crypto",
                &[("asset_type", asset_type), ("reference", reference)],
            ))
        }
        "ssh_host_key" => {
            let (algorithm, fingerprint) = entity_value.split_once(':')?;
            Some(build_query_url(
                "/entities/ssh",
                &[("algorithm", algorithm), ("fingerprint", fingerprint)],
            ))
        }
        _ => None,
    }
}

fn intel_evidence_source_url(
    conn: &mut PgConnection,
    evidence: &IntelLeadEvidenceRecord,
) -> Result<Option<String>> {
    match evidence.source_type.as_str() {
        "page" if evidence.source_id > 0 => Ok(Some(format!("/pages/{}", evidence.source_id))),
        "page_scan" if evidence.source_id > 0 => {
            use crate::schema::page_scan::dsl as scan_dsl;
            let page_id = scan_dsl::page_scan
                .filter(scan_dsl::id.eq(evidence.source_id))
                .select(scan_dsl::page_id)
                .first::<i32>(conn)
                .optional()
                .context("error loading page scan source url")?;
            Ok(page_id.map(|page_id| build_page_scan_detail_url(page_id, evidence.source_id)))
        }
        "email" => Ok(Some(build_query_url(
            "/entities/emails",
            &[("value", &evidence.source_key)],
        ))),
        "crypto" => {
            let Some((asset_type, reference)) = evidence.source_key.split_once(':') else {
                return Ok(None);
            };
            Ok(Some(build_query_url(
                "/entities/crypto",
                &[("asset_type", asset_type), ("reference", reference)],
            )))
        }
        "ssh_host_key" => {
            let Some((algorithm, fingerprint)) = evidence.source_key.split_once(':') else {
                return Ok(None);
            };
            Ok(Some(build_query_url(
                "/entities/ssh",
                &[("algorithm", algorithm), ("fingerprint", fingerprint)],
            )))
        }
        "ssh_endpoint" => Ok(Some("/entities/ssh".to_string())),
        "http_endpoint" => Ok(http_detail_url_from_endpoint(&evidence.source_key)),
        "service_endpoint" => Ok(service_detail_url_from_endpoint(&evidence.source_key)),
        "relationship" => Ok(Some("/relationships".to_string())),
        _ => Ok(None),
    }
}

fn http_detail_url_from_endpoint(endpoint: &str) -> Option<String> {
    let endpoint = endpoint_from_url(endpoint)?;
    let port = endpoint.port.to_string();
    Some(build_query_url(
        "/entities/http",
        &[
            ("host", &endpoint.host),
            ("scheme", &endpoint.scheme),
            ("port", &port),
        ],
    ))
}

fn service_detail_url_from_endpoint(endpoint: &str) -> Option<String> {
    let parsed = Url::parse(endpoint).ok()?;
    let host = parsed.host_str()?.to_ascii_lowercase();
    let service = parsed.scheme().to_ascii_lowercase();
    let port = parsed.port()?.to_string();
    Some(build_query_url(
        "/entities/services",
        &[("host", &host), ("service", &service), ("port", &port)],
    ))
}

fn load_active_lead_badges_for_sources(
    conn: &mut PgConnection,
    sources: &[LeadEvidenceSource],
) -> Result<Vec<IntelLeadBadge>> {
    use crate::schema::intel_lead::dsl as lead_dsl;
    use crate::schema::intel_lead_evidence::dsl as evidence_dsl;

    let mut seen_sources = HashSet::new();
    let mut seen_leads = HashSet::new();
    let mut badges = Vec::new();
    for source in sources {
        if !seen_sources.insert(source.clone()) {
            continue;
        }
        let records = lead_dsl::intel_lead
            .inner_join(evidence_dsl::intel_lead_evidence)
            .filter(lead_dsl::status.ne(LEAD_STATUS_SUPPRESSED))
            .filter(evidence_dsl::source_type.eq(&source.source_type))
            .filter(evidence_dsl::source_id.eq(source.source_id))
            .filter(evidence_dsl::source_key.eq(&source.source_key))
            .select(IntelLeadRecord::as_select())
            .load::<IntelLeadRecord>(conn)
            .context("error loading active lead badges")?;
        for record in records {
            if seen_leads.insert(record.id) {
                badges.push(intel_lead_badge_from_record(record));
            }
        }
    }
    badges.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| right.confidence.cmp(&left.confidence))
            .then_with(|| left.title.cmp(&right.title))
    });
    Ok(badges)
}

fn source_ref(
    source_type: &str,
    source_id: i32,
    source_key: impl Into<String>,
) -> LeadEvidenceSource {
    LeadEvidenceSource {
        source_type: source_type.to_string(),
        source_id,
        source_key: source_key.into(),
    }
}

fn build_shared_email_lead_candidates(
    conn: &mut PgConnection,
    since_scan_id: Option<i32>,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    let host_expr = sql_host_expr("p.url", conn);
    let sql = format!(
        "
        WITH candidate_emails AS (
            SELECT DISTINCT email
            FROM (
                SELECT scan_id, email
                FROM page_scan_email
                WHERE $1 <= 0 OR scan_id > $1
                ORDER BY scan_id DESC
                LIMIT $3
            ) recent_scan_email
            WHERE email != ''
        )
        SELECT
            pe.email,
            COUNT(DISTINCT {host_expr}) AS host_count,
            COUNT(DISTINCT pe.page_id) AS page_count,
            MIN(p.created_at) AS first_seen_at,
            MAX(p.last_scanned_at) AS last_seen_at
        FROM page_email pe
        JOIN page p ON p.id = pe.page_id
        WHERE pe.email != ''
            AND pe.email IN (SELECT email FROM candidate_emails)
            AND {host_expr} != ''
        GROUP BY pe.email
        HAVING COUNT(DISTINCT {host_expr}) > 1
        ORDER BY host_count DESC, page_count DESC, last_seen_at DESC, pe.email ASC
        LIMIT $2
        "
    );
    let rows = sql_query(sql)
        .bind::<diesel::sql_types::Integer, _>(since_scan_id.unwrap_or(0))
        .bind::<BigInt, _>(rule_limit)
        .bind::<BigInt, _>(candidate_scan_sample_limit(rule_limit))
        .load::<SharedEmailLeadRow>(conn)
        .context("error building shared email lead candidates")?;

    rows.into_iter()
        .map(|row| {
            let mut evidence = vec![evidence_candidate(
                "email",
                0,
                row.email.clone(),
                format!(
                    "Email {} appears on {} hosts across {} pages",
                    row.email, row.host_count, row.page_count
                ),
                row.last_seen_at.clone(),
            )];
            evidence.extend(load_entity_page_evidence(
                conn,
                "page_email",
                "email",
                &row.email,
                rule_limit.min(20),
            )?);
            let score =
                (45 + (row.host_count as i32 * 10) + (row.page_count as i32 * 2)).clamp(1, 92);
            Ok(IntelLeadCandidate {
                rule_id: "shared-email".to_string(),
                lead_key: format!("shared-email:{}", row.email.to_ascii_lowercase()),
                title: format!("Shared email across {} hosts", row.host_count),
                summary: format!(
                    "{} appears on {} unrelated hosts and {} pages.",
                    row.email, row.host_count, row.page_count
                ),
                score,
                confidence: (60 + (row.host_count as i32 * 8)).clamp(50, 95),
                primary_entity_type: "email".to_string(),
                primary_entity_value: row.email,
                related_entity_type: None,
                related_entity_value: None,
                first_seen_at: row.first_seen_at,
                last_seen_at: row.last_seen_at,
                evidence,
            })
        })
        .collect()
}

fn build_shared_crypto_lead_candidates(
    conn: &mut PgConnection,
    since_scan_id: Option<i32>,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    let host_expr = sql_host_expr("p.url", conn);
    let sql = format!(
        "
        WITH candidate_crypto AS (
            SELECT DISTINCT asset_type, reference
            FROM (
                SELECT scan_id, asset_type, reference
                FROM page_scan_crypto
                WHERE $1 <= 0 OR scan_id > $1
                ORDER BY scan_id DESC
                LIMIT $3
            ) recent_scan_crypto
            WHERE reference != ''
        )
        SELECT
            pc.asset_type,
            pc.reference,
            COUNT(DISTINCT {host_expr}) AS host_count,
            COUNT(DISTINCT pc.page_id) AS page_count,
            MIN(p.created_at) AS first_seen_at,
            MAX(p.last_scanned_at) AS last_seen_at
        FROM page_crypto pc
        JOIN page p ON p.id = pc.page_id
        WHERE pc.reference != ''
            AND {host_expr} != ''
            AND EXISTS (
                SELECT 1
                FROM candidate_crypto cc
                WHERE cc.asset_type = pc.asset_type
                  AND cc.reference = pc.reference
            )
        GROUP BY pc.asset_type, pc.reference
        HAVING COUNT(DISTINCT {host_expr}) > 1
        ORDER BY host_count DESC, page_count DESC, last_seen_at DESC, pc.asset_type ASC, pc.reference ASC
        LIMIT $2
        "
    );
    let rows = sql_query(sql)
        .bind::<diesel::sql_types::Integer, _>(since_scan_id.unwrap_or(0))
        .bind::<BigInt, _>(rule_limit)
        .bind::<BigInt, _>(candidate_scan_sample_limit(rule_limit))
        .load::<SharedCryptoLeadRow>(conn)
        .context("error building shared crypto lead candidates")?;

    rows.into_iter()
        .map(|row| {
            let entity_key = crypto_entity_key(&row.asset_type, &row.reference);
            let mut evidence = vec![evidence_candidate(
                "crypto",
                0,
                entity_key.clone(),
                format!(
                    "{} reference appears on {} hosts across {} pages",
                    row.asset_type, row.host_count, row.page_count
                ),
                row.last_seen_at.clone(),
            )];
            evidence.extend(load_crypto_page_evidence(
                conn,
                &row.asset_type,
                &row.reference,
                rule_limit.min(20),
            )?);
            let score =
                (50 + (row.host_count as i32 * 11) + (row.page_count as i32 * 2)).clamp(1, 94);
            Ok(IntelLeadCandidate {
                rule_id: "shared-crypto".to_string(),
                lead_key: format!(
                    "shared-crypto:{}:{}",
                    row.asset_type.to_ascii_lowercase(),
                    row.reference.to_ascii_lowercase()
                ),
                title: format!("Shared {} reference", row.asset_type),
                summary: format!(
                    "{} appears on {} unrelated hosts and {} pages.",
                    row.reference, row.host_count, row.page_count
                ),
                score,
                confidence: (62 + (row.host_count as i32 * 8)).clamp(50, 96),
                primary_entity_type: "crypto".to_string(),
                primary_entity_value: entity_key,
                related_entity_type: None,
                related_entity_value: None,
                first_seen_at: row.first_seen_at,
                last_seen_at: row.last_seen_at,
                evidence,
            })
        })
        .collect()
}

fn load_entity_page_evidence(
    conn: &mut PgConnection,
    table_name: &str,
    column_name: &str,
    value: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let host_expr = sql_host_expr("p.url", conn);
    let sql = format!(
        "
        SELECT
            p.id AS page_id,
            p.title AS page_title,
            p.url AS page_url,
            {host_expr} AS host,
            p.last_scanned_at AS observed_at
        FROM {table_name} entity
        JOIN page p ON p.id = entity.page_id
        WHERE entity.{column_name} = $1
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT $2
        "
    );
    let rows = sql_query(sql)
        .bind::<Text, _>(value)
        .bind::<BigInt, _>(limit)
        .load::<LeadPageEvidenceRow>(conn)
        .context("error loading entity page lead evidence")?;
    Ok(page_evidence_candidates(rows, value))
}

fn load_crypto_page_evidence(
    conn: &mut PgConnection,
    asset_type: &str,
    reference: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let host_expr = sql_host_expr("p.url", conn);
    let sql = format!(
        "
        SELECT
            p.id AS page_id,
            p.title AS page_title,
            p.url AS page_url,
            {host_expr} AS host,
            p.last_scanned_at AS observed_at
        FROM page_crypto pc
        JOIN page p ON p.id = pc.page_id
        WHERE pc.asset_type = $1
          AND pc.reference = $2
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT $3
        "
    );
    let rows = sql_query(sql)
        .bind::<Text, _>(asset_type)
        .bind::<Text, _>(reference)
        .bind::<BigInt, _>(limit)
        .load::<LeadPageEvidenceRow>(conn)
        .context("error loading crypto page lead evidence")?;
    Ok(page_evidence_candidates(rows, reference))
}

fn page_evidence_candidates(
    rows: Vec<LeadPageEvidenceRow>,
    value: &str,
) -> Vec<IntelLeadEvidenceCandidate> {
    let mut evidence = Vec::new();
    let mut seen_sites = HashSet::new();
    for row in rows {
        evidence.push(evidence_candidate(
            "page",
            row.page_id,
            row.page_url.clone(),
            format!(
                "{} observed on {} ({})",
                value, row.page_url, row.page_title
            ),
            row.observed_at.clone(),
        ));
        if !row.host.is_empty() && seen_sites.insert(row.host.clone()) {
            evidence.push(evidence_candidate(
                "site",
                0,
                row.host.clone(),
                format!("{} observed on host {}", value, row.host),
                row.observed_at.clone(),
            ));
        }
    }
    evidence
}

fn crypto_entity_key(asset_type: &str, reference: &str) -> String {
    format!("{asset_type}:{reference}")
}

fn build_watchlist_lead_candidates(
    conn: &mut PgConnection,
    _since_scan_id: Option<i32>,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    let items = list_watchlist_items(conn)?
        .into_iter()
        .filter(|item| item.enabled)
        .collect::<Vec<_>>();
    let mut candidates = Vec::new();

    for item in items {
        let evidence = dedupe_watchlist_evidence(load_watchlist_match_evidence(
            conn,
            &item,
            rule_limit.min(50),
        )?);
        if evidence.is_empty() {
            continue;
        }

        let first_seen_at = evidence
            .iter()
            .map(|row| row.observed_at.as_str())
            .min()
            .unwrap_or_default()
            .to_string();
        let last_seen_at = evidence
            .iter()
            .map(|row| row.observed_at.as_str())
            .max()
            .unwrap_or_default()
            .to_string();
        let score = watchlist_match_score(&item.item_type, evidence.len());
        let display_value = watchlist_display_value(&item);
        let item_type_label = watchlist_item_type_label(&item.item_type);

        candidates.push(IntelLeadCandidate {
            rule_id: "watchlist-match".to_string(),
            lead_key: format!("watchlist-match:{}:{}", item.id, item.value),
            title: format!(
                "Watchlist {} matched: {}",
                item_type_label,
                truncate(&display_value, 80)
            ),
            summary: format!(
                "{} matched {} observation{} for watched {}.",
                display_value,
                evidence.len(),
                if evidence.len() == 1 { "" } else { "s" },
                item_type_label
            ),
            score,
            confidence: watchlist_match_confidence(&item.item_type),
            primary_entity_type: item.item_type.clone(),
            primary_entity_value: item.value.clone(),
            related_entity_type: (!item.label.is_empty()).then_some("watchlist_label".to_string()),
            related_entity_value: (!item.label.is_empty()).then_some(item.label.clone()),
            first_seen_at,
            last_seen_at,
            evidence,
        });
    }

    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| right.last_seen_at.cmp(&left.last_seen_at))
            .then_with(|| left.lead_key.cmp(&right.lead_key))
    });
    candidates.truncate(rule_limit.max(0) as usize);
    Ok(candidates)
}

fn load_watchlist_match_evidence(
    conn: &mut PgConnection,
    item: &WatchlistItem,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    match item.item_type.as_str() {
        WATCHLIST_TYPE_DOMAIN => load_watchlist_domain_evidence(conn, &item.value, limit),
        WATCHLIST_TYPE_URL => load_watchlist_url_evidence(conn, &item.value, limit),
        WATCHLIST_TYPE_EMAIL => load_watchlist_email_evidence(conn, &item.value, limit),
        WATCHLIST_TYPE_CRYPTO => load_watchlist_crypto_evidence(conn, &item.value, limit),
        WATCHLIST_TYPE_KEYWORD => load_watchlist_keyword_evidence(conn, &item.value, limit),
        WATCHLIST_TYPE_SSH_FINGERPRINT => {
            load_watchlist_ssh_fingerprint_evidence(conn, &item.value, limit)
        }
        WATCHLIST_TYPE_HTTP_FINGERPRINT => {
            load_watchlist_http_fingerprint_evidence(conn, &item.value, limit)
        }
        WATCHLIST_TYPE_FAVICON_HASH => {
            load_watchlist_favicon_hash_evidence(conn, &item.value, limit)
        }
        _ => Ok(Vec::new()),
    }
}

fn load_watchlist_domain_evidence(
    conn: &mut PgConnection,
    value: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let host_expr = sql_host_without_port_expr("p.url", conn);
    let source_host_expr = sql_host_without_port_expr("p.url", conn);
    let page_sql = format!(
        "
        SELECT
            'page' AS source_type,
            p.id AS source_id,
            p.url AS source_key,
            'Watched domain matched page host ' || ({host_expr}) AS evidence_text,
            p.last_scanned_at AS observed_at,
            ({host_expr}) AS site_host
        FROM page p
        WHERE ({host_expr}) = $1 OR ({host_expr}) LIKE ('%.' || $1)
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT $2
        "
    );
    let link_sql = format!(
        "
        SELECT
            'page' AS source_type,
            p.id AS source_id,
            p.url AS source_key,
            'Watched domain appeared in outbound link ' || pl.target_url AS evidence_text,
            p.last_scanned_at AS observed_at,
            ({source_host_expr}) AS site_host
        FROM page_link pl
        JOIN page p ON p.id = pl.source_page_id
        WHERE lower(pl.target_host) = $1 OR lower(pl.target_host) LIKE ('%.' || $1)
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT $2
        "
    );

    let mut evidence =
        watchlist_rows_to_evidence(load_watchlist_rows(conn, page_sql, value, limit)?);
    evidence.extend(watchlist_rows_to_evidence(load_watchlist_rows(
        conn, link_sql, value, limit,
    )?));
    Ok(evidence)
}

fn load_watchlist_url_evidence(
    conn: &mut PgConnection,
    value: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let source_host_expr = sql_host_without_port_expr("p.url", conn);
    let page_sql = format!(
        "
        SELECT
            'page' AS source_type,
            p.id AS source_id,
            p.url AS source_key,
            'Watched URL matched scanned page ' || p.url AS evidence_text,
            p.last_scanned_at AS observed_at,
            ({source_host_expr}) AS site_host
        FROM page p
        WHERE lower(p.url) = $1
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT $2
        "
    );
    let link_sql = format!(
        "
        SELECT
            'page' AS source_type,
            p.id AS source_id,
            p.url AS source_key,
            'Watched URL appeared in outbound link ' || pl.target_url AS evidence_text,
            p.last_scanned_at AS observed_at,
            ({source_host_expr}) AS site_host
        FROM page_link pl
        JOIN page p ON p.id = pl.source_page_id
        WHERE lower(pl.target_url) = $1
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT $2
        "
    );

    let mut evidence =
        watchlist_rows_to_evidence(load_watchlist_rows(conn, page_sql, value, limit)?);
    evidence.extend(watchlist_rows_to_evidence(load_watchlist_rows(
        conn, link_sql, value, limit,
    )?));
    Ok(evidence)
}

fn load_watchlist_email_evidence(
    conn: &mut PgConnection,
    value: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let host_expr = sql_host_without_port_expr("p.url", conn);
    let sql = format!(
        "
        SELECT
            'page' AS source_type,
            p.id AS source_id,
            p.url AS source_key,
            'Watched email appeared on page ' || p.url AS evidence_text,
            p.last_scanned_at AS observed_at,
            ({host_expr}) AS site_host
        FROM page_email pe
        JOIN page p ON p.id = pe.page_id
        WHERE pe.email = $1
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT $2
        "
    );
    Ok(watchlist_rows_to_evidence(load_watchlist_rows(
        conn, sql, value, limit,
    )?))
}

fn load_watchlist_crypto_evidence(
    conn: &mut PgConnection,
    value: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let host_expr = sql_host_without_port_expr("p.url", conn);
    let sql = format!(
        "
        SELECT
            'page' AS source_type,
            p.id AS source_id,
            p.url AS source_key,
            'Watched crypto reference appeared on page ' || p.url AS evidence_text,
            p.last_scanned_at AS observed_at,
            ({host_expr}) AS site_host
        FROM page_crypto pc
        JOIN page p ON p.id = pc.page_id
        WHERE lower(pc.asset_type || ':' || pc.reference) = $1
           OR lower(pc.reference) = $1
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT $2
        "
    );
    Ok(watchlist_rows_to_evidence(load_watchlist_rows(
        conn, sql, value, limit,
    )?))
}

fn load_watchlist_keyword_evidence(
    conn: &mut PgConnection,
    value: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let host_expr = sql_host_without_port_expr("p.url", conn);
    let pattern = format!("%{}%", escape_like(value));
    let sql = format!(
        "
        SELECT
            'page' AS source_type,
            p.id AS source_id,
            p.url AS source_key,
            'Watched keyword matched page title, URL, entities, or tags' AS evidence_text,
            p.last_scanned_at AS observed_at,
            ({host_expr}) AS site_host
        FROM page p
        WHERE p.title ILIKE $1 ESCAPE '\\'
           OR p.url ILIKE $1 ESCAPE '\\'
           OR p.links ILIKE $1 ESCAPE '\\'
           OR p.emails ILIKE $1 ESCAPE '\\'
           OR p.coins ILIKE $1 ESCAPE '\\'
           OR EXISTS (
                SELECT 1
                FROM page_keyword_tag pkt
                WHERE pkt.page_id = p.id
                  AND pkt.tag ILIKE $1 ESCAPE '\\'
           )
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT $2
        "
    );
    Ok(watchlist_rows_to_evidence(load_watchlist_rows(
        conn, sql, &pattern, limit,
    )?))
}

fn load_watchlist_http_fingerprint_evidence(
    conn: &mut PgConnection,
    value: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    load_watchlist_http_observation_evidence(
        conn,
        value,
        "lower(COALESCE(header_fingerprint, '')) = $1",
        "Watched HTTP header fingerprint matched endpoint ",
        limit,
    )
}

fn load_watchlist_favicon_hash_evidence(
    conn: &mut PgConnection,
    value: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    load_watchlist_http_observation_evidence(
        conn,
        value,
        "lower(COALESCE(favicon_hash, '')) = $1",
        "Watched favicon hash matched endpoint ",
        limit,
    )
}

fn load_watchlist_http_observation_evidence(
    conn: &mut PgConnection,
    value: &str,
    predicate: &str,
    evidence_prefix: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let endpoint_expr = "scheme || '://' || host || ':' || port::text || '/'";
    let sql = format!(
        "
        SELECT
            'http_endpoint' AS source_type,
            id AS source_id,
            {endpoint_expr} AS source_key,
            {prefix} || {endpoint_expr} AS evidence_text,
            COALESCE(last_success_at, last_attempt_at) AS observed_at,
            lower(host) AS site_host
        FROM host_http_observation
        WHERE {predicate}
        ORDER BY COALESCE(last_success_at, last_attempt_at) DESC, id DESC
        LIMIT $2
        ",
        prefix = quote_sql_text_literal(evidence_prefix)
    );
    Ok(watchlist_rows_to_evidence(load_watchlist_rows(
        conn, sql, value, limit,
    )?))
}

fn load_watchlist_ssh_fingerprint_evidence(
    conn: &mut PgConnection,
    value: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let endpoint_expr = "'ssh://' || host || ':' || port::text";
    let sql = format!(
        "
        SELECT
            'ssh_endpoint' AS source_type,
            id AS source_id,
            {endpoint_expr} AS source_key,
            'Watched SSH fingerprint matched endpoint ' || {endpoint_expr} AS evidence_text,
            COALESCE(last_success_at, last_attempt_at) AS observed_at,
            lower(host) AS site_host
        FROM host_ssh_observation
        WHERE lower(COALESCE(host_key_fingerprint, '')) = $1
           OR lower(COALESCE(host_key_algorithm, '') || ':' || COALESCE(host_key_fingerprint, '')) = $1
        ORDER BY COALESCE(last_success_at, last_attempt_at) DESC, id DESC
        LIMIT $2
        "
    );
    Ok(watchlist_rows_to_evidence(load_watchlist_rows(
        conn, sql, value, limit,
    )?))
}

fn load_watchlist_rows(
    conn: &mut PgConnection,
    query: String,
    value: &str,
    limit: i64,
) -> Result<Vec<WatchlistLeadEvidenceRow>> {
    sql_query(query)
        .bind::<Text, _>(value)
        .bind::<BigInt, _>(limit.max(1))
        .load::<WatchlistLeadEvidenceRow>(conn)
        .context("error loading watchlist match evidence")
}

fn watchlist_rows_to_evidence(
    rows: Vec<WatchlistLeadEvidenceRow>,
) -> Vec<IntelLeadEvidenceCandidate> {
    let mut evidence = Vec::new();
    let mut seen_sites = HashSet::new();
    for row in rows {
        evidence.push(evidence_candidate(
            &row.source_type,
            row.source_id,
            row.source_key,
            row.evidence_text,
            row.observed_at.clone(),
        ));
        if let Some(site_host) = row.site_host {
            if !site_host.is_empty() && seen_sites.insert(site_host.clone()) {
                evidence.push(evidence_candidate(
                    "site",
                    0,
                    site_host.clone(),
                    format!("Watchlist match observed on host {site_host}"),
                    row.observed_at,
                ));
            }
        }
    }
    evidence
}

fn dedupe_watchlist_evidence(
    evidence: Vec<IntelLeadEvidenceCandidate>,
) -> Vec<IntelLeadEvidenceCandidate> {
    let mut seen = HashSet::new();
    evidence
        .into_iter()
        .filter(|row| {
            seen.insert((
                row.source_type.clone(),
                row.source_id,
                row.source_key.clone(),
                row.evidence_text.clone(),
            ))
        })
        .collect()
}

fn watchlist_match_score(item_type: &str, evidence_count: usize) -> i32 {
    let base = match item_type {
        WATCHLIST_TYPE_KEYWORD => 62,
        WATCHLIST_TYPE_DOMAIN | WATCHLIST_TYPE_URL => 76,
        WATCHLIST_TYPE_EMAIL | WATCHLIST_TYPE_CRYPTO => 82,
        WATCHLIST_TYPE_SSH_FINGERPRINT
        | WATCHLIST_TYPE_HTTP_FINGERPRINT
        | WATCHLIST_TYPE_FAVICON_HASH => 86,
        _ => 70,
    };
    (base + (evidence_count as i32 * 3)).clamp(1, 98)
}

fn watchlist_match_confidence(item_type: &str) -> i32 {
    match item_type {
        WATCHLIST_TYPE_KEYWORD => 70,
        WATCHLIST_TYPE_DOMAIN | WATCHLIST_TYPE_URL => 86,
        WATCHLIST_TYPE_EMAIL | WATCHLIST_TYPE_CRYPTO => 92,
        WATCHLIST_TYPE_SSH_FINGERPRINT
        | WATCHLIST_TYPE_HTTP_FINGERPRINT
        | WATCHLIST_TYPE_FAVICON_HASH => 95,
        _ => 80,
    }
}

fn watchlist_display_value(item: &WatchlistItem) -> String {
    if item.label.is_empty() {
        item.value.clone()
    } else {
        format!("{} ({})", item.label, item.value)
    }
}

fn watchlist_item_type_label(item_type: &str) -> &'static str {
    match item_type {
        WATCHLIST_TYPE_DOMAIN => "domain",
        WATCHLIST_TYPE_URL => "URL",
        WATCHLIST_TYPE_EMAIL => "email",
        WATCHLIST_TYPE_CRYPTO => "crypto reference",
        WATCHLIST_TYPE_KEYWORD => "keyword",
        WATCHLIST_TYPE_SSH_FINGERPRINT => "SSH fingerprint",
        WATCHLIST_TYPE_HTTP_FINGERPRINT => "HTTP fingerprint",
        WATCHLIST_TYPE_FAVICON_HASH => "favicon hash",
        _ => "item",
    }
}

fn candidate_scan_sample_limit(rule_limit: i64) -> i64 {
    (rule_limit * 50).clamp(1_000, 250_000)
}

fn build_scan_update_lead_candidates(
    conn: &mut PgConnection,
    since_scan_id: Option<i32>,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    let host_expr = sql_host_expr("p.url", conn);
    let sql = format!(
        "
        SELECT
            ps.id AS scan_id,
            ps.page_id,
            ps.title AS page_title,
            p.url AS page_url,
            {host_expr} AS page_host,
            ps.scanned_at,
            (
                SELECT previous.id
                FROM page_scan previous
                WHERE previous.page_id = ps.page_id
                  AND previous.id < ps.id
                ORDER BY previous.id DESC
                LIMIT 1
            ) AS previous_scan_id
        FROM page_scan ps
        JOIN page p ON p.id = ps.page_id
        WHERE $1 <= 0 OR ps.id > $1
        ORDER BY ps.id DESC
        LIMIT $2
        "
    );
    let rows = sql_query(sql)
        .bind::<diesel::sql_types::Integer, _>(since_scan_id.unwrap_or(0))
        .bind::<BigInt, _>(rule_limit)
        .load::<RecentScanLeadRow>(conn)
        .context("error loading recent scans for intel leads")?;
    let scan_ids = rows
        .iter()
        .flat_map(|row| [Some(row.scan_id), row.previous_scan_id])
        .flatten()
        .collect::<Vec<_>>();
    let scan_links = load_scan_link_rows(conn, &scan_ids)?;
    let scan_emails = load_scan_email_rows(conn, &scan_ids)?;
    let scan_crypto_refs = load_scan_crypto_rows(conn, &scan_ids)?;
    let blacklist_domains = load_blacklist_domains(conn)?;
    let mut candidates = Vec::new();

    for row in rows {
        let Some(previous_scan_id) = row.previous_scan_id else {
            continue;
        };
        let current_links = scan_links.get(&row.scan_id).cloned().unwrap_or_default();
        let previous_links = scan_links
            .get(&previous_scan_id)
            .cloned()
            .unwrap_or_default();
        let current_emails = scan_emails.get(&row.scan_id).cloned().unwrap_or_default();
        let previous_emails = scan_emails
            .get(&previous_scan_id)
            .cloned()
            .unwrap_or_default();
        let current_crypto = scan_crypto_refs
            .get(&row.scan_id)
            .cloned()
            .unwrap_or_default();
        let previous_crypto = scan_crypto_refs
            .get(&previous_scan_id)
            .cloned()
            .unwrap_or_default();

        let current_link_set = link_set_from_scan_rows(&current_links);
        let previous_link_set = link_set_from_scan_rows(&previous_links);
        let current_email_set = email_set_from_scan_rows(&current_emails);
        let previous_email_set = email_set_from_scan_rows(&previous_emails);
        let current_crypto_set = crypto_set_from_scan_rows(&current_crypto);
        let previous_crypto_set = crypto_set_from_scan_rows(&previous_crypto);
        let added_links = current_link_set
            .difference(&previous_link_set)
            .cloned()
            .collect::<Vec<_>>();
        let added_emails = current_email_set
            .difference(&previous_email_set)
            .cloned()
            .collect::<Vec<_>>();
        let added_crypto = current_crypto_set
            .difference(&previous_crypto_set)
            .cloned()
            .collect::<Vec<_>>();
        let added_blacklisted_links = added_links
            .iter()
            .filter_map(|(_, target_host)| {
                find_matching_blacklist_domain(target_host, &blacklist_domains)
            })
            .collect::<HashSet<_>>();

        if added_emails.is_empty()
            && added_crypto.is_empty()
            && added_blacklisted_links.is_empty()
            && added_links.len() < MANY_NEW_OUTBOUND_LINK_THRESHOLD
        {
            continue;
        }

        let mut evidence = vec![
            evidence_candidate(
                "page_scan",
                row.scan_id,
                row.page_url.clone(),
                format!("Scan {} added notable observations", row.scan_id),
                row.scanned_at.clone(),
            ),
            evidence_candidate(
                "page",
                row.page_id,
                row.page_url.clone(),
                format!("Page {} changed during scan {}", row.page_url, row.scan_id),
                row.scanned_at.clone(),
            ),
        ];
        if !row.page_host.is_empty() {
            evidence.push(evidence_candidate(
                "site",
                0,
                row.page_host.clone(),
                format!("Host {} had a notable scan update", row.page_host),
                row.scanned_at.clone(),
            ));
        }
        for email in added_emails.iter().take(10) {
            evidence.push(evidence_candidate(
                "email",
                0,
                email.clone(),
                format!("New email observed: {email}"),
                row.scanned_at.clone(),
            ));
        }
        for (asset_type, reference) in added_crypto.iter().take(10) {
            evidence.push(evidence_candidate(
                "crypto",
                0,
                crypto_entity_key(asset_type, reference),
                format!("New {asset_type} reference observed: {reference}"),
                row.scanned_at.clone(),
            ));
        }
        for (target_url, target_host) in added_links.iter().take(10) {
            if find_matching_blacklist_domain(target_host, &blacklist_domains).is_some() {
                evidence.push(evidence_candidate(
                    "relationship",
                    0,
                    relationship_key(&row.page_host, target_host),
                    format!("New blacklisted outbound link: {target_url}"),
                    row.scanned_at.clone(),
                ));
            }
        }

        let score = (35
            + (added_emails.len() as i32 * 8)
            + (added_crypto.len() as i32 * 10)
            + (added_blacklisted_links.len() as i32 * 18)
            + if added_links.len() >= MANY_NEW_OUTBOUND_LINK_THRESHOLD {
                20
            } else {
                0
            })
        .clamp(1, 94);
        candidates.push(IntelLeadCandidate {
            rule_id: "scan-new-observations".to_string(),
            lead_key: format!("scan-new-observations:{}", row.scan_id),
            title: format!("Notable update on {}", row.page_title),
            summary: format!(
                "Scan {} added {} links, {} emails, {} crypto refs, and links to {} blacklisted domains.",
                row.scan_id,
                added_links.len(),
                added_emails.len(),
                added_crypto.len(),
                added_blacklisted_links.len()
            ),
            score,
            confidence: 82,
            primary_entity_type: "page".to_string(),
            primary_entity_value: row.page_url,
            related_entity_type: Some("site".to_string()),
            related_entity_value: Some(row.page_host),
            first_seen_at: row.scanned_at.clone(),
            last_seen_at: row.scanned_at,
            evidence,
        });
    }

    Ok(candidates)
}

fn build_blacklisted_site_link_lead_candidates(
    conn: &mut PgConnection,
    rule_limit: i64,
    after_link_id: i32,
    link_batch_size: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    let blacklist_domains = load_blacklist_domains(conn)?;
    if blacklist_domains.is_empty() {
        return Ok(Vec::new());
    }
    let host_expr = sql_host_expr("p.url", conn);
    let mut grouped = HashMap::<(String, String), Vec<BlacklistedLinkLeadRow>>::new();
    let sql = format!(
        "
        WITH candidate_links AS (
            SELECT
                pl.id AS link_id,
                pl.source_page_id,
                pl.target_url,
                lower(pl.target_host) AS target_host
            FROM page_link pl
            WHERE pl.id > $1
              AND pl.target_host != ''
            ORDER BY pl.id ASC
            LIMIT $2
        )
        SELECT
                p.id AS page_id,
                p.title AS page_title,
                p.url AS page_url,
                {host_expr} AS source_host,
                pl.target_url,
                pl.target_host,
                p.last_scanned_at AS observed_at,
                lower(db.domain) AS blacklist_domain
        FROM candidate_links pl
        JOIN page p ON p.id = pl.source_page_id
        JOIN domain_blacklist db
          ON pl.target_host = lower(db.domain)
          OR pl.target_host LIKE ('%.' || lower(db.domain))
        WHERE {host_expr} != ''
              AND {host_expr} != pl.target_host
        ORDER BY p.last_scanned_at DESC, p.id DESC
        "
    );
    let rows = sql_query(sql)
        .bind::<diesel::sql_types::Integer, _>(after_link_id)
        .bind::<BigInt, _>(link_batch_size)
        .load::<BlacklistedLinkLeadRow>(conn)
        .context("error loading links to blacklisted domains")?;
    for row in rows {
        grouped
            .entry((row.source_host.clone(), row.blacklist_domain.clone()))
            .or_default()
            .push(row);
    }

    let mut candidates = Vec::new();
    for ((source_host, blacklist_domain), rows) in grouped {
        let Some(last_seen_at) = rows.iter().map(|row| row.observed_at.clone()).max() else {
            continue;
        };
        let Some(first_seen_at) = rows.iter().map(|row| row.observed_at.clone()).min() else {
            continue;
        };
        let mut evidence = vec![
            evidence_candidate(
                "site",
                0,
                source_host.clone(),
                format!("{source_host} links to blacklisted domain {blacklist_domain}"),
                last_seen_at.clone(),
            ),
            evidence_candidate(
                "site",
                0,
                blacklist_domain.clone(),
                format!("{blacklist_domain} is configured in the domain blacklist"),
                last_seen_at.clone(),
            ),
        ];
        for row in rows.iter().take(20) {
            evidence.push(evidence_candidate(
                "page",
                row.page_id,
                row.page_url.clone(),
                format!(
                    "{} ({}) links to {}",
                    row.page_url, row.page_title, row.target_url
                ),
                row.observed_at.clone(),
            ));
            evidence.push(evidence_candidate(
                "relationship",
                0,
                relationship_key(&source_host, &row.target_host),
                format!("{} -> {}", source_host, row.target_host),
                row.observed_at.clone(),
            ));
        }
        let reference_count = rows.len();
        candidates.push(IntelLeadCandidate {
            rule_id: "blacklisted-site-link".to_string(),
            lead_key: format!("blacklisted-site-link:{source_host}->{blacklist_domain}"),
            title: format!("{source_host} links to blacklisted domain"),
            summary: format!(
                "{source_host} has {reference_count} observed links to {blacklist_domain}."
            ),
            score: (62 + (reference_count as i32 * 3)).clamp(1, 92),
            confidence: 90,
            primary_entity_type: "site".to_string(),
            primary_entity_value: source_host,
            related_entity_type: Some("site".to_string()),
            related_entity_value: Some(blacklist_domain),
            first_seen_at,
            last_seen_at,
            evidence,
        });
    }

    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| right.last_seen_at.cmp(&left.last_seen_at))
            .then_with(|| left.lead_key.cmp(&right.lead_key))
    });
    candidates.truncate(rule_limit as usize);

    Ok(candidates)
}

fn relationship_key(source_host: &str, target_host: &str) -> String {
    format!("{source_host}->{target_host}")
}

fn build_shared_ssh_lead_candidates(
    conn: &mut PgConnection,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    let rows = sql_query(
        "
        SELECT
            host_key_algorithm AS algorithm,
            host_key_fingerprint AS fingerprint,
            COUNT(DISTINCT host) AS host_count,
            COUNT(*) AS endpoint_count,
            MIN(COALESCE(last_success_at, last_attempt_at)) AS first_seen_at,
            MAX(COALESCE(last_success_at, last_attempt_at)) AS last_seen_at
        FROM host_ssh_observation
        WHERE host_key_algorithm IS NOT NULL
          AND host_key_algorithm != ''
          AND host_key_fingerprint IS NOT NULL
          AND host_key_fingerprint != ''
          AND last_success_at IS NOT NULL
        GROUP BY host_key_algorithm, host_key_fingerprint
        HAVING COUNT(DISTINCT host) > 1
        ORDER BY host_count DESC, endpoint_count DESC, last_seen_at DESC
        LIMIT $1
        ",
    )
    .bind::<BigInt, _>(rule_limit)
    .load::<SharedSshLeadRow>(conn)
    .context("error building shared ssh host-key lead candidates")?;

    rows.into_iter()
        .map(|row| {
            let key = format!("{}:{}", row.algorithm, row.fingerprint);
            let mut evidence = vec![evidence_candidate(
                "ssh_host_key",
                0,
                key.clone(),
                format!(
                    "SSH host key appears on {} hosts and {} endpoints",
                    row.host_count, row.endpoint_count
                ),
                row.last_seen_at.clone(),
            )];
            evidence.extend(load_ssh_endpoint_evidence(
                conn,
                &row.algorithm,
                &row.fingerprint,
                20,
            )?);
            evidence.extend(host_site_evidence_from_fingerprint_evidence(
                &evidence,
                "SSH key reused on host",
            ));
            Ok(IntelLeadCandidate {
                rule_id: "shared-ssh-host-key".to_string(),
                lead_key: format!(
                    "shared-ssh-host-key:{}:{}",
                    row.algorithm.to_ascii_lowercase(),
                    row.fingerprint.to_ascii_lowercase()
                ),
                title: format!("Shared SSH host key across {} hosts", row.host_count),
                summary: format!(
                    "{} {} appears on {} hosts and {} endpoints.",
                    row.algorithm, row.fingerprint, row.host_count, row.endpoint_count
                ),
                score: (58 + (row.host_count as i32 * 10)).clamp(1, 95),
                confidence: 88,
                primary_entity_type: "ssh_host_key".to_string(),
                primary_entity_value: key,
                related_entity_type: None,
                related_entity_value: None,
                first_seen_at: row.first_seen_at,
                last_seen_at: row.last_seen_at,
                evidence,
            })
        })
        .collect()
}

fn load_ssh_endpoint_evidence(
    conn: &mut PgConnection,
    algorithm: &str,
    fingerprint: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let rows = sql_query(
        "
        SELECT
            id AS source_id,
            host,
            port,
            COALESCE(last_success_at, last_attempt_at) AS observed_at
        FROM host_ssh_observation
        WHERE host_key_algorithm = $1
          AND host_key_fingerprint = $2
          AND last_success_at IS NOT NULL
        ORDER BY host ASC, port ASC
        LIMIT $3
        ",
    )
    .bind::<Text, _>(algorithm)
    .bind::<Text, _>(fingerprint)
    .bind::<BigInt, _>(limit)
    .load::<SshEndpointEvidenceRow>(conn)
    .context("error loading ssh endpoint lead evidence")?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let endpoint = format_service_endpoint_url("ssh", &row.host, row.port);
            evidence_candidate(
                "ssh_endpoint",
                row.source_id,
                endpoint.clone(),
                format!("SSH endpoint {endpoint} presented the shared host key"),
                row.observed_at,
            )
        })
        .collect())
}

fn build_shared_http_fingerprint_lead_candidates(
    conn: &mut PgConnection,
    column_name: &str,
    rule_id: &str,
    source_type: &str,
    label: &str,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    anyhow::ensure!(
        matches!(column_name, "header_fingerprint" | "favicon_hash"),
        "unsupported http fingerprint column"
    );
    let sql = format!(
        "
        SELECT
            {column_name} AS fingerprint_value,
            COUNT(DISTINCT host) AS host_count,
            COUNT(*) AS endpoint_count,
            MIN(COALESCE(last_success_at, last_attempt_at)) AS first_seen_at,
            MAX(COALESCE(last_success_at, last_attempt_at)) AS last_seen_at
        FROM host_http_observation
        WHERE {column_name} IS NOT NULL
          AND {column_name} != ''
          AND last_success_at IS NOT NULL
        GROUP BY {column_name}
        HAVING COUNT(DISTINCT host) > 1
        ORDER BY host_count DESC, endpoint_count DESC, last_seen_at DESC
        LIMIT $1
        "
    );
    let rows = sql_query(sql)
        .bind::<BigInt, _>(rule_limit)
        .load::<SharedFingerprintLeadRow>(conn)
        .with_context(|| format!("error building shared {label} lead candidates"))?;

    rows.into_iter()
        .map(|row| {
            let mut evidence = vec![evidence_candidate(
                source_type,
                0,
                row.fingerprint_value.clone(),
                format!(
                    "{label} appears on {} hosts and {} endpoints",
                    row.host_count, row.endpoint_count
                ),
                row.last_seen_at.clone(),
            )];
            evidence.extend(load_http_endpoint_fingerprint_evidence(
                conn,
                column_name,
                &row.fingerprint_value,
                20,
            )?);
            evidence.extend(host_site_evidence_from_fingerprint_evidence(
                &evidence,
                "HTTP fingerprint reused on host",
            ));
            Ok(IntelLeadCandidate {
                rule_id: rule_id.to_string(),
                lead_key: format!("{rule_id}:{}", row.fingerprint_value.to_ascii_lowercase()),
                title: format!("Shared {label} across {} hosts", row.host_count),
                summary: format!(
                    "{} appears on {} hosts and {} HTTP endpoints.",
                    row.fingerprint_value, row.host_count, row.endpoint_count
                ),
                score: (52 + (row.host_count as i32 * 9)).clamp(1, 92),
                confidence: 78,
                primary_entity_type: source_type.to_string(),
                primary_entity_value: row.fingerprint_value,
                related_entity_type: None,
                related_entity_value: None,
                first_seen_at: row.first_seen_at,
                last_seen_at: row.last_seen_at,
                evidence,
            })
        })
        .collect()
}

fn load_http_endpoint_fingerprint_evidence(
    conn: &mut PgConnection,
    column_name: &str,
    fingerprint_value: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let sql = format!(
        "
        SELECT
            id AS source_id,
            host,
            scheme || '://' || host || ':' || port AS endpoint,
            COALESCE(last_success_at, last_attempt_at) AS observed_at
        FROM host_http_observation
        WHERE {column_name} = $1
          AND last_success_at IS NOT NULL
        ORDER BY host ASC, scheme ASC, port ASC
        LIMIT $2
        "
    );
    let rows = sql_query(sql)
        .bind::<Text, _>(fingerprint_value)
        .bind::<BigInt, _>(limit)
        .load::<FingerprintEndpointEvidenceRow>(conn)
        .context("error loading http fingerprint endpoint evidence")?;

    Ok(rows
        .into_iter()
        .map(|row| {
            evidence_candidate(
                "http_endpoint",
                row.source_id,
                row.endpoint.clone(),
                format!(
                    "HTTP endpoint {} on {} had the shared fingerprint",
                    row.endpoint, row.host
                ),
                row.observed_at,
            )
        })
        .collect())
}

fn build_shared_service_fingerprint_lead_candidates(
    conn: &mut PgConnection,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    let rows = sql_query(
        "
        SELECT
            banner_fingerprint AS fingerprint_value,
            COUNT(DISTINCT host) AS host_count,
            COUNT(*) AS endpoint_count,
            MIN(COALESCE(last_success_at, last_attempt_at)) AS first_seen_at,
            MAX(COALESCE(last_success_at, last_attempt_at)) AS last_seen_at
        FROM host_service_observation
        WHERE banner_fingerprint IS NOT NULL
          AND banner_fingerprint != ''
          AND last_success_at IS NOT NULL
        GROUP BY banner_fingerprint
        HAVING COUNT(DISTINCT host) > 1
        ORDER BY host_count DESC, endpoint_count DESC, last_seen_at DESC
        LIMIT $1
        ",
    )
    .bind::<BigInt, _>(rule_limit)
    .load::<SharedFingerprintLeadRow>(conn)
    .context("error building shared service banner lead candidates")?;

    rows.into_iter()
        .map(|row| {
            let mut evidence = vec![evidence_candidate(
                "service_fingerprint",
                0,
                row.fingerprint_value.clone(),
                format!(
                    "Service banner fingerprint appears on {} hosts and {} endpoints",
                    row.host_count, row.endpoint_count
                ),
                row.last_seen_at.clone(),
            )];
            evidence.extend(load_service_endpoint_fingerprint_evidence(
                conn,
                &row.fingerprint_value,
                20,
            )?);
            evidence.extend(host_site_evidence_from_fingerprint_evidence(
                &evidence,
                "Service fingerprint reused on host",
            ));
            Ok(IntelLeadCandidate {
                rule_id: "shared-service-banner".to_string(),
                lead_key: format!(
                    "shared-service-banner:{}",
                    row.fingerprint_value.to_ascii_lowercase()
                ),
                title: format!("Shared service banner across {} hosts", row.host_count),
                summary: format!(
                    "{} appears on {} hosts and {} service endpoints.",
                    row.fingerprint_value, row.host_count, row.endpoint_count
                ),
                score: (50 + (row.host_count as i32 * 9)).clamp(1, 90),
                confidence: 76,
                primary_entity_type: "service_fingerprint".to_string(),
                primary_entity_value: row.fingerprint_value,
                related_entity_type: None,
                related_entity_value: None,
                first_seen_at: row.first_seen_at,
                last_seen_at: row.last_seen_at,
                evidence,
            })
        })
        .collect()
}

fn load_service_endpoint_fingerprint_evidence(
    conn: &mut PgConnection,
    fingerprint_value: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let rows = sql_query(
        "
        SELECT
            id AS source_id,
            host,
            service || '://' || host || ':' || port AS endpoint,
            COALESCE(last_success_at, last_attempt_at) AS observed_at
        FROM host_service_observation
        WHERE banner_fingerprint = $1
          AND last_success_at IS NOT NULL
        ORDER BY host ASC, service ASC, port ASC
        LIMIT $2
        ",
    )
    .bind::<Text, _>(fingerprint_value)
    .bind::<BigInt, _>(limit)
    .load::<FingerprintEndpointEvidenceRow>(conn)
    .context("error loading service fingerprint endpoint evidence")?;

    Ok(rows
        .into_iter()
        .map(|row| {
            evidence_candidate(
                "service_endpoint",
                row.source_id,
                row.endpoint.clone(),
                format!("Service endpoint {} had the shared banner", row.endpoint),
                row.observed_at,
            )
        })
        .collect())
}

fn host_site_evidence_from_fingerprint_evidence(
    evidence: &[IntelLeadEvidenceCandidate],
    label: &str,
) -> Vec<IntelLeadEvidenceCandidate> {
    let mut seen_hosts = HashSet::new();
    evidence
        .iter()
        .filter_map(|item| {
            let host = endpoint_host_from_source_key(&item.source_key)?;
            if seen_hosts.insert(host.clone()) {
                Some(evidence_candidate(
                    "site",
                    0,
                    host.clone(),
                    format!("{label} {host}"),
                    item.observed_at.clone(),
                ))
            } else {
                None
            }
        })
        .collect()
}

fn endpoint_host_from_source_key(source_key: &str) -> Option<String> {
    if let Ok(parsed) = Url::parse(source_key) {
        return parsed.host_str().map(|host| host.to_ascii_lowercase());
    }
    None
}

fn build_duplicate_site_title_lead_candidates(
    conn: &mut PgConnection,
    _since_scan_id: Option<i32>,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    let rows = sql_query(
        "
        SELECT
            p.title,
            COUNT(DISTINCT sp.host) AS host_count,
            COUNT(DISTINCT p.id) AS page_count,
            MIN(sp.first_found_at) AS first_seen_at,
            MAX(sp.last_scanned_at) AS last_seen_at
        FROM site_profile sp
        JOIN page p ON p.id = sp.source_page_id
        WHERE p.title != ''
          AND length(trim(p.title)) >= 8
        GROUP BY lower(trim(p.title)), p.title
        HAVING COUNT(DISTINCT sp.host) > 1
        ORDER BY host_count DESC, page_count DESC, last_seen_at DESC, p.title ASC
        LIMIT $1
        ",
    )
    .bind::<BigInt, _>(rule_limit)
    .load::<DuplicateSiteTitleLeadRow>(conn)
    .context("error building duplicate site title lead candidates")?;

    rows.into_iter()
        .map(|row| {
            let mut evidence = vec![evidence_candidate(
                "site_title",
                0,
                row.title.clone(),
                format!(
                    "Title appears on {} hosts and {} pages",
                    row.host_count, row.page_count
                ),
                row.last_seen_at.clone(),
            )];
            evidence.extend(load_duplicate_title_evidence(conn, &row.title, 20)?);
            let score = (42 + (row.host_count as i32 * 7)).clamp(1, 86);
            Ok(IntelLeadCandidate {
                rule_id: "duplicate-site-title".to_string(),
                lead_key: format!("duplicate-site-title:{}", row.title.to_ascii_lowercase()),
                title: format!("Duplicate site title across {} hosts", row.host_count),
                summary: format!(
                    "The title {:?} appears on {} hosts and {} pages.",
                    row.title, row.host_count, row.page_count
                ),
                score,
                confidence: 65,
                primary_entity_type: "site_title".to_string(),
                primary_entity_value: row.title,
                related_entity_type: None,
                related_entity_value: None,
                first_seen_at: row.first_seen_at,
                last_seen_at: row.last_seen_at,
                evidence,
            })
        })
        .collect()
}

fn load_duplicate_title_evidence(
    conn: &mut PgConnection,
    title: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let host_expr = sql_host_expr("p.url", conn);
    let rows = sql_query(format!(
        "
        SELECT
            p.id AS page_id,
            p.title AS page_title,
            p.url AS page_url,
            {host_expr} AS host,
            p.last_scanned_at AS observed_at
        FROM page p
        WHERE lower(trim(p.title)) = lower(trim($1))
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT $2
        "
    ))
    .bind::<Text, _>(title)
    .bind::<BigInt, _>(limit)
    .load::<LeadPageEvidenceRow>(conn)
    .context("error loading duplicate title evidence")?;

    Ok(page_evidence_candidates(rows, title))
}

fn build_category_change_lead_candidates(
    conn: &mut PgConnection,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    let rows = sql_query(
        "
        SELECT
            sp.host,
            sp.category AS current_category,
            'unknown' AS previous_category,
            p.id AS page_id,
            p.title AS page_title,
            p.url AS page_url,
            sp.last_classified_at AS observed_at
        FROM site_profile sp
        JOIN page p ON p.id = sp.source_page_id
        WHERE sp.category IN ('market', 'forum', 'escrow', 'shop')
          AND EXISTS (
              SELECT 1
              FROM page_classification pc
              WHERE pc.host = sp.host
                AND pc.category = 'unknown'
          )
        ORDER BY sp.last_classified_at DESC, sp.score DESC, sp.host ASC
        LIMIT $1
        ",
    )
    .bind::<BigInt, _>(rule_limit)
    .load::<CategoryChangeLeadRow>(conn)
    .context("error building host category change lead candidates")?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let evidence = vec![
                evidence_candidate(
                    "site",
                    0,
                    row.host.clone(),
                    format!(
                        "Host category changed materially from {} to {}",
                        row.previous_category, row.current_category
                    ),
                    row.observed_at.clone(),
                ),
                evidence_candidate(
                    "page",
                    row.page_id,
                    row.page_url.clone(),
                    format!(
                        "{} ({}) supports current category {}",
                        row.page_url, row.page_title, row.current_category
                    ),
                    row.observed_at.clone(),
                ),
            ];
            IntelLeadCandidate {
                rule_id: "host-category-change".to_string(),
                lead_key: format!("host-category-change:{}:{}", row.host, row.current_category),
                title: format!("{} now classified as {}", row.host, row.current_category),
                summary: format!(
                    "{} has evidence of prior unknown classification and is now {}.",
                    row.host, row.current_category
                ),
                score: 72,
                confidence: 70,
                primary_entity_type: "site".to_string(),
                primary_entity_value: row.host,
                related_entity_type: Some("category".to_string()),
                related_entity_value: Some(row.current_category),
                first_seen_at: row.observed_at.clone(),
                last_seen_at: row.observed_at,
                evidence,
            }
        })
        .collect())
}

fn build_high_degree_target_lead_candidates(
    conn: &mut PgConnection,
    since_scan_id: Option<i32>,
    rule_limit: i64,
) -> Result<Vec<IntelLeadCandidate>> {
    let source_host_expr = sql_host_expr("p.url", conn);
    let sql = format!(
        "
        WITH candidate_targets AS (
            SELECT DISTINCT target_host
            FROM (
                SELECT scan_id, target_host
                FROM page_scan_link
                WHERE target_host != ''
                  AND ($1 <= 0 OR scan_id > $1)
                ORDER BY scan_id DESC
                LIMIT $4
            ) recent_scan_link
        )
        SELECT
            pl.target_host,
            COUNT(DISTINCT {source_host_expr}) AS source_host_count,
            COUNT(*) AS reference_count,
            MIN(p.created_at) AS first_seen_at,
            MAX(p.last_scanned_at) AS last_seen_at
        FROM page_link pl
        JOIN page p ON p.id = pl.source_page_id
        WHERE pl.target_host != ''
          AND {source_host_expr} != ''
          AND {source_host_expr} != pl.target_host
          AND ($1 <= 0 OR pl.target_host IN (SELECT target_host FROM candidate_targets))
        GROUP BY pl.target_host
        HAVING COUNT(DISTINCT {source_host_expr}) >= $3
        ORDER BY source_host_count DESC, reference_count DESC, last_seen_at DESC, pl.target_host ASC
        LIMIT $2
        "
    );
    let rows = sql_query(sql)
        .bind::<diesel::sql_types::Integer, _>(since_scan_id.unwrap_or(0))
        .bind::<BigInt, _>(rule_limit)
        .bind::<BigInt, _>(HIGH_DEGREE_SOURCE_HOST_THRESHOLD)
        .bind::<BigInt, _>(candidate_scan_sample_limit(rule_limit))
        .load::<HighDegreeTargetLeadRow>(conn)
        .context("error building high-degree target lead candidates")?;

    rows.into_iter()
        .map(|row| {
            let mut evidence = vec![evidence_candidate(
                "site",
                0,
                row.target_host.clone(),
                format!(
                    "{} is referenced by {} source hosts",
                    row.target_host, row.source_host_count
                ),
                row.last_seen_at.clone(),
            )];
            evidence.extend(load_high_degree_relationship_evidence(
                conn,
                &row.target_host,
                20,
            )?);
            Ok(IntelLeadCandidate {
                rule_id: "high-degree-target".to_string(),
                lead_key: format!("high-degree-target:{}", row.target_host),
                title: format!("{} is a common target", row.target_host),
                summary: format!(
                    "{} is referenced {} times by {} distinct source hosts.",
                    row.target_host, row.reference_count, row.source_host_count
                ),
                score: (45 + (row.source_host_count as i32 * 5)).clamp(1, 90),
                confidence: 72,
                primary_entity_type: "site".to_string(),
                primary_entity_value: row.target_host,
                related_entity_type: Some("relationship".to_string()),
                related_entity_value: Some(format!("{} references", row.reference_count)),
                first_seen_at: row.first_seen_at,
                last_seen_at: row.last_seen_at,
                evidence,
            })
        })
        .collect()
}

fn load_high_degree_relationship_evidence(
    conn: &mut PgConnection,
    target_host: &str,
    limit: i64,
) -> Result<Vec<IntelLeadEvidenceCandidate>> {
    let source_host_expr = sql_host_expr("p.url", conn);
    let sql = format!(
        "
        SELECT
            {source_host_expr} AS source_host,
            pl.target_host,
            COUNT(*) AS reference_count,
            MAX(p.last_scanned_at) AS observed_at
        FROM page_link pl
        JOIN page p ON p.id = pl.source_page_id
        WHERE pl.target_host = $1
          AND {source_host_expr} != ''
          AND {source_host_expr} != pl.target_host
        GROUP BY {source_host_expr}, pl.target_host
        ORDER BY reference_count DESC, observed_at DESC, source_host ASC
        LIMIT $2
        "
    );
    let rows = sql_query(sql)
        .bind::<Text, _>(target_host)
        .bind::<BigInt, _>(limit)
        .load::<RelationshipEvidenceRow>(conn)
        .context("error loading high-degree relationship evidence")?;
    let mut evidence = Vec::new();
    for row in rows {
        evidence.push(evidence_candidate(
            "relationship",
            0,
            relationship_key(&row.source_host, &row.target_host),
            format!(
                "{} references {} {} times",
                row.source_host, row.target_host, row.reference_count
            ),
            row.observed_at.clone(),
        ));
        evidence.push(evidence_candidate(
            "site",
            0,
            row.source_host.clone(),
            format!(
                "{} references common target {}",
                row.source_host, row.target_host
            ),
            row.observed_at,
        ));
    }
    Ok(evidence)
}

pub fn search_pages(
    conn: &mut PgConnection,
    query: &str,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<SearchResult>> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        let pagination = normalize_pagination(requested_limit, requested_offset, 10, 50);
        return Ok(PaginatedResult {
            items: Vec::new(),
            total_count: 0,
            limit: pagination.limit,
            offset: pagination.offset,
        });
    }

    let pagination = normalize_pagination(requested_limit, requested_offset, 10, 50);
    if let Some(keyword_query) = parse_keyword_search_query(trimmed) {
        return search_sites_by_keyword_tag(conn, &keyword_query, pagination);
    }

    let pattern = format!("%{}%", escape_like(trimmed));
    let host_expr = sql_host_expr("p.url", conn);
    let title_match = sql_case_insensitive_match_expr("p.title", "$1", conn);
    let url_match = sql_case_insensitive_match_expr("p.url", "$2", conn);
    let language_match = sql_case_insensitive_match_expr("p.language", "$3", conn);
    let email_match = sql_case_insensitive_match_expr("pe.email", "$4", conn);
    let crypto_match =
        sql_case_insensitive_match_expr("(pc.asset_type || ':' || pc.reference)", "$5", conn);
    let detected_language_name_match =
        sql_case_insensitive_match_expr("pld.language_name", "$6", conn);
    let detected_language_code_match =
        sql_case_insensitive_match_expr("pld.language_code", "$7", conn);
    let topic_match = sql_case_insensitive_match_expr("pt.topic", "$8", conn);
    let count_sql = format!(
        "
        SELECT COUNT(*) AS count
        FROM page p
        WHERE (
            {title_match}
            OR {url_match}
            OR {language_match}
            OR EXISTS (
                SELECT 1
                FROM page_email pe
                WHERE pe.page_id = p.id
                    AND {email_match}
            )
            OR EXISTS (
                SELECT 1
                FROM page_crypto pc
                WHERE pc.page_id = p.id
                    AND {crypto_match}
            )
            OR EXISTS (
                SELECT 1
                FROM page_language_detection pld
                WHERE pld.page_id = p.id
                    AND ({detected_language_name_match} OR {detected_language_code_match})
            )
            OR EXISTS (
                SELECT 1
                FROM page_topic_tag pt
                WHERE pt.page_id = p.id
                    AND {topic_match}
            )
          )
        "
    );
    let total_count = sql_query(count_sql)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .get_result::<CountRow>(conn)
        .context("error counting search results")?
        .count;
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
        WHERE (
            {title_match}
            OR {url_match}
            OR {language_match}
            OR EXISTS (
                SELECT 1
                FROM page_email pe
                WHERE pe.page_id = p.id
                    AND {email_match}
            )
            OR EXISTS (
                SELECT 1
                FROM page_crypto pc
                WHERE pc.page_id = p.id
                    AND {crypto_match}
            )
            OR EXISTS (
                SELECT 1
                FROM page_language_detection pld
                WHERE pld.page_id = p.id
                    AND ({detected_language_name_match} OR {detected_language_code_match})
            )
            OR EXISTS (
                SELECT 1
                FROM page_topic_tag pt
                WHERE pt.page_id = p.id
                    AND {topic_match}
            )
          )
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT $9 OFFSET $10
    "
    );
    let rows = sql_query(sql)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<BigInt, _>(pagination.limit)
        .bind::<BigInt, _>(pagination.offset)
        .load::<SearchResultRow>(conn)
        .context("error searching pages")?;
    let site_profiles = load_site_profile_badges_by_hosts(
        conn,
        &rows.iter().map(|row| row.host.clone()).collect::<Vec<_>>(),
    )?;

    Ok(PaginatedResult {
        items: rows
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
            .collect(),
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

fn parse_keyword_search_query(query: &str) -> Option<String> {
    let trimmed = query.trim();
    let (prefix, remainder) = trimmed.split_once(':')?;
    if !prefix.eq_ignore_ascii_case("keyword") {
        return None;
    }

    let normalized = remainder.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn search_sites_by_keyword_tag(
    conn: &mut PgConnection,
    keyword_query: &str,
    pagination: PaginationInput,
) -> Result<PaginatedResult<SearchResult>> {
    let tag_pattern = format!("%keyword:{}%", escape_like(keyword_query));
    let host_expr = sql_host_expr("p.url", conn);
    let tag_match = sql_case_insensitive_match_expr("pkt.tag", "$1", conn);
    let count_sql = format!(
        "
        SELECT COUNT(*) AS count
        FROM (
            SELECT DISTINCT {host_expr} AS host
            FROM page_keyword_tag pkt
            JOIN page p ON p.id = pkt.page_id
            JOIN site_profile sp ON sp.host = {host_expr}
            WHERE sp.category = 'forum'
                AND {host_expr} != ''
                AND {tag_match}
        ) AS matching_hosts
        "
    );
    let total_count = sql_query(count_sql)
        .bind::<Text, _>(&tag_pattern)
        .get_result::<CountRow>(conn)
        .context("error counting keyword-tagged site search results")?
        .count;
    let sql = format!(
        "
        WITH matching_hosts AS (
            SELECT DISTINCT {host_expr} AS host
            FROM page_keyword_tag pkt
            JOIN page p ON p.id = pkt.page_id
            JOIN site_profile sp ON sp.host = {host_expr}
            WHERE sp.category = 'forum'
                AND {host_expr} != ''
                AND {tag_match}
        ),
        ranked_pages AS (
            SELECT
                p.id AS page_id,
                p.title,
                p.url,
                {host_expr} AS host,
                p.language,
                p.last_scanned_at AS scraped_at,
                ROW_NUMBER() OVER (
                    PARTITION BY {host_expr}
                    ORDER BY p.last_scanned_at DESC, p.id DESC
                ) AS row_number
            FROM page p
            JOIN matching_hosts mh ON mh.host = {host_expr}
        )
        SELECT
            page_id,
            title,
            url,
            host,
            language,
            scraped_at
        FROM ranked_pages
        WHERE row_number = 1
        ORDER BY scraped_at DESC, host ASC
        LIMIT $2 OFFSET $3
        "
    );
    let rows = sql_query(sql)
        .bind::<Text, _>(&tag_pattern)
        .bind::<BigInt, _>(pagination.limit)
        .bind::<BigInt, _>(pagination.offset)
        .load::<SearchResultRow>(conn)
        .context("error searching sites by keyword tag")?;
    let site_profiles = load_site_profile_badges_by_hosts(
        conn,
        &rows.iter().map(|row| row.host.clone()).collect::<Vec<_>>(),
    )?;

    Ok(PaginatedResult {
        items: rows
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
            .collect(),
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn list_email_entities(
    conn: &mut PgConnection,
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
        "SELECT COUNT(*) AS count FROM (SELECT email FROM page_email GROUP BY email) AS email_entities",
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
        LIMIT $1 OFFSET $2
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
    conn: &mut PgConnection,
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
        intel_leads: load_active_lead_badges_for_sources(
            conn,
            &[source_ref("email", 0, value.to_string())],
        )?,
    }))
}

pub fn list_crypto_entities(
    conn: &mut PgConnection,
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
        "SELECT COUNT(*) AS count FROM (SELECT asset_type, reference FROM page_crypto GROUP BY asset_type, reference) AS crypto_entities",
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
        LIMIT $1 OFFSET $2
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
    conn: &mut PgConnection,
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
        intel_leads: load_active_lead_badges_for_sources(
            conn,
            &[source_ref(
                "crypto",
                0,
                crypto_entity_key(asset_type, reference),
            )],
        )?,
    }))
}

pub fn list_recent_responding_hosts(
    conn: &mut PgConnection,
    recent_hours: i64,
    requested_limit: Option<i64>,
) -> Result<Vec<RecentHostCandidate>> {
    let recent_hours = recent_hours.clamp(1, 24 * 365);
    let limit = requested_limit.unwrap_or(200).clamp(1, 2_000);
    let host_expr = sql_host_expr("p.url", conn);
    let recent_cutoff = sql_timestamp_minus_hours_expr(conn, recent_hours);
    let query = format!(
        "
        SELECT
            {host_expr} AS host,
            MAX(p.last_scanned_at) AS last_scanned_at
        FROM page p
        WHERE {host_expr} != ''
            AND p.last_scanned_at >= {recent_cutoff}
        GROUP BY {host_expr}
        ORDER BY last_scanned_at DESC, host ASC
        LIMIT $1
        "
    );
    let rows = sql_query(query)
        .bind::<BigInt, _>(limit)
        .load::<RecentHostRow>(conn)
        .context("error loading recent responding hosts")?;

    Ok(rows
        .into_iter()
        .map(|row| RecentHostCandidate {
            host: row.host,
            last_scanned_at: row.last_scanned_at,
        })
        .collect())
}

fn normalize_observed_host(host_value: &str) -> String {
    host_value.trim().trim_end_matches('.').to_ascii_lowercase()
}

pub fn get_host_http_observation(
    conn: &mut PgConnection,
    host_value: &str,
    scheme_value: &str,
    port_value: i32,
) -> Result<Option<HostHttpObservationRecord>> {
    use crate::schema::host_http_observation::dsl as host_http_dsl;

    let normalized_host = normalize_observed_host(host_value);
    let normalized_scheme = scheme_value.trim().to_ascii_lowercase();
    if normalized_host.is_empty() || normalized_scheme.is_empty() {
        return Ok(None);
    }

    host_http_dsl::host_http_observation
        .filter(host_http_dsl::host.eq(normalized_host))
        .filter(host_http_dsl::scheme.eq(normalized_scheme))
        .filter(host_http_dsl::port.eq(port_value))
        .select(HostHttpObservationRecord::as_select())
        .first::<HostHttpObservationRecord>(conn)
        .optional()
        .context("error loading host http observation")
}

pub fn save_host_http_observation(
    conn: &mut PgConnection,
    observation: &NewHostHttpObservation,
) -> Result<()> {
    use crate::schema::host_http_observation::dsl as host_http_dsl;

    let normalized_host = normalize_observed_host(&observation.host);
    let normalized_scheme = observation.scheme.trim().to_ascii_lowercase();
    anyhow::ensure!(
        !normalized_host.is_empty(),
        "host http observation host must not be empty"
    );
    anyhow::ensure!(
        !normalized_scheme.is_empty(),
        "host http observation scheme must not be empty"
    );

    let existing =
        get_host_http_observation(conn, &normalized_host, &normalized_scheme, observation.port)?;
    let current_timestamp = current_timestamp_text(conn)?;
    let next_is_success = observation.status == SSH_STATUS_SUCCESS;
    let persisted = NewHostHttpObservation {
        host: normalized_host,
        scheme: normalized_scheme,
        port: observation.port,
        status: observation.status.clone(),
        http_status_code: if next_is_success {
            observation.http_status_code
        } else {
            observation
                .http_status_code
                .or(existing.as_ref().and_then(|row| row.http_status_code))
        },
        final_url: if next_is_success {
            observation.final_url.clone()
        } else {
            observation
                .final_url
                .clone()
                .or_else(|| existing.as_ref().and_then(|row| row.final_url.clone()))
        },
        server_header: if next_is_success {
            observation.server_header.clone()
        } else {
            observation
                .server_header
                .clone()
                .or_else(|| existing.as_ref().and_then(|row| row.server_header.clone()))
        },
        powered_by_header: if next_is_success {
            observation.powered_by_header.clone()
        } else {
            observation.powered_by_header.clone().or_else(|| {
                existing
                    .as_ref()
                    .and_then(|row| row.powered_by_header.clone())
            })
        },
        content_type_header: if next_is_success {
            observation.content_type_header.clone()
        } else {
            observation.content_type_header.clone().or_else(|| {
                existing
                    .as_ref()
                    .and_then(|row| row.content_type_header.clone())
            })
        },
        location_header: if next_is_success {
            observation.location_header.clone()
        } else {
            observation.location_header.clone().or_else(|| {
                existing
                    .as_ref()
                    .and_then(|row| row.location_header.clone())
            })
        },
        via_header: if next_is_success {
            observation.via_header.clone()
        } else {
            observation
                .via_header
                .clone()
                .or_else(|| existing.as_ref().and_then(|row| row.via_header.clone()))
        },
        alt_svc_header: if next_is_success {
            observation.alt_svc_header.clone()
        } else {
            observation
                .alt_svc_header
                .clone()
                .or_else(|| existing.as_ref().and_then(|row| row.alt_svc_header.clone()))
        },
        www_authenticate_header: if next_is_success {
            observation.www_authenticate_header.clone()
        } else {
            observation.www_authenticate_header.clone().or_else(|| {
                existing
                    .as_ref()
                    .and_then(|row| row.www_authenticate_header.clone())
            })
        },
        set_cookie_names: if next_is_success {
            observation.set_cookie_names.clone()
        } else {
            observation.set_cookie_names.clone().or_else(|| {
                existing
                    .as_ref()
                    .and_then(|row| row.set_cookie_names.clone())
            })
        },
        response_headers: if next_is_success {
            observation.response_headers.clone()
        } else {
            observation.response_headers.clone().or_else(|| {
                existing
                    .as_ref()
                    .and_then(|row| row.response_headers.clone())
            })
        },
        header_fingerprint: if next_is_success {
            observation.header_fingerprint.clone()
        } else {
            observation.header_fingerprint.clone().or_else(|| {
                existing
                    .as_ref()
                    .and_then(|row| row.header_fingerprint.clone())
            })
        },
        favicon_url: if next_is_success {
            observation.favicon_url.clone()
        } else {
            observation
                .favicon_url
                .clone()
                .or_else(|| existing.as_ref().and_then(|row| row.favicon_url.clone()))
        },
        favicon_hash: if next_is_success {
            observation.favicon_hash.clone()
        } else {
            observation
                .favicon_hash
                .clone()
                .or_else(|| existing.as_ref().and_then(|row| row.favicon_hash.clone()))
        },
        stack_versions: if next_is_success {
            observation.stack_versions.clone()
        } else {
            observation
                .stack_versions
                .clone()
                .or_else(|| existing.as_ref().and_then(|row| row.stack_versions.clone()))
        },
        exposed_resources: if next_is_success {
            observation.exposed_resources.clone()
        } else {
            observation.exposed_resources.clone().or_else(|| {
                existing
                    .as_ref()
                    .and_then(|row| row.exposed_resources.clone())
            })
        },
        last_error: observation.last_error.clone(),
        last_attempt_at: current_timestamp.clone(),
        last_success_at: if next_is_success {
            Some(current_timestamp.clone())
        } else {
            existing.and_then(|row| row.last_success_at)
        },
    };

    diesel::insert_into(host_http_dsl::host_http_observation)
        .values(&persisted)
        .on_conflict((
            host_http_dsl::host,
            host_http_dsl::scheme,
            host_http_dsl::port,
        ))
        .do_update()
        .set((
            host_http_dsl::status.eq(persisted.status.clone()),
            host_http_dsl::http_status_code.eq(persisted.http_status_code),
            host_http_dsl::final_url.eq(persisted.final_url.clone()),
            host_http_dsl::server_header.eq(persisted.server_header.clone()),
            host_http_dsl::powered_by_header.eq(persisted.powered_by_header.clone()),
            host_http_dsl::content_type_header.eq(persisted.content_type_header.clone()),
            host_http_dsl::location_header.eq(persisted.location_header.clone()),
            host_http_dsl::via_header.eq(persisted.via_header.clone()),
            host_http_dsl::alt_svc_header.eq(persisted.alt_svc_header.clone()),
            host_http_dsl::www_authenticate_header.eq(persisted.www_authenticate_header.clone()),
            host_http_dsl::set_cookie_names.eq(persisted.set_cookie_names.clone()),
            host_http_dsl::response_headers.eq(persisted.response_headers.clone()),
            host_http_dsl::header_fingerprint.eq(persisted.header_fingerprint.clone()),
            host_http_dsl::favicon_url.eq(persisted.favicon_url.clone()),
            host_http_dsl::favicon_hash.eq(persisted.favicon_hash.clone()),
            host_http_dsl::stack_versions.eq(persisted.stack_versions.clone()),
            host_http_dsl::exposed_resources.eq(persisted.exposed_resources.clone()),
            host_http_dsl::last_error.eq(persisted.last_error.clone()),
            host_http_dsl::last_attempt_at.eq(persisted.last_attempt_at.clone()),
            host_http_dsl::last_success_at.eq(persisted.last_success_at.clone()),
        ))
        .execute(conn)
        .context("error saving host http observation")?;

    Ok(())
}

pub fn get_host_tls_observation(
    conn: &mut PgConnection,
    host_value: &str,
    port_value: i32,
) -> Result<Option<HostTlsObservationRecord>> {
    use crate::schema::host_tls_observation::dsl as host_tls_dsl;

    let normalized_host = normalize_observed_host(host_value);
    if normalized_host.is_empty() {
        return Ok(None);
    }

    host_tls_dsl::host_tls_observation
        .filter(host_tls_dsl::host.eq(normalized_host))
        .filter(host_tls_dsl::port.eq(port_value))
        .select(HostTlsObservationRecord::as_select())
        .first::<HostTlsObservationRecord>(conn)
        .optional()
        .context("error loading host tls observation")
}

pub fn save_host_tls_observation(
    conn: &mut PgConnection,
    observation: &NewHostTlsObservation,
) -> Result<()> {
    use crate::schema::host_tls_observation::dsl as host_tls_dsl;

    let normalized_host = normalize_observed_host(&observation.host);
    anyhow::ensure!(
        !normalized_host.is_empty(),
        "host tls observation host must not be empty"
    );

    let existing = get_host_tls_observation(conn, &normalized_host, observation.port)?;
    let current_timestamp = current_timestamp_text(conn)?;
    let next_is_success = observation.status == SSH_STATUS_SUCCESS;
    let persisted = NewHostTlsObservation {
        host: normalized_host,
        port: observation.port,
        status: observation.status.clone(),
        certificate_sha256: if next_is_success {
            observation.certificate_sha256.clone()
        } else {
            observation.certificate_sha256.clone().or_else(|| {
                existing
                    .as_ref()
                    .and_then(|row| row.certificate_sha256.clone())
            })
        },
        last_error: observation.last_error.clone(),
        last_attempt_at: current_timestamp.clone(),
        last_success_at: if next_is_success {
            Some(current_timestamp.clone())
        } else {
            existing.and_then(|row| row.last_success_at)
        },
    };

    diesel::insert_into(host_tls_dsl::host_tls_observation)
        .values(&persisted)
        .on_conflict((host_tls_dsl::host, host_tls_dsl::port))
        .do_update()
        .set((
            host_tls_dsl::status.eq(persisted.status.clone()),
            host_tls_dsl::certificate_sha256.eq(persisted.certificate_sha256.clone()),
            host_tls_dsl::last_error.eq(persisted.last_error.clone()),
            host_tls_dsl::last_attempt_at.eq(persisted.last_attempt_at.clone()),
            host_tls_dsl::last_success_at.eq(persisted.last_success_at.clone()),
        ))
        .execute(conn)
        .context("error saving host tls observation")?;

    Ok(())
}

pub fn list_host_http_observations(
    conn: &mut PgConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<HostHttpObservationSummary>> {
    use crate::schema::host_http_observation::dsl as host_http_dsl;

    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = host_http_dsl::host_http_observation
        .filter(host_http_dsl::last_success_at.is_not_null())
        .select(count_star())
        .first::<i64>(conn)
        .context("error counting host http observations")?;
    let rows = host_http_dsl::host_http_observation
        .filter(host_http_dsl::last_success_at.is_not_null())
        .order(host_http_dsl::last_success_at.desc().nulls_last())
        .then_order_by(host_http_dsl::last_attempt_at.desc())
        .then_order_by(host_http_dsl::host.asc())
        .then_order_by(host_http_dsl::scheme.asc())
        .then_order_by(host_http_dsl::port.asc())
        .limit(pagination.limit)
        .offset(pagination.offset)
        .select(HostHttpObservationRecord::as_select())
        .load::<HostHttpObservationRecord>(conn)
        .context("error loading host http observations")?;
    let hosts = rows.iter().map(|row| row.host.clone()).collect::<Vec<_>>();
    let host_contexts = load_host_page_contexts_by_hosts(conn, &hosts)?;
    let site_categories = load_site_profile_badges_by_hosts(conn, &hosts)?;
    let items = rows
        .into_iter()
        .map(|row| {
            let port = row.port.to_string();
            let endpoint_url = format_endpoint_url(&row.scheme, &row.host, row.port);
            let detail_url = build_query_url(
                "/entities/http",
                &[
                    ("host", &row.host),
                    ("scheme", &row.scheme),
                    ("port", &port),
                ],
            );
            let host_context = host_contexts.get(&row.host);
            HostHttpObservationSummary {
                host: row.host.clone(),
                scheme: row.scheme,
                port: row.port,
                endpoint_url,
                status: row.status,
                http_status_code: row.http_status_code,
                final_url: row.final_url,
                server_header: row.server_header,
                header_fingerprint: row.header_fingerprint,
                favicon_hash: row.favicon_hash,
                stack_versions: row.stack_versions,
                last_success_at: row.last_success_at,
                detail_url,
                site_category: site_categories.get(&row.host).cloned(),
                source_page_id: host_context.map(|context| context.page_id),
                source_page_title: host_context.map(|context| context.page_title.clone()),
                source_page_url: host_context.map(|context| context.page_url.clone()),
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

pub fn get_host_http_observation_detail(
    conn: &mut PgConnection,
    host: &str,
    scheme: &str,
    port: i32,
) -> Result<Option<HostHttpObservationDetail>> {
    let Some(record) = get_host_http_observation(conn, host, scheme, port)? else {
        return Ok(None);
    };

    Ok(Some(build_host_http_observation_detail(conn, &record)?))
}

fn build_host_http_observation_detail(
    conn: &mut PgConnection,
    record: &HostHttpObservationRecord,
) -> Result<HostHttpObservationDetail> {
    let host_context =
        load_host_page_contexts_by_hosts(conn, &[record.host.clone()])?.remove(&record.host);
    let site_category =
        load_site_profile_badges_by_hosts(conn, &[record.host.clone()])?.remove(&record.host);
    let (tls_endpoint_url, tls_observation) = match record
        .final_url
        .as_deref()
        .and_then(endpoint_from_url)
        .filter(|endpoint| endpoint.scheme == "https")
    {
        Some(endpoint) => (
            Some(format_endpoint_url(
                &endpoint.scheme,
                &endpoint.host,
                endpoint.port,
            )),
            get_host_tls_observation(conn, &endpoint.host, endpoint.port)?,
        ),
        None => (None, None),
    };
    let port = record.port.to_string();
    let detail_url = build_query_url(
        "/entities/http",
        &[
            ("host", &record.host),
            ("scheme", &record.scheme),
            ("port", &port),
        ],
    );
    let mut lead_sources = vec![
        source_ref(
            "http_endpoint",
            record.id,
            format!("{}://{}:{}", record.scheme, record.host, record.port),
        ),
        source_ref("site", 0, record.host.clone()),
    ];
    if let Some(header_fingerprint) = record.header_fingerprint.as_ref() {
        lead_sources.push(source_ref(
            "http_fingerprint",
            0,
            header_fingerprint.clone(),
        ));
    }
    if let Some(favicon_hash) = record.favicon_hash.as_ref() {
        lead_sources.push(source_ref("favicon_hash", 0, favicon_hash.clone()));
    }
    let intel_leads = load_active_lead_badges_for_sources(conn, &lead_sources)?;

    Ok(HostHttpObservationDetail {
        host: record.host.clone(),
        scheme: record.scheme.clone(),
        port: record.port,
        endpoint_url: format_endpoint_url(&record.scheme, &record.host, record.port),
        status: record.status.clone(),
        http_status_code: record.http_status_code,
        final_url: record.final_url.clone(),
        server_header: record.server_header.clone(),
        powered_by_header: record.powered_by_header.clone(),
        content_type_header: record.content_type_header.clone(),
        location_header: record.location_header.clone(),
        via_header: record.via_header.clone(),
        alt_svc_header: record.alt_svc_header.clone(),
        www_authenticate_header: record.www_authenticate_header.clone(),
        set_cookie_names: record.set_cookie_names.clone(),
        response_headers: record.response_headers.clone(),
        header_fingerprint: record.header_fingerprint.clone(),
        favicon_url: record.favicon_url.clone(),
        favicon_hash: record.favicon_hash.clone(),
        stack_versions: record.stack_versions.clone(),
        exposed_resources: record.exposed_resources.clone(),
        last_error: record.last_error.clone(),
        last_attempt_at: record.last_attempt_at.clone(),
        last_success_at: record.last_success_at.clone(),
        detail_url,
        site_category,
        source_page_id: host_context.as_ref().map(|context| context.page_id),
        source_page_title: host_context
            .as_ref()
            .map(|context| context.page_title.clone()),
        source_page_url: host_context
            .as_ref()
            .map(|context| context.page_url.clone()),
        tls_endpoint_url,
        tls_observation,
        intel_leads,
    })
}

pub fn get_host_service_observation(
    conn: &mut PgConnection,
    host_value: &str,
    service_value: &str,
    port_value: i32,
) -> Result<Option<HostServiceObservationRecord>> {
    use crate::schema::host_service_observation::dsl as host_service_dsl;

    let normalized_host = normalize_observed_host(host_value);
    let normalized_service = service_value.trim().to_ascii_lowercase();
    if normalized_host.is_empty() || normalized_service.is_empty() {
        return Ok(None);
    }

    host_service_dsl::host_service_observation
        .filter(host_service_dsl::host.eq(normalized_host))
        .filter(host_service_dsl::service.eq(normalized_service))
        .filter(host_service_dsl::port.eq(port_value))
        .select(HostServiceObservationRecord::as_select())
        .first::<HostServiceObservationRecord>(conn)
        .optional()
        .context("error loading host service observation")
}

pub fn save_host_service_observation(
    conn: &mut PgConnection,
    observation: &NewHostServiceObservation,
) -> Result<()> {
    use crate::schema::host_service_observation::dsl as host_service_dsl;

    let normalized_host = normalize_observed_host(&observation.host);
    let normalized_service = observation.service.trim().to_ascii_lowercase();
    anyhow::ensure!(
        !normalized_host.is_empty(),
        "host service observation host must not be empty"
    );
    anyhow::ensure!(
        !normalized_service.is_empty(),
        "host service observation service must not be empty"
    );

    let existing = get_host_service_observation(
        conn,
        &normalized_host,
        &normalized_service,
        observation.port,
    )?;
    let current_timestamp = current_timestamp_text(conn)?;
    let next_is_success = observation.status == SSH_STATUS_SUCCESS;
    let persisted = NewHostServiceObservation {
        host: normalized_host,
        service: normalized_service,
        port: observation.port,
        status: observation.status.clone(),
        banner: if next_is_success {
            observation.banner.clone()
        } else {
            observation
                .banner
                .clone()
                .or_else(|| existing.as_ref().and_then(|row| row.banner.clone()))
        },
        banner_fingerprint: if next_is_success {
            observation.banner_fingerprint.clone()
        } else {
            observation.banner_fingerprint.clone().or_else(|| {
                existing
                    .as_ref()
                    .and_then(|row| row.banner_fingerprint.clone())
            })
        },
        last_error: observation.last_error.clone(),
        last_attempt_at: current_timestamp.clone(),
        last_success_at: if next_is_success {
            Some(current_timestamp.clone())
        } else {
            existing.and_then(|row| row.last_success_at)
        },
    };

    diesel::insert_into(host_service_dsl::host_service_observation)
        .values(&persisted)
        .on_conflict((
            host_service_dsl::host,
            host_service_dsl::service,
            host_service_dsl::port,
        ))
        .do_update()
        .set((
            host_service_dsl::status.eq(persisted.status.clone()),
            host_service_dsl::banner.eq(persisted.banner.clone()),
            host_service_dsl::banner_fingerprint.eq(persisted.banner_fingerprint.clone()),
            host_service_dsl::last_error.eq(persisted.last_error.clone()),
            host_service_dsl::last_attempt_at.eq(persisted.last_attempt_at.clone()),
            host_service_dsl::last_success_at.eq(persisted.last_success_at.clone()),
        ))
        .execute(conn)
        .context("error saving host service observation")?;

    Ok(())
}

pub fn list_host_service_observations(
    conn: &mut PgConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<HostServiceObservationSummary>> {
    use crate::schema::host_service_observation::dsl as host_service_dsl;

    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = host_service_dsl::host_service_observation
        .filter(host_service_dsl::last_success_at.is_not_null())
        .select(count_star())
        .first::<i64>(conn)
        .context("error counting host service observations")?;
    let rows = host_service_dsl::host_service_observation
        .filter(host_service_dsl::last_success_at.is_not_null())
        .order(host_service_dsl::last_success_at.desc().nulls_last())
        .then_order_by(host_service_dsl::last_attempt_at.desc())
        .then_order_by(host_service_dsl::service.asc())
        .then_order_by(host_service_dsl::host.asc())
        .then_order_by(host_service_dsl::port.asc())
        .select(HostServiceObservationRecord::as_select())
        .limit(pagination.limit)
        .offset(pagination.offset)
        .load::<HostServiceObservationRecord>(conn)
        .context("error loading host service observations")?;
    let hosts = rows.iter().map(|row| row.host.clone()).collect::<Vec<_>>();
    let host_contexts = load_host_page_contexts_by_hosts(conn, &hosts)?;
    let site_categories = load_site_profile_badges_by_hosts(conn, &hosts)?;
    let items = rows
        .into_iter()
        .map(|row| {
            let port = row.port.to_string();
            let endpoint_url = format_service_endpoint_url(&row.service, &row.host, row.port);
            let detail_url = build_query_url(
                "/entities/services",
                &[
                    ("host", &row.host),
                    ("service", &row.service),
                    ("port", &port),
                ],
            );
            let host_context = host_contexts.get(&row.host);
            HostServiceObservationSummary {
                host: row.host.clone(),
                service: row.service,
                port: row.port,
                endpoint_url,
                status: row.status,
                banner: row.banner,
                banner_fingerprint: row.banner_fingerprint,
                last_success_at: row.last_success_at,
                detail_url,
                site_category: site_categories.get(&row.host).cloned(),
                source_page_id: host_context.map(|context| context.page_id),
                source_page_title: host_context.map(|context| context.page_title.clone()),
                source_page_url: host_context.map(|context| context.page_url.clone()),
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

pub fn get_host_service_observation_detail(
    conn: &mut PgConnection,
    host: &str,
    service: &str,
    port: i32,
) -> Result<Option<HostServiceObservationDetail>> {
    let Some(record) = get_host_service_observation(conn, host, service, port)? else {
        return Ok(None);
    };

    let host_context =
        load_host_page_contexts_by_hosts(conn, &[record.host.clone()])?.remove(&record.host);
    let site_category =
        load_site_profile_badges_by_hosts(conn, &[record.host.clone()])?.remove(&record.host);
    let port_value = record.port.to_string();
    let detail_url = build_query_url(
        "/entities/services",
        &[
            ("host", &record.host),
            ("service", &record.service),
            ("port", &port_value),
        ],
    );
    let mut lead_sources = vec![
        source_ref(
            "service_endpoint",
            record.id,
            format_service_endpoint_url(&record.service, &record.host, record.port),
        ),
        source_ref("site", 0, record.host.clone()),
    ];
    if let Some(banner_fingerprint) = record.banner_fingerprint.as_ref() {
        lead_sources.push(source_ref(
            "service_fingerprint",
            0,
            banner_fingerprint.clone(),
        ));
    }
    let intel_leads = load_active_lead_badges_for_sources(conn, &lead_sources)?;

    Ok(Some(HostServiceObservationDetail {
        host: record.host.clone(),
        service: record.service.clone(),
        port: record.port,
        endpoint_url: format_service_endpoint_url(&record.service, &record.host, record.port),
        status: record.status.clone(),
        banner: record.banner.clone(),
        banner_fingerprint: record.banner_fingerprint.clone(),
        last_error: record.last_error.clone(),
        last_attempt_at: record.last_attempt_at.clone(),
        last_success_at: record.last_success_at.clone(),
        detail_url,
        site_category,
        source_page_id: host_context.as_ref().map(|context| context.page_id),
        source_page_title: host_context
            .as_ref()
            .map(|context| context.page_title.clone()),
        source_page_url: host_context
            .as_ref()
            .map(|context| context.page_url.clone()),
        intel_leads,
    }))
}

pub fn get_host_ssh_observation(
    conn: &mut PgConnection,
    host_value: &str,
    port_value: i32,
) -> Result<Option<HostSshObservationRecord>> {
    use crate::schema::host_ssh_observation::dsl as host_ssh_dsl;

    let normalized_host = host_value.trim().trim_end_matches('.').to_ascii_lowercase();
    if normalized_host.is_empty() {
        return Ok(None);
    }

    host_ssh_dsl::host_ssh_observation
        .filter(host_ssh_dsl::host.eq(normalized_host))
        .filter(host_ssh_dsl::port.eq(port_value))
        .select(HostSshObservationRecord::as_select())
        .first::<HostSshObservationRecord>(conn)
        .optional()
        .context("error loading host ssh observation")
}

pub fn save_host_ssh_observation(
    conn: &mut PgConnection,
    observation: &NewHostSshObservation,
) -> Result<()> {
    use crate::schema::host_ssh_observation::dsl as host_ssh_dsl;

    let normalized_host = observation
        .host
        .trim()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    anyhow::ensure!(
        !normalized_host.is_empty(),
        "host ssh observation host must not be empty"
    );

    let existing = get_host_ssh_observation(conn, &normalized_host, observation.port)?;
    let current_timestamp = current_timestamp_text(conn)?;
    let next_is_success = observation.status == SSH_STATUS_SUCCESS;
    let next_host_key_algorithm = if next_is_success {
        observation.host_key_algorithm.clone()
    } else {
        observation.host_key_algorithm.clone().or_else(|| {
            existing
                .as_ref()
                .and_then(|row| row.host_key_algorithm.clone())
        })
    };
    let next_host_key = if next_is_success {
        observation.host_key.clone()
    } else {
        observation
            .host_key
            .clone()
            .or_else(|| existing.as_ref().and_then(|row| row.host_key.clone()))
    };
    let next_host_key_fingerprint = if next_is_success {
        observation.host_key_fingerprint.clone()
    } else {
        observation.host_key_fingerprint.clone().or_else(|| {
            existing
                .as_ref()
                .and_then(|row| row.host_key_fingerprint.clone())
        })
    };
    let persisted = NewHostSshObservation {
        host: normalized_host,
        port: observation.port,
        status: observation.status.clone(),
        host_key_algorithm: next_host_key_algorithm,
        host_key: next_host_key,
        host_key_fingerprint: next_host_key_fingerprint,
        server_banner: observation.server_banner.clone(),
        last_error: observation.last_error.clone(),
        last_attempt_at: current_timestamp.clone(),
        last_success_at: if next_is_success {
            Some(current_timestamp.clone())
        } else {
            existing.and_then(|row| row.last_success_at)
        },
    };

    diesel::insert_into(host_ssh_dsl::host_ssh_observation)
        .values(&persisted)
        .on_conflict((host_ssh_dsl::host, host_ssh_dsl::port))
        .do_update()
        .set((
            host_ssh_dsl::status.eq(persisted.status.clone()),
            host_ssh_dsl::host_key_algorithm.eq(persisted.host_key_algorithm.clone()),
            host_ssh_dsl::host_key.eq(persisted.host_key.clone()),
            host_ssh_dsl::host_key_fingerprint.eq(persisted.host_key_fingerprint.clone()),
            host_ssh_dsl::server_banner.eq(persisted.server_banner.clone()),
            host_ssh_dsl::last_error.eq(persisted.last_error.clone()),
            host_ssh_dsl::last_attempt_at.eq(persisted.last_attempt_at.clone()),
            host_ssh_dsl::last_success_at.eq(persisted.last_success_at.clone()),
        ))
        .execute(conn)
        .context("error saving host ssh observation")?;

    Ok(())
}

pub fn list_ssh_host_keys(
    conn: &mut PgConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<SshHostKeySummary>> {
    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = scalar_count(
        conn,
        "
        SELECT COUNT(*) AS count
        FROM (
            SELECT host_key_algorithm, host_key_fingerprint
            FROM host_ssh_observation
            WHERE host_key_algorithm IS NOT NULL
                AND host_key_algorithm != ''
                AND host_key_fingerprint IS NOT NULL
                AND host_key_fingerprint != ''
                AND last_success_at IS NOT NULL
            GROUP BY host_key_algorithm, host_key_fingerprint
        ) AS ssh_host_keys
        ",
    )
    .context("error counting ssh host keys")?;
    let rows = sql_query(
        "
        SELECT
            host_key_algorithm AS algorithm,
            host_key_fingerprint AS fingerprint,
            COUNT(DISTINCT host) AS host_count,
            COUNT(*) AS endpoint_count,
            MAX(last_success_at) AS last_success_at
        FROM host_ssh_observation
        WHERE host_key_algorithm IS NOT NULL
            AND host_key_algorithm != ''
            AND host_key_fingerprint IS NOT NULL
            AND host_key_fingerprint != ''
            AND last_success_at IS NOT NULL
        GROUP BY host_key_algorithm, host_key_fingerprint
        ORDER BY host_count DESC, endpoint_count DESC, last_success_at DESC, algorithm ASC, fingerprint ASC
        LIMIT $1 OFFSET $2
        ",
    )
    .bind::<BigInt, _>(pagination.limit)
    .bind::<BigInt, _>(pagination.offset)
    .load::<SshHostKeySummaryRow>(conn)
    .context("error loading ssh host key summaries")?;
    let items = rows
        .into_iter()
        .map(|row| SshHostKeySummary {
            detail_url: build_query_url(
                "/entities/ssh",
                &[
                    ("algorithm", &row.algorithm),
                    ("fingerprint", &row.fingerprint),
                ],
            ),
            algorithm: row.algorithm,
            fingerprint: row.fingerprint,
            host_count: row.host_count.max(0) as usize,
            endpoint_count: row.endpoint_count.max(0) as usize,
            last_success_at: row.last_success_at,
        })
        .collect();

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn get_ssh_host_key_detail(
    conn: &mut PgConnection,
    algorithm: &str,
    fingerprint: &str,
) -> Result<Option<SshHostKeyDetail>> {
    use crate::schema::host_ssh_observation::dsl as host_ssh_dsl;

    let rows = host_ssh_dsl::host_ssh_observation
        .filter(host_ssh_dsl::host_key_algorithm.eq(algorithm))
        .filter(host_ssh_dsl::host_key_fingerprint.eq(fingerprint))
        .filter(host_ssh_dsl::last_success_at.is_not_null())
        .order(host_ssh_dsl::host.asc())
        .then_order_by(host_ssh_dsl::port.asc())
        .select(HostSshObservationRecord::as_select())
        .load::<HostSshObservationRecord>(conn)
        .context("error loading ssh host key detail")?;
    if rows.is_empty() {
        return Ok(None);
    }

    let unique_hosts = rows.iter().map(|row| row.host.clone()).collect::<Vec<_>>();
    let site_profiles = load_site_profiles_by_hosts(conn, &unique_hosts)?;
    let endpoints = rows
        .iter()
        .map(|row| {
            let site_profile = site_profiles.get(&row.host);
            SshHostKeyEndpoint {
                host: row.host.clone(),
                port: row.port,
                status: row.status.clone(),
                last_error: row.last_error.clone(),
                last_attempt_at: row.last_attempt_at.clone(),
                last_success_at: row.last_success_at.clone(),
                server_banner: row.server_banner.clone(),
                host_key: row.host_key.clone(),
                site_category: site_profile
                    .map(|profile| site_category_badge(&profile.category, &profile.confidence)),
                source_page_id: site_profile.and_then(|profile| profile.source_page_id),
                source_page_title: site_profile
                    .and_then(|profile| profile.source_page_title.clone()),
                source_page_url: site_profile.and_then(|profile| profile.source_page_url.clone()),
            }
        })
        .collect::<Vec<_>>();

    Ok(Some(SshHostKeyDetail {
        algorithm: algorithm.to_string(),
        fingerprint: fingerprint.to_string(),
        host_count: endpoints
            .iter()
            .map(|item| item.host.clone())
            .collect::<HashSet<_>>()
            .len(),
        endpoint_count: endpoints.len(),
        endpoints,
        intel_leads: load_active_lead_badges_for_sources(
            conn,
            &[source_ref(
                "ssh_host_key",
                0,
                format!("{algorithm}:{fingerprint}"),
            )],
        )?,
    }))
}

pub fn list_top_sites_by_email_refs(
    conn: &mut PgConnection,
    requested_limit: Option<i64>,
) -> Result<Vec<TopSiteEntry>> {
    let limit = requested_limit
        .unwrap_or(DEFAULT_TOP_SITE_LIMIT)
        .clamp(1, MAX_PAGE_LIMIT);
    let host_expr = sql_host_expr("p.url", conn);
    let query = format!(
        "
        SELECT
            {host_expr} AS host,
            COUNT(*) AS count,
            MAX(p.last_scanned_at) AS last_scanned_at
        FROM page_email pe
        JOIN page p ON p.id = pe.page_id
        WHERE {host_expr} != ''
        GROUP BY {host_expr}
        ORDER BY count DESC, last_scanned_at DESC, host ASC
        LIMIT $1
        "
    );
    load_top_site_entries_from_query(conn, &query, limit)
}

pub fn list_top_sites_by_crypto_refs(
    conn: &mut PgConnection,
    requested_limit: Option<i64>,
) -> Result<Vec<TopSiteEntry>> {
    let limit = requested_limit
        .unwrap_or(DEFAULT_TOP_SITE_LIMIT)
        .clamp(1, MAX_PAGE_LIMIT);
    let host_expr = sql_host_expr("p.url", conn);
    let query = format!(
        "
        SELECT
            {host_expr} AS host,
            COUNT(*) AS count,
            MAX(p.last_scanned_at) AS last_scanned_at
        FROM page_crypto pc
        JOIN page p ON p.id = pc.page_id
        WHERE {host_expr} != ''
        GROUP BY {host_expr}
        ORDER BY count DESC, last_scanned_at DESC, host ASC
        LIMIT $1
        "
    );
    load_top_site_entries_from_query(conn, &query, limit)
}

pub fn list_top_sites_by_outgoing_links(
    conn: &mut PgConnection,
    requested_limit: Option<i64>,
) -> Result<Vec<TopSiteEntry>> {
    let limit = requested_limit
        .unwrap_or(DEFAULT_TOP_SITE_LIMIT)
        .clamp(1, MAX_PAGE_LIMIT);
    let host_expr = sql_host_expr("p.url", conn);
    let query = format!(
        "
        SELECT
            {host_expr} AS host,
            COUNT(*) AS count,
            MAX(p.last_scanned_at) AS last_scanned_at
        FROM page_link pl
        JOIN page p ON p.id = pl.source_page_id
        WHERE {host_expr} != ''
        GROUP BY {host_expr}
        ORDER BY count DESC, last_scanned_at DESC, host ASC
        LIMIT $1
        "
    );
    load_top_site_entries_from_query(conn, &query, limit)
}

pub fn list_top_referenced_sites(
    conn: &mut PgConnection,
    requested_limit: Option<i64>,
) -> Result<Vec<TopSiteEntry>> {
    let limit = requested_limit
        .unwrap_or(DEFAULT_TOP_SITE_LIMIT)
        .clamp(1, MAX_PAGE_LIMIT);
    let host_expr = sql_host_expr("p.url", conn);
    let query = format!(
        "
        WITH target_recency AS (
            SELECT
                {host_expr} AS host,
                MAX(p.last_scanned_at) AS last_scanned_at
            FROM page p
            WHERE {host_expr} != ''
            GROUP BY {host_expr}
        )
        SELECT
            pl.target_host AS host,
            COUNT(*) AS count,
            MAX(target_recency.last_scanned_at) AS last_scanned_at
        FROM page_link pl
        LEFT JOIN target_recency ON target_recency.host = pl.target_host
        WHERE pl.target_host != ''
        GROUP BY pl.target_host
        ORDER BY count DESC, COALESCE(MAX(target_recency.last_scanned_at), '') DESC, pl.target_host ASC
        LIMIT $1
        "
    );
    load_top_site_entries_from_query(conn, &query, limit)
}

pub fn list_site_category_distribution(
    conn: &mut PgConnection,
) -> Result<Vec<CategoryDistributionEntry>> {
    let rows = sql_query(
        "
        SELECT
            category,
            COUNT(*) AS host_count
        FROM site_profile
        WHERE category != ''
        GROUP BY category
        ORDER BY host_count DESC, category ASC
        ",
    )
    .load::<CategoryDistributionRow>(conn)
    .context("error loading site category distribution")?;

    Ok(rows
        .into_iter()
        .map(|row| CategoryDistributionEntry {
            label: site_category_label(&row.category).to_string(),
            category: row.category,
            host_count: row.host_count.max(0) as usize,
        })
        .collect())
}

pub fn list_site_category_timeline(conn: &mut PgConnection) -> Result<Vec<CategoryTimelinePoint>> {
    let day_expr = sql_day_bucket_expr("sp.created_at", conn);
    let query = format!(
        "
        SELECT
            {day_expr} AS day,
            sp.category,
            COUNT(*) AS host_count
        FROM site_profile sp
        WHERE sp.category != ''
        GROUP BY {day_expr}, sp.category
        ORDER BY day ASC, sp.category ASC
        "
    );
    let rows = sql_query(query)
        .load::<CategoryTimelineRow>(conn)
        .context("error loading site category timeline")?;

    Ok(rows
        .into_iter()
        .map(|row| CategoryTimelinePoint {
            day: row.day,
            label: site_category_label(&row.category).to_string(),
            category: row.category,
            host_count: row.host_count.max(0) as usize,
        })
        .collect())
}

fn keyword_tag_label(tag: &str) -> String {
    tag.strip_prefix("keyword:").unwrap_or(tag).to_string()
}

pub fn list_site_keyword_distribution(
    conn: &mut PgConnection,
) -> Result<Vec<CategoryDistributionEntry>> {
    let host_expr = sql_host_without_port_expr("p.url", conn);
    let query = format!(
        "
        WITH keyword_pages AS (
            SELECT
                {host_expr} AS host,
                pkt.tag AS category
            FROM page_keyword_tag pkt
            JOIN page p ON p.id = pkt.page_id
            WHERE pkt.tag LIKE 'keyword:%'
        ),
        keyword_hosts AS (
            SELECT
                kp.host,
                kp.category
            FROM keyword_pages kp
            JOIN site_profile sp
              ON sp.host = kp.host
             AND sp.category = 'forum'
            WHERE kp.host != ''
            GROUP BY kp.host, kp.category
        )
        SELECT
            keyword_hosts.category,
            COUNT(*) AS host_count
        FROM keyword_hosts
        GROUP BY keyword_hosts.category
        ORDER BY host_count DESC, keyword_hosts.category ASC
        "
    );
    let rows = sql_query(query)
        .load::<CategoryDistributionRow>(conn)
        .context("error loading site keyword distribution")?;

    Ok(rows
        .into_iter()
        .map(|row| CategoryDistributionEntry {
            label: keyword_tag_label(&row.category),
            category: row.category,
            host_count: row.host_count.max(0) as usize,
        })
        .collect())
}

pub fn list_site_keyword_timeline(conn: &mut PgConnection) -> Result<Vec<CategoryTimelinePoint>> {
    let host_expr = sql_host_without_port_expr("p.url", conn);
    let day_expr = sql_day_bucket_expr("keyword_hosts.first_seen_at", conn);
    let query = format!(
        "
        WITH keyword_pages AS (
            SELECT
                {host_expr} AS host,
                pkt.tag AS category,
                pkt.created_at
            FROM page_keyword_tag pkt
            JOIN page p ON p.id = pkt.page_id
            WHERE pkt.tag LIKE 'keyword:%'
        ),
        keyword_hosts AS (
            SELECT
                kp.host,
                kp.category,
                MIN(kp.created_at) AS first_seen_at
            FROM keyword_pages kp
            JOIN site_profile sp
              ON sp.host = kp.host
             AND sp.category = 'forum'
            WHERE kp.host != ''
            GROUP BY kp.host, kp.category
        )
        SELECT
            {day_expr} AS day,
            keyword_hosts.category,
            COUNT(*) AS host_count
        FROM keyword_hosts
        GROUP BY {day_expr}, keyword_hosts.category
        ORDER BY day ASC, keyword_hosts.category ASC
        "
    );
    let rows = sql_query(query)
        .load::<CategoryTimelineRow>(conn)
        .context("error loading site keyword timeline")?;

    Ok(rows
        .into_iter()
        .map(|row| CategoryTimelinePoint {
            day: row.day,
            label: keyword_tag_label(&row.category),
            category: row.category,
            host_count: row.host_count.max(0) as usize,
        })
        .collect())
}

pub fn count_discovered_service_endpoints(conn: &mut PgConnection) -> Result<i64> {
    scalar_count(
        conn,
        "
        SELECT
            (
                SELECT COUNT(*)
                FROM host_http_observation
                WHERE last_success_at IS NOT NULL
            )
            + (
                SELECT COUNT(*)
                FROM host_service_observation
                WHERE last_success_at IS NOT NULL
            )
            + (
                SELECT COUNT(*)
                FROM host_ssh_observation
                WHERE last_success_at IS NOT NULL
            ) AS count
        ",
    )
    .context("error counting discovered service endpoints")
}

pub fn list_page_language_distribution(
    conn: &mut PgConnection,
) -> Result<Vec<CategoryDistributionEntry>> {
    let rows = sql_query(
        "
        SELECT
            language_name AS category,
            COUNT(*) AS host_count
        FROM (
            SELECT
                COALESCE(
                    NULLIF(pld.language_name, ''),
                    NULLIF(p.language, ''),
                    'Unknown'
                ) AS language_name
            FROM page p
            LEFT JOIN page_language_detection pld ON pld.page_id = p.id
        ) AS page_languages
        WHERE language_name != ''
        GROUP BY language_name
        ORDER BY host_count DESC, language_name ASC
        ",
    )
    .load::<CategoryDistributionRow>(conn)
    .context("error loading page language distribution")?;

    Ok(rows
        .into_iter()
        .map(|row| CategoryDistributionEntry {
            label: row.category.clone(),
            category: row.category,
            host_count: row.host_count.max(0) as usize,
        })
        .collect())
}

pub fn list_page_topic_distribution(
    conn: &mut PgConnection,
) -> Result<Vec<CategoryDistributionEntry>> {
    let rows = sql_query(
        "
        SELECT
            topic AS category,
            COUNT(DISTINCT page_id) AS host_count
        FROM page_topic_tag
        WHERE topic != ''
        GROUP BY topic
        ORDER BY host_count DESC, topic ASC
        ",
    )
    .load::<CategoryDistributionRow>(conn)
    .context("error loading page topic distribution")?;

    Ok(rows
        .into_iter()
        .map(|row| CategoryDistributionEntry {
            label: page_topic_label(&row.category),
            category: row.category,
            host_count: row.host_count.max(0) as usize,
        })
        .collect())
}

pub fn list_page_topic_timeline(conn: &mut PgConnection) -> Result<Vec<CategoryTimelinePoint>> {
    let day_expr = sql_day_bucket_expr("pt.created_at", conn);
    let query = format!(
        "
        SELECT
            {day_expr} AS day,
            pt.topic AS category,
            COUNT(DISTINCT pt.page_id) AS host_count
        FROM page_topic_tag pt
        WHERE pt.topic != ''
        GROUP BY {day_expr}, pt.topic
        ORDER BY day ASC, pt.topic ASC
        "
    );
    let rows = sql_query(query)
        .load::<CategoryTimelineRow>(conn)
        .context("error loading page topic timeline")?;

    Ok(rows
        .into_iter()
        .map(|row| CategoryTimelinePoint {
            day: row.day,
            label: page_topic_label(&row.category),
            category: row.category,
            host_count: row.host_count.max(0) as usize,
        })
        .collect())
}

pub fn list_site_relationships(
    conn: &mut PgConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<SiteRelationship>> {
    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = scalar_count(
        conn,
        "
            SELECT COUNT(*) AS count
            FROM (
                SELECT 1
                FROM page_link pl
                WHERE pl.target_host != ''
                    AND pl.source_host != ''
                    AND pl.source_host != lower(pl.target_host)
                GROUP BY pl.source_host, lower(pl.target_host)
            ) AS site_relationships
            ",
    )
    .context("error counting site relationships")?;
    let query = "
        SELECT
            pl.source_host,
            lower(pl.target_host) AS target_host,
            COUNT(*) AS reference_count
        FROM page_link pl
        WHERE pl.target_host != ''
            AND pl.source_host != ''
            AND pl.source_host != lower(pl.target_host)
        GROUP BY pl.source_host, lower(pl.target_host)
        ORDER BY reference_count DESC, source_host ASC, target_host ASC
        LIMIT $1 OFFSET $2
        ";
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
    let mut items = Vec::new();
    for row in rows {
        let blacklist_match_domain =
            find_matching_blacklist_domain(&row.target_host, &blacklist_domains);
        let intel_leads = load_active_lead_badges_for_sources(
            conn,
            &[source_ref(
                "relationship",
                0,
                relationship_key(&row.source_host, &row.target_host),
            )],
        )?;
        items.push(SiteRelationship {
            source_site_category: site_profiles.get(&row.source_host).cloned(),
            target_site_category: site_profiles.get(&row.target_host).cloned(),
            source_host: row.source_host,
            target_host: row.target_host,
            reference_count: row.reference_count.max(0) as usize,
            is_blacklisted: blacklist_match_domain.is_some(),
            blacklist_match_domain,
            intel_leads,
        });
    }

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn get_site_relationship_graph(
    conn: &mut PgConnection,
    focus_host: Option<&str>,
    requested_depth: Option<i64>,
    requested_limit: Option<i64>,
) -> Result<SiteRelationshipGraph> {
    // Set a 30-second timeout for relationship graph queries to prevent long-running queries
    // on large databases from hanging the frontend
    conn.batch_execute("SET LOCAL statement_timeout = '30s'")
        .context("error setting statement timeout for relationship graph")?;

    let limit = requested_limit
        .unwrap_or(DEFAULT_RELATIONSHIP_GRAPH_LIMIT)
        .clamp(MIN_RELATIONSHIP_GRAPH_LIMIT, MAX_PAGE_LIMIT);
    let depth = requested_depth
        .unwrap_or(DEFAULT_RELATIONSHIP_GRAPH_DEPTH)
        .clamp(1, MAX_RELATIONSHIP_GRAPH_DEPTH);
    let normalized_focus = focus_host
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .map(|host| host.trim_end_matches('.').to_ascii_lowercase());
    let rows = match normalized_focus.as_deref() {
        Some(host) => load_focused_site_relationship_graph_edges(conn, host, depth, limit)?,
        None => load_overview_site_relationship_graph_edges(conn, limit)?,
    };

    build_site_relationship_graph(conn, normalized_focus, depth as usize, rows)
}

fn load_overview_site_relationship_graph_edges(
    conn: &mut PgConnection,
    limit: i64,
) -> Result<Vec<SiteRelationshipGraphEdgeRow>> {
    // For overview mode, we just want the top cross-site relationships.
    // The existing indexes on source_host and target_host should help here.
    let query = "
        SELECT
            pl.source_host,
            lower(pl.target_host) AS target_host,
            COUNT(*) AS reference_count,
            0 AS depth
        FROM page_link pl
        WHERE pl.target_host != ''
          AND pl.source_host != ''
          AND pl.source_host != lower(pl.target_host)
        GROUP BY pl.source_host, lower(pl.target_host)
        ORDER BY reference_count DESC, source_host ASC, target_host ASC
        LIMIT $1
        ";

    sql_query(query)
        .bind::<BigInt, _>(limit)
        .load::<SiteRelationshipGraphEdgeRow>(conn)
        .context("error loading relationship overview graph edges")
}

fn load_focused_site_relationship_graph_edges(
    conn: &mut PgConnection,
    focus_host: &str,
    depth: i64,
    limit: i64,
) -> Result<Vec<SiteRelationshipGraphEdgeRow>> {
    let query = "
        WITH RECURSIVE walk(source_host, target_host, reference_count, depth, path) AS (
            SELECT
                pl.source_host,
                lower(pl.target_host) AS target_host,
                COUNT(*) AS reference_count,
                1 AS depth,
                ARRAY[$1::text, pl.source_host]::text[] AS path
            FROM page_link pl
            WHERE pl.target_host != ''
              AND lower(pl.target_host) = $1
              AND pl.source_host != ''
              AND pl.source_host != $1
              AND pl.source_host != lower(pl.target_host)
            GROUP BY pl.source_host, lower(pl.target_host)
            UNION ALL
            SELECT
                r.source_host,
                r.target_host,
                r.reference_count,
                walk.depth + 1 AS depth,
                array_append(walk.path, r.source_host)
            FROM walk
            JOIN LATERAL (
                SELECT
                    pl.source_host,
                    lower(pl.target_host) AS target_host,
                    COUNT(*) AS reference_count
                FROM page_link pl
                WHERE pl.target_host != ''
                  AND lower(pl.target_host) = walk.source_host
                  AND pl.source_host != ''
                  AND pl.source_host != lower(pl.target_host)
                GROUP BY pl.source_host, lower(pl.target_host)
            ) r ON true
            WHERE walk.depth < $2
              AND r.source_host <> ALL(walk.path)
        )
        SELECT
            source_host,
            target_host,
            MAX(reference_count) AS reference_count,
            MIN(depth) AS depth
        FROM walk
        GROUP BY source_host, target_host
        ORDER BY depth ASC, reference_count DESC, source_host ASC, target_host ASC
        LIMIT $3
        ";

    sql_query(query)
        .bind::<Text, _>(focus_host)
        .bind::<diesel::sql_types::Integer, _>(depth as i32)
        .bind::<BigInt, _>(limit)
        .load::<SiteRelationshipGraphEdgeRow>(conn)
        .context("error loading focused relationship graph edges")
}

fn build_site_relationship_graph(
    conn: &mut PgConnection,
    focus_host: Option<String>,
    depth: usize,
    rows: Vec<SiteRelationshipGraphEdgeRow>,
) -> Result<SiteRelationshipGraph> {
    let blacklist_domains = load_blacklist_domains(conn)?;
    let mut hosts = HashSet::<String>::new();
    let mut incoming_counts = HashMap::<String, usize>::new();
    let mut outgoing_counts = HashMap::<String, usize>::new();
    let mut node_depths = HashMap::<String, usize>::new();

    if let Some(host) = focus_host.as_ref() {
        hosts.insert(host.clone());
        node_depths.insert(host.clone(), 0);
    }

    for row in &rows {
        let edge_depth = row.depth.max(0) as usize;
        let reference_count = row.reference_count.max(0) as usize;
        hosts.insert(row.source_host.clone());
        hosts.insert(row.target_host.clone());
        *outgoing_counts.entry(row.source_host.clone()).or_insert(0) += reference_count;
        *incoming_counts.entry(row.target_host.clone()).or_insert(0) += reference_count;
        merge_min_depth(&mut node_depths, &row.source_host, edge_depth);
        merge_min_depth(
            &mut node_depths,
            &row.target_host,
            edge_depth.saturating_sub(1),
        );
    }

    let mut host_list = hosts.into_iter().collect::<Vec<_>>();
    host_list.sort();
    let site_profiles = load_site_profile_badges_by_hosts(conn, &host_list)?;
    let mut nodes = host_list
        .into_iter()
        .map(|host| {
            let blacklist_match_domain = find_matching_blacklist_domain(&host, &blacklist_domains);
            SiteRelationshipGraphNode {
                site_category: site_profiles.get(&host).cloned(),
                incoming_count: incoming_counts.get(&host).copied().unwrap_or(0),
                outgoing_count: outgoing_counts.get(&host).copied().unwrap_or(0),
                is_focus: focus_host.as_deref() == Some(host.as_str()),
                is_blacklisted: blacklist_match_domain.is_some(),
                blacklist_match_domain,
                depth: node_depths.get(&host).copied().unwrap_or(0),
                host,
            }
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| {
        left.depth
            .cmp(&right.depth)
            .then_with(|| right.is_focus.cmp(&left.is_focus))
            .then_with(|| {
                (right.incoming_count + right.outgoing_count)
                    .cmp(&(left.incoming_count + left.outgoing_count))
            })
            .then_with(|| left.host.cmp(&right.host))
    });

    let edges = rows
        .into_iter()
        .map(|row| {
            let is_blacklisted =
                find_matching_blacklist_domain(&row.target_host, &blacklist_domains).is_some();
            SiteRelationshipGraphEdge {
                relationship_key: relationship_key(&row.source_host, &row.target_host),
                source_host: row.source_host,
                target_host: row.target_host,
                reference_count: row.reference_count.max(0) as usize,
                depth: row.depth.max(0) as usize,
                is_blacklisted,
            }
        })
        .collect::<Vec<_>>();

    Ok(SiteRelationshipGraph {
        mode: if focus_host.is_some() {
            "focus".to_string()
        } else {
            "overview".to_string()
        },
        focus_host,
        depth,
        nodes,
        edges,
    })
}

fn merge_min_depth(depths: &mut HashMap<String, usize>, host: &str, depth: usize) {
    depths
        .entry(host.to_string())
        .and_modify(|current| *current = (*current).min(depth))
        .or_insert(depth);
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
    conn: &mut PgConnection,
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

    let scan_stats = load_site_scan_stats(conn, host_value)?;
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
        page_count: scan_stats.page_count,
        first_found_at: scan_stats.first_found_at,
        last_scanned_at: scan_stats.last_scanned_at,
        evidence: serialize_evidence(&evidence),
        source_page_id: source_row.map(|row| row.page_id),
    })
}

fn load_site_scan_stats(conn: &mut PgConnection, host_value: &str) -> Result<SiteScanStats> {
    let host_expr = sql_host_expr("p.url", conn);
    let query = format!(
        "
        SELECT
            COUNT(*) AS page_count,
            COALESCE(MIN(p.created_at), {current_timestamp}) AS first_found_at,
            COALESCE(MAX(p.last_scanned_at), {current_timestamp}) AS last_scanned_at
        FROM page p
        WHERE {host_expr} = $1
        ",
        current_timestamp = sql_current_timestamp_expr(conn),
    );
    let row = sql_query(query)
        .bind::<Text, _>(host_value)
        .get_result::<SiteScanStatsRow>(conn)
        .context("error loading site scan stats")?;

    Ok(SiteScanStats {
        page_count: row.page_count.max(0) as i32,
        first_found_at: row.first_found_at,
        last_scanned_at: row.last_scanned_at,
    })
}

fn site_profile_record_from_row(row: SiteProfileListRow) -> SiteProfileRecord {
    SiteProfileRecord {
        id: row.id,
        host: row.host,
        category: row.category,
        confidence: row.confidence,
        score: row.score,
        page_count: row.page_count,
        first_found_at: row.first_found_at,
        last_scanned_at: row.last_scanned_at,
        evidence: row.evidence,
        source_page_id: row.source_page_id,
        last_classified_at: row.last_classified_at,
        created_at: row.created_at,
    }
}

fn load_site_profile_by_host(
    conn: &mut PgConnection,
    host: &str,
) -> Result<Option<SiteProfileSummary>> {
    Ok(load_site_profiles_by_hosts(conn, &[host.to_string()])?.remove(host))
}

fn load_site_profile_badges_by_hosts(
    conn: &mut PgConnection,
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
    conn: &mut PgConnection,
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
    conn: &mut PgConnection,
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
    conn: &mut PgConnection,
    records: &[SiteProfileRecord],
) -> Result<Vec<SiteProfileSummary>> {
    let lead_badges_by_host = load_lead_badges_by_site_hosts(
        conn,
        &records
            .iter()
            .map(|record| record.host.clone())
            .collect::<Vec<_>>(),
    )?;
    let keyword_tags_by_host = load_forum_keyword_tags_by_hosts(
        conn,
        &records
            .iter()
            .map(|record| record.host.clone())
            .collect::<Vec<_>>(),
    )?;
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
                keyword_tags: if record.category == CATEGORY_FORUM {
                    keyword_tags_by_host
                        .get(&record.host)
                        .cloned()
                        .unwrap_or_default()
                } else {
                    Vec::new()
                },
                page_count: record.page_count.max(0) as usize,
                first_found_at: record.first_found_at.clone(),
                source_page_id: record.source_page_id,
                source_page_title: source_page.map(|page| page.title.clone()),
                source_page_url: source_page.map(|page| page.url.clone()),
                last_scanned_at: record.last_scanned_at.clone(),
                last_classified_at: record.last_classified_at.clone(),
                intel_leads: lead_badges_by_host
                    .get(&record.host)
                    .cloned()
                    .unwrap_or_default(),
            }
        })
        .collect())
}

fn load_lead_badges_by_site_hosts(
    conn: &mut PgConnection,
    hosts: &[String],
) -> Result<HashMap<String, Vec<IntelLeadBadge>>> {
    let mut output = HashMap::new();
    for host in hosts
        .iter()
        .map(|host| host.trim().to_ascii_lowercase())
        .filter(|host| !host.is_empty())
        .collect::<HashSet<_>>()
    {
        output.insert(
            host.clone(),
            load_active_lead_badges_for_sources(conn, &[source_ref("site", 0, host.clone())])?,
        );
    }
    Ok(output)
}

fn load_top_site_entries_from_query(
    conn: &mut PgConnection,
    query: &str,
    limit: i64,
) -> Result<Vec<TopSiteEntry>> {
    let rows = sql_query(query)
        .bind::<BigInt, _>(limit)
        .load::<HostMetricSummaryRow>(conn)
        .context("error loading top site leaderboard rows")?;
    build_top_site_entries(conn, rows)
}

fn build_top_site_entries(
    conn: &mut PgConnection,
    rows: Vec<HostMetricSummaryRow>,
) -> Result<Vec<TopSiteEntry>> {
    let hosts = rows.iter().map(|row| row.host.clone()).collect::<Vec<_>>();
    let host_contexts = load_host_page_contexts_by_hosts(conn, &hosts)?;
    let site_categories = load_site_profile_badges_by_hosts(conn, &hosts)?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let host_context = host_contexts.get(&row.host);
            TopSiteEntry {
                host: row.host.clone(),
                count: row.count.max(0) as usize,
                last_scanned_at: row
                    .last_scanned_at
                    .or_else(|| host_context.map(|context| context.last_scanned_at.clone())),
                page_id: host_context.map(|context| context.page_id),
                page_title: host_context.map(|context| context.page_title.clone()),
                page_url: host_context.map(|context| context.page_url.clone()),
                site_category: site_categories.get(&row.host).cloned(),
            }
        })
        .collect())
}

fn load_host_page_contexts_by_hosts(
    conn: &mut PgConnection,
    hosts: &[String],
) -> Result<HashMap<String, HostPageContext>> {
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

    let host_expr = sql_host_expr("p.url", conn);
    let host_literals = unique_hosts
        .iter()
        .map(|host| quote_sql_text_literal(host))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "
        WITH ranked_pages AS (
            SELECT
                p.id AS page_id,
                p.title AS page_title,
                p.url AS page_url,
                {host_expr} AS host,
                p.last_scanned_at,
                ROW_NUMBER() OVER (
                    PARTITION BY {host_expr}
                    ORDER BY p.last_scanned_at DESC, p.id DESC
                ) AS row_number
            FROM page p
            WHERE {host_expr} IN ({host_literals})
        )
        SELECT
            page_id,
            page_title,
            page_url,
            host,
            last_scanned_at
        FROM ranked_pages
        WHERE row_number = 1
        "
    );
    Ok(sql_query(query)
        .load::<HostPageContextRow>(conn)
        .context("error loading host page contexts")?
        .into_iter()
        .map(|row| {
            (
                row.host,
                HostPageContext {
                    page_id: row.page_id,
                    page_title: row.page_title,
                    page_url: row.page_url,
                    last_scanned_at: row.last_scanned_at,
                },
            )
        })
        .collect())
}

fn load_forum_keyword_tags_by_hosts(
    conn: &mut PgConnection,
    hosts: &[String],
) -> Result<HashMap<String, Vec<String>>> {
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

    let host_expr = sql_host_expr("p.url", conn);
    let host_literals = unique_hosts
        .iter()
        .map(|host| quote_sql_text_literal(host))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "
        SELECT
            {host_expr} AS host,
            pkt.tag
        FROM page_keyword_tag pkt
        JOIN page p ON p.id = pkt.page_id
        WHERE {host_expr} IN ({host_literals})
        GROUP BY {host_expr}, pkt.tag
        ORDER BY {host_expr} ASC, pkt.tag ASC
        "
    );
    let rows = sql_query(query)
        .load::<HostTagRow>(conn)
        .context("error loading forum keyword tags by host")?;
    let mut grouped = HashMap::<String, Vec<String>>::new();
    for row in rows {
        grouped.entry(row.host).or_default().push(row.tag);
    }
    Ok(grouped)
}

fn load_page_by_id(conn: &mut PgConnection, page_id: i32) -> Result<Option<Page>> {
    use crate::schema::page::dsl as page_dsl;

    page_dsl::page
        .filter(page_dsl::id.eq(page_id))
        .select(Page::as_select())
        .first::<Page>(conn)
        .optional()
        .context("error loading page by id")
}

fn load_scan_observation_sets(
    conn: &mut PgConnection,
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
    conn: &mut PgConnection,
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
    conn: &mut PgConnection,
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
    conn: &mut PgConnection,
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
    conn: &mut PgConnection,
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

fn load_blacklist_domains(conn: &mut PgConnection) -> Result<Vec<String>> {
    Ok(list_domain_blacklist_rules(conn)?
        .into_iter()
        .map(|rule| rule.domain)
        .collect())
}

fn list_enabled_auto_blacklist_rules(conn: &mut PgConnection) -> Result<Vec<AutoBlacklistRule>> {
    use crate::schema::auto_blacklist_rule::dsl as rule_dsl;

    rule_dsl::auto_blacklist_rule
        .filter(rule_dsl::enabled.eq(true))
        .order(rule_dsl::rule_type.asc())
        .then_order_by(rule_dsl::value.asc())
        .then_order_by(rule_dsl::id.asc())
        .select(AutoBlacklistRule::as_select())
        .load::<AutoBlacklistRule>(conn)
        .context("error loading enabled auto blacklist rules")
}

fn apply_auto_blacklist_rules_for_page(
    conn: &mut PgConnection,
    snapshot: &PageSnapshot,
    stored_page_id: i32,
    site_profile_record: &NewSiteProfile,
) -> Result<()> {
    let rules = list_enabled_auto_blacklist_rules(conn)?;
    if rules.is_empty() {
        return Ok(());
    }
    let domain = normalize_blacklist_domain(&site_profile_record.host)?;
    let corpus = snapshot.keyword_corpus.to_ascii_lowercase();

    for rule in rules {
        let evidence = match rule.rule_type.as_str() {
            AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY
                if site_profile_record.category == rule.value =>
            {
                Some(format!(
                    "site category {}",
                    site_category_label(&site_profile_record.category)
                ))
            }
            AUTO_BLACKLIST_RULE_TYPE_KEYWORD
                if auto_blacklist_keyword_matches(&corpus, &rule.value) =>
            {
                Some(format!(
                    "keyword phrase '{}' matched scan corpus",
                    rule.value
                ))
            }
            _ => None,
        };

        let Some(evidence) = evidence else {
            continue;
        };
        add_domain_blacklist_entry(conn, &domain)?;
        insert_auto_blacklist_event(
            conn,
            rule.id,
            &domain,
            Some(stored_page_id),
            &rule.rule_type,
            &rule.value,
            &truncate(&evidence, 240),
        )?;
    }

    Ok(())
}

fn insert_auto_blacklist_event(
    conn: &mut PgConnection,
    rule_id: i32,
    domain: &str,
    source_page_id: Option<i32>,
    rule_type: &str,
    matched_value: &str,
    evidence: &str,
) -> Result<usize> {
    diesel::insert_into(crate::schema::auto_blacklist_event::table)
        .values(NewAutoBlacklistEvent {
            rule_id,
            domain,
            source_page_id,
            rule_type,
            matched_value,
            evidence,
        })
        .on_conflict_do_nothing()
        .execute(conn)
        .context("error saving auto blacklist event")
}

fn load_auto_blacklist_profile_backfill_rows(
    conn: &mut PgConnection,
    limit: i64,
) -> Result<Vec<SiteProfileRecord>> {
    use crate::schema::site_profile::dsl as site_profile_dsl;

    site_profile_dsl::site_profile
        .order(site_profile_dsl::last_scanned_at.desc())
        .then_order_by(site_profile_dsl::id.desc())
        .limit(limit)
        .select(SiteProfileRecord::as_select())
        .load::<SiteProfileRecord>(conn)
        .context("error loading site profiles for auto blacklist backfill")
}

fn load_auto_blacklist_page_backfill_rows(
    conn: &mut PgConnection,
    limit: i64,
) -> Result<Vec<Page>> {
    use crate::schema::page::dsl as page_dsl;

    page_dsl::page
        .order(page_dsl::last_scanned_at.desc())
        .then_order_by(page_dsl::id.desc())
        .limit(limit)
        .select(Page::as_select())
        .load::<Page>(conn)
        .context("error loading pages for auto blacklist backfill")
}

fn load_page_keyword_tags_by_page_ids(
    conn: &mut PgConnection,
    page_ids: &[i32],
) -> Result<HashMap<i32, Vec<String>>> {
    use crate::schema::page_keyword_tag::dsl as keyword_tag_dsl;

    if page_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = keyword_tag_dsl::page_keyword_tag
        .filter(keyword_tag_dsl::page_id.eq_any(page_ids))
        .select(PageKeywordTag::as_select())
        .load::<PageKeywordTag>(conn)
        .context("error loading page keyword tags for auto blacklist backfill")?;
    let mut grouped = HashMap::<i32, Vec<String>>::new();
    for row in rows {
        grouped.entry(row.page_id).or_default().push(row.tag);
    }
    Ok(grouped)
}

fn auto_blacklist_backfill_corpus(page: &Page, tags: &[String]) -> String {
    let tag_text = tags.join("\n");
    [
        page.url.as_str(),
        page.title.as_str(),
        page.links.as_str(),
        page.emails.as_str(),
        page.coins.as_str(),
        page.language.as_str(),
        tag_text.as_str(),
    ]
    .join("\n")
    .to_ascii_lowercase()
}

fn auto_blacklist_keyword_matches(haystack: &str, phrase: &str) -> bool {
    !phrase.is_empty() && haystack.to_ascii_lowercase().contains(phrase)
}

fn valid_auto_blacklist_site_categories() -> [&'static str; 13] {
    [
        CATEGORY_SEARCH_ENGINE,
        CATEGORY_FORUM,
        CATEGORY_MARKET,
        CATEGORY_DIRECTORY,
        CATEGORY_WIKI,
        CATEGORY_BLOG,
        CATEGORY_ESCROW,
        CATEGORY_SHOP,
        CATEGORY_VENDOR_PAGE,
        CATEGORY_DOCS,
        CATEGORY_INDEXER,
        CATEGORY_CONTENT,
        CATEGORY_SEO_SPAM,
    ]
}

fn normalize_auto_blacklist_site_category(raw_category: &str) -> Result<String> {
    let normalized = raw_category
        .trim()
        .to_ascii_lowercase()
        .replace('_', "-")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("-");
    anyhow::ensure!(
        !normalized.is_empty(),
        "auto blacklist site category must not be empty"
    );

    for category in valid_auto_blacklist_site_categories() {
        let label = site_category_label(category)
            .to_ascii_lowercase()
            .replace(' ', "-");
        if normalized == category || normalized == label {
            return Ok(category.to_string());
        }
    }

    anyhow::bail!("unsupported auto blacklist site category: {raw_category}")
}

fn normalize_auto_blacklist_keyword(raw_keyword: &str) -> Result<String> {
    let normalized = raw_keyword
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase();
    anyhow::ensure!(
        !normalized.is_empty(),
        "auto blacklist keyword must not be empty"
    );
    Ok(normalized)
}

fn load_grouped_target_host_counts(
    conn: &mut PgConnection,
    query: &str,
) -> Result<Vec<TargetHostCountRow>> {
    sql_query(query)
        .load::<TargetHostCountRow>(conn)
        .context("error loading grouped target host counts")
}

fn load_known_targets_by_url(
    conn: &mut PgConnection,
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

fn load_pages_by_ids(conn: &mut PgConnection, page_ids: &[i32]) -> Result<Vec<Page>> {
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

fn scalar_count(conn: &mut PgConnection, query: &str) -> Result<i64> {
    Ok(sql_query(query)
        .get_result::<CountRow>(conn)
        .context("error loading count result")?
        .count)
}

fn scalar_nullable_text(conn: &mut PgConnection, query: &str) -> Result<Option<String>> {
    Ok(sql_query(query)
        .get_result::<NullableTextRow>(conn)
        .context("error loading text result")?
        .value)
}

fn current_timestamp_text(conn: &mut PgConnection) -> Result<String> {
    scalar_nullable_text(
        conn,
        &format!("SELECT {} AS value", sql_current_timestamp_expr(conn)),
    )
    .context("error loading current timestamp")?
    .context("current timestamp query returned no value")
}

fn sql_current_timestamp_expr(conn: &impl AppConnection) -> &'static str {
    match conn.dialect() {
        SqlDialect::Postgres => "to_char(timezone('UTC', now()), 'YYYY-MM-DD HH24:MI:SS')",
        SqlDialect::Sqlite => "CURRENT_TIMESTAMP",
    }
}

fn sql_timestamp_plus_minutes_expr(conn: &impl AppConnection, minutes: i32) -> String {
    match conn.dialect() {
        SqlDialect::Postgres => format!(
            "to_char(timezone('UTC', now()) + INTERVAL '{} minutes', 'YYYY-MM-DD HH24:MI:SS')",
            minutes
        ),
        SqlDialect::Sqlite => format!("datetime(CURRENT_TIMESTAMP, '+{} minutes')", minutes),
    }
}

fn sql_timestamp_minus_hours_expr(conn: &impl AppConnection, hours: i64) -> String {
    match conn.dialect() {
        SqlDialect::Postgres => format!(
            "to_char(timezone('UTC', now()) - INTERVAL '{} hours', 'YYYY-MM-DD HH24:MI:SS')",
            hours
        ),
        SqlDialect::Sqlite => format!("datetime(CURRENT_TIMESTAMP, '-{} hours')", hours),
    }
}

fn sql_now_comparison_expr(column: &str, conn: &impl AppConnection) -> String {
    format!("{column} <= {}", sql_current_timestamp_expr(conn))
}

fn sql_case_insensitive_match_expr(
    column: &str,
    placeholder: &str,
    conn: &impl AppConnection,
) -> String {
    match conn.dialect() {
        SqlDialect::Postgres => format!("{column} ILIKE {placeholder} ESCAPE '\\'"),
        SqlDialect::Sqlite => {
            format!("{column} LIKE {placeholder} ESCAPE '\\' COLLATE NOCASE")
        }
    }
}

fn sql_day_bucket_expr(column: &str, conn: &impl AppConnection) -> String {
    match conn.dialect() {
        SqlDialect::Postgres => format!("LEFT({column}, 10)"),
        SqlDialect::Sqlite => format!("substr({column}, 1, 10)"),
    }
}

fn host_from_url(value: &str) -> String {
    Url::parse(value)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_ascii_lowercase()))
        .unwrap_or_default()
}

fn endpoint_from_url(value: &str) -> Option<UrlEndpoint> {
    let parsed = Url::parse(value).ok()?;
    let host = parsed
        .host_str()?
        .trim()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    if host.is_empty() {
        return None;
    }

    let scheme = parsed.scheme().to_ascii_lowercase();
    let port = i32::from(parsed.port_or_known_default()?);

    Some(UrlEndpoint { host, scheme, port })
}

fn format_endpoint_url(scheme: &str, host: &str, port: i32) -> String {
    let mut output = format!("{scheme}://{host}");
    let is_default_port = (scheme == "http" && port == 80) || (scheme == "https" && port == 443);
    if !is_default_port {
        output.push(':');
        output.push_str(&port.to_string());
    }
    output
}

fn format_service_endpoint_url(service: &str, host: &str, port: i32) -> String {
    let mut output = format!("{service}://{host}");
    output.push(':');
    output.push_str(&port.to_string());
    output
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

fn quote_sql_text_literal(input: &str) -> String {
    format!("'{}'", input.replace('\'', "''"))
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
            | CATEGORY_SEO_SPAM
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
        CATEGORY_SEO_SPAM => "SEO Spam",
        _ => "Unknown",
    }
}

fn page_topic_label(topic: &str) -> String {
    let label = topic
        .split(|character: char| character == '-' || character == '_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => {
                    let mut segment = String::new();
                    segment.extend(first.to_uppercase());
                    segment.push_str(chars.as_str());
                    segment
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    if label.is_empty() {
        "Unknown".to_string()
    } else {
        label
    }
}

fn sql_host_expr(column: &str, conn: &impl AppConnection) -> String {
    match conn.dialect() {
        SqlDialect::Postgres => format!(
            "
            CASE
                WHEN position('://' IN {column}) > 0 THEN
                    split_part(split_part({column}, '://', 2), '/', 1)
                ELSE ''
            END
            "
        ),
        SqlDialect::Sqlite => format!(
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
        ),
    }
}

fn sql_host_without_port_expr(column: &str, conn: &impl AppConnection) -> String {
    let host_expr = sql_host_expr(column, conn);
    match conn.dialect() {
        SqlDialect::Postgres => format!("lower(split_part(({host_expr}), ':', 1))"),
        SqlDialect::Sqlite => format!(
            "
            lower(
                CASE
                    WHEN instr(({host_expr}), ':') > 0 THEN substr(({host_expr}), 1, instr(({host_expr}), ':') - 1)
                    ELSE ({host_expr})
                END
            )
            "
        ),
    }
}

fn truncate(input: &str, max_len: usize) -> String {
    input.chars().take(max_len).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::connection::SimpleConnection;
    use std::env;
    use std::fs;
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(QueryableByName)]
    struct JournalModeRow {
        #[diesel(sql_type = Text)]
        journal_mode: String,
    }

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
            CREATE TABLE auto_blacklist_rule(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              rule_type VARCHAR NOT NULL,
              value VARCHAR NOT NULL,
              label VARCHAR NOT NULL DEFAULT '',
              enabled BOOLEAN NOT NULL DEFAULT 1,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(rule_type, value)
            );
            CREATE TABLE auto_blacklist_event(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              rule_id INTEGER NOT NULL,
              domain VARCHAR NOT NULL,
              source_page_id INTEGER,
              rule_type VARCHAR NOT NULL,
              matched_value VARCHAR NOT NULL,
              evidence VARCHAR NOT NULL DEFAULT '',
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE UNIQUE INDEX idx_auto_blacklist_event_unique_page
              ON auto_blacklist_event(domain, rule_id, COALESCE(source_page_id, 0));
            CREATE TABLE forum_keyword_rule(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              label VARCHAR NOT NULL,
              pattern VARCHAR NOT NULL,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(label, pattern)
            );
            CREATE TABLE host_ssh_observation(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              host VARCHAR NOT NULL,
              port INTEGER NOT NULL,
              status VARCHAR NOT NULL,
              host_key_algorithm VARCHAR,
              host_key VARCHAR,
              host_key_fingerprint VARCHAR,
              server_banner VARCHAR,
              last_error VARCHAR,
              last_attempt_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              last_success_at VARCHAR,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(host, port)
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
              source_host VARCHAR NOT NULL DEFAULT '',
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
            CREATE TABLE page_keyword_tag(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              page_id INTEGER NOT NULL,
              tag VARCHAR NOT NULL,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(page_id, tag)
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
              first_found_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              last_scanned_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
            language_detection: LanguageDetection::unknown(),
            keyword_corpus: "http://alpha.onion\nAlpha Market\nmarketplace listings".to_string(),
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
            topic_observations: Vec::new(),
        }
    }

    fn beta_snapshot() -> PageSnapshot {
        PageSnapshot {
            title: "Beta Forum".to_string(),
            url: "http://beta.onion".to_string(),
            language: "French".to_string(),
            language_detection: LanguageDetection::unknown(),
            keyword_corpus: "http://beta.onion\nBeta Forum\nthread reply topic discussion"
                .to_string(),
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
            topic_observations: Vec::new(),
        }
    }

    fn gamma_snapshot() -> PageSnapshot {
        PageSnapshot {
            title: "Gamma Directory".to_string(),
            url: "http://gamma.onion".to_string(),
            language: "German".to_string(),
            language_detection: LanguageDetection::unknown(),
            keyword_corpus: "http://gamma.onion\nGamma Directory\nresource directory".to_string(),
            links: vec![
                LinkObservation {
                    target_url: "http://beta.onion".to_string(),
                    target_host: "beta.onion".to_string(),
                },
                LinkObservation {
                    target_url: "http://alpha.onion".to_string(),
                    target_host: "alpha.onion".to_string(),
                },
            ],
            emails: vec![
                "ops@gamma.onion".to_string(),
                "sales@gamma.onion".to_string(),
            ],
            crypto_refs: vec![CryptoReference {
                asset_type: "monero".to_string(),
                reference: "84A1gammaExampleAddress".to_string(),
            }],
            classification_signals: ClassificationSignals {
                word_count: 200,
                hints: vec![CategoryHint {
                    category: CATEGORY_DIRECTORY.to_string(),
                    evidence: "title:directory".to_string(),
                    weight: 6,
                }],
                ..ClassificationSignals::default()
            },
            topic_observations: Vec::new(),
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
    fn work_units_ignore_url_fragments() {
        let mut conn = setup_connection();

        create_work_unit(&mut conn, "https://example.com/page#faq").expect("fragment insert");
        create_work_unit(&mut conn, "https://example.com/page").expect("canonical insert");

        let work_units = list_work_units(&mut conn, None, None).expect("load work units");
        assert_eq!(work_units.items.len(), 1);
        assert_eq!(work_units.items[0].url, "https://example.com");
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
    fn file_backed_connections_enable_wal_mode() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let database_path =
            env::temp_dir().join(format!("spyder-lib-{}-{unique}.sqlite", process::id()));
        let database_url = database_path.to_string_lossy().into_owned();

        let mut conn = SqliteConnection::establish(&database_url).expect("sqlite file");
        configure_sqlite_connection(&mut conn, &database_url).expect("configure sqlite");

        let journal_mode = sql_query("PRAGMA journal_mode")
            .get_result::<JournalModeRow>(&mut conn)
            .expect("journal mode")
            .journal_mode;
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");

        drop(conn);
        let _ = fs::remove_file(&database_url);
        let _ = fs::remove_file(format!("{database_url}-wal"));
        let _ = fs::remove_file(format!("{database_url}-shm"));
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
    fn forum_keyword_rules_are_normalized_idempotent_and_removable() {
        let mut conn = setup_connection();

        add_forum_keyword_rule(&mut conn, "  Acme   Corp  ", "  ACME Corp ").expect("first add");
        add_forum_keyword_rule(&mut conn, "acme corp", "acme corp").expect("duplicate add");

        let rules = list_forum_keyword_rules(&mut conn).expect("list rules");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].label, "acme corp");
        assert_eq!(rules[0].pattern, "acme corp");

        assert_eq!(
            remove_forum_keyword_rule(&mut conn, " Acme Corp ", "ACME Corp").expect("remove rule"),
            Some(("acme corp".to_string(), "acme corp".to_string()))
        );
        assert!(
            remove_forum_keyword_rule(&mut conn, "acme corp", "acme corp")
                .expect("remove absent rule")
                .is_none()
        );
        assert!(list_forum_keyword_rules(&mut conn)
            .expect("list after remove")
            .is_empty());
    }

    #[test]
    fn forum_keyword_tags_surface_only_for_forum_sites() {
        let mut conn = setup_connection();
        add_forum_keyword_rule(&mut conn, "Acme Corp", "acme corp").expect("add keyword rule");

        let mut alpha = alpha_snapshot();
        alpha.keyword_corpus = "http://alpha.onion\nAlpha Market\nseller acme corp".to_string();
        let mut beta = beta_snapshot();
        beta.keyword_corpus =
            "http://beta.onion\nBeta Forum\nthread about acme corp and mirrors".to_string();

        save_page_info(&mut conn, &alpha).expect("save alpha");
        save_page_info(&mut conn, &beta).expect("save beta");

        let sites = list_site_profiles(&mut conn, None, None).expect("site profiles");
        let alpha_site = sites
            .items
            .iter()
            .find(|site| site.host == "alpha.onion")
            .expect("alpha site");
        let beta_site = sites
            .items
            .iter()
            .find(|site| site.host == "beta.onion")
            .expect("beta site");

        assert!(alpha_site.keyword_tags.is_empty());
        assert_eq!(
            beta_site.keyword_tags,
            vec!["keyword:acme corp".to_string()]
        );
        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_keyword_tag")
                .expect("keyword tag count"),
            2
        );
    }

    #[test]
    fn rescanning_a_forum_page_replaces_keyword_tags() {
        let mut conn = setup_connection();
        add_forum_keyword_rule(&mut conn, "Acme Corp", "acme corp").expect("add acme rule");
        add_forum_keyword_rule(&mut conn, "LockBit", "lockbit").expect("add lockbit rule");

        let mut snapshot = beta_snapshot();
        snapshot.keyword_corpus =
            "http://beta.onion\nBeta Forum\nthread about acme corp and lockbit".to_string();
        save_page_info(&mut conn, &snapshot).expect("save forum page");

        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_keyword_tag")
                .expect("initial keyword tag count"),
            2
        );
        let initial_sites = list_site_profiles(&mut conn, None, None).expect("site profiles");
        let initial_beta = initial_sites
            .items
            .iter()
            .find(|site| site.host == "beta.onion")
            .expect("beta site");
        assert_eq!(
            initial_beta.keyword_tags,
            vec![
                "keyword:acme corp".to_string(),
                "keyword:lockbit".to_string()
            ]
        );

        snapshot.keyword_corpus =
            "http://beta.onion\nBeta Forum\nthread about acme corp".to_string();
        save_page_info(&mut conn, &snapshot).expect("resave forum page");

        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_keyword_tag")
                .expect("updated keyword tag count"),
            1
        );
        let updated_sites = list_site_profiles(&mut conn, None, None).expect("updated sites");
        let updated_beta = updated_sites
            .items
            .iter()
            .find(|site| site.host == "beta.onion")
            .expect("beta site");
        assert_eq!(
            updated_beta.keyword_tags,
            vec!["keyword:acme corp".to_string()]
        );
    }

    #[test]
    fn keyword_analytics_are_host_level_and_timed_by_first_tag_observation() {
        let mut conn = setup_connection();
        add_forum_keyword_rule(&mut conn, "Acme Corp", "acme corp").expect("add acme rule");
        add_forum_keyword_rule(&mut conn, "LockBit", "lockbit").expect("add lockbit rule");

        let mut alpha = alpha_snapshot();
        alpha.keyword_corpus = "http://alpha.onion\nAlpha Market\nseller acme corp".to_string();

        let mut beta = beta_snapshot();
        beta.keyword_corpus =
            "http://beta.onion\nBeta Forum\nthread about acme corp and mirrors".to_string();

        let mut gamma = beta_snapshot();
        gamma.url = "http://gamma.onion".to_string();
        gamma.title = "Gamma Forum".to_string();
        gamma.keyword_corpus =
            "http://gamma.onion\nGamma Forum\nthread about acme corp and lockbit".to_string();
        gamma.links = vec![LinkObservation {
            target_url: "http://beta.onion".to_string(),
            target_host: "beta.onion".to_string(),
        }];

        save_page_info(&mut conn, &alpha).expect("save alpha");
        save_page_info(&mut conn, &beta).expect("save beta");
        save_page_info(&mut conn, &gamma).expect("save gamma");
        conn.batch_execute(
            "
            UPDATE page_keyword_tag
            SET created_at = '2026-05-01 08:00:00'
            WHERE page_id = (SELECT id FROM page WHERE url = 'http://beta.onion')
              AND tag = 'keyword:acme corp';
            UPDATE page_keyword_tag
            SET created_at = '2026-05-02 09:00:00'
            WHERE page_id = (SELECT id FROM page WHERE url = 'http://gamma.onion')
              AND tag = 'keyword:acme corp';
            UPDATE page_keyword_tag
            SET created_at = '2026-05-03 10:00:00'
            WHERE page_id = (SELECT id FROM page WHERE url = 'http://gamma.onion')
              AND tag = 'keyword:lockbit';
            ",
        )
        .expect("seed keyword timestamps");

        save_page_info(&mut conn, &beta).expect("resave beta");

        let distribution = list_site_keyword_distribution(&mut conn).expect("keyword distribution");
        assert_eq!(distribution.len(), 2);
        assert_eq!(distribution[0].category, "keyword:acme corp");
        assert_eq!(distribution[0].label, "acme corp");
        assert_eq!(distribution[0].host_count, 2);
        assert_eq!(distribution[1].category, "keyword:lockbit");
        assert_eq!(distribution[1].host_count, 1);

        let timeline = list_site_keyword_timeline(&mut conn).expect("keyword timeline");
        assert_eq!(timeline.len(), 3);
        assert_eq!(timeline[0].day, "2026-05-01");
        assert_eq!(timeline[0].category, "keyword:acme corp");
        assert_eq!(timeline[0].host_count, 1);
        assert_eq!(timeline[1].day, "2026-05-02");
        assert_eq!(timeline[1].category, "keyword:acme corp");
        assert_eq!(timeline[1].host_count, 1);
        assert_eq!(timeline[2].day, "2026-05-03");
        assert_eq!(timeline[2].category, "keyword:lockbit");
        assert_eq!(timeline[2].host_count, 1);
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
            Some(0),
        )
        .expect("search pages");
        assert_eq!(search_results.total_count, 1);
        assert_eq!(search_results.items.len(), 1);
        assert_eq!(search_results.items[0].title, "Beta Forum");
        assert_eq!(
            search_results.items[0]
                .site_category
                .as_ref()
                .map(|badge| badge.category.as_str()),
            Some(CATEGORY_FORUM)
        );

        let keyword_search_results = search_pages(&mut conn, "keyword:acme", Some(5), Some(0))
            .expect("keyword search pages");
        assert_eq!(keyword_search_results.total_count, 1);
        assert_eq!(keyword_search_results.items.len(), 1);
        assert_eq!(keyword_search_results.items[0].host, "beta.onion");
        assert_eq!(keyword_search_results.items[0].title, "Beta Forum");
        assert_eq!(
            keyword_search_results.items[0]
                .site_category
                .as_ref()
                .map(|badge| badge.category.as_str()),
            Some(CATEGORY_FORUM)
        );

        let paginated_search_results =
            search_pages(&mut conn, "shared.test", Some(1), Some(1)).expect("paginated search");
        assert_eq!(paginated_search_results.total_count, 2);
        assert_eq!(paginated_search_results.limit, 1);
        assert_eq!(paginated_search_results.offset, 1);
        assert_eq!(paginated_search_results.items.len(), 1);
    }

    #[test]
    fn top_site_rankings_are_host_level_and_tie_break_by_recency() {
        let mut conn = setup_connection();

        save_page_info(&mut conn, &alpha_snapshot()).expect("save alpha");
        save_page_info(&mut conn, &beta_snapshot()).expect("save beta");
        save_page_info(&mut conn, &gamma_snapshot()).expect("save gamma");
        conn.batch_execute(
            "
            UPDATE page SET last_scanned_at = '2026-05-02 08:00:00' WHERE url = 'http://alpha.onion';
            UPDATE page SET last_scanned_at = '2026-05-03 09:00:00' WHERE url = 'http://beta.onion';
            UPDATE page SET last_scanned_at = '2026-05-01 07:00:00' WHERE url = 'http://gamma.onion';
            UPDATE site_profile SET last_scanned_at = '2026-05-02 08:00:00' WHERE host = 'alpha.onion';
            UPDATE site_profile SET last_scanned_at = '2026-05-03 09:00:00' WHERE host = 'beta.onion';
            UPDATE site_profile SET last_scanned_at = '2026-05-01 07:00:00' WHERE host = 'gamma.onion';
            ",
        )
        .expect("update page recency");

        let email_leaders =
            list_top_sites_by_email_refs(&mut conn, Some(10)).expect("email leaders");
        assert_eq!(email_leaders[0].host, "gamma.onion");
        assert_eq!(email_leaders[0].count, 2);
        assert_eq!(email_leaders[1].host, "beta.onion");
        assert_eq!(email_leaders[2].host, "alpha.onion");

        let crypto_leaders =
            list_top_sites_by_crypto_refs(&mut conn, Some(10)).expect("crypto leaders");
        assert_eq!(crypto_leaders[0].host, "beta.onion");
        assert_eq!(crypto_leaders[0].count, 2);
        assert_eq!(crypto_leaders[1].host, "alpha.onion");
        assert_eq!(crypto_leaders[2].host, "gamma.onion");

        let outgoing_leaders =
            list_top_sites_by_outgoing_links(&mut conn, Some(10)).expect("outgoing leaders");
        assert_eq!(outgoing_leaders[0].host, "gamma.onion");
        assert_eq!(outgoing_leaders[0].count, 2);
        assert_eq!(outgoing_leaders[1].host, "beta.onion");
        assert_eq!(outgoing_leaders[2].host, "alpha.onion");

        let referenced_leaders =
            list_top_referenced_sites(&mut conn, Some(10)).expect("referenced leaders");
        assert_eq!(referenced_leaders[0].host, "beta.onion");
        assert_eq!(referenced_leaders[0].count, 2);
        assert_eq!(referenced_leaders[1].host, "alpha.onion");
        assert_eq!(referenced_leaders[1].count, 2);
        assert_eq!(
            referenced_leaders[0].last_scanned_at.as_deref(),
            Some("2026-05-03 09:00:00")
        );
    }

    #[test]
    fn site_profiles_are_sorted_by_most_recent_scan() {
        let mut conn = setup_connection();

        save_page_info(&mut conn, &alpha_snapshot()).expect("save alpha");
        save_page_info(&mut conn, &beta_snapshot()).expect("save beta");
        save_page_info(&mut conn, &gamma_snapshot()).expect("save gamma");
        conn.batch_execute(
            "
            UPDATE page SET last_scanned_at = '2026-05-02 08:00:00' WHERE url = 'http://alpha.onion';
            UPDATE page SET last_scanned_at = '2026-05-03 09:00:00' WHERE url = 'http://beta.onion';
            UPDATE page SET last_scanned_at = '2026-05-01 07:00:00' WHERE url = 'http://gamma.onion';
            UPDATE site_profile SET last_scanned_at = '2026-05-02 08:00:00' WHERE host = 'alpha.onion';
            UPDATE site_profile SET last_scanned_at = '2026-05-03 09:00:00' WHERE host = 'beta.onion';
            UPDATE site_profile SET last_scanned_at = '2026-05-01 07:00:00' WHERE host = 'gamma.onion';
            ",
        )
        .expect("update page recency");

        let sites = list_site_profiles(&mut conn, None, None).expect("site profiles");
        assert_eq!(sites.items[0].host, "beta.onion");
        assert_eq!(sites.items[1].host, "alpha.onion");
        assert_eq!(sites.items[2].host, "gamma.onion");
        assert_eq!(sites.items[0].last_scanned_at, "2026-05-03 09:00:00");
    }

    #[test]
    fn host_ssh_observations_are_grouped_by_shared_key() {
        let mut conn = setup_connection();

        save_page_info(&mut conn, &alpha_snapshot()).expect("save alpha");
        save_page_info(&mut conn, &beta_snapshot()).expect("save beta");
        save_host_ssh_observation(
            &mut conn,
            &NewHostSshObservation {
                host: "alpha.onion".to_string(),
                port: 22,
                status: SSH_STATUS_SUCCESS.to_string(),
                host_key_algorithm: Some("ssh-ed25519".to_string()),
                host_key: Some("001122".to_string()),
                host_key_fingerprint: Some("sha256:feedbeef".to_string()),
                server_banner: Some("SSH-2.0-OpenSSH_9.9".to_string()),
                last_error: None,
                last_attempt_at: String::new(),
                last_success_at: None,
            },
        )
        .expect("save alpha ssh observation");
        save_host_ssh_observation(
            &mut conn,
            &NewHostSshObservation {
                host: "beta.onion".to_string(),
                port: 2222,
                status: SSH_STATUS_SUCCESS.to_string(),
                host_key_algorithm: Some("ssh-ed25519".to_string()),
                host_key: Some("001122".to_string()),
                host_key_fingerprint: Some("sha256:feedbeef".to_string()),
                server_banner: Some("SSH-2.0-OpenSSH_9.9".to_string()),
                last_error: None,
                last_attempt_at: String::new(),
                last_success_at: None,
            },
        )
        .expect("save beta ssh observation");

        let recent_hosts =
            list_recent_responding_hosts(&mut conn, 24, Some(10)).expect("recent hosts");
        assert_eq!(recent_hosts.len(), 2);

        let summaries = list_ssh_host_keys(&mut conn, None, None).expect("ssh summaries");
        assert_eq!(summaries.total_count, 1);
        assert_eq!(summaries.items[0].algorithm, "ssh-ed25519");
        assert_eq!(summaries.items[0].host_count, 2);
        assert_eq!(summaries.items[0].endpoint_count, 2);

        let detail = get_ssh_host_key_detail(&mut conn, "ssh-ed25519", "sha256:feedbeef")
            .expect("ssh detail")
            .expect("ssh detail exists");
        assert_eq!(detail.host_count, 2);
        assert_eq!(detail.endpoint_count, 2);
        assert_eq!(detail.endpoints[0].host, "alpha.onion");
        assert_eq!(detail.endpoints[1].host, "beta.onion");
        assert!(detail.endpoints[0].site_category.is_some());
    }

    #[test]
    fn failed_ssh_observations_preserve_last_successful_key() {
        let mut conn = setup_connection();

        save_host_ssh_observation(
            &mut conn,
            &NewHostSshObservation {
                host: "alpha.onion".to_string(),
                port: 22,
                status: SSH_STATUS_SUCCESS.to_string(),
                host_key_algorithm: Some("ssh-ed25519".to_string()),
                host_key: Some("001122".to_string()),
                host_key_fingerprint: Some("sha256:feedbeef".to_string()),
                server_banner: Some("SSH-2.0-OpenSSH_9.9".to_string()),
                last_error: None,
                last_attempt_at: String::new(),
                last_success_at: None,
            },
        )
        .expect("save ssh success");
        save_host_ssh_observation(
            &mut conn,
            &NewHostSshObservation {
                host: "alpha.onion".to_string(),
                port: 22,
                status: "timeout".to_string(),
                host_key_algorithm: None,
                host_key: None,
                host_key_fingerprint: None,
                server_banner: None,
                last_error: Some("timed out".to_string()),
                last_attempt_at: String::new(),
                last_success_at: None,
            },
        )
        .expect("save ssh failure");

        let observation = get_host_ssh_observation(&mut conn, "alpha.onion", 22)
            .expect("load ssh observation")
            .expect("ssh observation exists");
        assert_eq!(observation.status, "timeout");
        assert_eq!(
            observation.host_key_fingerprint.as_deref(),
            Some("sha256:feedbeef")
        );
        assert!(observation.last_success_at.is_some());

        let summaries = list_ssh_host_keys(&mut conn, None, None).expect("ssh summaries");
        assert_eq!(summaries.total_count, 1);
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
        assert_eq!(gamma.category, CATEGORY_DOCS);
        assert_eq!(gamma.page_count, 1);
        assert_eq!(gamma.source_page_url.as_deref(), Some("http://gamma.onion"));
        assert!(gamma.evidence.iter().any(|item| item == "pages:1"));
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
    fn saving_pages_ignores_url_fragments() {
        let mut conn = setup_connection();

        let snapshot = PageSnapshot {
            title: "Anchor Heavy Page".to_string(),
            url: "https://example.com/docs/page#overview".to_string(),
            language: "English".to_string(),
            language_detection: LanguageDetection::unknown(),
            keyword_corpus: "https://example.com/docs/page#overview\nAnchor Heavy Page\nhttps://example.com/docs/faq#shipping".to_string(),
            links: vec![
                LinkObservation {
                    target_url: "https://example.com/docs/faq#shipping".to_string(),
                    target_host: "EXAMPLE.com".to_string(),
                },
                LinkObservation {
                    target_url: "https://example.com/docs/faq#returns".to_string(),
                    target_host: "example.com".to_string(),
                },
            ],
            emails: Vec::new(),
            crypto_refs: Vec::new(),
            classification_signals: ClassificationSignals::default(),
            topic_observations: Vec::new(),
        };
        save_page_info(&mut conn, &snapshot).expect("save fragment page");

        let mut rescanned = snapshot.clone();
        rescanned.url = "https://example.com/docs/page".to_string();
        rescanned.title = "Anchor Heavy Page Rescanned".to_string();
        save_page_info(&mut conn, &rescanned).expect("save canonical page");

        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page").expect("page count"),
            1
        );
        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_link")
                .expect("page link count"),
            1
        );
        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_scan").expect("scan count"),
            2
        );
        assert_eq!(
            scalar_nullable_text(&mut conn, "SELECT url AS value FROM page LIMIT 1")
                .expect("page url"),
            Some("https://example.com".to_string())
        );
        assert_eq!(
            scalar_nullable_text(
                &mut conn,
                "SELECT target_url AS value FROM page_link LIMIT 1"
            )
            .expect("stored target url"),
            Some("https://example.com".to_string())
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
    fn host_ssh_observation_migration_adds_host_key_table() {
        let mut conn = SqliteConnection::establish(":memory:").expect("in-memory sqlite");
        conn.batch_execute(include_str!(
            "../migrations/2026-05-02-110000_host_ssh_observations/up.sql"
        ))
        .expect("host ssh observation migration");

        save_host_ssh_observation(
            &mut conn,
            &NewHostSshObservation {
                host: "alpha.onion".to_string(),
                port: 22,
                status: SSH_STATUS_SUCCESS.to_string(),
                host_key_algorithm: Some("ssh-ed25519".to_string()),
                host_key: Some("001122".to_string()),
                host_key_fingerprint: Some("sha256:feedbeef".to_string()),
                server_banner: Some("SSH-2.0-OpenSSH_9.9".to_string()),
                last_error: None,
                last_attempt_at: String::new(),
                last_success_at: None,
            },
        )
        .expect("save migrated ssh observation");

        let observation = get_host_ssh_observation(&mut conn, "alpha.onion", 22)
            .expect("load ssh observation")
            .expect("ssh observation exists");
        assert_eq!(observation.status, SSH_STATUS_SUCCESS);
        assert_eq!(
            observation.host_key_algorithm.as_deref(),
            Some("ssh-ed25519")
        );
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
              source_host VARCHAR NOT NULL DEFAULT '',
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

    #[test]
    fn host_level_cleanup_migration_collapses_existing_path_rows() {
        let mut conn = setup_connection();
        conn.batch_execute(
            "
            INSERT INTO work_unit(url, status, retry_count, next_attempt_at, last_attempt_at, last_error, created_at)
            VALUES
              ('http://alpha.onion/bob', 'done', 1, '2026-04-29 10:00:00', '2026-04-29 10:05:00', NULL, '2026-04-29 10:00:00'),
              ('http://alpha.onion/alice', 'pending', 2, '2026-04-30 08:30:00', NULL, 'timeout', '2026-04-30 08:00:00');

            INSERT INTO page(id, title, url, links, emails, coins, language, last_scanned_at, created_at)
            VALUES
              (1, 'Alpha Bob', 'http://alpha.onion/bob', '', '', '', 'English', '2026-04-29 10:05:00', '2026-04-29 10:00:00'),
              (2, 'Alpha Alice', 'http://alpha.onion/alice', '', '', '', 'French', '2026-04-30 11:00:00', '2026-04-30 08:00:00');

            INSERT INTO page_scan(id, page_id, title, language, scanned_at)
            VALUES
              (10, 1, 'Alpha Bob Scan', 'English', '2026-04-29 10:05:00'),
              (11, 2, 'Alpha Alice Scan', 'French', '2026-04-30 11:00:00');

            INSERT INTO page_scan_link(scan_id, target_url, target_host)
            VALUES
              (10, 'http://beta.onion/about', 'beta.onion'),
              (10, 'http://beta.onion/contact', 'beta.onion'),
              (11, 'http://gamma.onion/faq', 'gamma.onion');

            INSERT INTO page_scan_email(scan_id, email)
            VALUES
              (10, 'team@alpha.onion'),
              (11, 'ops@alpha.onion');

            INSERT INTO page_scan_crypto(scan_id, asset_type, reference)
            VALUES
              (10, 'bitcoin', 'bc1qalpha000000000000000000000000000000000'),
              (11, 'ethereum', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');

            INSERT INTO page_link(source_page_id, source_host, target_url, target_host, created_at)
            VALUES
              (1, 'alpha.onion', 'http://beta.onion/about', 'beta.onion', '2026-04-29 10:05:00'),
              (2, 'alpha.onion', 'http://beta.onion/contact', 'beta.onion', '2026-04-30 11:00:00'),
              (2, 'alpha.onion', 'http://gamma.onion/faq', 'gamma.onion', '2026-04-30 11:00:00');

            INSERT INTO page_email(page_id, email, created_at)
            VALUES
              (1, 'team@alpha.onion', '2026-04-29 10:05:00'),
              (2, 'team@alpha.onion', '2026-04-30 11:00:00'),
              (2, 'ops@alpha.onion', '2026-04-30 11:00:00');

            INSERT INTO page_crypto(page_id, asset_type, reference, created_at)
            VALUES
              (1, 'bitcoin', 'bc1qalpha000000000000000000000000000000000', '2026-04-29 10:05:00'),
              (2, 'bitcoin', 'bc1qalpha000000000000000000000000000000000', '2026-04-30 11:00:00'),
              (2, 'ethereum', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '2026-04-30 11:00:00');

            INSERT INTO page_classification(page_id, host, category, confidence, score, evidence, last_classified_at)
            VALUES
              (1, 'alpha.onion', 'docs', 'medium', 6, 'title:docs', '2026-04-29 10:05:00'),
              (2, 'alpha.onion', 'market', 'high', 9, 'title:market', '2026-04-30 11:00:00');

            INSERT INTO site_profile(host, category, confidence, score, page_count, evidence, source_page_id, last_classified_at, created_at)
            VALUES
              ('alpha.onion', 'market', 'high', 9, 2, 'pages:2', 2, '2026-04-30 11:00:00', '2026-04-29 10:00:00');
            ",
        )
        .expect("legacy path data");

        conn.batch_execute(include_str!(
            "../migrations/2026-04-30-170423_host_level_cleanup/up.sql"
        ))
        .expect("host cleanup migration");

        let work_units = list_work_units(&mut conn, None, None).expect("work units");
        assert_eq!(work_units.items.len(), 1);
        assert_eq!(work_units.items[0].url, "http://alpha.onion");
        assert_eq!(work_units.items[0].status, STATUS_DONE);

        let pages = list_page_summaries(&mut conn, None, None).expect("page summaries");
        assert_eq!(pages.items.len(), 1);
        assert_eq!(pages.items[0].url, "http://alpha.onion");
        assert_eq!(pages.items[0].title, "Alpha Alice");
        assert_eq!(pages.items[0].language, "French");
        assert_eq!(pages.items[0].outbound_link_count, 2);
        assert_eq!(pages.items[0].email_count, 2);
        assert_eq!(pages.items[0].crypto_count, 2);

        let detail = get_page_detail(&mut conn, pages.items[0].id)
            .expect("page detail")
            .expect("page detail exists");
        assert_eq!(detail.outgoing_links.len(), 2);
        assert_eq!(detail.emails.len(), 2);
        assert_eq!(detail.crypto_refs.len(), 2);

        let history = list_page_scan_summaries(&mut conn, pages.items[0].id).expect("scan history");
        assert_eq!(history.len(), 2);
        assert_eq!(
            scalar_count(
                &mut conn,
                "SELECT COUNT(*) AS count FROM page_scan WHERE page_id = 1"
            )
            .expect("scan count for canonical page"),
            2
        );
        assert_eq!(
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_scan_link")
                .expect("scan link count"),
            2
        );
        assert_eq!(
            scalar_nullable_text(
                &mut conn,
                "SELECT target_url AS value FROM page_scan_link WHERE scan_id = 10 LIMIT 1"
            )
            .expect("normalized scan link"),
            Some("http://beta.onion".to_string())
        );

        assert_eq!(
            scalar_nullable_text(&mut conn, "SELECT links AS value FROM page LIMIT 1")
                .expect("page links summary"),
            Some("http://beta.onion,http://gamma.onion".to_string())
        );
        assert_eq!(
            scalar_nullable_text(&mut conn, "SELECT emails AS value FROM page LIMIT 1")
                .expect("page emails summary"),
            Some("ops@alpha.onion,team@alpha.onion".to_string())
        );
        assert_eq!(
            scalar_nullable_text(&mut conn, "SELECT coins AS value FROM page LIMIT 1")
                .expect("page coins summary"),
            Some(
                "bitcoin:bc1qalpha000000000000000000000000000000000,ethereum:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string()
            )
        );

        assert_eq!(
            scalar_count(
                &mut conn,
                "SELECT COUNT(*) AS count FROM page_classification"
            )
            .expect("classification count"),
            1
        );
        assert_eq!(
            scalar_nullable_text(
                &mut conn,
                "SELECT category AS value FROM page_classification LIMIT 1"
            )
            .expect("classification category"),
            Some("market".to_string())
        );

        let sites = list_site_profiles(&mut conn, None, None).expect("site profiles");
        assert_eq!(sites.items.len(), 1);
        assert_eq!(sites.items[0].host, "alpha.onion");
        assert_eq!(sites.items[0].category, "market");
        assert_eq!(sites.items[0].page_count, 1);
        assert_eq!(
            sites.items[0].source_page_url.as_deref(),
            Some("http://alpha.onion")
        );
    }

    #[test]
    fn auto_blacklist_category_rules_normalize_slugs_and_labels() {
        assert_eq!(
            normalize_auto_blacklist_rule_value(
                AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY,
                "Vendor Page"
            )
            .expect("display label normalizes"),
            CATEGORY_VENDOR_PAGE
        );
        assert_eq!(
            normalize_auto_blacklist_rule_value(AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY, "indexer")
                .expect("slug normalizes"),
            CATEGORY_INDEXER
        );
        assert!(normalize_auto_blacklist_rule_value(
            AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY,
            "unknown"
        )
        .is_err());
        assert_eq!(
            normalize_auto_blacklist_rule_value(AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY, "SEO Spam")
                .expect("seo spam label normalizes"),
            CATEGORY_SEO_SPAM
        );
    }

    #[test]
    fn auto_blacklist_keyword_rules_normalize_literal_phrases() {
        assert_eq!(
            normalize_auto_blacklist_rule_value(
                AUTO_BLACKLIST_RULE_TYPE_KEYWORD,
                "  Escrow   Required  "
            )
            .expect("keyword normalizes"),
            "escrow required"
        );
        assert!(
            normalize_auto_blacklist_rule_value(AUTO_BLACKLIST_RULE_TYPE_KEYWORD, "  ").is_err()
        );
        assert!(auto_blacklist_keyword_matches(
            "Forum post says ESCROW REQUIRED before delivery",
            "escrow required"
        ));
    }

    #[test]
    fn seo_spam_site_category_can_be_auto_blacklisted() {
        let mut conn = setup_connection();
        add_auto_blacklist_rule(
            &mut conn,
            AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY,
            "seo-spam",
            None,
        )
        .expect("add seo spam auto blacklist rule");

        let snapshot = PageSnapshot {
            title: "Promo Gateway".to_string(),
            url: "http://spam.onion".to_string(),
            language: "English".to_string(),
            language_detection: LanguageDetection::unknown(),
            keyword_corpus: "http://spam.onion\nPromo Gateway\nkeyword stuffed doorway".to_string(),
            links: vec![LinkObservation {
                target_url: "https://money.example".to_string(),
                target_host: "money.example".to_string(),
            }],
            emails: Vec::new(),
            crypto_refs: Vec::new(),
            classification_signals: ClassificationSignals {
                word_count: 40,
                hints: vec![
                    CategoryHint {
                        category: CATEGORY_SEO_SPAM.to_string(),
                        evidence: "meta-keywords:many-languages:10".to_string(),
                        weight: 8,
                    },
                    CategoryHint {
                        category: CATEGORY_SEO_SPAM.to_string(),
                        evidence: "links:single-external-visible-host:money.example".to_string(),
                        weight: 6,
                    },
                ],
                ..ClassificationSignals::default()
            },
            topic_observations: Vec::new(),
        };
        save_page_info(&mut conn, &snapshot).expect("save seo spam page");

        let sites = list_site_profiles(&mut conn, None, None).expect("site profiles");
        let site = sites
            .items
            .iter()
            .find(|site| site.host == "spam.onion")
            .expect("seo spam site profile");
        assert_eq!(site.category, CATEGORY_SEO_SPAM);
        assert_eq!(site.label, "SEO Spam");

        let blacklist = list_domain_blacklist_rules(&mut conn).expect("blacklist rules");
        assert!(blacklist.iter().any(|rule| rule.domain == "spam.onion"));
        let events =
            list_recent_auto_blacklist_events(&mut conn, None).expect("auto blacklist events");
        assert!(events.iter().any(|event| {
            event.domain == "spam.onion"
                && event.rule_type == AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY
                && event.matched_value == CATEGORY_SEO_SPAM
        }));
    }
}
