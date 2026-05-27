use anyhow::{Context, Result};
use diesel::connection::SimpleConnection;
use tracing::{error, info, warn};
use diesel::deserialize::QueryableByName;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use native_tls::TlsConnector;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, CONTENT_TYPE, RANGE};
use reqwest::{Proxy, StatusCode};
use sha2::{Digest, Sha256};
use spyder::extraction::{extract_favicon_url, extract_page_snapshot};
use spyder::models::{
    DomainBlacklistRule, ForumKeywordRule, HostSshObservationRecord, NewHostHttpObservation,
    NewHostServiceObservation, NewHostSshObservation, NewHostTlsObservation, Page,
    PageClassificationRecord, PageCrypto, PageEmail, PageKeywordTag, PageLink, PageScan,
    PageScanCrypto, PageScanEmail, PageScanLink, PageSnapshot, SiteProfileRecord, WorkUnit,
};
use spyder::{
    add_auto_blacklist_rule, add_domain_blacklist_entry, add_forum_keyword_rule,
    add_watchlist_item, apply_auto_blacklist_rules_to_existing,
    create_work_unit_unless_blacklisted, establish_connection, get_host_http_observation,
    get_host_service_observation, get_host_ssh_observation, get_pending_work_units,
    list_auto_blacklist_rules, list_domain_blacklist_rules, list_forum_keyword_rules,
    list_recent_responding_hosts, list_watchlist_items, mark_work_unit_as_done,
    normalize_crawl_url, page_link_batch_upper_bound, queue_known_pages_for_rescan,
    recompute_intel_leads_with_reporter, record_work_unit_failure,
    refresh_relationship_overview, remove_auto_blacklist_rule, remove_domain_blacklist_entry,
    remove_forum_keyword_rule, remove_watchlist_item, save_host_http_observation,
    save_host_service_observation, save_host_ssh_observation, save_host_tls_observation,
    save_page_info, set_auto_blacklist_rule_enabled, suppress_intel_lead,
    url_matches_blacklist, AppConnection, IntelLeadRecomputeOptions, PageSaveOutcome,
    SqlDialect, WorkQueueOutcome, AUTO_BLACKLIST_RULE_TYPE_KEYWORD,
    AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY, DEFAULT_BLACKLIST_LEAD_LINK_BATCH_SIZE,
    SSH_STATUS_SUCCESS,
};
use ssh2::{HashType, HostKeyType, Session};
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::env;
use std::fmt::Display;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use url::Url;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailureKind {
    Retriable,
    Permanent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorkOptions {
    onion_only: bool,
    concurrency: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RescanKnownOptions {
    onion_only: bool,
    concurrency: usize,
    limit: Option<i64>,
    queue_only: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SshScanOptions {
    recent_hours: i64,
    stale_hours: i64,
    limit: i64,
    concurrency: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LeadsRecomputeCliOptions {
    limit: Option<i64>,
    since_scan_id: Option<i32>,
    rule_ids: Vec<String>,
    blacklist_after_link_id: Option<i32>,
    blacklist_link_batch_size: Option<i64>,
}

struct CrawlFailure {
    error: anyhow::Error,
    kind: FailureKind,
}

struct DiscoveryEnqueueOutcome {
    queued_count: usize,
    skipped_blacklisted_count: usize,
}

#[derive(Clone)]
struct SocksProxyConfig {
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
}

struct SshHandshakeCapture {
    algorithm: String,
    host_key: String,
    fingerprint: String,
    server_banner: Option<String>,
}

#[derive(Clone, Copy)]
enum HostProbeKind {
    Ssh,
    Http,
    Ftp,
    Irc,
}

struct ServiceBannerCapture {
    status: String,
    banner: Option<String>,
    banner_fingerprint: Option<String>,
}

struct HttpObservationCapture {
    http_observation: NewHostHttpObservation,
    tls_observation: Option<NewHostTlsObservation>,
}

struct HostProbeJob {
    host: String,
    kind: HostProbeKind,
    port: u16,
}

enum HostProbeCapture {
    Ssh(Result<SshHandshakeCapture>),
    Http(Result<HttpObservationCapture>),
    Service(Result<ServiceBannerCapture>),
}

struct HostProbeResult {
    job: HostProbeJob,
    capture: HostProbeCapture,
}

struct WorkFetchJob {
    work_unit_id: i32,
    work_unit_url: String,
    crawl_url: String,
}

struct WorkFetchResult {
    job: WorkFetchJob,
    capture: std::result::Result<HttpEndpointCapture, CrawlFailure>,
}

struct HttpEndpointCapture {
    snapshot: PageSnapshot,
    http_observation: NewHostHttpObservation,
    tls_observation: Option<NewHostTlsObservation>,
}

#[derive(Clone)]
struct UrlEndpoint {
    host: String,
    scheme: String,
    port: i32,
}

struct WebResourceProbe {
    path: &'static str,
    markers: &'static [&'static str],
    allow_html: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExposedWebResourceFinding {
    path: String,
    status_code: u16,
    content_type: Option<String>,
    content_length: Option<u64>,
    bytes_read: usize,
    truncated: bool,
    sample_sha256: String,
    preview: Option<String>,
    body_sample: String,
}

#[derive(QueryableByName)]
struct NullableTextValueRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    value: Option<String>,
}

#[derive(QueryableByName)]
struct TableNameRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
}

#[derive(QueryableByName)]
struct BigIntValueRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    value: i64,
}

#[derive(QueryableByName)]
struct IntValueRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    value: i32,
}

const SSH_PORTS: [u16; 2] = [22, 2222];
const HTTP_PROBE_PORTS: [u16; 2] = [8000, 8080];
const FTP_PORTS: [u16; 2] = [21, 2121];
const IRC_PORTS: [u16; 2] = [6667, 7000];
const DEFAULT_SSH_SCAN_RECENT_HOURS: i64 = 24 * 7;
const DEFAULT_SSH_SCAN_STALE_HOURS: i64 = 24;
const DEFAULT_SSH_SCAN_LIMIT: i64 = 200;
const DEFAULT_SSH_SCAN_CONCURRENCY: usize = 4;
const DEFAULT_WORK_CONCURRENCY: usize = 4;
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_IO_TIMEOUT: Duration = Duration::from_secs(15);
const IMPORT_BATCH_SIZE: i64 = 5_000;
const WEB_RESOURCE_PROBE_MAX_BYTES: usize = 8 * 1024;
const WEB_RESOURCE_PREVIEW_CHARS: usize = 600;
const WEB_RESOURCE_PROBES: &[WebResourceProbe] = &[
    WebResourceProbe {
        path: "/robots.txt",
        markers: &["user-agent", "disallow", "allow:", "sitemap:"],
        allow_html: false,
    },
    WebResourceProbe {
        path: "/sitemap.xml",
        markers: &["<urlset", "<sitemapindex"],
        allow_html: false,
    },
    WebResourceProbe {
        path: "/.well-known/security.txt",
        markers: &["contact:", "expires:", "encryption:", "policy:"],
        allow_html: false,
    },
    WebResourceProbe {
        path: "/.env",
        markers: &[
            "app_key",
            "database_url",
            "password",
            "passwd",
            "db_password",
            "db_pass",
            "db_host",
            "aws_access_key",
            "secret",
            "token",
        ],
        allow_html: false,
    },
    WebResourceProbe {
        path: "/.git/config",
        markers: &["[core]", "repositoryformatversion", "[remote"],
        allow_html: false,
    },
    WebResourceProbe {
        path: "/composer.json",
        markers: &["\"require\"", "\"autoload\"", "\"minimum-stability\""],
        allow_html: false,
    },
    WebResourceProbe {
        path: "/package.json",
        markers: &["\"dependencies\"", "\"devdependencies\"", "\"scripts\""],
        allow_html: false,
    },
    WebResourceProbe {
        path: "/phpinfo.php",
        markers: &["php version", "phpinfo()"],
        allow_html: true,
    },
    WebResourceProbe {
        path: "/info.php",
        markers: &["php version", "phpinfo()"],
        allow_html: true,
    },
    WebResourceProbe {
        path: "/.profile",
        markers: &["export ", "alias ", "path=", "umask"],
        allow_html: false,
    },
    WebResourceProbe {
        path: "/.bash_history",
        markers: &["sudo ", "ssh ", "mysql ", "psql ", "curl ", "wget ", "git "],
        allow_html: false,
    },
    WebResourceProbe {
        path: "/.zsh_history",
        markers: &["sudo ", "ssh ", "mysql ", "psql ", "curl ", "wget ", "git "],
        allow_html: false,
    },
];

trait HasId {
    fn id(&self) -> i32;
}

macro_rules! impl_has_id {
    ($($ty:ty),+ $(,)?) => {
        $(impl HasId for $ty {
            fn id(&self) -> i32 {
                self.id
            }
        })+
    };
}

impl_has_id!(
    WorkUnit,
    Page,
    PageScan,
    PageScanLink,
    PageScanEmail,
    PageScanCrypto,
    PageLink,
    SourcePageLinkImportRow,
    PageEmail,
    PageCrypto,
    PageClassificationRecord,
    SiteProfileRecord,
    SourceSiteProfile,
    DomainBlacklistRule,
    HostSshObservationRecord,
    ForumKeywordRule,
    PageKeywordTag,
);

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::work_unit)]
struct ImportedWorkUnit {
    id: i32,
    url: String,
    status: String,
    retry_count: i32,
    next_attempt_at: String,
    last_attempt_at: Option<String>,
    last_error: Option<String>,
    created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::page)]
struct ImportedPage {
    id: i32,
    title: String,
    url: String,
    links: String,
    emails: String,
    coins: String,
    language: String,
    last_scanned_at: String,
    created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::page_scan)]
struct ImportedPageScan {
    id: i32,
    page_id: i32,
    title: String,
    language: String,
    scanned_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::page_scan_link)]
struct ImportedPageScanLink {
    id: i32,
    scan_id: i32,
    target_url: String,
    target_host: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::page_scan_email)]
struct ImportedPageScanEmail {
    id: i32,
    scan_id: i32,
    email: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::page_scan_crypto)]
struct ImportedPageScanCrypto {
    id: i32,
    scan_id: i32,
    asset_type: String,
    reference: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::page_link)]
struct ImportedPageLink {
    id: i32,
    source_page_id: i32,
    source_host: String,
    target_url: String,
    target_host: String,
    created_at: String,
}

#[derive(QueryableByName)]
struct SourcePageLinkImportRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    source_page_id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    source_host: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    target_url: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    target_host: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::page_email)]
struct ImportedPageEmail {
    id: i32,
    page_id: i32,
    email: String,
    created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::page_crypto)]
struct ImportedPageCrypto {
    id: i32,
    page_id: i32,
    asset_type: String,
    reference: String,
    created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::page_classification)]
struct ImportedPageClassification {
    id: i32,
    page_id: i32,
    host: String,
    category: String,
    confidence: String,
    score: i32,
    evidence: String,
    last_classified_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::site_profile)]
struct ImportedSiteProfile {
    id: i32,
    host: String,
    category: String,
    confidence: String,
    score: i32,
    page_count: i32,
    first_found_at: String,
    last_scanned_at: String,
    evidence: String,
    source_page_id: Option<i32>,
    last_classified_at: String,
    created_at: String,
}

#[derive(Queryable)]
struct SourceSiteProfile {
    id: i32,
    host: String,
    category: String,
    confidence: String,
    score: i32,
    page_count: i32,
    evidence: String,
    source_page_id: Option<i32>,
    last_classified_at: String,
    created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::domain_blacklist)]
struct ImportedDomainBlacklistRule {
    id: i32,
    domain: String,
    created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::host_ssh_observation)]
struct ImportedHostSshObservation {
    id: i32,
    host: String,
    port: i32,
    status: String,
    host_key_algorithm: Option<String>,
    host_key: Option<String>,
    host_key_fingerprint: Option<String>,
    server_banner: Option<String>,
    last_error: Option<String>,
    last_attempt_at: String,
    last_success_at: Option<String>,
    created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::forum_keyword_rule)]
struct ImportedForumKeywordRule {
    id: i32,
    label: String,
    pattern: String,
    created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = spyder::schema::page_keyword_tag)]
struct ImportedPageKeywordTag {
    id: i32,
    page_id: i32,
    tag: String,
    created_at: String,
}

impl Default for SshScanOptions {
    fn default() -> Self {
        Self {
            recent_hours: DEFAULT_SSH_SCAN_RECENT_HOURS,
            stale_hours: DEFAULT_SSH_SCAN_STALE_HOURS,
            limit: DEFAULT_SSH_SCAN_LIMIT,
            concurrency: DEFAULT_SSH_SCAN_CONCURRENCY,
        }
    }
}

impl Default for WorkOptions {
    fn default() -> Self {
        Self {
            onion_only: false,
            concurrency: DEFAULT_WORK_CONCURRENCY,
        }
    }
}

impl Default for RescanKnownOptions {
    fn default() -> Self {
        Self {
            onion_only: false,
            concurrency: DEFAULT_WORK_CONCURRENCY,
            limit: None,
            queue_only: false,
        }
    }
}

impl CrawlFailure {
    fn retriable(error: anyhow::Error) -> Self {
        Self {
            error,
            kind: FailureKind::Retriable,
        }
    }

    fn permanent(error: anyhow::Error) -> Self {
        Self {
            error,
            kind: FailureKind::Permanent,
        }
    }
}

fn print_status(message: impl Display) {
    info!("{}", message);
}

fn print_progress(current: usize, total: usize, message: impl Display) {
    info!(current, total, "{}", message);
}

fn count_label(count: usize, singular: &str, plural: &str) -> String {
    format!("{} {}", count, if count == 1 { singular } else { plural })
}

fn compact_for_terminal(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return normalized;
    }

    let truncated = normalized.chars().take(max_chars).collect::<String>();
    if normalized.chars().count() > max_chars {
        format!("{truncated}...")
    } else {
        normalized
    }
}

fn summarize_page_snapshot(snapshot: &spyder::models::PageSnapshot) -> String {
    let mut parts = vec![
        count_label(snapshot.links.len(), "link", "links"),
        count_label(snapshot.emails.len(), "email", "emails"),
        count_label(snapshot.crypto_refs.len(), "crypto ref", "crypto refs"),
    ];
    let language = compact_for_terminal(&snapshot.language, 24);
    if !language.is_empty() {
        parts.push(format!("language {language}"));
    }

    let title = compact_for_terminal(&snapshot.title, 48);
    if title.is_empty() {
        parts.join(", ")
    } else {
        format!("title \"{title}\", {}", parts.join(", "))
    }
}

fn failure_kind_label(kind: FailureKind) -> &'static str {
    match kind {
        FailureKind::Retriable => "retriable",
        FailureKind::Permanent => "permanent",
    }
}

fn page_save_outcome_label(outcome: PageSaveOutcome) -> &'static str {
    match outcome {
        PageSaveOutcome::Stored => "Stored",
        PageSaveOutcome::SkippedBlacklisted => "Skipped blacklisted",
        PageSaveOutcome::PurgedAfterAutoBlacklist => "Purged newly blacklisted",
    }
}

fn fetch_page_capture(
    client: &Client,
    url: &str,
    tls_proxy: Option<&SocksProxyConfig>,
) -> std::result::Result<HttpEndpointCapture, CrawlFailure> {
    let requested_endpoint = endpoint_from_url(url).ok_or_else(|| {
        CrawlFailure::permanent(anyhow::anyhow!("invalid observed endpoint url: {url}"))
    })?;
    let response = client.get(url).send().map_err(|error| {
        let wrapped = anyhow::Error::new(error).context(format!("request failed for {url}"));
        if is_retriable_request_error(&wrapped) {
            CrawlFailure::retriable(wrapped)
        } else {
            CrawlFailure::permanent(wrapped)
        }
    })?;
    let status = response.status();
    if !status.is_success() {
        let error = anyhow::anyhow!("non-success status {} for {}", status.as_u16(), url);
        return if is_retriable_status(status) {
            Err(CrawlFailure::retriable(error))
        } else {
            Err(CrawlFailure::permanent(error))
        };
    }
    let final_url = response.url().as_str().to_string();
    let headers = response.headers().clone();
    let body = response.text().map_err(|error| {
        let wrapped =
            anyhow::Error::new(error).context(format!("failed to read response body for {url}"));
        CrawlFailure::retriable(wrapped)
    })?;
    let snapshot = extract_page_snapshot(url, &body).map_err(|error| {
        CrawlFailure::permanent(error.context(format!("failed to parse {url}")))
    })?;
    let http_observation = build_http_observation(
        client,
        &requested_endpoint,
        status,
        &final_url,
        &headers,
        &body,
    );
    let tls_observation = build_tls_observation(tls_proxy, &final_url);

    Ok(HttpEndpointCapture {
        snapshot,
        http_observation,
        tls_observation,
    })
}

fn probe_http_endpoint(
    client: &Client,
    tls_proxy: Option<&SocksProxyConfig>,
    host: &str,
    port: u16,
) -> Result<HttpObservationCapture> {
    let endpoint = UrlEndpoint {
        host: host.to_ascii_lowercase(),
        scheme: "http".to_string(),
        port: i32::from(port),
    };
    let url = format!("http://{}:{}/", host, port);
    let response = client
        .get(&url)
        .send()
        .with_context(|| format!("http request failed for {url}"))?;
    let status = response.status();
    let final_url = response.url().as_str().to_string();
    let headers = response.headers().clone();
    let body_bytes = response
        .bytes()
        .with_context(|| format!("failed to read http response body for {url}"))?;
    let body = String::from_utf8_lossy(&body_bytes).into_owned();
    let http_observation =
        build_http_observation(client, &endpoint, status, &final_url, &headers, &body);
    let tls_observation = build_tls_observation(tls_proxy, &final_url);

    Ok(HttpObservationCapture {
        http_observation,
        tls_observation,
    })
}

fn probe_ftp_endpoint(
    proxy: Option<&SocksProxyConfig>,
    host: &str,
    port: u16,
) -> Result<ServiceBannerCapture> {
    let mut stream = connect_tcp_endpoint(proxy, host, port)?;
    let banner = read_stream_banner(&mut stream, 2048)?;
    let status = if banner_is_ftp(&banner) {
        SSH_STATUS_SUCCESS.to_string()
    } else if banner.is_some() {
        "unexpected-banner".to_string()
    } else {
        "no-banner".to_string()
    };
    let banner_fingerprint = banner.as_deref().map(banner_fingerprint);
    let _ = stream.write_all(b"QUIT\r\n");

    Ok(ServiceBannerCapture {
        status,
        banner,
        banner_fingerprint,
    })
}

fn probe_irc_endpoint(
    proxy: Option<&SocksProxyConfig>,
    host: &str,
    port: u16,
) -> Result<ServiceBannerCapture> {
    let mut stream = connect_tcp_endpoint(proxy, host, port)?;
    let _ = stream.write_all(b"NICK spyder_scan\r\nUSER spyder_scan 0 * :spyder\r\n");
    let banner = read_stream_banner(&mut stream, 4096)?;
    let status = if banner_is_irc(&banner) {
        SSH_STATUS_SUCCESS.to_string()
    } else if banner.is_some() {
        "unexpected-banner".to_string()
    } else {
        "no-banner".to_string()
    };
    let banner_fingerprint = banner.as_deref().map(banner_fingerprint);
    let _ = stream.write_all(b"QUIT\r\n");

    Ok(ServiceBannerCapture {
        status,
        banner,
        banner_fingerprint,
    })
}

fn enqueue_seed_and_links(client: &Client, url: &str) -> Result<usize> {
    let normalized_url = normalize_crawl_url(url);
    let tls_proxy = load_best_effort_tls_proxy_config();
    let mut connection = establish_connection()?;
    print_status(format!("Queueing seed URL {normalized_url}"));
    let blacklist_domains = list_domain_blacklist_rules(&mut connection)?
        .into_iter()
        .map(|rule| rule.domain)
        .collect::<Vec<_>>();
    if matches!(
        create_work_unit_unless_blacklisted(&mut connection, &normalized_url, &blacklist_domains)?,
        WorkQueueOutcome::SkippedBlacklisted
    ) {
        warn!(url = %normalized_url, "Skipped seed URL because its domain is blacklisted");
        return Ok(0);
    }

    Url::parse(&normalized_url).with_context(|| format!("invalid url: {normalized_url}"))?;
    print_status(format!("Fetching seed page {normalized_url}"));
    let capture = fetch_page_capture(client, &normalized_url, tls_proxy.as_ref())
        .map_err(|failure| failure.error)
        .with_context(|| format!("unable to discover links for seed {normalized_url}"))?;
    print_status(format!(
        "Extracted {}",
        summarize_page_snapshot(&capture.snapshot)
    ));
    save_host_http_observation(&mut connection, &capture.http_observation)
        .context("saving seed http fingerprint")?;
    if let Some(tls_observation) = capture.tls_observation.as_ref() {
        save_host_tls_observation(&mut connection, tls_observation)
            .context("saving seed tls fingerprint")?;
    }
    print_status("Queueing discovered links from the seed page");
    let outcome = enqueue_discovered_links(&mut connection, &capture.snapshot)?;
    info!(
        queued_count = outcome.queued_count,
        "Queued discovered URLs from the seed page"
    );
    if outcome.skipped_blacklisted_count > 0 {
        info!(
            skipped_count = outcome.skipped_blacklisted_count,
            "Skipped discovered URLs whose domains are blacklisted"
        );
    }

    Ok(1 + outcome.queued_count)
}

fn work_queue(client: &Client, options: WorkOptions) -> Result<()> {
    let mut connection = establish_connection()?;
    let tls_proxy = load_best_effort_tls_proxy_config();
    print_status("Loading pending work units");
    let pending_work_units = get_pending_work_units(&mut connection)?;
    let pending_count = pending_work_units.len();
    let work_units = select_work_units_for_processing(pending_work_units, options);

    if work_units.is_empty() {
        if options.onion_only {
            info!(
                skipped_count = pending_count,
                "No pending .onion work units to process"
            );
        } else {
            info!("No pending work units to process");
        }
        return Ok(());
    }

    if options.onion_only {
        let skipped_count = pending_count - work_units.len();
        info!(
            work_unit_count = work_units.len(),
            skipped_count,
            "Working with pending .onion work units"
        );
    } else {
        info!(work_unit_count = work_units.len(), "Working with pending work units");
    }
    let mut processed_urls = HashSet::new();
    let mut jobs = Vec::new();
    let mut duplicate_count = 0usize;
    let mut blacklisted_count = 0usize;
    let blacklist_domains = list_domain_blacklist_rules(&mut connection)?
        .into_iter()
        .map(|rule| rule.domain)
        .collect::<Vec<_>>();

    for work_unit in work_units {
        let crawl_url = normalize_crawl_url(&work_unit.url);
        if url_matches_blacklist(&crawl_url, &blacklist_domains) {
            mark_work_unit_as_done(&mut connection, work_unit.id)?;
            blacklisted_count += 1;
            continue;
        }
        if processed_urls.contains(&crawl_url) {
            mark_work_unit_as_done(&mut connection, work_unit.id)?;
            duplicate_count += 1;
            continue;
        }

        jobs.push(WorkFetchJob {
            work_unit_id: work_unit.id,
            work_unit_url: work_unit.url,
            crawl_url: crawl_url.clone(),
        });
        processed_urls.insert(crawl_url);
    }

    let attempted = jobs.len();
    if attempted == 0 {
        info!(
            duplicate_count,
            blacklisted_count,
            "No unique pending work units to process"
        );
        return Ok(());
    }
    if duplicate_count > 0 {
        print_status(format!(
            "Skipped {} duplicate work unit{} before fetching",
            duplicate_count,
            if duplicate_count == 1 { "" } else { "s" }
        ));
    }
    if blacklisted_count > 0 {
        print_status(format!(
            "Skipped {} blacklisted work unit{} before fetching",
            blacklisted_count,
            if blacklisted_count == 1 { "" } else { "s" }
        ));
    }

    let worker_count = options.concurrency.min(attempted).max(1);
    print_status(format!(
        "Fetching {} unique work unit{} with {} worker{}",
        attempted,
        if attempted == 1 { "" } else { "s" },
        worker_count,
        if worker_count == 1 { "" } else { "s" }
    ));

    let job_queue = Arc::new(Mutex::new(VecDeque::from(jobs)));
    let (result_tx, result_rx) = mpsc::channel::<WorkFetchResult>();

    thread::scope(|scope| {
        for _ in 0..worker_count {
            let client = client.clone();
            let job_queue = Arc::clone(&job_queue);
            let result_tx = result_tx.clone();
            let tls_proxy = tls_proxy.clone();

            scope.spawn(move || loop {
                let job = {
                    let mut queue = job_queue.lock().expect("work queue lock poisoned");
                    queue.pop_front()
                };

                let Some(job) = job else {
                    break;
                };

                let capture = fetch_page_capture(&client, &job.crawl_url, tls_proxy.as_ref());
                if result_tx.send(WorkFetchResult { job, capture }).is_err() {
                    break;
                }
            });
        }

        drop(result_tx);

        for (completed, result) in result_rx.into_iter().enumerate() {
            let current = completed + 1;
            match result.capture {
                Ok(capture) => {
                    print_progress(
                        current,
                        attempted,
                        format!("Extracted {}", summarize_page_snapshot(&capture.snapshot)),
                    );
                    let save_outcome = save_page_info(&mut connection, &capture.snapshot)?;
                    let discovery_outcome = match save_outcome {
                        PageSaveOutcome::Stored => {
                            save_host_http_observation(&mut connection, &capture.http_observation)?;
                            if let Some(tls_observation) = capture.tls_observation.as_ref() {
                                save_host_tls_observation(&mut connection, tls_observation)?;
                            }
                            enqueue_discovered_links(&mut connection, &capture.snapshot)?
                        }
                        PageSaveOutcome::SkippedBlacklisted
                        | PageSaveOutcome::PurgedAfterAutoBlacklist => DiscoveryEnqueueOutcome {
                            queued_count: 0,
                            skipped_blacklisted_count: 0,
                        },
                    };
                    mark_work_unit_as_done(&mut connection, result.job.work_unit_id)?;
                    print_progress(
                        current,
                        attempted,
                        format!(
                            "{} {} and queued {} discovered URLs",
                            page_save_outcome_label(save_outcome),
                            result.job.crawl_url,
                            discovery_outcome.queued_count
                        ),
                    );
                    if discovery_outcome.skipped_blacklisted_count > 0 {
                        print_progress(
                            current,
                            attempted,
                            format!(
                                "Skipped {} discovered URLs whose domains are blacklisted",
                                discovery_outcome.skipped_blacklisted_count
                            ),
                        );
                    }
                }
                Err(failure) => {
                    error!(
                        current,
                        attempted,
                        url = %result.job.crawl_url,
                        failure_kind = ?failure.kind,
                        "Failed to process work unit"
                    );
                    error!(
                        error = ?failure.error,
                        "Couldn't extract page information"
                    );
                    record_work_unit_failure(
                        &mut connection,
                        result.job.work_unit_id,
                        &failure.error.to_string(),
                        failure.kind == FailureKind::Retriable,
                    )?;
                    info!(
                        current,
                        attempted,
                        url = %result.job.work_unit_url,
                        "Recorded failure state"
                    );
                }
            }
        }

        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}

fn rescan_known_pages(client: &Client, options: RescanKnownOptions) -> Result<()> {
    let mut connection = establish_connection()?;
    print_status("Queueing known pages for rescan");
    let queued_count =
        queue_known_pages_for_rescan(&mut connection, options.limit, options.onion_only)?;
    info!(
        queued_count,
        "Queued {} known page{} for rescan",
        queued_count,
        if queued_count == 1 { "" } else { "s" }
    );
    drop(connection);

    if queued_count == 0 || options.queue_only {
        return Ok(());
    }

    work_queue(
        client,
        WorkOptions {
            onion_only: options.onion_only,
            concurrency: options.concurrency,
        },
    )
}

fn ssh_scan_hosts(options: SshScanOptions) -> Result<()> {
    let mut connection = establish_connection()?;
    print_status(format!(
        "Loading hosts scanned in the last {} hours",
        options.recent_hours
    ));
    let candidates =
        list_recent_responding_hosts(&mut connection, options.recent_hours, Some(options.limit))?;
    if candidates.is_empty() {
        info!("No recently responding hosts to scan");
        return Ok(());
    }

    let stale_cutoff = ssh_stale_cutoff_timestamp(&mut connection, options.stale_hours)?;
    let proxy = load_socks_proxy_config()?;
    let tls_proxy = proxy.clone();
    let http_client = build_http_client()?;
    match proxy.as_ref() {
        Some(config) => print_status(format!(
            "Scanning SSH through SOCKS proxy {}",
            describe_socks_endpoint(config)
        )),
        None => print_status("No SOCKS proxy configured, scanning directly"),
    }

    let total_hosts = candidates.len();
    let mut skipped = 0usize;
    let mut successes = 0usize;
    let mut failures = 0usize;
    let mut jobs = Vec::new();

    for candidate in candidates {
        for port in SSH_PORTS {
            let existing =
                get_host_ssh_observation(&mut connection, &candidate.host, i32::from(port))?;
            if should_skip_network_attempt(
                existing.as_ref().map(|row| row.last_attempt_at.as_str()),
                &candidate.last_scanned_at,
                &stale_cutoff,
            ) {
                skipped += 1;
                continue;
            }

            jobs.push(HostProbeJob {
                host: candidate.host.clone(),
                kind: HostProbeKind::Ssh,
                port,
            });
        }
        for port in HTTP_PROBE_PORTS {
            let existing = get_host_http_observation(
                &mut connection,
                &candidate.host,
                "http",
                i32::from(port),
            )?;
            if should_skip_network_attempt(
                existing.as_ref().map(|row| row.last_attempt_at.as_str()),
                &candidate.last_scanned_at,
                &stale_cutoff,
            ) {
                skipped += 1;
                continue;
            }

            jobs.push(HostProbeJob {
                host: candidate.host.clone(),
                kind: HostProbeKind::Http,
                port,
            });
        }
        for port in FTP_PORTS {
            let existing = get_host_service_observation(
                &mut connection,
                &candidate.host,
                "ftp",
                i32::from(port),
            )?;
            if should_skip_network_attempt(
                existing.as_ref().map(|row| row.last_attempt_at.as_str()),
                &candidate.last_scanned_at,
                &stale_cutoff,
            ) {
                skipped += 1;
                continue;
            }

            jobs.push(HostProbeJob {
                host: candidate.host.clone(),
                kind: HostProbeKind::Ftp,
                port,
            });
        }
        for port in IRC_PORTS {
            let existing = get_host_service_observation(
                &mut connection,
                &candidate.host,
                "irc",
                i32::from(port),
            )?;
            if should_skip_network_attempt(
                existing.as_ref().map(|row| row.last_attempt_at.as_str()),
                &candidate.last_scanned_at,
                &stale_cutoff,
            ) {
                skipped += 1;
                continue;
            }

            jobs.push(HostProbeJob {
                host: candidate.host.clone(),
                kind: HostProbeKind::Irc,
                port,
            });
        }
    }

    let attempted = jobs.len();
    if attempted == 0 {
        info!("No stale service endpoints to scan across {} hosts ({} skipped)", total_hosts, skipped
        );
        return Ok(());
    }

    let worker_count = options.concurrency.min(attempted).max(1);
    print_status(format!(
        "Scanning {} service endpoints across {} hosts with {} worker{}",
        attempted,
        total_hosts,
        worker_count,
        if worker_count == 1 { "" } else { "s" }
    ));

    let job_queue = Arc::new(Mutex::new(VecDeque::from(jobs)));
    let (result_tx, result_rx) = mpsc::channel::<HostProbeResult>();

    thread::scope(|scope| {
        for _ in 0..worker_count {
            let job_queue = Arc::clone(&job_queue);
            let result_tx = result_tx.clone();
            let proxy = proxy.clone();
            let tls_proxy = tls_proxy.clone();
            let http_client = http_client.clone();

            scope.spawn(move || loop {
                let job = {
                    let mut queue = job_queue.lock().expect("host probe queue lock poisoned");
                    queue.pop_front()
                };

                let Some(job) = job else {
                    break;
                };

                let capture = match job.kind {
                    HostProbeKind::Ssh => HostProbeCapture::Ssh(probe_ssh_endpoint(
                        proxy.as_ref(),
                        &job.host,
                        job.port,
                    )),
                    HostProbeKind::Http => HostProbeCapture::Http(probe_http_endpoint(
                        &http_client,
                        tls_proxy.as_ref(),
                        &job.host,
                        job.port,
                    )),
                    HostProbeKind::Ftp => HostProbeCapture::Service(probe_ftp_endpoint(
                        proxy.as_ref(),
                        &job.host,
                        job.port,
                    )),
                    HostProbeKind::Irc => HostProbeCapture::Service(probe_irc_endpoint(
                        proxy.as_ref(),
                        &job.host,
                        job.port,
                    )),
                };
                if result_tx.send(HostProbeResult { job, capture }).is_err() {
                    break;
                }
            });
        }

        drop(result_tx);

        for (completed, result) in result_rx.into_iter().enumerate() {
            let current = completed + 1;
            match result.capture {
                HostProbeCapture::Ssh(capture) => match capture {
                    Ok(capture) => {
                        let fingerprint_preview = compact_for_terminal(&capture.fingerprint, 42);
                        save_host_ssh_observation(
                            &mut connection,
                            &NewHostSshObservation {
                                host: result.job.host.clone(),
                                port: i32::from(result.job.port),
                                status: SSH_STATUS_SUCCESS.to_string(),
                                host_key_algorithm: Some(capture.algorithm.clone()),
                                host_key: Some(capture.host_key),
                                host_key_fingerprint: Some(capture.fingerprint.clone()),
                                server_banner: capture.server_banner,
                                last_error: None,
                                last_attempt_at: String::new(),
                                last_success_at: None,
                            },
                        )?;
                        successes += 1;
                        print_progress(
                            current,
                            attempted,
                            format!(
                                "Saved SSH {} for {}:{}",
                                fingerprint_preview, result.job.host, result.job.port
                            ),
                        );
                    }
                    Err(error) => {
                        let status = classify_ssh_probe_error(&error);
                        failures += 1;
                        info!("[{current}/{attempted}] SSH scan failed for {}:{} ({status})", result.job.host, result.job.port
                        );
                        error!("{error:?}");
                    }
                },
                HostProbeCapture::Http(capture) => match capture {
                    Ok(capture) => {
                        let http_status = capture
                            .http_observation
                            .http_status_code
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "ok".to_string());
                        save_host_http_observation(&mut connection, &capture.http_observation)?;
                        if let Some(tls_observation) = capture
                            .tls_observation
                            .as_ref()
                            .filter(|row| row.status == SSH_STATUS_SUCCESS)
                        {
                            save_host_tls_observation(&mut connection, tls_observation)?;
                        }
                        successes += 1;
                        print_progress(
                            current,
                            attempted,
                            format!(
                                "Saved HTTP {} for {}:{}",
                                http_status, result.job.host, result.job.port
                            ),
                        );
                    }
                    Err(error) => {
                        let status = classify_service_probe_error(&error);
                        failures += 1;
                        info!("[{current}/{attempted}] HTTP probe failed for {}:{} ({status})", result.job.host, result.job.port
                        );
                        error!("{error:?}");
                    }
                },
                HostProbeCapture::Service(capture) => match capture {
                    Ok(capture) => {
                        let service = match result.job.kind {
                            HostProbeKind::Ftp => "ftp",
                            HostProbeKind::Irc => "irc",
                            _ => "service",
                        };
                        if capture.status == SSH_STATUS_SUCCESS {
                            save_host_service_observation(
                                &mut connection,
                                &NewHostServiceObservation {
                                    host: result.job.host.clone(),
                                    service: service.to_string(),
                                    port: i32::from(result.job.port),
                                    status: capture.status.clone(),
                                    banner: capture.banner.clone(),
                                    banner_fingerprint: capture.banner_fingerprint.clone(),
                                    last_error: None,
                                    last_attempt_at: String::new(),
                                    last_success_at: None,
                                },
                            )?;
                            successes += 1;
                            print_progress(
                                current,
                                attempted,
                                format!(
                                    "Saved {} banner for {}:{}",
                                    service.to_uppercase(),
                                    result.job.host,
                                    result.job.port
                                ),
                            );
                        } else {
                            failures += 1;
                            warn!(
                                current,
                                attempted,
                                service = service.to_uppercase(),
                                host = %result.job.host,
                                port = result.job.port,
                                status = %capture.status,
                                "Service probe mismatch"
                            );
                        }
                    }
                    Err(error) => {
                        let service = match result.job.kind {
                            HostProbeKind::Ftp => "ftp",
                            HostProbeKind::Irc => "irc",
                            _ => "service",
                        };
                        let status = classify_service_probe_error(&error);
                        failures += 1;
                        error!(
                            current,
                            attempted,
                            service = service.to_uppercase(),
                            host = %result.job.host,
                            port = result.job.port,
                            status = %status,
                            error = ?error,
                            "Service probe failed"
                        );
                    }
                },
            }
        }

        Ok::<(), anyhow::Error>(())
    })?;

    info!("Attempted {} service endpoints across {} hosts ({} successes, {} failures, {} skipped)", attempted, total_hosts, successes, failures, skipped
    );
    Ok(())
}

fn load_socks_proxy_config() -> Result<Option<SocksProxyConfig>> {
    configured_proxy_url()
        .map(|proxy_url| parse_socks_proxy_config(&proxy_url))
        .transpose()
}

fn load_best_effort_tls_proxy_config() -> Option<SocksProxyConfig> {
    match load_socks_proxy_config() {
        Ok(proxy) => proxy,
        Err(error) => {
            warn!("TLS fingerprint probe disabled: {error:#}");
            None
        }
    }
}

fn parse_socks_proxy_config(proxy_url: &str) -> Result<SocksProxyConfig> {
    let parsed =
        Url::parse(proxy_url).with_context(|| format!("invalid proxy url: {proxy_url}"))?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "socks5" | "socks5h"),
        "ssh-scan service probes require a socks5 proxy url, got {}",
        parsed.scheme()
    );

    let host = parsed
        .host_str()
        .map(|value| value.to_string())
        .context("proxy url must include a host")?;
    let port = parsed
        .port_or_known_default()
        .context("proxy url must include a port")?;
    let username = if parsed.username().is_empty() {
        None
    } else {
        Some(parsed.username().to_string())
    };

    Ok(SocksProxyConfig {
        host,
        port,
        username,
        password: parsed.password().map(|value| value.to_string()),
    })
}

fn endpoint_from_url(url: &str) -> Option<UrlEndpoint> {
    let parsed = Url::parse(url).ok()?;
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

fn headers_by_name(headers: &HeaderMap) -> BTreeMap<String, Vec<String>> {
    let mut grouped = BTreeMap::new();
    for (name, value) in headers {
        let rendered = normalize_header_value(value);
        if rendered.is_empty() {
            continue;
        }

        grouped
            .entry(name.as_str().to_ascii_lowercase())
            .or_insert_with(Vec::new)
            .push(rendered);
    }

    for values in grouped.values_mut() {
        values.sort();
        values.dedup();
    }

    grouped
}

fn normalize_header_value(value: &reqwest::header::HeaderValue) -> String {
    String::from_utf8_lossy(value.as_bytes())
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn first_header_value(headers: &BTreeMap<String, Vec<String>>, name: &str) -> Option<String> {
    headers.get(name).and_then(|values| values.first()).cloned()
}

fn collect_set_cookie_names(headers: &BTreeMap<String, Vec<String>>) -> Option<String> {
    let values = headers.get("set-cookie")?;
    let mut names = values
        .iter()
        .filter_map(|value| {
            value
                .split(';')
                .next()
                .and_then(|pair| pair.split_once('='))
                .map(|(name, _)| name.trim().to_string())
                .filter(|name| !name.is_empty())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if names.is_empty() {
        None
    } else {
        names.sort();
        Some(names.join(", "))
    }
}

fn render_response_headers(headers: &BTreeMap<String, Vec<String>>) -> Option<String> {
    let rendered = headers
        .iter()
        .map(|(name, values)| format!("{name}: {}", values.join(" | ")))
        .collect::<Vec<_>>()
        .join("\n");
    if rendered.is_empty() {
        None
    } else {
        Some(truncate_for_storage(&rendered, 12_000))
    }
}

fn build_header_fingerprint(headers: &BTreeMap<String, Vec<String>>) -> Option<String> {
    const EXCLUDED_HEADERS: &[&str] = &[
        "cf-ray",
        "content-length",
        "date",
        "etag",
        "last-modified",
        "request-id",
        "server-timing",
        "set-cookie",
        "traceparent",
        "x-amz-cf-id",
        "x-amzn-requestid",
        "x-request-id",
    ];

    let stable_lines = headers
        .iter()
        .filter(|(name, _)| !EXCLUDED_HEADERS.contains(&name.as_str()))
        .map(|(name, values)| format!("{name}: {}", values.join(" | ")))
        .collect::<Vec<_>>();
    if stable_lines.is_empty() {
        return None;
    }

    let digest = Sha256::digest(stable_lines.join("\n").as_bytes());
    Some(format!("sha256:{}", hex_encode(digest.as_slice())))
}

fn favicon_hash_for_page(
    client: &Client,
    final_url: &str,
    body: &str,
) -> (Option<String>, Option<String>) {
    let Some(favicon_url) = extract_favicon_url(final_url, body) else {
        return (None, None);
    };
    let same_host = match (Url::parse(final_url).ok(), Url::parse(&favicon_url).ok()) {
        (Some(page_url), Some(icon_url)) => page_url.host_str() == icon_url.host_str(),
        _ => false,
    };
    if !same_host {
        return (Some(favicon_url), None);
    }

    let response = match client.get(&favicon_url).send() {
        Ok(response) => response,
        Err(_) => return (Some(favicon_url), None),
    };
    if !response.status().is_success() {
        return (Some(favicon_url), None);
    }

    let bytes = match response.bytes() {
        Ok(bytes) => bytes,
        Err(_) => return (Some(favicon_url), None),
    };
    if bytes.is_empty() {
        return (Some(favicon_url), None);
    }

    let digest = Sha256::digest(&bytes);
    (
        Some(favicon_url),
        Some(format!("sha256:{}", hex_encode(digest.as_slice()))),
    )
}

fn probe_exposed_web_resources(client: &Client, final_url: &str) -> Vec<ExposedWebResourceFinding> {
    let Ok(base_url) = Url::parse(final_url) else {
        return Vec::new();
    };

    WEB_RESOURCE_PROBES
        .iter()
        .filter_map(|probe| {
            let url = base_url.join(probe.path).ok()?;
            probe_exposed_web_resource(client, probe, url)
        })
        .collect()
}

fn probe_exposed_web_resource(
    client: &Client,
    probe: &WebResourceProbe,
    url: Url,
) -> Option<ExposedWebResourceFinding> {
    let mut response = client
        .get(url)
        .header(
            RANGE,
            format!("bytes=0-{}", WEB_RESOURCE_PROBE_MAX_BYTES.saturating_sub(1)),
        )
        .send()
        .ok()?;
    let status = response.status();
    if !status.is_success() {
        return None;
    }

    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .map(normalize_header_value);
    if !probe.allow_html
        && content_type
            .as_deref()
            .map(|value| value.to_ascii_lowercase().contains("text/html"))
            .unwrap_or(false)
    {
        return None;
    }
    let content_length = response.content_length();
    let mut bytes = Vec::new();
    if (&mut response)
        .take((WEB_RESOURCE_PROBE_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return None;
    }
    if bytes.is_empty() {
        return None;
    }

    let truncated = bytes.len() > WEB_RESOURCE_PROBE_MAX_BYTES;
    if truncated {
        bytes.truncate(WEB_RESOURCE_PROBE_MAX_BYTES);
    }
    let body_sample = String::from_utf8_lossy(&bytes).into_owned();
    if !web_resource_sample_matches(probe, &body_sample) {
        return None;
    }

    let digest = Sha256::digest(&bytes);
    Some(ExposedWebResourceFinding {
        path: probe.path.to_string(),
        status_code: status.as_u16(),
        content_type,
        content_length,
        bytes_read: bytes.len(),
        truncated,
        sample_sha256: format!("sha256:{}", hex_encode(digest.as_slice())),
        preview: exposed_resource_preview(&body_sample),
        body_sample,
    })
}

fn web_resource_sample_matches(probe: &WebResourceProbe, body_sample: &str) -> bool {
    let normalized = body_sample.to_ascii_lowercase();
    probe
        .markers
        .iter()
        .any(|marker| normalized.contains(marker))
}

fn exposed_resource_preview(body_sample: &str) -> Option<String> {
    let redacted = redact_sensitive_preview(body_sample);
    let preview = redacted
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(8)
        .collect::<Vec<_>>()
        .join(" | ");
    let preview = if preview.is_empty() {
        redacted.split_whitespace().collect::<Vec<_>>().join(" ")
    } else {
        preview
    };
    let preview = truncate_for_storage(&preview, WEB_RESOURCE_PREVIEW_CHARS);
    if preview.is_empty() {
        None
    } else {
        Some(preview)
    }
}

fn redact_sensitive_preview(value: &str) -> String {
    let patterns = [
        r#"(?i)\b([a-z0-9_.-]*(?:password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|secret[_-]?key|private[_-]?key|authorization|bearer|aws_secret_access_key)[a-z0-9_.-]*)\b\s*[:=]\s*([^\s'"&;]+)"#,
        r#"(?i)(postgres|mysql|mongodb|redis)://([^:@/\s]+):([^@/\s]+)@"#,
    ];
    let mut redacted = value.to_string();
    for pattern in patterns {
        if let Ok(regex) = regex::Regex::new(pattern) {
            redacted = regex
                .replace_all(&redacted, |captures: &regex::Captures<'_>| {
                    if captures.len() >= 4 {
                        format!("{}://{}:<redacted>@", &captures[1], &captures[2])
                    } else {
                        format!("{}=<redacted>", &captures[1])
                    }
                })
                .into_owned();
        }
    }
    redacted
}

fn render_exposed_resources(findings: &[ExposedWebResourceFinding]) -> Option<String> {
    if findings.is_empty() {
        return None;
    }

    let rendered = findings
        .iter()
        .map(|finding| {
            let mut fields = vec![
                format!("{} [{}]", finding.path, finding.status_code),
                format!("read_bytes={}", finding.bytes_read),
                format!("sample_sha256={}", finding.sample_sha256),
            ];
            if let Some(content_type) = finding.content_type.as_ref() {
                fields.push(format!("type={content_type}"));
            }
            if let Some(content_length) = finding.content_length {
                fields.push(format!("declared_bytes={content_length}"));
            }
            if finding.truncated {
                fields.push("truncated=true".to_string());
            }
            if let Some(preview) = finding.preview.as_ref() {
                fields.push(format!("preview=\"{}\"", preview.replace('"', "'")));
            }
            fields.join("; ")
        })
        .collect::<Vec<_>>()
        .join("\n");

    Some(truncate_for_storage(&rendered, 12_000))
}

fn build_stack_version_summary(
    headers: &BTreeMap<String, Vec<String>>,
    body: &str,
    resource_findings: &[ExposedWebResourceFinding],
) -> Option<String> {
    let mut lines = Vec::new();
    for header_name in [
        "server",
        "x-powered-by",
        "x-generator",
        "x-aspnet-version",
        "x-aspnetmvc-version",
        "x-drupal-cache",
        "via",
    ] {
        if let Some(values) = headers.get(header_name) {
            for value in values {
                push_unique(&mut lines, format!("{header_name}: {value}"));
            }
        }
    }

    if let Some(generator) = extract_meta_generator(body) {
        push_unique(&mut lines, format!("html-generator: {generator}"));
    }

    for finding in resource_findings {
        if matches!(finding.path.as_str(), "/phpinfo.php" | "/info.php") {
            if let Some(version) = extract_php_version(&finding.body_sample) {
                push_unique(&mut lines, format!("{}: PHP/{version}", finding.path));
            }
        }
    }

    if lines.is_empty() {
        None
    } else {
        Some(truncate_for_storage(&lines.join("\n"), 4_000))
    }
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !value.trim().is_empty() && !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn extract_meta_generator(body: &str) -> Option<String> {
    for pattern in [
        r#"(?is)<meta\s+[^>]*name=["']generator["'][^>]*content=["']([^"']+)["']"#,
        r#"(?is)<meta\s+[^>]*content=["']([^"']+)["'][^>]*name=["']generator["']"#,
    ] {
        let Ok(regex) = regex::Regex::new(pattern) else {
            continue;
        };
        let Some(captures) = regex.captures(body) else {
            continue;
        };
        let Some(value) = captures.get(1) else {
            continue;
        };
        let normalized = value
            .as_str()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if !normalized.is_empty() {
            return Some(truncate_for_storage(&normalized, 300));
        }
    }
    None
}

fn extract_php_version(body: &str) -> Option<String> {
    for pattern in [
        r#"(?i)PHP Version\s+([0-9][0-9A-Za-z._-]*)"#,
        r#"(?i)\bPHP/([0-9][0-9A-Za-z._-]*)"#,
    ] {
        let Ok(regex) = regex::Regex::new(pattern) else {
            continue;
        };
        if let Some(captures) = regex.captures(body) {
            let version = captures.get(1)?.as_str().trim();
            if !version.is_empty() {
                return Some(version.to_string());
            }
        }
    }
    None
}

fn build_http_observation(
    client: &Client,
    requested_endpoint: &UrlEndpoint,
    status: StatusCode,
    final_url: &str,
    headers: &HeaderMap,
    body: &str,
) -> NewHostHttpObservation {
    let normalized_headers = headers_by_name(headers);
    let (favicon_url, favicon_hash) = favicon_hash_for_page(client, final_url, body);
    let exposed_resource_findings = probe_exposed_web_resources(client, final_url);
    let stack_versions =
        build_stack_version_summary(&normalized_headers, body, &exposed_resource_findings);

    NewHostHttpObservation {
        host: requested_endpoint.host.clone(),
        scheme: requested_endpoint.scheme.clone(),
        port: requested_endpoint.port,
        status: SSH_STATUS_SUCCESS.to_string(),
        http_status_code: Some(i32::from(status.as_u16())),
        final_url: Some(final_url.to_string()),
        server_header: first_header_value(&normalized_headers, "server"),
        powered_by_header: first_header_value(&normalized_headers, "x-powered-by"),
        content_type_header: first_header_value(&normalized_headers, "content-type"),
        location_header: first_header_value(&normalized_headers, "location"),
        via_header: first_header_value(&normalized_headers, "via"),
        alt_svc_header: first_header_value(&normalized_headers, "alt-svc"),
        www_authenticate_header: first_header_value(&normalized_headers, "www-authenticate"),
        set_cookie_names: collect_set_cookie_names(&normalized_headers),
        response_headers: render_response_headers(&normalized_headers),
        header_fingerprint: build_header_fingerprint(&normalized_headers),
        favicon_url,
        favicon_hash,
        stack_versions,
        exposed_resources: render_exposed_resources(&exposed_resource_findings),
        last_error: None,
        last_attempt_at: String::new(),
        last_success_at: None,
    }
}

fn build_tls_observation(
    tls_proxy: Option<&SocksProxyConfig>,
    final_url: &str,
) -> Option<NewHostTlsObservation> {
    let endpoint = endpoint_from_url(final_url)?;
    if endpoint.scheme != "https" {
        return None;
    }

    match probe_tls_certificate(tls_proxy, &endpoint.host, endpoint.port as u16) {
        Ok(fingerprint) => Some(NewHostTlsObservation {
            host: endpoint.host,
            port: endpoint.port,
            status: SSH_STATUS_SUCCESS.to_string(),
            certificate_sha256: Some(fingerprint),
            last_error: None,
            last_attempt_at: String::new(),
            last_success_at: None,
        }),
        Err(error) => Some(NewHostTlsObservation {
            host: endpoint.host,
            port: endpoint.port,
            status: classify_tls_probe_error(&error).to_string(),
            certificate_sha256: None,
            last_error: Some(truncate_for_storage(&error.to_string(), 500)),
            last_attempt_at: String::new(),
            last_success_at: None,
        }),
    }
}

fn probe_tls_certificate(
    proxy: Option<&SocksProxyConfig>,
    host: &str,
    port: u16,
) -> Result<String> {
    let stream = connect_tcp_endpoint(proxy, host, port)?;
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()
        .context("failed to build tls connector")?;
    let tls_stream = connector
        .connect(host, stream)
        .with_context(|| format!("tls handshake failed for {host}:{port}"))?;
    let certificate = tls_stream
        .peer_certificate()
        .context("failed to read peer certificate")?
        .context("peer completed tls handshake without a certificate")?;
    let der = certificate
        .to_der()
        .context("failed to serialize peer certificate")?;
    let digest = Sha256::digest(&der);
    Ok(format!("sha256:{}", hex_encode(digest.as_slice())))
}

fn classify_tls_probe_error(error: &anyhow::Error) -> &'static str {
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|io_error| io_error.kind() == std::io::ErrorKind::TimedOut)
            .unwrap_or(false)
    }) {
        return "timeout";
    }
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|io_error| io_error.kind() == std::io::ErrorKind::ConnectionRefused)
            .unwrap_or(false)
    }) {
        return "connection-refused";
    }

    let rendered = error.to_string().to_ascii_lowercase();
    if rendered.contains("without a certificate") {
        "no-certificate"
    } else if rendered.contains("socks") || rendered.contains("proxy") {
        "proxy-error"
    } else if error
        .chain()
        .any(|cause| cause.downcast_ref::<native_tls::Error>().is_some())
    {
        "tls-handshake-failed"
    } else {
        "network-error"
    }
}

fn probe_ssh_endpoint(
    proxy: Option<&SocksProxyConfig>,
    host: &str,
    port: u16,
) -> Result<SshHandshakeCapture> {
    let stream = connect_tcp_endpoint(proxy, host, port)?;
    let mut session = Session::new().context("failed to create ssh session")?;
    session.set_timeout(TCP_IO_TIMEOUT.as_millis() as u32);
    session.set_tcp_stream(stream);
    session
        .handshake()
        .with_context(|| format!("ssh handshake failed for {host}:{port}"))?;

    let (host_key, host_key_type) = session
        .host_key()
        .context("ssh server completed handshake without a host key")?;
    let algorithm = ssh_host_key_algorithm_name(host_key_type).to_string();
    let fingerprint = session
        .host_key_hash(HashType::Sha256)
        .map(|bytes| format!("sha256:{}", hex_encode(bytes)))
        .unwrap_or_else(|| format!("sha256:{}", hex_encode(host_key)));
    let server_banner = session.banner().map(|value| value.to_string());
    let _ = session.disconnect(None, "done", None);

    Ok(SshHandshakeCapture {
        algorithm,
        host_key: hex_encode(host_key),
        fingerprint,
        server_banner,
    })
}

fn read_stream_banner(stream: &mut impl Read, max_bytes: usize) -> Result<Option<String>> {
    let mut buffer = vec![0_u8; max_bytes];
    let bytes_read = stream
        .read(&mut buffer)
        .context("failed to read service banner")?;
    if bytes_read == 0 {
        return Ok(None);
    }

    Ok(normalize_banner_text(&String::from_utf8_lossy(
        &buffer[..bytes_read],
    )))
}

fn normalize_banner_text(value: &str) -> Option<String> {
    let normalized = value
        .replace('\r', " ")
        .replace('\n', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if normalized.is_empty() {
        None
    } else {
        Some(truncate_for_storage(&normalized, 500))
    }
}

fn banner_is_ftp(banner: &Option<String>) -> bool {
    banner
        .as_deref()
        .map(|value| {
            let upper = value.to_ascii_uppercase();
            upper.starts_with("220") || upper.contains("FTP")
        })
        .unwrap_or(false)
}

fn banner_is_irc(banner: &Option<String>) -> bool {
    banner
        .as_deref()
        .map(|value| {
            let upper = value.to_ascii_uppercase();
            upper.contains("NOTICE AUTH")
                || upper.contains(" PING ")
                || upper.contains(" CAP ")
                || upper.contains(" 001 ")
                || upper.contains("ERROR :")
        })
        .unwrap_or(false)
}

fn banner_fingerprint(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    format!("sha256:{}", hex_encode(digest.as_slice()))
}

fn connect_tcp_endpoint(
    proxy: Option<&SocksProxyConfig>,
    host: &str,
    port: u16,
) -> Result<TcpStream> {
    match proxy {
        Some(config) => connect_via_socks_proxy(config, host, port),
        None => {
            let address = resolve_socket_addr(host, port)?;
            let stream = TcpStream::connect_timeout(&address, TCP_CONNECT_TIMEOUT)
                .with_context(|| format!("tcp connect failed for {host}:{port}"))?;
            apply_tcp_timeouts(&stream)?;
            Ok(stream)
        }
    }
}

fn connect_via_socks_proxy(
    proxy: &SocksProxyConfig,
    target_host: &str,
    target_port: u16,
) -> Result<TcpStream> {
    let proxy_address = resolve_socket_addr(&proxy.host, proxy.port)?;
    let mut stream =
        TcpStream::connect_timeout(&proxy_address, TCP_CONNECT_TIMEOUT).with_context(|| {
            format!(
                "tcp connect failed for SOCKS proxy {}",
                describe_socks_endpoint(proxy)
            )
        })?;
    apply_tcp_timeouts(&stream)?;

    let methods = if proxy.username.is_some() || proxy.password.is_some() {
        vec![0x00_u8, 0x02_u8]
    } else {
        vec![0x00_u8]
    };
    let mut greeting = vec![0x05_u8, methods.len() as u8];
    greeting.extend_from_slice(&methods);
    stream
        .write_all(&greeting)
        .context("SOCKS proxy greeting write failed")?;

    let mut greeting_response = [0_u8; 2];
    stream
        .read_exact(&mut greeting_response)
        .context("SOCKS proxy greeting response read failed")?;
    anyhow::ensure!(
        greeting_response[0] == 0x05,
        "SOCKS proxy replied with unsupported version {}",
        greeting_response[0]
    );
    match greeting_response[1] {
        0x00 => {}
        0x02 => perform_socks5_username_password_auth(&mut stream, proxy)?,
        0xFF => anyhow::bail!("SOCKS proxy rejected all authentication methods"),
        method => anyhow::bail!("SOCKS proxy selected unsupported auth method {method}"),
    }

    let mut request = vec![0x05_u8, 0x01_u8, 0x00_u8];
    if let Ok(ip) = target_host.parse::<IpAddr>() {
        match ip {
            IpAddr::V4(value) => {
                request.push(0x01);
                request.extend_from_slice(&value.octets());
            }
            IpAddr::V6(value) => {
                request.push(0x04);
                request.extend_from_slice(&value.octets());
            }
        }
    } else {
        let host_bytes = target_host.as_bytes();
        anyhow::ensure!(
            host_bytes.len() <= u8::MAX as usize,
            "target host is too long for SOCKS5: {target_host}"
        );
        request.push(0x03);
        request.push(host_bytes.len() as u8);
        request.extend_from_slice(host_bytes);
    }
    request.extend_from_slice(&target_port.to_be_bytes());

    stream.write_all(&request).with_context(|| {
        format!("SOCKS connect request write failed for {target_host}:{target_port}")
    })?;

    let mut response_header = [0_u8; 4];
    stream
        .read_exact(&mut response_header)
        .context("SOCKS proxy connect response read failed")?;
    anyhow::ensure!(
        response_header[0] == 0x05,
        "SOCKS proxy connect reply used unsupported version {}",
        response_header[0]
    );
    anyhow::ensure!(
        response_header[1] == 0x00,
        "SOCKS proxy connect failed for {}:{} ({})",
        target_host,
        target_port,
        socks_reply_label(response_header[1])
    );
    read_socks_reply_tail(&mut stream, response_header[3])?;

    Ok(stream)
}

fn perform_socks5_username_password_auth(
    stream: &mut TcpStream,
    proxy: &SocksProxyConfig,
) -> Result<()> {
    let username = proxy.username.as_deref().unwrap_or_default().as_bytes();
    let password = proxy.password.as_deref().unwrap_or_default().as_bytes();
    anyhow::ensure!(
        username.len() <= u8::MAX as usize && password.len() <= u8::MAX as usize,
        "SOCKS proxy credentials are too long"
    );

    let mut request = vec![0x01_u8, username.len() as u8];
    request.extend_from_slice(username);
    request.push(password.len() as u8);
    request.extend_from_slice(password);
    stream
        .write_all(&request)
        .context("SOCKS proxy auth request write failed")?;

    let mut response = [0_u8; 2];
    stream
        .read_exact(&mut response)
        .context("SOCKS proxy auth response read failed")?;
    anyhow::ensure!(
        response[1] == 0x00,
        "SOCKS proxy username/password authentication failed"
    );
    Ok(())
}

fn read_socks_reply_tail(stream: &mut TcpStream, address_type: u8) -> Result<()> {
    match address_type {
        0x01 => {
            let mut buffer = [0_u8; 4];
            stream.read_exact(&mut buffer)?;
        }
        0x03 => {
            let mut length = [0_u8; 1];
            stream.read_exact(&mut length)?;
            let mut buffer = vec![0_u8; length[0] as usize];
            stream.read_exact(&mut buffer)?;
        }
        0x04 => {
            let mut buffer = [0_u8; 16];
            stream.read_exact(&mut buffer)?;
        }
        other => anyhow::bail!("SOCKS proxy returned unsupported address type {other}"),
    }

    let mut port = [0_u8; 2];
    stream.read_exact(&mut port)?;
    Ok(())
}

fn resolve_socket_addr(host: &str, port: u16) -> Result<SocketAddr> {
    (host, port)
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve {host}:{port}"))?
        .next()
        .with_context(|| format!("no socket address found for {host}:{port}"))
}

fn apply_tcp_timeouts(stream: &TcpStream) -> Result<()> {
    stream
        .set_read_timeout(Some(TCP_IO_TIMEOUT))
        .context("failed to set tcp read timeout")?;
    stream
        .set_write_timeout(Some(TCP_IO_TIMEOUT))
        .context("failed to set tcp write timeout")?;
    Ok(())
}

fn should_skip_network_attempt(
    last_attempt_at: Option<&str>,
    host_last_scanned_at: &str,
    stale_cutoff: &str,
) -> bool {
    last_attempt_at
        .map(|last_attempt_at| {
            last_attempt_at >= stale_cutoff && last_attempt_at >= host_last_scanned_at
        })
        .unwrap_or(false)
}

fn ssh_stale_cutoff_timestamp(conn: &mut PgConnection, stale_hours: i64) -> Result<String> {
    let stale_hours = stale_hours.clamp(1, 24 * 365);
    let query = match conn.dialect() {
        SqlDialect::Postgres => format!(
            "SELECT to_char(timezone('UTC', now()) - INTERVAL '{stale_hours} hours', 'YYYY-MM-DD HH24:MI:SS') AS value"
        ),
        SqlDialect::Sqlite => {
            format!("SELECT datetime(CURRENT_TIMESTAMP, '-{stale_hours} hours') AS value")
        }
    };
    diesel::sql_query(query)
        .get_result::<NullableTextValueRow>(conn)
        .context("error loading ssh stale cutoff timestamp")?
        .value
        .context("ssh stale cutoff query returned no value")
}

fn classify_ssh_probe_error(error: &anyhow::Error) -> &'static str {
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|io_error| io_error.kind() == std::io::ErrorKind::TimedOut)
            .unwrap_or(false)
    }) {
        return "timeout";
    }
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|io_error| io_error.kind() == std::io::ErrorKind::ConnectionRefused)
            .unwrap_or(false)
    }) {
        return "connection-refused";
    }

    let rendered = error.to_string().to_ascii_lowercase();
    if rendered.contains("socks") || rendered.contains("proxy") {
        "proxy-error"
    } else if error
        .chain()
        .any(|cause| cause.downcast_ref::<ssh2::Error>().is_some())
    {
        "handshake-failed"
    } else {
        "network-error"
    }
}

fn classify_service_probe_error(error: &anyhow::Error) -> &'static str {
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|io_error| io_error.kind() == std::io::ErrorKind::TimedOut)
            .unwrap_or(false)
    }) {
        return "timeout";
    }
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|io_error| io_error.kind() == std::io::ErrorKind::ConnectionRefused)
            .unwrap_or(false)
    }) {
        return "connection-refused";
    }

    let rendered = error.to_string().to_ascii_lowercase();
    if rendered.contains("socks") || rendered.contains("proxy") {
        "proxy-error"
    } else {
        "network-error"
    }
}

fn ssh_host_key_algorithm_name(key_type: HostKeyType) -> &'static str {
    match key_type {
        HostKeyType::Rsa => "ssh-rsa",
        HostKeyType::Dss => "ssh-dss",
        HostKeyType::Ecdsa256 => "ecdsa-sha2-nistp256",
        HostKeyType::Ecdsa384 => "ecdsa-sha2-nistp384",
        HostKeyType::Ecdsa521 => "ecdsa-sha2-nistp521",
        HostKeyType::Ed25519 => "ssh-ed25519",
        HostKeyType::Unknown => "unknown",
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn truncate_for_storage(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn describe_socks_endpoint(proxy: &SocksProxyConfig) -> String {
    format!("{}:{}", proxy.host, proxy.port)
}

fn socks_reply_label(code: u8) -> &'static str {
    match code {
        0x01 => "general failure",
        0x02 => "connection not allowed",
        0x03 => "network unreachable",
        0x04 => "host unreachable",
        0x05 => "connection refused",
        0x06 => "ttl expired",
        0x07 => "command not supported",
        0x08 => "address type not supported",
        _ => "unknown error",
    }
}

fn select_work_units_for_processing(
    work_units: Vec<spyder::models::WorkUnit>,
    options: WorkOptions,
) -> Vec<spyder::models::WorkUnit> {
    work_units
        .into_iter()
        .filter(|work_unit| !options.onion_only || url_targets_onion(&work_unit.url))
        .collect()
}

fn url_targets_onion(url: &str) -> bool {
    Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed
                .host_str()
                .map(|host| host.trim_end_matches('.').to_string())
        })
        .map(|host| host.to_ascii_lowercase().ends_with(".onion"))
        .unwrap_or(false)
}

fn enqueue_discovered_links(
    connection: &mut PgConnection,
    snapshot: &spyder::models::PageSnapshot,
) -> Result<DiscoveryEnqueueOutcome> {
    let blacklist_domains = list_domain_blacklist_rules(connection)?
        .into_iter()
        .map(|rule| rule.domain)
        .collect::<Vec<_>>();
    enqueue_discovered_links_with(snapshot, &blacklist_domains, |url, blacklist_domains| {
        create_work_unit_unless_blacklisted(connection, url, blacklist_domains)
    })
}

fn enqueue_discovered_links_with(
    snapshot: &spyder::models::PageSnapshot,
    blacklist_domains: &[String],
    mut queue_url: impl FnMut(&str, &[String]) -> Result<WorkQueueOutcome>,
) -> Result<DiscoveryEnqueueOutcome> {
    let mut outcome = DiscoveryEnqueueOutcome {
        queued_count: 0,
        skipped_blacklisted_count: 0,
    };

    for link in &snapshot.links {
        let parsed = Url::parse(&link.target_url)
            .with_context(|| format!("invalid discovered url: {}", link.target_url))?;
        match parsed.scheme() {
            "http" | "https" => match queue_url(parsed.as_str(), blacklist_domains)? {
                WorkQueueOutcome::Queued => outcome.queued_count += 1,
                WorkQueueOutcome::SkippedBlacklisted => outcome.skipped_blacklisted_count += 1,
            },
            _ => {}
        }
    }
    Ok(outcome)
}

fn list_blacklist() -> Result<()> {
    let mut connection = establish_connection()?;
    let entries = list_domain_blacklist_rules(&mut connection)?;

    if entries.is_empty() {
        info!("No blacklisted domains configured");
        return Ok(());
    }

    for entry in entries {
        info!(domain = %entry.domain, "Blacklist entry");
    }
    Ok(())
}

fn add_blacklist_domain(raw_domain: &str) -> Result<()> {
    let mut connection = establish_connection()?;
    let entry = add_domain_blacklist_entry(&mut connection, raw_domain)?;
    info!(domain = %entry.domain, "Domain blacklisted");
    Ok(())
}

fn remove_blacklist_domain(raw_domain: &str) -> Result<()> {
    let mut connection = establish_connection()?;
    let domain = remove_domain_blacklist_entry(&mut connection, raw_domain)?;
    info!(domain = %domain, "Removed blacklist entry");
    Ok(())
}

fn list_auto_blacklist() -> Result<()> {
    let mut connection = establish_connection()?;
    let rules = list_auto_blacklist_rules(&mut connection)?;

    if rules.is_empty() {
        info!("No auto blacklist rules configured");
        return Ok(());
    }

    for rule in rules {
        info!("#{} [{}] {} = {} ({})", rule.id,
            if rule.enabled { "enabled" } else { "disabled" },
            rule.rule_type,
            rule.value,
            rule.label
        );
    }
    Ok(())
}

fn add_auto_blacklist_category(category: &str, label: Option<&str>) -> Result<()> {
    let mut connection = establish_connection()?;
    let rule = add_auto_blacklist_rule(
        &mut connection,
        AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY,
        category,
        label,
    )?;
    info!("Auto blacklist rule #{}: site_category = {} ({})", rule.id, rule.value, rule.label
    );
    Ok(())
}

fn add_auto_blacklist_keyword(keyword: &str, label: Option<&str>) -> Result<()> {
    let mut connection = establish_connection()?;
    let rule = add_auto_blacklist_rule(
        &mut connection,
        AUTO_BLACKLIST_RULE_TYPE_KEYWORD,
        keyword,
        label,
    )?;
    info!("Auto blacklist rule #{}: keyword = {} ({})", rule.id, rule.value, rule.label
    );
    Ok(())
}

fn set_auto_blacklist_enabled(rule_id: i32, enabled: bool) -> Result<()> {
    let mut connection = establish_connection()?;
    match set_auto_blacklist_rule_enabled(&mut connection, rule_id, enabled)? {
        Some(rule) => info!(
            action = if enabled { "Enabled" } else { "Disabled" },
            rule_id = rule.id,
            rule_type = %rule.rule_type,
            value = %rule.value,
            "Auto blacklist rule updated"
        ),
        None => info!("No matching auto blacklist rule found"),
    }
    Ok(())
}

fn remove_auto_blacklist(rule_id: i32) -> Result<()> {
    let mut connection = establish_connection()?;
    match remove_auto_blacklist_rule(&mut connection, rule_id)? {
        Some(rule) => info!(
            rule_id = rule.id,
            rule_type = %rule.rule_type,
            value = %rule.value,
            "Removed auto blacklist rule"
        ),
        None => info!("No matching auto blacklist rule found"),
    }
    Ok(())
}

fn apply_existing_auto_blacklist(dry_run: bool, limit: Option<i64>) -> Result<()> {
    let mut connection = establish_connection()?;
    let result = apply_auto_blacklist_rules_to_existing(&mut connection, dry_run, limit)?;
    info!("{} scanned {} rows, matched {}, blacklisted {}, recorded {} events", if result.dry_run {
            "Dry run"
        } else {
            "Auto blacklist backfill"
        },
        result.scanned_count,
        result.matched_count,
        result.blacklisted_count,
        result.event_count
    );
    for matched in result.matches.iter().take(25) {
        info!("- {} via #{} [{}:{}] {}", matched.domain,
            matched.rule_id,
            matched.rule_type,
            matched.matched_value,
            matched.evidence
        );
    }
    if result.matches.len() > 25 {
        info!("Additional matches truncated");
    }
    Ok(())
}

fn list_forum_keywords() -> Result<()> {
    let mut connection = establish_connection()?;
    let rules = list_forum_keyword_rules(&mut connection)?;

    if rules.is_empty() {
        info!("No forum keyword rules configured");
        return Ok(());
    }

    for rule in rules {
        info!("keyword:{} => {}", rule.label, rule.pattern);
    }
    Ok(())
}

fn add_forum_keyword(label: &str, pattern: &str) -> Result<()> {
    let mut connection = establish_connection()?;
    let rule = add_forum_keyword_rule(&mut connection, label, pattern)?;
    info!("Added keyword:{} => {}", rule.label, rule.pattern);
    Ok(())
}

fn remove_forum_keyword(label: &str, pattern: &str) -> Result<()> {
    let mut connection = establish_connection()?;
    let removed = remove_forum_keyword_rule(&mut connection, label, pattern)?;
    match removed {
        Some((label, pattern)) => info!(label = %label, pattern = %pattern, "Removed forum keyword rule"),
        None => info!("No matching forum keyword rule found"),
    }
    Ok(())
}

fn list_watchlist() -> Result<()> {
    let mut connection = establish_connection()?;
    let items = list_watchlist_items(&mut connection)?;
    if items.is_empty() {
        info!("No watchlist items configured");
        return Ok(());
    }
    for item in items {
        let label = if item.label.is_empty() {
            String::new()
        } else {
            format!(" ({})", item.label)
        };
        info!("#{} [{}] {}{}", item.id, item.item_type, item.value, label);
    }
    Ok(())
}

fn add_watchlist(item_type: &str, value: &str, label: Option<&str>) -> Result<()> {
    let mut connection = establish_connection()?;
    let item = add_watchlist_item(&mut connection, item_type, value, label)?;
    info!("Watching #{} [{}] {}", item.id, item.item_type, item.value);
    Ok(())
}

fn remove_watchlist(item_id: i32) -> Result<()> {
    let mut connection = establish_connection()?;
    match remove_watchlist_item(&mut connection, item_id)? {
        Some(item) => info!(id = item.id, item_type = %item.item_type, value = %item.value, "Removed watchlist item"),
        None => info!("No matching watchlist item found"),
    }
    Ok(())
}

fn import_sqlite(sqlite_path: &str) -> Result<()> {
    anyhow::ensure!(
        !sqlite_path.trim().is_empty(),
        "sqlite path must not be empty"
    );
    anyhow::ensure!(
        sqlite_path.starts_with("file:")
            || sqlite_path == ":memory:"
            || Path::new(sqlite_path).exists(),
        "sqlite database does not exist: {sqlite_path}"
    );

    let mut source = SqliteConnection::establish(sqlite_path)
        .with_context(|| format!("error opening sqlite database {sqlite_path}"))?;
    let mut target = establish_connection()?;
    ensure_postgres_import_target_is_empty(&mut target)?;

    let source_tables = load_sqlite_table_names(&mut source)?;
    ensure_sqlite_source_looks_like_spyder_database(&source_tables)?;
    print_status(format!("Importing SQLite data from {sqlite_path}"));

    import_table_if_present(
        "work_unit",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::work_unit::dsl as work_unit_dsl;

            work_unit_dsl::work_unit
                .filter(work_unit_dsl::id.gt(last_id))
                .order(work_unit_dsl::id.asc())
                .limit(limit)
                .select(WorkUnit::as_select())
                .load::<WorkUnit>(conn)
                .map_err(Into::into)
        },
        |row| ImportedWorkUnit {
            id: row.id,
            url: row.url,
            status: row.status,
            retry_count: row.retry_count,
            next_attempt_at: row.next_attempt_at,
            last_attempt_at: row.last_attempt_at,
            last_error: row.last_error,
            created_at: row.created_at,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::work_unit::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "page",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::page::dsl as page_dsl;

            page_dsl::page
                .filter(page_dsl::id.gt(last_id))
                .order(page_dsl::id.asc())
                .limit(limit)
                .select(Page::as_select())
                .load::<Page>(conn)
                .map_err(Into::into)
        },
        |row| ImportedPage {
            id: row.id,
            title: row.title,
            url: row.url,
            links: row.links,
            emails: row.emails,
            coins: row.coins,
            language: row.language,
            last_scanned_at: row.last_scanned_at,
            created_at: row.created_at,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::page::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "page_scan",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::page_scan::dsl as page_scan_dsl;

            page_scan_dsl::page_scan
                .filter(page_scan_dsl::id.gt(last_id))
                .order(page_scan_dsl::id.asc())
                .limit(limit)
                .select(PageScan::as_select())
                .load::<PageScan>(conn)
                .map_err(Into::into)
        },
        |row| ImportedPageScan {
            id: row.id,
            page_id: row.page_id,
            title: row.title,
            language: row.language,
            scanned_at: row.scanned_at,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::page_scan::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "page_scan_link",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::page_scan_link::dsl as page_scan_link_dsl;

            page_scan_link_dsl::page_scan_link
                .filter(page_scan_link_dsl::id.gt(last_id))
                .order(page_scan_link_dsl::id.asc())
                .limit(limit)
                .select(PageScanLink::as_select())
                .load::<PageScanLink>(conn)
                .map_err(Into::into)
        },
        |row| ImportedPageScanLink {
            id: row.id,
            scan_id: row.scan_id,
            target_url: row.target_url,
            target_host: row.target_host,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::page_scan_link::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "page_scan_email",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::page_scan_email::dsl as page_scan_email_dsl;

            page_scan_email_dsl::page_scan_email
                .filter(page_scan_email_dsl::id.gt(last_id))
                .order(page_scan_email_dsl::id.asc())
                .limit(limit)
                .select(PageScanEmail::as_select())
                .load::<PageScanEmail>(conn)
                .map_err(Into::into)
        },
        |row| ImportedPageScanEmail {
            id: row.id,
            scan_id: row.scan_id,
            email: row.email,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::page_scan_email::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "page_scan_crypto",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::page_scan_crypto::dsl as page_scan_crypto_dsl;

            page_scan_crypto_dsl::page_scan_crypto
                .filter(page_scan_crypto_dsl::id.gt(last_id))
                .order(page_scan_crypto_dsl::id.asc())
                .limit(limit)
                .select(PageScanCrypto::as_select())
                .load::<PageScanCrypto>(conn)
                .map_err(Into::into)
        },
        |row| ImportedPageScanCrypto {
            id: row.id,
            scan_id: row.scan_id,
            asset_type: row.asset_type,
            reference: row.reference,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::page_scan_crypto::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "page_link",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            diesel::sql_query(
                "
                SELECT
                    pl.id,
                    pl.source_page_id,
                    lower(
                        CASE
                            WHEN instr(p.url, '://') > 0 THEN
                                CASE
                                    WHEN instr(substr(p.url, instr(p.url, '://') + 3), '/') > 0 THEN
                                        substr(
                                            substr(p.url, instr(p.url, '://') + 3),
                                            1,
                                            instr(substr(p.url, instr(p.url, '://') + 3), '/') - 1
                                        )
                                    ELSE substr(p.url, instr(p.url, '://') + 3)
                                END
                            ELSE ''
                        END
                    ) AS source_host,
                    pl.target_url,
                    pl.target_host,
                    pl.created_at
                FROM page_link pl
                JOIN page p ON p.id = pl.source_page_id
                WHERE pl.id > ?
                ORDER BY pl.id ASC
                LIMIT ?
                ",
            )
            .bind::<diesel::sql_types::Integer, _>(last_id)
            .bind::<diesel::sql_types::BigInt, _>(limit)
            .load::<SourcePageLinkImportRow>(conn)
            .map_err(Into::into)
        },
        |row| ImportedPageLink {
            id: row.id,
            source_page_id: row.source_page_id,
            source_host: row.source_host,
            target_url: row.target_url,
            target_host: row.target_host,
            created_at: row.created_at,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::page_link::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "page_email",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::page_email::dsl as page_email_dsl;

            page_email_dsl::page_email
                .filter(page_email_dsl::id.gt(last_id))
                .order(page_email_dsl::id.asc())
                .limit(limit)
                .select(PageEmail::as_select())
                .load::<PageEmail>(conn)
                .map_err(Into::into)
        },
        |row| ImportedPageEmail {
            id: row.id,
            page_id: row.page_id,
            email: row.email,
            created_at: row.created_at,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::page_email::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "page_crypto",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::page_crypto::dsl as page_crypto_dsl;

            page_crypto_dsl::page_crypto
                .filter(page_crypto_dsl::id.gt(last_id))
                .order(page_crypto_dsl::id.asc())
                .limit(limit)
                .select(PageCrypto::as_select())
                .load::<PageCrypto>(conn)
                .map_err(Into::into)
        },
        |row| ImportedPageCrypto {
            id: row.id,
            page_id: row.page_id,
            asset_type: row.asset_type,
            reference: row.reference,
            created_at: row.created_at,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::page_crypto::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "page_classification",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::page_classification::dsl as page_classification_dsl;

            page_classification_dsl::page_classification
                .filter(page_classification_dsl::id.gt(last_id))
                .order(page_classification_dsl::id.asc())
                .limit(limit)
                .select(PageClassificationRecord::as_select())
                .load::<PageClassificationRecord>(conn)
                .map_err(Into::into)
        },
        |row| ImportedPageClassification {
            id: row.id,
            page_id: row.page_id,
            host: row.host,
            category: row.category,
            confidence: row.confidence,
            score: row.score,
            evidence: row.evidence,
            last_classified_at: row.last_classified_at,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::page_classification::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "site_profile",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::site_profile::dsl as site_profile_dsl;

            site_profile_dsl::site_profile
                .filter(site_profile_dsl::id.gt(last_id))
                .order(site_profile_dsl::id.asc())
                .limit(limit)
                .select((
                    site_profile_dsl::id,
                    site_profile_dsl::host,
                    site_profile_dsl::category,
                    site_profile_dsl::confidence,
                    site_profile_dsl::score,
                    site_profile_dsl::page_count,
                    site_profile_dsl::evidence,
                    site_profile_dsl::source_page_id,
                    site_profile_dsl::last_classified_at,
                    site_profile_dsl::created_at,
                ))
                .load::<SourceSiteProfile>(conn)
                .map_err(Into::into)
        },
        |row| ImportedSiteProfile {
            id: row.id,
            host: row.host,
            category: row.category,
            confidence: row.confidence,
            score: row.score,
            page_count: row.page_count,
            first_found_at: row.created_at.clone(),
            last_scanned_at: row.last_classified_at.clone(),
            evidence: row.evidence,
            source_page_id: row.source_page_id,
            last_classified_at: row.last_classified_at,
            created_at: row.created_at,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::site_profile::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "domain_blacklist",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::domain_blacklist::dsl as domain_blacklist_dsl;

            domain_blacklist_dsl::domain_blacklist
                .filter(domain_blacklist_dsl::id.gt(last_id))
                .order(domain_blacklist_dsl::id.asc())
                .limit(limit)
                .select(DomainBlacklistRule::as_select())
                .load::<DomainBlacklistRule>(conn)
                .map_err(Into::into)
        },
        |row| ImportedDomainBlacklistRule {
            id: row.id,
            domain: row.domain,
            created_at: row.created_at,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::domain_blacklist::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "host_ssh_observation",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::host_ssh_observation::dsl as host_ssh_observation_dsl;

            host_ssh_observation_dsl::host_ssh_observation
                .filter(host_ssh_observation_dsl::id.gt(last_id))
                .order(host_ssh_observation_dsl::id.asc())
                .limit(limit)
                .select(HostSshObservationRecord::as_select())
                .load::<HostSshObservationRecord>(conn)
                .map_err(Into::into)
        },
        |row| ImportedHostSshObservation {
            id: row.id,
            host: row.host,
            port: row.port,
            status: row.status,
            host_key_algorithm: row.host_key_algorithm,
            host_key: row.host_key,
            host_key_fingerprint: row.host_key_fingerprint,
            server_banner: row.server_banner,
            last_error: row.last_error,
            last_attempt_at: row.last_attempt_at,
            last_success_at: row.last_success_at,
            created_at: row.created_at,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::host_ssh_observation::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "forum_keyword_rule",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::forum_keyword_rule::dsl as forum_keyword_rule_dsl;

            forum_keyword_rule_dsl::forum_keyword_rule
                .filter(forum_keyword_rule_dsl::id.gt(last_id))
                .order(forum_keyword_rule_dsl::id.asc())
                .limit(limit)
                .select(ForumKeywordRule::as_select())
                .load::<ForumKeywordRule>(conn)
                .map_err(Into::into)
        },
        |row| ImportedForumKeywordRule {
            id: row.id,
            label: row.label,
            pattern: row.pattern,
            created_at: row.created_at,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::forum_keyword_rule::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;
    import_table_if_present(
        "page_keyword_tag",
        &source_tables,
        &mut source,
        &mut target,
        |conn, last_id, limit| {
            use spyder::schema::page_keyword_tag::dsl as page_keyword_tag_dsl;

            page_keyword_tag_dsl::page_keyword_tag
                .filter(page_keyword_tag_dsl::id.gt(last_id))
                .order(page_keyword_tag_dsl::id.asc())
                .limit(limit)
                .select(PageKeywordTag::as_select())
                .load::<PageKeywordTag>(conn)
                .map_err(Into::into)
        },
        |row| ImportedPageKeywordTag {
            id: row.id,
            page_id: row.page_id,
            tag: row.tag,
            created_at: row.created_at,
        },
        |conn, batch| {
            diesel::insert_into(spyder::schema::page_keyword_tag::table)
                .values(batch)
                .execute(conn)?;
            Ok(())
        },
    )?;

    refresh_imported_site_profile_scan_stats(&mut target)?;
    info!("SQLite import completed successfully");
    Ok(())
}

fn refresh_imported_site_profile_scan_stats(conn: &mut PgConnection) -> Result<()> {
    diesel::sql_query(
        "
        WITH page_stats AS (
            SELECT
                split_part(split_part(url, '://', 2), '/', 1) AS host,
                COUNT(*)::INTEGER AS page_count,
                MIN(created_at) AS first_found_at,
                MAX(last_scanned_at) AS last_scanned_at
            FROM page
            WHERE position('://' IN url) > 0
            GROUP BY split_part(split_part(url, '://', 2), '/', 1)
        )
        UPDATE site_profile sp
        SET
            page_count = page_stats.page_count,
            first_found_at = page_stats.first_found_at,
            last_scanned_at = page_stats.last_scanned_at
        FROM page_stats
        WHERE page_stats.host = sp.host
        ",
    )
    .execute(conn)
    .context("error refreshing imported site scan metadata")?;
    Ok(())
}

fn load_sqlite_table_names(conn: &mut SqliteConnection) -> Result<HashSet<String>> {
    Ok(diesel::sql_query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )
    .load::<TableNameRow>(conn)?
    .into_iter()
    .map(|row| row.name)
    .collect())
}

fn ensure_sqlite_source_looks_like_spyder_database(source_tables: &HashSet<String>) -> Result<()> {
    anyhow::ensure!(
        source_tables.contains("work_unit") || source_tables.contains("page"),
        "source sqlite database does not look like a spyder database"
    );
    Ok(())
}

fn ensure_postgres_import_target_is_empty(conn: &mut PgConnection) -> Result<()> {
    let table_names = [
        "work_unit",
        "page",
        "page_scan",
        "page_scan_link",
        "page_scan_email",
        "page_scan_crypto",
        "page_link",
        "page_email",
        "page_crypto",
        "page_classification",
        "site_profile",
        "domain_blacklist",
        "host_ssh_observation",
        "forum_keyword_rule",
        "page_keyword_tag",
    ];
    let existing_rows = table_names
        .iter()
        .map(|table_name| postgres_table_row_count(conn, table_name))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sum::<i64>();
    anyhow::ensure!(
        existing_rows == 0,
        "target PostgreSQL database is not empty; import-sqlite expects a fresh database"
    );
    Ok(())
}

fn postgres_table_row_count(conn: &mut PgConnection, table_name: &str) -> Result<i64> {
    Ok(
        diesel::sql_query(format!("SELECT COUNT(*) AS value FROM {table_name}"))
            .get_result::<BigIntValueRow>(conn)?
            .value,
    )
}

fn reset_postgres_identity_sequence(conn: &mut PgConnection, table_name: &str) -> Result<()> {
    let next_id = diesel::sql_query(format!(
        "SELECT COALESCE(MAX(id), 0) + 1 AS value FROM {table_name}"
    ))
    .get_result::<IntValueRow>(conn)?
    .value;
    conn.batch_execute(&format!(
        "ALTER TABLE {table_name} ALTER COLUMN id RESTART WITH {next_id}"
    ))?;
    Ok(())
}

fn import_table_if_present<SourceRow, TargetRow, LoadBatch, MapRow, InsertBatch>(
    table_name: &str,
    source_tables: &HashSet<String>,
    source: &mut SqliteConnection,
    target: &mut PgConnection,
    load_batch: LoadBatch,
    map_row: MapRow,
    insert_batch: InsertBatch,
) -> Result<usize>
where
    SourceRow: HasId,
    LoadBatch: Fn(&mut SqliteConnection, i32, i64) -> Result<Vec<SourceRow>>,
    MapRow: Fn(SourceRow) -> TargetRow,
    InsertBatch: Fn(&mut PgConnection, &[TargetRow]) -> Result<()>,
{
    if !source_tables.contains(table_name) {
        print_status(format!("Skipping missing source table {table_name}"));
        return Ok(0);
    }

    print_status(format!("Importing {table_name}"));
    let mut last_id = 0;
    let mut total = 0usize;
    let mut batches = 0usize;

    loop {
        let rows = load_batch(source, last_id, IMPORT_BATCH_SIZE)?;
        if rows.is_empty() {
            break;
        }

        last_id = rows.last().map(HasId::id).unwrap_or(last_id);
        let mapped = rows.into_iter().map(&map_row).collect::<Vec<_>>();
        insert_batch(target, &mapped)?;
        total += mapped.len();
        batches += 1;
        if batches % 20 == 0 {
            print_status(format!("Imported {total} rows into {table_name}"));
        }
    }

    reset_postgres_identity_sequence(target, table_name)?;
    info!("Imported {total} rows into {table_name}");
    Ok(total)
}

fn recompute_leads(options: LeadsRecomputeCliOptions) -> Result<()> {
    let mut connection = establish_connection()?;
    let should_page_blacklist = options.rule_ids.is_empty()
        || options
            .rule_ids
            .iter()
            .any(|rule_id| rule_id == "blacklisted-site-link");
    let blacklist_after_link_id = options.blacklist_after_link_id.unwrap_or(0);
    let blacklist_link_batch_size = options
        .blacklist_link_batch_size
        .unwrap_or(DEFAULT_BLACKLIST_LEAD_LINK_BATCH_SIZE);
    let blacklist_batch_upper_bound = if should_page_blacklist {
        page_link_batch_upper_bound(
            &mut connection,
            blacklist_after_link_id,
            blacklist_link_batch_size,
        )?
    } else {
        None
    };
    print_status("Starting intel lead recompute");
    let summary = recompute_intel_leads_with_reporter(
        &mut connection,
        IntelLeadRecomputeOptions {
            limit: options.limit,
            since_scan_id: options.since_scan_id,
            rule_ids: options.rule_ids.clone(),
            blacklist_after_link_id: options.blacklist_after_link_id,
            blacklist_link_batch_size: options.blacklist_link_batch_size,
        },
        |message| print_status(message),
    )?;
    for rule in &summary.rule_summaries {
        info!("Lead rule {}: {} candidates, {} created, {} updated, {} evidence rows touched", rule.rule_id,
            rule.candidate_count,
            rule.created_count,
            rule.updated_count,
            rule.evidence_count
        );
    }
    info!("Recomputed intel leads: {} candidates, {} created, {} updated, {} evidence rows touched", summary.candidate_count,
        summary.created_count,
        summary.updated_count,
        summary.evidence_count
    );
    if should_page_blacklist {
        match blacklist_batch_upper_bound {
            Some(next_after_link_id) => info!(
                next_after_link_id,
                blacklist_link_batch_size,
                "Next blacklist batch available - run with --blacklist-after-link-id"
            ),
            None => info!(after_link_id = blacklist_after_link_id, "No page_link rows remain"),
        }
    }
    Ok(())
}

fn suppress_lead(lead_id: i32) -> Result<()> {
    let mut connection = establish_connection()?;
    match suppress_intel_lead(&mut connection, lead_id)? {
        Some(lead) => {
            info!("Suppressed lead #{} [{}] {}", lead.id, lead.severity, lead.title
            );
            Ok(())
        }
        None => anyhow::bail!("intel lead {lead_id} was not found"),
    }
}

fn refresh_relationships() -> Result<()> {
    info!("Refreshing relationship overview materialized view...");
    let start = std::time::Instant::now();
    let mut connection = establish_connection()?;
    refresh_relationship_overview(&mut connection)?;
    let elapsed = start.elapsed();
    info!(duration_secs = elapsed.as_secs_f64(), "Relationship overview refreshed successfully");
    Ok(())
}

fn usage(program: &str) {
    info!("Usage: {program} [SUBCOMMAND] [OPTIONS]");
    info!("Subcommands:");
    info!("    add <url>      enqueue the seed page and discovered links.");
    info!("    blacklist list");
    info!("    blacklist add <domain>");
    info!("    blacklist remove <domain>");
    info!("    blacklist auto list");
    info!("    blacklist auto add-category <category> [label]");
    info!("    blacklist auto add-keyword <phrase> [label]");
    info!("    blacklist auto enable <id>");
    info!("    blacklist auto disable <id>");
    info!("    blacklist auto remove <id>");
    info!("    blacklist auto apply-existing --dry-run|--apply [--limit N]");
    info!("    forum-keywords list");
    info!("    forum-keywords add <label> <pattern>");
    info!("    forum-keywords remove <label> <pattern>");
    info!("    watchlist list");
    info!("    watchlist add <type> <value> [label]");
    info!("    watchlist remove <id>");
    info!("    import-sqlite <sqlite_path> import an existing SQLite database into PostgreSQL.");
    info!("    ssh-scan [--recent-hours N] [--stale-hours N] [--limit N] [--concurrency N] scan recent hosts for SSH, auxiliary HTTP, IRC, and FTP services.");
    info!("    work [--onion-only] [--concurrency N] process pending work units and store page metadata.");
    info!("    rescan-known [--onion-only] [--limit N] [--concurrency N] [--queue-only] queue known pages and scan them for updates.");
    info!("    leads recompute [--limit N] [--since-scan-id ID] [--rule RULE] [--blacklist-after-link-id ID] [--blacklist-link-batch-size N]");
    info!("    leads suppress <lead_id>");
    info!("    refresh-relationships              refresh the relationship graph overview cache.");
}

fn print_error(error: &anyhow::Error) {
    error!("{error:?}");

    if error
        .chain()
        .any(|cause| cause.to_string().contains("no such table:"))
    {
        warn!("HINT: database schema is missing. Run `diesel setup` and `diesel migration run`.");
    }
}

fn build_http_client() -> Result<Client> {
    // Crawl targets may present self-signed, expired, or otherwise invalid certificates.
    let mut builder = Client::builder()
        .timeout(Duration::from_secs(15))
        .danger_accept_invalid_certs(true)
        .no_proxy();
    if let Some(proxy_url) = configured_proxy_url() {
        builder = builder.proxy(
            Proxy::all(&proxy_url).with_context(|| format!("invalid proxy url: {proxy_url}"))?,
        );
    }

    builder.build().context("http client should build")
}

fn configured_proxy_url() -> Option<String> {
    configured_proxy_url_from_values(env::var("ALL_PROXY").ok(), env::var("all_proxy").ok())
}

fn configured_proxy_url_from_values(
    upper: Option<String>,
    lower: Option<String>,
) -> Option<String> {
    upper
        .into_iter()
        .chain(lower)
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

fn parse_work_options(args: impl IntoIterator<Item = String>) -> Result<WorkOptions> {
    let mut options = WorkOptions::default();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--onion-only" => options.onion_only = true,
            "--concurrency" => {
                options.concurrency =
                    parse_usize_option_value(args.next(), "--concurrency", 1, 64)?;
            }
            _ => anyhow::bail!("invalid work option: {arg}"),
        }
    }

    Ok(options)
}

fn parse_rescan_known_options(
    args: impl IntoIterator<Item = String>,
) -> Result<RescanKnownOptions> {
    let mut options = RescanKnownOptions::default();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--onion-only" => options.onion_only = true,
            "--queue-only" => options.queue_only = true,
            "--limit" => {
                options.limit = Some(parse_i64_option_value(
                    args.next(),
                    "--limit",
                    1,
                    1_000_000,
                )?);
            }
            "--concurrency" => {
                options.concurrency =
                    parse_usize_option_value(args.next(), "--concurrency", 1, 64)?;
            }
            _ => anyhow::bail!("invalid rescan-known option: {arg}"),
        }
    }

    Ok(options)
}

fn parse_ssh_scan_options(args: impl IntoIterator<Item = String>) -> Result<SshScanOptions> {
    let mut options = SshScanOptions::default();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--recent-hours" => {
                options.recent_hours =
                    parse_i64_option_value(args.next(), "--recent-hours", 1, 24 * 365)?;
            }
            "--stale-hours" => {
                options.stale_hours =
                    parse_i64_option_value(args.next(), "--stale-hours", 1, 24 * 365)?;
            }
            "--limit" => {
                options.limit = parse_i64_option_value(args.next(), "--limit", 1, 2_000)?;
            }
            "--concurrency" => {
                options.concurrency =
                    parse_usize_option_value(args.next(), "--concurrency", 1, 64)?;
            }
            _ => anyhow::bail!("invalid ssh-scan option: {arg}"),
        }
    }

    Ok(options)
}

fn parse_leads_recompute_options(
    args: impl IntoIterator<Item = String>,
) -> Result<LeadsRecomputeCliOptions> {
    let mut options = LeadsRecomputeCliOptions::default();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--limit" => {
                options.limit = Some(parse_i64_option_value(
                    args.next(),
                    "--limit",
                    1,
                    1_000_000,
                )?);
            }
            "--since-scan-id" => {
                options.since_scan_id = Some(parse_i32_option_value(
                    args.next(),
                    "--since-scan-id",
                    0,
                    i32::MAX,
                )?);
            }
            "--rule" => {
                let rule_id = args
                    .next()
                    .with_context(|| "missing value for --rule".to_string())?;
                options.rule_ids.push(rule_id);
            }
            "--blacklist-after-link-id" => {
                options.blacklist_after_link_id = Some(parse_i32_option_value(
                    args.next(),
                    "--blacklist-after-link-id",
                    0,
                    i32::MAX,
                )?);
            }
            "--blacklist-link-batch-size" => {
                options.blacklist_link_batch_size = Some(parse_i64_option_value(
                    args.next(),
                    "--blacklist-link-batch-size",
                    1,
                    1_000_000,
                )?);
            }
            _ => anyhow::bail!("invalid leads recompute option: {arg}"),
        }
    }

    Ok(options)
}

fn parse_auto_blacklist_apply_args(
    args: impl IntoIterator<Item = String>,
) -> Result<(bool, Option<i64>)> {
    let mut dry_run = true;
    let mut mode_seen = false;
    let mut limit = None;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dry-run" => {
                anyhow::ensure!(!mode_seen, "choose only one of --dry-run or --apply");
                dry_run = true;
                mode_seen = true;
            }
            "--apply" => {
                anyhow::ensure!(!mode_seen, "choose only one of --dry-run or --apply");
                dry_run = false;
                mode_seen = true;
            }
            "--limit" => {
                limit = Some(parse_i64_option_value(args.next(), "--limit", 1, 5_000)?);
            }
            _ => anyhow::bail!("invalid blacklist auto apply-existing option: {arg}"),
        }
    }

    Ok((dry_run, limit))
}

fn parse_i64_option_value(value: Option<String>, option: &str, min: i64, max: i64) -> Result<i64> {
    let raw = value.with_context(|| format!("missing value for {option}"))?;
    let parsed = raw
        .parse::<i64>()
        .with_context(|| format!("invalid integer value for {option}: {raw}"))?;
    anyhow::ensure!(
        parsed >= min && parsed <= max,
        "{option} must be between {min} and {max}"
    );
    Ok(parsed)
}

fn parse_usize_option_value(
    value: Option<String>,
    option: &str,
    min: usize,
    max: usize,
) -> Result<usize> {
    let raw = value.with_context(|| format!("missing value for {option}"))?;
    let parsed = raw
        .parse::<usize>()
        .with_context(|| format!("invalid integer value for {option}: {raw}"))?;
    anyhow::ensure!(
        parsed >= min && parsed <= max,
        "{option} must be between {min} and {max}"
    );
    Ok(parsed)
}

fn parse_i32_option_value(value: Option<String>, option: &str, min: i32, max: i32) -> Result<i32> {
    let raw = value.with_context(|| format!("missing value for {option}"))?;
    let parsed = raw
        .parse::<i32>()
        .with_context(|| format!("invalid integer value for {option}: {raw}"))?;
    anyhow::ensure!(
        parsed >= min && parsed <= max,
        "{option} must be between {min} and {max}"
    );
    Ok(parsed)
}

fn is_retriable_request_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(reqwest_error) = cause.downcast_ref::<reqwest::Error>() {
            reqwest_error.is_timeout() || reqwest_error.is_connect() || reqwest_error.is_request()
        } else {
            false
        }
    })
}

fn is_retriable_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn main() {
    // Initialize structured logging
    spyder::logging::init_tracing();

    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "spyder".to_string());
    let result = match args.next().as_deref() {
        Some("add") => match args.next() {
            Some(url) => build_http_client().and_then(|client| {
                enqueue_seed_and_links(&client, &url).map(|count| {
                    info!("Enqueued {count} URLs");
                })
            }),
            None => {
                usage(&program);
                Err(anyhow::anyhow!("no url is provided"))
            }
        },
        Some("blacklist") => match args.next().as_deref() {
            Some("list") => list_blacklist(),
            Some("add") => match args.next() {
                Some(domain) => add_blacklist_domain(&domain),
                None => {
                    usage(&program);
                    Err(anyhow::anyhow!("no blacklist domain is provided"))
                }
            },
            Some("remove") => match args.next() {
                Some(domain) => remove_blacklist_domain(&domain),
                None => {
                    usage(&program);
                    Err(anyhow::anyhow!("no blacklist domain is provided"))
                }
            },
            Some("auto") => match args.next().as_deref() {
                Some("list") => list_auto_blacklist(),
                Some("add-category") => match args.next() {
                    Some(category) => {
                        add_auto_blacklist_category(&category, args.next().as_deref())
                    }
                    None => {
                        usage(&program);
                        Err(anyhow::anyhow!(
                            "blacklist auto add-category requires <category> [label]"
                        ))
                    }
                },
                Some("add-keyword") => match args.next() {
                    Some(keyword) => add_auto_blacklist_keyword(&keyword, args.next().as_deref()),
                    None => {
                        usage(&program);
                        Err(anyhow::anyhow!(
                            "blacklist auto add-keyword requires <phrase> [label]"
                        ))
                    }
                },
                Some("enable") => match args.next() {
                    Some(rule_id) => match rule_id.parse::<i32>() {
                        Ok(rule_id) => set_auto_blacklist_enabled(rule_id, true),
                        Err(_) => Err(anyhow::anyhow!("invalid auto blacklist rule id: {rule_id}")),
                    },
                    None => {
                        usage(&program);
                        Err(anyhow::anyhow!("blacklist auto enable requires <id>"))
                    }
                },
                Some("disable") => match args.next() {
                    Some(rule_id) => match rule_id.parse::<i32>() {
                        Ok(rule_id) => set_auto_blacklist_enabled(rule_id, false),
                        Err(_) => Err(anyhow::anyhow!("invalid auto blacklist rule id: {rule_id}")),
                    },
                    None => {
                        usage(&program);
                        Err(anyhow::anyhow!("blacklist auto disable requires <id>"))
                    }
                },
                Some("remove") => match args.next() {
                    Some(rule_id) => match rule_id.parse::<i32>() {
                        Ok(rule_id) => remove_auto_blacklist(rule_id),
                        Err(_) => Err(anyhow::anyhow!("invalid auto blacklist rule id: {rule_id}")),
                    },
                    None => {
                        usage(&program);
                        Err(anyhow::anyhow!("blacklist auto remove requires <id>"))
                    }
                },
                Some("apply-existing") => parse_auto_blacklist_apply_args(args)
                    .and_then(|(dry_run, limit)| apply_existing_auto_blacklist(dry_run, limit)),
                Some(_) | None => {
                    usage(&program);
                    Err(anyhow::anyhow!(
                        "invalid or missing blacklist auto subcommand"
                    ))
                }
            },
            Some(_) | None => {
                usage(&program);
                Err(anyhow::anyhow!("invalid or missing blacklist subcommand"))
            }
        },
        Some("forum-keywords") => match args.next().as_deref() {
            Some("list") => list_forum_keywords(),
            Some("add") => match (args.next(), args.next()) {
                (Some(label), Some(pattern)) => add_forum_keyword(&label, &pattern),
                _ => {
                    usage(&program);
                    Err(anyhow::anyhow!(
                        "forum-keywords add requires <label> <pattern>"
                    ))
                }
            },
            Some("remove") => match (args.next(), args.next()) {
                (Some(label), Some(pattern)) => remove_forum_keyword(&label, &pattern),
                _ => {
                    usage(&program);
                    Err(anyhow::anyhow!(
                        "forum-keywords remove requires <label> <pattern>"
                    ))
                }
            },
            Some(_) | None => {
                usage(&program);
                Err(anyhow::anyhow!(
                    "invalid or missing forum-keywords subcommand"
                ))
            }
        },
        Some("watchlist") => match args.next().as_deref() {
            Some("list") => list_watchlist(),
            Some("add") => match (args.next(), args.next()) {
                (Some(item_type), Some(value)) => {
                    let label = args.collect::<Vec<_>>().join(" ");
                    let label = (!label.trim().is_empty()).then_some(label);
                    add_watchlist(&item_type, &value, label.as_deref())
                }
                _ => {
                    usage(&program);
                    Err(anyhow::anyhow!(
                        "watchlist add requires <type> <value> [label]"
                    ))
                }
            },
            Some("remove") => match args.next() {
                Some(item_id) => match item_id.parse::<i32>() {
                    Ok(item_id) => remove_watchlist(item_id),
                    Err(_) => {
                        usage(&program);
                        Err(anyhow::anyhow!("invalid watchlist id: {item_id}"))
                    }
                },
                None => {
                    usage(&program);
                    Err(anyhow::anyhow!("watchlist remove requires <id>"))
                }
            },
            Some(_) | None => {
                usage(&program);
                Err(anyhow::anyhow!("invalid or missing watchlist subcommand"))
            }
        },
        Some("import-sqlite") => match args.next() {
            Some(sqlite_path) => import_sqlite(&sqlite_path),
            None => {
                usage(&program);
                Err(anyhow::anyhow!(
                    "import-sqlite requires a path to the source sqlite database"
                ))
            }
        },
        Some("ssh-scan") => match parse_ssh_scan_options(args) {
            Ok(options) => ssh_scan_hosts(options),
            Err(error) => {
                usage(&program);
                Err(error)
            }
        },
        Some("work") => match parse_work_options(args) {
            Ok(options) => build_http_client().and_then(|client| work_queue(&client, options)),
            Err(error) => {
                usage(&program);
                Err(error)
            }
        },
        Some("rescan-known") => match parse_rescan_known_options(args) {
            Ok(options) => {
                build_http_client().and_then(|client| rescan_known_pages(&client, options))
            }
            Err(error) => {
                usage(&program);
                Err(error)
            }
        },
        Some("leads") => match args.next().as_deref() {
            Some("recompute") => match parse_leads_recompute_options(args) {
                Ok(options) => recompute_leads(options),
                Err(error) => {
                    usage(&program);
                    Err(error)
                }
            },
            Some("suppress") => match args.next() {
                Some(lead_id) => match lead_id.parse::<i32>() {
                    Ok(lead_id) => suppress_lead(lead_id),
                    Err(_) => {
                        usage(&program);
                        Err(anyhow::anyhow!("invalid lead id: {lead_id}"))
                    }
                },
                None => {
                    usage(&program);
                    Err(anyhow::anyhow!("leads suppress requires <lead_id>"))
                }
            },
            Some(_) | None => {
                usage(&program);
                Err(anyhow::anyhow!("invalid or missing leads subcommand"))
            }
        },
        Some("refresh-relationships") => refresh_relationships(),
        Some(_) | None => {
            usage(&program);
            Err(anyhow::anyhow!("invalid or missing subcommand"))
        }
    };

    if let Err(error) = result {
        print_error(&error);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_work_unit(id: i32, url: &str) -> WorkUnit {
        WorkUnit {
            id,
            url: url.to_string(),
            status: "pending".to_string(),
            retry_count: 0,
            next_attempt_at: "2026-05-26T00:00:00Z".to_string(),
            last_attempt_at: None,
            last_error: None,
            created_at: "2026-05-26T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn proxy_prefers_uppercase_then_lowercase() {
        assert_eq!(
            configured_proxy_url_from_values(
                Some("socks5h://upper:9050".to_string()),
                Some("socks5h://lower:9050".to_string()),
            ),
            Some("socks5h://upper:9050".to_string())
        );
        assert_eq!(
            configured_proxy_url_from_values(None, Some(" socks5h://lower:9050 ".to_string())),
            Some("socks5h://lower:9050".to_string())
        );
        assert_eq!(
            configured_proxy_url_from_values(None, Some("   ".to_string())),
            None
        );
    }

    #[test]
    fn retryable_statuses_match_transient_http_failures() {
        assert!(is_retriable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retriable_status(StatusCode::BAD_GATEWAY));
        assert!(!is_retriable_status(StatusCode::NOT_FOUND));
    }

    #[test]
    fn compact_for_terminal_normalizes_whitespace_and_truncates() {
        assert_eq!(
            compact_for_terminal("  Alpha   Beta\tGamma  ", 10),
            "Alpha Beta..."
        );
    }

    #[test]
    fn snapshot_summary_includes_title_and_counts() {
        let snapshot = spyder::models::PageSnapshot {
            title: "Alpha Market".to_string(),
            url: "http://alpha.onion".to_string(),
            language: "English".to_string(),
            language_detection: spyder::models::LanguageDetection::unknown(),
            keyword_corpus: "http://alpha.onion\nAlpha Market".to_string(),
            links: vec![spyder::models::LinkObservation {
                target_url: "http://beta.onion".to_string(),
                target_host: "beta.onion".to_string(),
            }],
            emails: vec!["ops@alpha.onion".to_string()],
            crypto_refs: vec![spyder::models::CryptoReference {
                asset_type: "btc".to_string(),
                reference: "bc1test".to_string(),
            }],
            classification_signals: spyder::models::ClassificationSignals::default(),
            topic_observations: Vec::new(),
        };

        assert_eq!(
            summarize_page_snapshot(&snapshot),
            "title \"Alpha Market\", 1 link, 1 email, 1 crypto ref, language English"
        );
    }

    #[test]
    fn stack_summary_captures_headers_generator_and_phpinfo() {
        let mut headers = HeaderMap::new();
        headers.insert("server", "nginx/1.24.0".parse().expect("server header"));
        headers.insert(
            "x-powered-by",
            "PHP/8.2.12".parse().expect("powered-by header"),
        );
        let normalized_headers = headers_by_name(&headers);
        let resource_findings = vec![ExposedWebResourceFinding {
            path: "/phpinfo.php".to_string(),
            status_code: 200,
            content_type: Some("text/html".to_string()),
            content_length: None,
            bytes_read: 64,
            truncated: false,
            sample_sha256: "sha256:test".to_string(),
            preview: None,
            body_sample: "<h1>PHP Version 8.3.2</h1>".to_string(),
        }];

        let summary = build_stack_version_summary(
            &normalized_headers,
            r#"<meta name="generator" content="WordPress 6.5.4">"#,
            &resource_findings,
        )
        .expect("stack summary");

        assert!(summary.contains("server: nginx/1.24.0"));
        assert!(summary.contains("x-powered-by: PHP/8.2.12"));
        assert!(summary.contains("html-generator: WordPress 6.5.4"));
        assert!(summary.contains("/phpinfo.php: PHP/8.3.2"));
    }

    #[test]
    fn exposed_resource_preview_redacts_sensitive_values() {
        let preview = exposed_resource_preview(
            "DB_PASSWORD=swordfish\nAWS_SECRET_ACCESS_KEY=abc123\nDATABASE_URL=postgres://spyder:secret@example.test/db",
        )
        .expect("preview");

        assert!(preview.contains("DB_PASSWORD=<redacted>"));
        assert!(preview.contains("AWS_SECRET_ACCESS_KEY=<redacted>"));
        assert!(preview.contains("postgres://spyder:<redacted>@example.test/db"));
        assert!(!preview.contains("swordfish"));
        assert!(!preview.contains("abc123"));
    }

    #[test]
    fn work_options_accept_onion_only_flag() {
        let options = parse_work_options(vec!["--onion-only".to_string()]).expect("work options");
        assert_eq!(
            options,
            WorkOptions {
                onion_only: true,
                concurrency: DEFAULT_WORK_CONCURRENCY,
            }
        );
    }

    #[test]
    fn work_options_parse_custom_concurrency() {
        let options = parse_work_options(vec![
            "--onion-only".to_string(),
            "--concurrency".to_string(),
            "6".to_string(),
        ])
        .expect("work options");
        assert_eq!(
            options,
            WorkOptions {
                onion_only: true,
                concurrency: 6,
            }
        );
    }

    #[test]
    fn work_options_reject_unknown_flags() {
        let error = parse_work_options(vec!["--bogus".to_string()]).expect_err("invalid option");
        assert_eq!(error.to_string(), "invalid work option: --bogus");
    }

    #[test]
    fn ssh_scan_options_use_defaults() {
        let options = parse_ssh_scan_options(Vec::<String>::new()).expect("ssh scan options");
        assert_eq!(
            options,
            SshScanOptions {
                recent_hours: DEFAULT_SSH_SCAN_RECENT_HOURS,
                stale_hours: DEFAULT_SSH_SCAN_STALE_HOURS,
                limit: DEFAULT_SSH_SCAN_LIMIT,
                concurrency: DEFAULT_SSH_SCAN_CONCURRENCY,
            }
        );
    }

    #[test]
    fn ssh_scan_options_parse_custom_values() {
        let options = parse_ssh_scan_options(vec![
            "--recent-hours".to_string(),
            "12".to_string(),
            "--stale-hours".to_string(),
            "4".to_string(),
            "--limit".to_string(),
            "32".to_string(),
            "--concurrency".to_string(),
            "6".to_string(),
        ])
        .expect("ssh scan options");
        assert_eq!(
            options,
            SshScanOptions {
                recent_hours: 12,
                stale_hours: 4,
                limit: 32,
                concurrency: 6,
            }
        );
    }

    #[test]
    fn onion_only_work_selection_skips_non_onion_urls() {
        let selected = select_work_units_for_processing(
            vec![
                test_work_unit(1, "http://alpha.onion"),
                test_work_unit(2, "https://example.com"),
                test_work_unit(3, "notaurl"),
            ],
            WorkOptions {
                onion_only: true,
                concurrency: DEFAULT_WORK_CONCURRENCY,
            },
        );

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].url, "http://alpha.onion");
    }

    #[test]
    fn discovered_blacklisted_links_are_not_queued() {
        let snapshot = spyder::models::PageSnapshot {
            title: "Seed".to_string(),
            url: "http://seed.onion".to_string(),
            language: "English".to_string(),
            language_detection: spyder::models::LanguageDetection::unknown(),
            keyword_corpus: "http://seed.onion\nSeed".to_string(),
            links: vec![
                spyder::models::LinkObservation {
                    target_url: "http://allowed.onion".to_string(),
                    target_host: "allowed.onion".to_string(),
                },
                spyder::models::LinkObservation {
                    target_url: "http://sub.blocked.onion".to_string(),
                    target_host: "sub.blocked.onion".to_string(),
                },
            ],
            emails: Vec::new(),
            crypto_refs: Vec::new(),
            classification_signals: spyder::models::ClassificationSignals::default(),
            topic_observations: Vec::new(),
        };

        let blacklist_domains = vec!["blocked.onion".to_string()];
        let mut queued = BTreeSet::<String>::new();
        let outcome =
            enqueue_discovered_links_with(&snapshot, &blacklist_domains, |url, domains| {
                let normalized_url = normalize_crawl_url(url);
                if url_matches_blacklist(&normalized_url, domains) {
                    return Ok(WorkQueueOutcome::SkippedBlacklisted);
                }
                queued.insert(normalized_url);
                Ok(WorkQueueOutcome::Queued)
            })
            .expect("enqueue links");
        assert_eq!(outcome.queued_count, 1);
        assert_eq!(outcome.skipped_blacklisted_count, 1);
        assert_eq!(queued.len(), 1);
        assert!(queued.contains("http://allowed.onion"));
    }

    #[test]
    fn discovered_fragment_links_share_one_work_unit() {
        let snapshot = spyder::models::PageSnapshot {
            title: "Seed".to_string(),
            url: "http://seed.onion".to_string(),
            language: "English".to_string(),
            language_detection: spyder::models::LanguageDetection::unknown(),
            keyword_corpus: "http://seed.onion\nSeed".to_string(),
            links: vec![
                spyder::models::LinkObservation {
                    target_url: "http://allowed.onion/docs#faq".to_string(),
                    target_host: "allowed.onion".to_string(),
                },
                spyder::models::LinkObservation {
                    target_url: "http://allowed.onion/docs#pricing".to_string(),
                    target_host: "allowed.onion".to_string(),
                },
            ],
            emails: Vec::new(),
            crypto_refs: Vec::new(),
            classification_signals: spyder::models::ClassificationSignals::default(),
            topic_observations: Vec::new(),
        };

        let mut queued = BTreeSet::<String>::new();
        enqueue_discovered_links_with(&snapshot, &[], |url, domains| {
            let normalized_url = normalize_crawl_url(url);
            if url_matches_blacklist(&normalized_url, domains) {
                return Ok(WorkQueueOutcome::SkippedBlacklisted);
            }
            queued.insert(normalized_url);
            Ok(WorkQueueOutcome::Queued)
        })
        .expect("enqueue links");

        assert_eq!(queued.len(), 1);
        assert!(queued.contains("http://allowed.onion/docs"));
    }
}
