use anyhow::Context as AnyhowContext;
use diesel::connection::SimpleConnection;
use tracing::{error, info, warn};
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};
use rocket::fairing::{AdHoc, Fairing, Info, Kind};
use rocket::form::{Form, FromForm};
use rocket::fs::{relative, FileServer};
use rocket::http::Status;
use rocket::response::Redirect;
use rocket::response::{self, Responder};
use rocket::serde::json::serde_json::{to_value, Value};
use rocket::serde::{json::Json, Deserialize, Serialize};
use rocket::{get, launch, post, routes, Build, Data, Request, Response, Rocket, State};
use rocket_dyn_templates::{context, Template};
use spyder::models::{
    CategoryDistributionEntry, CategoryTimelinePoint, PaginatedResult, Stats, TopSiteSection,
};
use spyder::{
    add_auto_blacklist_rule, add_watchlist_item, collect_stats, count_discovered_service_endpoints,
    find_matching_blacklist_domain, get_auto_blacklist_config, get_crypto_entity_detail,
    get_email_entity_detail, get_host_http_observation_detail, get_host_service_observation_detail,
    get_intel_lead_detail, get_page_detail, get_page_scan_detail, get_site_relationship_graph,
    get_ssh_host_key_detail, intel_lead_rule_ids, list_crypto_entities,
    list_domain_blacklist_rules, list_domain_blacklist_summaries, list_email_entities,
    list_host_http_observations, list_host_service_observations, list_intel_leads,
    list_page_language_distribution, list_page_scan_summaries, list_page_summaries,
    list_page_topic_distribution, list_page_topic_timeline, list_site_category_distribution,
    list_site_category_timeline, list_site_keyword_distribution, list_site_keyword_timeline,
    list_site_profiles, list_site_relationships, list_site_relationships_fast, list_ssh_host_keys, list_top_referenced_sites,
    list_top_sites_by_crypto_refs, list_top_sites_by_email_refs, list_top_sites_by_outgoing_links,
    list_watchlist_items, list_work_units, remove_auto_blacklist_rule, remove_watchlist_item,
    search_pages, set_auto_blacklist_rule_enabled, update_intel_lead_status,
    valid_watchlist_item_types, AUTO_BLACKLIST_RULE_TYPE_KEYWORD,
    AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use url::form_urlencoded;
use url::Url;

type HtmlResult = Result<Template, FrontendError>;
type DbPool = Pool<ConnectionManager<PgConnection>>;
type DbConnection = PooledConnection<ConnectionManager<PgConnection>>;

const CACHE_DASHBOARD: &str = "/";
const CACHE_PAGES: &str = "/pages";
const CACHE_ANALYTICS: &str = "/analytics";
const CACHE_SITES: &str = "/sites";
const CACHE_WATCHLISTS: &str = "/watchlists";
const CACHE_LEADS: &str = "/leads";
const CACHE_RELATIONSHIPS: &str = "/relationships";
const CACHE_EMAILS: &str = "/entities/emails";
const CACHE_CRYPTO: &str = "/entities/crypto";
const CACHE_HTTP: &str = "/entities/http";
const CACHE_SERVICES: &str = "/entities/services";
const CACHE_SSH: &str = "/entities/ssh";
const CACHE_BLACKLIST: &str = "/blacklist";
const CACHE_WORK: &str = "/work";
const CACHE_TOP: &str = "/top";

#[derive(Debug)]
struct FrontendError {
    status: Status,
    title: &'static str,
    detail: String,
}

impl FrontendError {
    fn internal(context: &'static str, error: anyhow::Error) -> Self {
        let detail = format!("{context}: {error:#}");
        error!(context, error = ?error, "Frontend error");
        Self {
            status: Status::InternalServerError,
            title: "Internal Server Error",
            detail,
        }
    }
}

impl<'r> Responder<'r, 'static> for FrontendError {
    fn respond_to(self, request: &'r Request<'_>) -> response::Result<'static> {
        let mut response = Template::render(
            "error",
            context! {
                title: self.title,
                description: "The frontend hit an internal error.",
                status_code: self.status.code,
                reason: self.status.reason_lossy(),
                detail: self.detail,
            },
        )
        .respond_to(request)?;
        response.set_status(self.status);
        Ok(response)
    }
}

trait FrontendContext<T> {
    fn frontend_context(self, context: &'static str) -> Result<T, FrontendError>;
}

impl<T> FrontendContext<T> for anyhow::Result<T> {
    fn frontend_context(self, context: &'static str) -> Result<T, FrontendError> {
        self.map_err(|error| FrontendError::internal(context, error))
    }
}

#[derive(Clone)]
struct AppState {
    pool: DbPool,
    cache: Arc<FrontendCache>,
    cache_cold_wait: Duration,
    cache_warm_routes: HashSet<&'static str>,
    cache_slow_route_log_threshold: Duration,
    metrics: spyder::metrics::Metrics,
}

impl AppState {
    fn connection(&self) -> Result<DbConnection, FrontendError> {
        let mut connection = self.pool.get().map_err(|error| {
            FrontendError::internal(
                "checking out database connection",
                anyhow::Error::new(error),
            )
        })?;
        connection
            .batch_execute(
                "
                SET application_name = 'spyder-frontend';
                SET TIME ZONE 'UTC';
                ",
            )
            .map_err(|error| {
                FrontendError::internal(
                    "configuring database connection",
                    anyhow::Error::new(error),
                )
            })?;
        Ok(connection)
    }

    fn background_cached_context(
        &self,
        key: &'static str,
        label: &'static str,
        build: fn(&AppState) -> Result<Value, FrontendError>,
        fallback: fn() -> Result<Value, FrontendError>,
    ) -> Result<Value, FrontendError> {
        if let Some(value) = self.cache.fresh_context(key) {
            return Ok(value);
        }

        let stale_value = self.cache.stale_context(key);
        self.spawn_context_refresh(key, label, build);
        if let Some(value) = stale_value {
            return Ok(value);
        }

        let started_at = Instant::now();
        while started_at.elapsed() < self.cache_cold_wait {
            if let Some(value) = self.cache.stale_context(key) {
                return Ok(value);
            }
            let remaining = self
                .cache_cold_wait
                .checked_sub(started_at.elapsed())
                .unwrap_or_default();
            thread::sleep(remaining.min(Duration::from_millis(25)));
        }

        fallback()
    }

    fn spawn_context_refresh(
        &self,
        key: &'static str,
        label: &'static str,
        build: fn(&AppState) -> Result<Value, FrontendError>,
    ) {
        if !self.cache.begin_refresh(key) {
            return;
        }

        let state = self.clone();
        let spawn_result = thread::Builder::new()
            .name(format!("spyder-cache-refresh-{label}"))
            .spawn(move || {
                let _refresh_guard = CacheRefreshGuard {
                    cache: state.cache.clone(),
                    key,
                };
                let started_at = Instant::now();
                let result = state.cache.refresh_context(key, || build(&state));
                let elapsed = started_at.elapsed();
                if let Err(error) = result {
                    error!(
                        route = label,
                        duration_secs = elapsed.as_secs_f64(),
                        error = %error.detail,
                        "Cache refresh failed"
                    );
                } else {
                    state.log_cache_route_duration(label, elapsed);
                }
            });

        if let Err(error) = spawn_result {
            self.cache.finish_refresh(key);
            error!(label, error = %error, "Failed to spawn cache refresh");
        }
    }

    fn should_warm_cache_route(&self, key: &'static str) -> bool {
        self.cache_warm_routes.contains(key)
    }

    fn log_cache_route_duration(&self, label: &'static str, elapsed: Duration) {
        if self.cache_slow_route_log_threshold.is_zero()
            || elapsed < self.cache_slow_route_log_threshold
        {
            return;
        }

        warn!(
            route = label,
            duration_secs = elapsed.as_secs_f64(),
            "Slow cache refresh"
        );
    }
}

struct CacheRefreshGuard {
    cache: Arc<FrontendCache>,
    key: &'static str,
}

impl Drop for CacheRefreshGuard {
    fn drop(&mut self) {
        self.cache.finish_refresh(self.key);
    }
}

#[derive(Clone)]
struct CacheEntry<T> {
    value: T,
    refreshed_at: Instant,
}

#[derive(Debug, Eq, PartialEq)]
enum CacheRead<T, E> {
    Fresh(T),
    Refreshed(T),
    Stale { value: T, error: E },
}

struct TimedCache<T> {
    ttl: Duration,
    entries: Mutex<HashMap<&'static str, CacheEntry<T>>>,
}

impl<T: Clone> TimedCache<T> {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn ttl(&self) -> Duration {
        self.ttl
    }

    fn get_or_refresh<E, F>(&self, key: &'static str, refresh: F) -> Result<CacheRead<T, E>, E>
    where
        F: FnOnce() -> Result<T, E>,
    {
        self.get_or_refresh_at(key, Instant::now(), refresh)
    }

    fn get_or_refresh_at<E, F>(
        &self,
        key: &'static str,
        now: Instant,
        refresh: F,
    ) -> Result<CacheRead<T, E>, E>
    where
        F: FnOnce() -> Result<T, E>,
    {
        if let Some(value) = self.fresh_value_at(key, now) {
            return Ok(CacheRead::Fresh(value));
        }

        match refresh() {
            Ok(value) => {
                self.store_at(key, value.clone(), now);
                Ok(CacheRead::Refreshed(value))
            }
            Err(error) => {
                if let Some(value) = self.stale_value(key) {
                    Ok(CacheRead::Stale { value, error })
                } else {
                    Err(error)
                }
            }
        }
    }

    fn refresh<E, F>(&self, key: &'static str, refresh: F) -> Result<T, E>
    where
        F: FnOnce() -> Result<T, E>,
    {
        let value = refresh()?;
        self.store_at(key, value.clone(), Instant::now());
        Ok(value)
    }

    fn invalidate_many(&self, keys: &[&'static str]) {
        let mut entries = self.entries.lock().expect("frontend cache mutex poisoned");
        for key in keys {
            entries.remove(key);
        }
    }

    fn fresh_value_at(&self, key: &'static str, now: Instant) -> Option<T> {
        self.entries
            .lock()
            .expect("frontend cache mutex poisoned")
            .get(key)
            .filter(|entry| {
                now.checked_duration_since(entry.refreshed_at)
                    .unwrap_or_default()
                    < self.ttl
            })
            .map(|entry| entry.value.clone())
    }

    fn stale_value(&self, key: &'static str) -> Option<T> {
        self.entries
            .lock()
            .expect("frontend cache mutex poisoned")
            .get(key)
            .map(|entry| entry.value.clone())
    }

    fn store_at(&self, key: &'static str, value: T, refreshed_at: Instant) {
        self.entries
            .lock()
            .expect("frontend cache mutex poisoned")
            .insert(
                key,
                CacheEntry {
                    value,
                    refreshed_at,
                },
            );
    }
}

struct FrontendCache {
    contexts: TimedCache<Value>,
    refreshing: Mutex<HashSet<&'static str>>,
}

impl FrontendCache {
    fn new(ttl: Duration) -> Self {
        Self {
            contexts: TimedCache::new(ttl),
            refreshing: Mutex::new(HashSet::new()),
        }
    }

    fn ttl(&self) -> Duration {
        self.contexts.ttl()
    }

    fn context<F>(&self, key: &'static str, refresh: F) -> Result<Value, FrontendError>
    where
        F: FnOnce() -> Result<Value, FrontendError>,
    {
        match self.contexts.get_or_refresh(key, refresh)? {
            CacheRead::Fresh(value) | CacheRead::Refreshed(value) => Ok(value),
            CacheRead::Stale { value, error } => {
                error!(route = key, error = %error.detail, "Cache refresh error, serving stale data");
                Ok(value)
            }
        }
    }

    fn fresh_context(&self, key: &'static str) -> Option<Value> {
        self.contexts.fresh_value_at(key, Instant::now())
    }

    fn stale_context(&self, key: &'static str) -> Option<Value> {
        self.contexts.stale_value(key)
    }

    fn refresh_context<F>(&self, key: &'static str, refresh: F) -> Result<Value, FrontendError>
    where
        F: FnOnce() -> Result<Value, FrontendError>,
    {
        self.contexts.refresh(key, refresh)
    }

    fn invalidate_many(&self, keys: &[&'static str]) {
        self.contexts.invalidate_many(keys);
    }

    fn begin_refresh(&self, key: &'static str) -> bool {
        self.refreshing
            .lock()
            .expect("frontend cache refresh mutex poisoned")
            .insert(key)
    }

    fn finish_refresh(&self, key: &'static str) {
        self.refreshing
            .lock()
            .expect("frontend cache refresh mutex poisoned")
            .remove(key);
    }
}

struct RequestTimer;

#[rocket::async_trait]
impl Fairing for RequestTimer {
    fn info(&self) -> Info {
        Info {
            name: "frontend request timing",
            kind: Kind::Request | Kind::Response,
        }
    }

    async fn on_request(&self, request: &mut Request<'_>, _: &mut Data<'_>) {
        request.local_cache(Instant::now);
    }

    async fn on_response<'r>(&self, request: &'r Request<'_>, response: &mut Response<'r>) {
        let started_at = request.local_cache(Instant::now);
        info!(
            method = %request.method(),
            uri = %request.uri(),
            duration_secs = started_at.elapsed().as_secs_f64(),
            status = response.status().code,
            "Request served"
        );
    }
}

fn build_app_state() -> Result<AppState, FrontendError> {
    dotenvy::dotenv().ok();

    // Load and validate configuration
    let config = spyder::config::SpyderConfig::from_env()
        .frontend_context("loading configuration")?;
    config.validate()
        .frontend_context("validating configuration")?;

    let cache_warm_routes =
        parse_cache_warm_routes(Some(&config.frontend.cache_warm_routes));

    let manager = ConnectionManager::<PgConnection>::new(config.database.url.clone());
    let pool = Pool::builder()
        .max_size(config.frontend.pool_size)
        .build(manager)
        .map_err(|error| {
            FrontendError::internal("building database pool", anyhow::Error::new(error))
        })?;

    Ok(AppState {
        pool,
        cache: Arc::new(FrontendCache::new(config.cache_ttl())),
        cache_cold_wait: config.cache_cold_wait(),
        cache_warm_routes,
        cache_slow_route_log_threshold: config.cache_slow_route_threshold(),
        metrics: spyder::metrics::Metrics::new(),
    })
}

fn parse_cache_warm_routes(raw_value: Option<&str>) -> HashSet<&'static str> {
    let raw_value = raw_value.unwrap_or(spyder::config::DEFAULT_FRONTEND_CACHE_WARM_ROUTES);
    let trimmed = raw_value.trim();
    if trimmed.is_empty()
        || matches!(
            trimmed.to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "none" | "off"
        )
    {
        return HashSet::new();
    }

    if trimmed.eq_ignore_ascii_case("all") {
        return CACHED_ROUTES.iter().map(|route| route.key).collect();
    }

    trimmed
        .split(',')
        .filter_map(|entry| {
            let entry = entry.trim();
            let route = CACHED_ROUTES
                .iter()
                .find(|route| route.key == entry || route.label == entry);
            if route.is_none() && !entry.is_empty() {
                warn!(route = %entry, "Unknown route in cache configuration");
            }
            route.map(|route| route.key)
        })
        .collect()
}

fn template_context<T: Serialize>(context: T) -> Result<Value, FrontendError> {
    to_value(context).map_err(|error| {
        FrontendError::internal("serializing template context", anyhow::Error::new(error))
    })
}

#[derive(Serialize)]
#[serde(crate = "rocket::serde")]
struct ApiResponse<T> {
    success: bool,
    data: T,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
struct PaginationView {
    total_count: i64,
    limit: i64,
    offset: i64,
    has_previous_page: bool,
    has_next_page: bool,
    previous_page_url: String,
    next_page_url: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
struct WorkUnitView {
    id: i32,
    url: String,
    status: String,
    retry_count: i32,
    next_attempt_at: String,
    last_attempt_at: Option<String>,
    last_error: Option<String>,
    created_at: String,
    is_blacklisted: bool,
    blacklist_match_domain: Option<String>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
struct CategoryMetricView {
    value: String,
    label: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
struct CategoryLegendView {
    category: String,
    label: String,
    color: String,
    host_count: usize,
    count_label: String,
    percentage_label: String,
}

#[derive(FromForm, Clone)]
struct ListQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(FromForm, Clone)]
struct RelationshipQuery {
    limit: Option<i64>,
    offset: Option<i64>,
    focus: Option<String>,
    depth: Option<i64>,
}

#[derive(FromForm, Clone)]
struct SearchQuery {
    query: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(FromForm, Clone)]
struct EmailQuery {
    value: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(FromForm, Clone)]
struct CryptoQuery {
    asset_type: Option<String>,
    reference: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(FromForm, Clone)]
struct SshQuery {
    algorithm: Option<String>,
    fingerprint: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(FromForm, Clone)]
struct HttpQuery {
    host: Option<String>,
    scheme: Option<String>,
    port: Option<i32>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(FromForm, Clone)]
struct ServiceQuery {
    host: Option<String>,
    service: Option<String>,
    port: Option<i32>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(FromForm, Serialize, Clone)]
#[serde(crate = "rocket::serde")]
struct LeadQuery {
    status: Option<String>,
    severity: Option<String>,
    rule_id: Option<String>,
    entity: Option<String>,
    sort: Option<String>,
    direction: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
struct LeadFilterOption {
    value: String,
    selected: bool,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
struct WatchlistTypeOption {
    value: String,
    label: String,
}

#[derive(FromForm)]
struct LeadStatusForm {
    status: String,
}

#[derive(FromForm)]
struct WatchlistItemForm {
    item_type: String,
    value: String,
    label: Option<String>,
}

#[derive(FromForm)]
struct AutoBlacklistRuleForm {
    rule_type: String,
    value: String,
    label: Option<String>,
}

#[derive(Deserialize)]
#[serde(crate = "rocket::serde")]
struct LeadStatusRequest {
    status: String,
}

fn render_cached_context(
    state: &State<AppState>,
    key: &'static str,
    template: &'static str,
    build: fn(&AppState) -> Result<Value, FrontendError>,
) -> HtmlResult {
    let context = state.inner().cache.context(key, || build(state.inner()))?;
    Ok(Template::render(template, context))
}

fn render_background_cached_context(
    state: &State<AppState>,
    key: &'static str,
    label: &'static str,
    template: &'static str,
    build: fn(&AppState) -> Result<Value, FrontendError>,
    fallback: fn() -> Result<Value, FrontendError>,
) -> HtmlResult {
    let context = state
        .inner()
        .background_cached_context(key, label, build, fallback)?;
    Ok(Template::render(template, context))
}

fn render_pages(
    state: &State<AppState>,
    title: &str,
    description: &str,
    list_query: Option<ListQuery>,
    cache_key: Option<&'static str>,
) -> HtmlResult {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    if list_query_is_default(&list_query) {
        if let Some(cache_key) = cache_key {
            let context = state.inner().cache.context(cache_key, || {
                build_pages_context(state.inner(), title, description, list_query.clone())
            })?;
            return Ok(Template::render("pages", context));
        }
    }

    let context = build_pages_context(state.inner(), title, description, list_query)?;
    Ok(Template::render("pages", context))
}

fn build_pages_default_context(state: &AppState) -> Result<Value, FrontendError> {
    build_pages_context(
        state,
        "Scanned Pages",
        "Browse indexed pages with scan time, language, and extracted-entity counts.",
        ListQuery {
            limit: None,
            offset: None,
        },
    )
}

fn build_pages_context(
    state: &AppState,
    title: &str,
    description: &str,
    list_query: ListQuery,
) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let pages = list_page_summaries(&mut connection, list_query.limit, list_query.offset)
        .frontend_context("loading page summaries")?;
    let has_pages = !pages.items.is_empty();
    let pagination = pagination_context("/pages", &pages, &[]);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;
    let page_count_label = format!("{} records", pages.total_count);

    template_context(context! {
        title: title,
        description: description,
        pages: pages.items,
        has_pages: has_pages,
        page_count: pages.total_count,
        page_count_label: page_count_label,
        pagination: pagination,
        has_pagination: has_pagination,
    })
}

fn list_query_is_default(query: &ListQuery) -> bool {
    query.limit.is_none() && query.offset.unwrap_or(0) == 0
}

#[get("/")]
fn index(state: &State<AppState>) -> HtmlResult {
    render_background_cached_context(
        state,
        CACHE_DASHBOARD,
        "/",
        "dashboard",
        build_dashboard_context,
        build_dashboard_warming_context,
    )
}

fn build_dashboard_context(state: &AppState) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let stats = collect_stats(&mut connection).frontend_context("loading dashboard stats")?;
    let show_deep_sections = dashboard_deep_sections_enabled();

    let pages = list_page_summaries(&mut connection, Some(8), Some(0))
        .frontend_context("loading dashboard pages")?;
    let email_entities = if show_deep_sections {
        list_email_entities(&mut connection, Some(8), Some(0))
            .frontend_context("loading dashboard email entities")?
    } else {
        empty_paginated_result()
    };
    let crypto_entities = if show_deep_sections {
        list_crypto_entities(&mut connection, Some(8), Some(0))
            .frontend_context("loading dashboard crypto entities")?
    } else {
        empty_paginated_result()
    };
    let relationships = if show_deep_sections {
        list_site_relationships(&mut connection, Some(8), Some(0))
            .frontend_context("loading dashboard relationships")?
    } else {
        empty_paginated_result()
    };
    let http_observations = if show_deep_sections {
        list_host_http_observations(&mut connection, Some(6), Some(0))
            .frontend_context("loading dashboard http observations")?
    } else {
        empty_paginated_result()
    };
    let service_observations = if show_deep_sections {
        list_host_service_observations(&mut connection, Some(6), Some(0))
            .frontend_context("loading dashboard service observations")?
    } else {
        empty_paginated_result()
    };
    let ssh_host_keys = if show_deep_sections {
        list_ssh_host_keys(&mut connection, Some(8), Some(0))
            .frontend_context("loading dashboard ssh host keys")?
    } else {
        empty_paginated_result()
    };

    template_context(context! {
        title: "Spyder Dashboard",
        description: "Track scanned pages, shared entities, retries, and site-to-site references across clearnet and Tor targets.",
        stats: stats,
        pages: pages.items,
        email_entities: email_entities.items,
        crypto_entities: crypto_entities.items,
        relationships: relationships.items,
        http_observations: http_observations.items,
        service_observations: service_observations.items,
        ssh_host_keys: ssh_host_keys.items,
        has_pages: pages.total_count > 0,
        has_email_entities: email_entities.total_count > 0,
        has_crypto_entities: crypto_entities.total_count > 0,
        has_relationships: relationships.total_count > 0,
        has_http_observations: http_observations.total_count > 0,
        has_service_observations: service_observations.total_count > 0,
        has_ssh_host_keys: ssh_host_keys.total_count > 0,
        has_any_service_intel: http_observations.total_count > 0 || service_observations.total_count > 0 || ssh_host_keys.total_count > 0,
        show_dashboard_deep_sections: show_deep_sections,
    })
}

fn build_dashboard_warming_context() -> Result<Value, FrontendError> {
    let stats = Stats {
        total_pages: 0,
        total_domains: 0,
        pending_work_units: 0,
        failed_work_units: 0,
        last_scrape: "Refreshing".to_string(),
    };

    template_context(context! {
        title: "Spyder Dashboard",
        description: "Track scanned pages, shared entities, retries, and site-to-site references across clearnet and Tor targets.",
        stats: stats,
        pages: Vec::<Value>::new(),
        email_entities: Vec::<Value>::new(),
        crypto_entities: Vec::<Value>::new(),
        relationships: Vec::<Value>::new(),
        http_observations: Vec::<Value>::new(),
        service_observations: Vec::<Value>::new(),
        ssh_host_keys: Vec::<Value>::new(),
        has_pages: false,
        has_email_entities: false,
        has_crypto_entities: false,
        has_relationships: false,
        has_http_observations: false,
        has_service_observations: false,
        has_ssh_host_keys: false,
        has_any_service_intel: false,
        show_dashboard_deep_sections: dashboard_deep_sections_enabled(),
        cache_refresh_pending: true,
    })
}

fn dashboard_deep_sections_enabled() -> bool {
    env::var("SPYDER_DASHBOARD_DEEP")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn empty_paginated_result<T>() -> PaginatedResult<T> {
    PaginatedResult {
        items: Vec::new(),
        total_count: 0,
        limit: 0,
        offset: 0,
    }
}

#[get("/data?<list_query..>")]
fn data(state: &State<AppState>, list_query: Option<ListQuery>) -> HtmlResult {
    render_pages(
        state,
        "Scanned Pages",
        "Browse indexed pages with scan time, language, and extracted-entity counts.",
        list_query,
        None,
    )
}

#[get("/pages?<list_query..>")]
fn pages(state: &State<AppState>, list_query: Option<ListQuery>) -> HtmlResult {
    render_pages(
        state,
        "Scanned Pages",
        "Browse indexed pages with scan time, language, and extracted-entity counts.",
        list_query,
        Some(CACHE_PAGES),
    )
}

#[get("/pages/<page_id>")]
fn page_detail(state: &State<AppState>, page_id: i32) -> HtmlResult {
    let mut connection = state.inner().connection()?;
    let page = get_page_detail(&mut connection, page_id).frontend_context("loading page detail")?;
    let Some(page) = page else {
        return Err(FrontendError {
            status: Status::NotFound,
            title: "Not Found",
            detail: format!("page {page_id} was not found"),
        });
    };
    let has_outgoing_links = !page.outgoing_links.is_empty();
    let has_incoming_links = !page.incoming_links.is_empty();
    let has_emails = !page.emails.is_empty();
    let has_crypto_refs = !page.crypto_refs.is_empty();
    let has_topic_tags = !page.topic_tags.is_empty();
    let outgoing_link_count = page.outgoing_links.len();
    let incoming_link_count = page.incoming_links.len();
    let email_count = page.emails.len();
    let crypto_ref_count = page.crypto_refs.len();
    let topic_tag_count = page.topic_tags.len();
    let scan_history = list_page_scan_summaries(&mut connection, page_id)
        .frontend_context("loading page scan history")?;
    let recent_scans = scan_history.iter().take(5).cloned().collect::<Vec<_>>();
    let scan_count = scan_history.len();
    let has_scan_history = !recent_scans.is_empty();
    let latest_change_summary = recent_scans
        .first()
        .and_then(|scan| scan.change_summary.clone());
    let has_latest_change_summary = latest_change_summary.is_some();
    let history_url = format!("/pages/{page_id}/history");
    let has_more_scans = scan_count > recent_scans.len();

    Ok(Template::render(
        "page_detail",
        context! {
            title: page.title.clone(),
            description: "Page detail with outbound links, inbound references, emails, wallets, and scan metadata.",
            page: page,
            scan_history: recent_scans,
            scan_count: scan_count,
            has_scan_history: has_scan_history,
            latest_change_summary: latest_change_summary,
            has_latest_change_summary: has_latest_change_summary,
            history_url: history_url,
            has_more_scans: has_more_scans,
            has_outgoing_links: has_outgoing_links,
            has_incoming_links: has_incoming_links,
            has_emails: has_emails,
            has_crypto_refs: has_crypto_refs,
            has_topic_tags: has_topic_tags,
            outgoing_link_count: outgoing_link_count,
            incoming_link_count: incoming_link_count,
            email_count: email_count,
            crypto_ref_count: crypto_ref_count,
            topic_tag_count: topic_tag_count,
        },
    ))
}

#[get("/pages/<page_id>/history")]
fn page_history(state: &State<AppState>, page_id: i32) -> HtmlResult {
    let mut connection = state.inner().connection()?;
    let page = get_page_detail(&mut connection, page_id)
        .frontend_context("loading page history detail")?;
    let Some(page) = page else {
        return Err(FrontendError {
            status: Status::NotFound,
            title: "Not Found",
            detail: format!("page {page_id} was not found"),
        });
    };
    let scans = list_page_scan_summaries(&mut connection, page_id)
        .frontend_context("loading page history scans")?;
    let latest_scan_url = scans.first().map(|scan| scan.detail_url.clone());
    let has_scans = !scans.is_empty();
    let scan_count = scans.len();

    Ok(Template::render(
        "page_history",
        context! {
            title: format!("{} History", page.title.clone()),
            description: "Review historical scan snapshots and scan-to-scan changes for this page.",
            page: page,
            scans: scans,
            has_scans: has_scans,
            scan_count: scan_count,
            latest_scan_url: latest_scan_url,
        },
    ))
}

#[get("/pages/<page_id>/history/<scan_id>")]
fn page_scan_detail(state: &State<AppState>, page_id: i32, scan_id: i32) -> HtmlResult {
    let mut connection = state.inner().connection()?;
    let detail = get_page_scan_detail(&mut connection, page_id, scan_id)
        .frontend_context("loading page scan detail")?;
    let Some(detail) = detail else {
        return Err(FrontendError {
            status: Status::NotFound,
            title: "Not Found",
            detail: format!("page scan {scan_id} for page {page_id} was not found"),
        });
    };
    let has_previous_scan = detail.diff.has_previous_scan;
    let has_added_links = !detail.diff.added_links.is_empty();
    let has_removed_links = !detail.diff.removed_links.is_empty();
    let has_added_emails = !detail.diff.added_emails.is_empty();
    let has_removed_emails = !detail.diff.removed_emails.is_empty();
    let has_added_crypto_refs = !detail.diff.added_crypto_refs.is_empty();
    let has_removed_crypto_refs = !detail.diff.removed_crypto_refs.is_empty();
    let has_outgoing_links = !detail.outgoing_links.is_empty();
    let has_emails = !detail.emails.is_empty();
    let has_crypto_refs = !detail.crypto_refs.is_empty();
    let history_url = format!("/pages/{page_id}/history");
    let page_url = format!("/pages/{page_id}");

    Ok(Template::render(
        "page_scan_detail",
        context! {
            title: format!("{} Scan", detail.page_title.clone()),
            description: "Inspect one historical page snapshot and compare it with the previous successful scan.",
            detail: detail,
            history_url: history_url,
            page_url: page_url,
            has_previous_scan: has_previous_scan,
            has_added_links: has_added_links,
            has_removed_links: has_removed_links,
            has_added_emails: has_added_emails,
            has_removed_emails: has_removed_emails,
            has_added_crypto_refs: has_added_crypto_refs,
            has_removed_crypto_refs: has_removed_crypto_refs,
            has_outgoing_links: has_outgoing_links,
            has_emails: has_emails,
            has_crypto_refs: has_crypto_refs,
        },
    ))
}

#[get("/work?<list_query..>")]
fn list_work(state: &State<AppState>, list_query: Option<ListQuery>) -> HtmlResult {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    if list_query_is_default(&list_query) {
        let context = state.inner().cache.context(CACHE_WORK, || {
            build_work_context(state.inner(), list_query.clone())
        })?;
        return Ok(Template::render("work", context));
    }

    let context = build_work_context(state.inner(), list_query)?;
    Ok(Template::render("work", context))
}

fn build_work_default_context(state: &AppState) -> Result<Value, FrontendError> {
    build_work_context(
        state,
        ListQuery {
            limit: None,
            offset: None,
        },
    )
}

fn build_work_context(state: &AppState, list_query: ListQuery) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let workunits = list_work_units(&mut connection, list_query.limit, list_query.offset)
        .frontend_context("loading work queue")?;
    let blacklist_domains = list_domain_blacklist_rules(&mut connection)
        .frontend_context("loading blacklist rules")?
        .into_iter()
        .map(|rule| rule.domain)
        .collect::<Vec<_>>();
    let workunit_views = workunits
        .items
        .iter()
        .map(|workunit| work_unit_view_from_model(workunit, &blacklist_domains))
        .collect::<Vec<_>>();
    let has_workunits = !workunits.items.is_empty();
    let pagination = pagination_context("/work", &workunits, &[]);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;

    template_context(context! {
        title: "Queue",
        description: "Inspect queued, completed, retried, and terminally failed URLs.",
        workunits: workunit_views,
        has_workunits: has_workunits,
        workunit_count: workunits.total_count,
        pagination: pagination,
        has_pagination: has_pagination,
    })
}

#[get("/blacklist")]
fn blacklist(state: &State<AppState>) -> HtmlResult {
    render_cached_context(state, CACHE_BLACKLIST, "blacklist", build_blacklist_context)
}

fn build_blacklist_context(state: &AppState) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let entries = list_domain_blacklist_summaries(&mut connection)
        .frontend_context("loading blacklist summaries")?;
    let auto_blacklist = get_auto_blacklist_config(&mut connection)
        .frontend_context("loading auto blacklist config")?;
    let has_entries = !entries.is_empty();
    let entry_count = entries.len();
    let has_auto_rules = !auto_blacklist.rules.is_empty();
    let auto_rule_count = auto_blacklist.rules.len();
    let has_auto_events = !auto_blacklist.events.is_empty();

    template_context(context! {
        title: "Domain Blacklist",
        description: "Review domains blocked from discovered-link queueing and see how often they appear in stored links.",
        entries: entries,
        has_entries: has_entries,
        entry_count: entry_count,
        auto_rules: auto_blacklist.rules,
        has_auto_rules: has_auto_rules,
        auto_rule_count: auto_rule_count,
        auto_events: auto_blacklist.events,
        has_auto_events: has_auto_events,
        category_options: auto_blacklist.category_options,
        category_rule_type: AUTO_BLACKLIST_RULE_TYPE_SITE_CATEGORY,
        keyword_rule_type: AUTO_BLACKLIST_RULE_TYPE_KEYWORD,
    })
}

#[post("/blacklist/auto", data = "<form>")]
fn add_blacklist_auto_rule(
    state: &State<AppState>,
    form: Form<AutoBlacklistRuleForm>,
) -> Result<Redirect, FrontendError> {
    let mut connection = state.inner().connection()?;
    add_auto_blacklist_rule(
        &mut connection,
        &form.rule_type,
        &form.value,
        form.label.as_deref(),
    )
    .frontend_context("saving auto blacklist rule")?;
    invalidate_blacklist_caches(state.inner());
    Ok(Redirect::to("/blacklist"))
}

#[post("/blacklist/auto/<rule_id>/enable")]
fn enable_blacklist_auto_rule(
    state: &State<AppState>,
    rule_id: i32,
) -> Result<Redirect, FrontendError> {
    let mut connection = state.inner().connection()?;
    set_auto_blacklist_rule_enabled(&mut connection, rule_id, true)
        .frontend_context("enabling auto blacklist rule")?;
    invalidate_blacklist_caches(state.inner());
    Ok(Redirect::to("/blacklist"))
}

#[post("/blacklist/auto/<rule_id>/disable")]
fn disable_blacklist_auto_rule(
    state: &State<AppState>,
    rule_id: i32,
) -> Result<Redirect, FrontendError> {
    let mut connection = state.inner().connection()?;
    set_auto_blacklist_rule_enabled(&mut connection, rule_id, false)
        .frontend_context("disabling auto blacklist rule")?;
    invalidate_blacklist_caches(state.inner());
    Ok(Redirect::to("/blacklist"))
}

#[post("/blacklist/auto/<rule_id>/delete")]
fn delete_blacklist_auto_rule(
    state: &State<AppState>,
    rule_id: i32,
) -> Result<Redirect, FrontendError> {
    let mut connection = state.inner().connection()?;
    remove_auto_blacklist_rule(&mut connection, rule_id)
        .frontend_context("removing auto blacklist rule")?;
    invalidate_blacklist_caches(state.inner());
    Ok(Redirect::to("/blacklist"))
}

#[get("/top")]
fn top(state: &State<AppState>) -> HtmlResult {
    render_cached_context(state, CACHE_TOP, "top", build_top_context)
}

fn build_top_context(state: &AppState) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let sections = vec![
        TopSiteSection {
            title: "Most Email Refs".to_string(),
            description: "Hosts with the most current email references.".to_string(),
            count_label: "emails".to_string(),
            items: list_top_sites_by_email_refs(&mut connection, Some(25))
                .frontend_context("loading top sites section: most email refs")?,
            has_items: false,
        },
        TopSiteSection {
            title: "Most Crypto Refs".to_string(),
            description: "Hosts with the most current crypto references.".to_string(),
            count_label: "crypto refs".to_string(),
            items: list_top_sites_by_crypto_refs(&mut connection, Some(25))
                .frontend_context("loading top sites section: most crypto refs")?,
            has_items: false,
        },
        TopSiteSection {
            title: "Most Outgoing Links".to_string(),
            description: "Hosts linking out to the most destinations.".to_string(),
            count_label: "links".to_string(),
            items: list_top_sites_by_outgoing_links(&mut connection, Some(25))
                .frontend_context("loading top sites section: most outgoing links")?,
            has_items: false,
        },
        TopSiteSection {
            title: "Most Referenced Sites".to_string(),
            description: "Hosts receiving the most inbound link observations.".to_string(),
            count_label: "inbound refs".to_string(),
            items: list_top_referenced_sites(&mut connection, Some(25))
                .frontend_context("loading top sites section: most referenced sites")?,
            has_items: false,
        },
    ]
    .into_iter()
    .map(|mut section| {
        section.has_items = !section.items.is_empty();
        section
    })
    .collect::<Vec<_>>();
    let has_sections = sections.iter().any(|section| section.has_items);

    template_context(context! {
        title: "Top Sites",
        description: "Host-level leaderboards for the most active and most referenced sites in the current index.",
        sections: sections,
        has_sections: has_sections,
    })
}

#[get("/analytics")]
fn analytics(state: &State<AppState>) -> HtmlResult {
    render_background_cached_context(
        state,
        CACHE_ANALYTICS,
        "/analytics",
        "analytics",
        build_analytics_context,
        build_analytics_warming_context,
    )
}

fn build_analytics_context(state: &AppState) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let category_distribution = list_site_category_distribution(&mut connection)
        .frontend_context("loading site category distribution")?;
    let category_timeline = list_site_category_timeline(&mut connection)
        .frontend_context("loading site category timeline")?;
    let keyword_distribution = list_site_keyword_distribution(&mut connection)
        .frontend_context("loading site keyword distribution")?;
    let keyword_timeline = list_site_keyword_timeline(&mut connection)
        .frontend_context("loading site keyword timeline")?;
    let language_distribution = list_page_language_distribution(&mut connection)
        .frontend_context("loading page language distribution")?;
    let topic_distribution = list_page_topic_distribution(&mut connection)
        .frontend_context("loading page topic distribution")?;
    let topic_timeline = list_page_topic_timeline(&mut connection)
        .frontend_context("loading page topic timeline")?;
    let service_endpoint_count = count_discovered_service_endpoints(&mut connection)
        .frontend_context("counting discovered service endpoints")?;
    let total_hosts = category_distribution
        .iter()
        .map(|entry| entry.host_count)
        .sum::<usize>();
    let total_language_pages = language_distribution
        .iter()
        .map(|entry| entry.host_count)
        .sum::<usize>();
    let total_topic_pages = topic_distribution
        .iter()
        .map(|entry| entry.host_count)
        .sum::<usize>();
    let first_day = category_timeline
        .first()
        .map(|item| item.day.clone())
        .unwrap_or_else(|| "Never".to_string());
    let last_day = category_timeline
        .last()
        .map(|item| item.day.clone())
        .unwrap_or_else(|| "Never".to_string());
    let keyword_first_day = keyword_timeline
        .first()
        .map(|item| item.day.clone())
        .unwrap_or_else(|| "Never".to_string());
    let keyword_last_day = keyword_timeline
        .last()
        .map(|item| item.day.clone())
        .unwrap_or_else(|| "Never".to_string());
    let topic_first_day = topic_timeline
        .first()
        .map(|item| item.day.clone())
        .unwrap_or_else(|| "Never".to_string());
    let topic_last_day = topic_timeline
        .last()
        .map(|item| item.day.clone())
        .unwrap_or_else(|| "Never".to_string());
    let metrics = vec![
        CategoryMetricView {
            value: total_hosts.to_string(),
            label: "Classified Hosts".to_string(),
        },
        CategoryMetricView {
            value: category_distribution.len().to_string(),
            label: "Active Categories".to_string(),
        },
        CategoryMetricView {
            value: service_endpoint_count.max(0).to_string(),
            label: "Service Endpoints".to_string(),
        },
        CategoryMetricView {
            value: total_language_pages.to_string(),
            label: "Pages With Language".to_string(),
        },
        CategoryMetricView {
            value: total_topic_pages.to_string(),
            label: "Topic-Tagged Pages".to_string(),
        },
        CategoryMetricView {
            value: first_day.clone(),
            label: "First Classified Day".to_string(),
        },
        CategoryMetricView {
            value: last_day.clone(),
            label: "Latest Classified Day".to_string(),
        },
    ];
    let category_legend = build_category_legend_items(&category_distribution, "hosts");
    let category_pie_svg = render_distribution_pie_chart(
        &category_distribution,
        "Classified",
        "hosts",
        "Current site category distribution",
        "No data",
        "Run more scans to classify hosts",
    );
    let category_histogram_svg = render_timeline_histogram(
        &category_distribution,
        &category_timeline,
        "hosts",
        "Daily histogram of newly classified hosts by category",
        "No timeline yet",
        "Newly classified hosts will appear here over time",
    );
    let keyword_legend = build_category_legend_items(&keyword_distribution, "hosts");
    let keyword_pie_svg = render_distribution_pie_chart(
        &keyword_distribution,
        "Keyword Tags",
        "hosts",
        "Current site keyword tag distribution",
        "No keyword data",
        "Tagged forum hosts will appear here after keyword matches are found",
    );
    let keyword_histogram_svg = render_timeline_histogram(
        &keyword_distribution,
        &keyword_timeline,
        "hosts",
        "Daily histogram of first observed host keyword tags",
        "No keyword timeline yet",
        "Newly tagged forum hosts will appear here over time",
    );
    let language_legend = build_category_legend_items(&language_distribution, "pages");
    let language_pie_svg = render_distribution_pie_chart(
        &language_distribution,
        "Languages",
        "pages",
        "Current page language distribution",
        "No language data",
        "Language detections will appear here after pages are scanned",
    );
    let topic_legend = build_category_legend_items(&topic_distribution, "pages");
    let topic_pie_svg = render_distribution_pie_chart(
        &topic_distribution,
        "Topics",
        "pages",
        "Current static page topic distribution",
        "No topic data",
        "Static topic matches will appear here after pages are scanned",
    );
    let topic_histogram_svg = render_timeline_histogram(
        &topic_distribution,
        &topic_timeline,
        "pages",
        "Daily histogram of first observed page topics",
        "No topic timeline yet",
        "Newly matched page topics will appear here over time",
    );
    let has_distribution = !category_distribution.is_empty();
    let has_timeline = !category_timeline.is_empty();
    let has_keyword_distribution = !keyword_distribution.is_empty();
    let has_keyword_timeline = !keyword_timeline.is_empty();
    let has_language_distribution = !language_distribution.is_empty();
    let has_topic_distribution = !topic_distribution.is_empty();
    let has_topic_timeline = !topic_timeline.is_empty();

    template_context(context! {
        title: "Site Analytics",
        description: "See site classifications, discovered services, page language detections, and static topic tags across the current index.",
        metrics: metrics,
        category_legend: category_legend,
        category_pie_svg: category_pie_svg,
        category_histogram_svg: category_histogram_svg,
        keyword_legend: keyword_legend,
        keyword_pie_svg: keyword_pie_svg,
        keyword_histogram_svg: keyword_histogram_svg,
        language_legend: language_legend,
        language_pie_svg: language_pie_svg,
        topic_legend: topic_legend,
        topic_pie_svg: topic_pie_svg,
        topic_histogram_svg: topic_histogram_svg,
        has_distribution: has_distribution,
        has_timeline: has_timeline,
        has_keyword_distribution: has_keyword_distribution,
        has_keyword_timeline: has_keyword_timeline,
        has_language_distribution: has_language_distribution,
        has_topic_distribution: has_topic_distribution,
        has_topic_timeline: has_topic_timeline,
        total_hosts: total_hosts,
        first_day: first_day,
        last_day: last_day,
        keyword_first_day: keyword_first_day,
        keyword_last_day: keyword_last_day,
        topic_first_day: topic_first_day,
        topic_last_day: topic_last_day,
    })
}

fn build_analytics_warming_context() -> Result<Value, FrontendError> {
    let metrics = vec![
        CategoryMetricView {
            value: "0".to_string(),
            label: "Classified Hosts".to_string(),
        },
        CategoryMetricView {
            value: "0".to_string(),
            label: "Active Categories".to_string(),
        },
        CategoryMetricView {
            value: "0".to_string(),
            label: "Service Endpoints".to_string(),
        },
        CategoryMetricView {
            value: "0".to_string(),
            label: "Pages With Language".to_string(),
        },
        CategoryMetricView {
            value: "0".to_string(),
            label: "Topic-Tagged Pages".to_string(),
        },
        CategoryMetricView {
            value: "Refreshing".to_string(),
            label: "First Classified Day".to_string(),
        },
        CategoryMetricView {
            value: "Refreshing".to_string(),
            label: "Latest Classified Day".to_string(),
        },
    ];

    template_context(context! {
        title: "Site Analytics",
        description: "See site classifications, discovered services, page language detections, and static topic tags across the current index.",
        metrics: metrics,
        category_legend: Vec::<Value>::new(),
        category_pie_svg: "",
        category_histogram_svg: "",
        keyword_legend: Vec::<Value>::new(),
        keyword_pie_svg: "",
        keyword_histogram_svg: "",
        language_legend: Vec::<Value>::new(),
        language_pie_svg: "",
        topic_legend: Vec::<Value>::new(),
        topic_pie_svg: "",
        topic_histogram_svg: "",
        has_distribution: false,
        has_timeline: false,
        has_keyword_distribution: false,
        has_keyword_timeline: false,
        has_language_distribution: false,
        has_topic_distribution: false,
        has_topic_timeline: false,
        total_hosts: 0,
        first_day: "Refreshing",
        last_day: "Refreshing",
        keyword_first_day: "Refreshing",
        keyword_last_day: "Refreshing",
        topic_first_day: "Refreshing",
        topic_last_day: "Refreshing",
        cache_refresh_pending: true,
    })
}

#[get("/sites?<list_query..>")]
fn sites(state: &State<AppState>, list_query: Option<ListQuery>) -> HtmlResult {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    if list_query_is_default(&list_query) {
        let context = state.inner().cache.context(CACHE_SITES, || {
            build_sites_context(state.inner(), list_query.clone())
        })?;
        return Ok(Template::render("sites", context));
    }

    let context = build_sites_context(state.inner(), list_query)?;
    Ok(Template::render("sites", context))
}

fn build_sites_default_context(state: &AppState) -> Result<Value, FrontendError> {
    build_sites_context(
        state,
        ListQuery {
            limit: None,
            offset: None,
        },
    )
}

fn build_sites_context(state: &AppState, list_query: ListQuery) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let sites = list_site_profiles(&mut connection, list_query.limit, list_query.offset)
        .frontend_context("loading site profiles")?;
    let has_sites = !sites.items.is_empty();
    let pagination = pagination_context("/sites", &sites, &[]);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;

    template_context(context! {
        title: "Site Profiles",
        description: "Heuristic host categorization derived from crawled page content and structure.",
        sites: sites.items,
        site_count: sites.total_count,
        has_sites: has_sites,
        pagination: pagination,
        has_pagination: has_pagination,
    })
}

#[get("/watchlists")]
fn watchlists(state: &State<AppState>) -> HtmlResult {
    render_cached_context(
        state,
        CACHE_WATCHLISTS,
        "watchlists",
        build_watchlists_context,
    )
}

fn build_watchlists_context(state: &AppState) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let items =
        list_watchlist_items(&mut connection).frontend_context("loading watchlist items")?;
    let has_items = !items.is_empty();
    let item_count = items.len();
    let type_options = watchlist_type_options();

    template_context(context! {
        title: "Customer Watchlists",
        description: "Customer-specific indicators that generate watchlist-match leads when crawler or service observations match.",
        items: items,
        item_count: item_count,
        has_items: has_items,
        type_options: type_options,
    })
}

#[post("/watchlists", data = "<form>")]
fn add_watchlist(
    state: &State<AppState>,
    form: Form<WatchlistItemForm>,
) -> Result<Redirect, FrontendError> {
    let mut connection = state.inner().connection()?;
    add_watchlist_item(
        &mut connection,
        &form.item_type,
        &form.value,
        form.label.as_deref(),
    )
    .frontend_context("saving watchlist item")?;
    invalidate_watchlist_caches(state.inner());
    Ok(Redirect::to("/watchlists"))
}

#[post("/watchlists/<item_id>/delete")]
fn delete_watchlist_item(state: &State<AppState>, item_id: i32) -> Result<Redirect, FrontendError> {
    let mut connection = state.inner().connection()?;
    remove_watchlist_item(&mut connection, item_id).frontend_context("removing watchlist item")?;
    invalidate_watchlist_caches(state.inner());
    Ok(Redirect::to("/watchlists"))
}

#[get("/leads?<query..>")]
fn leads(state: &State<AppState>, query: Option<LeadQuery>) -> HtmlResult {
    let query = query.unwrap_or(LeadQuery {
        status: None,
        severity: None,
        rule_id: None,
        entity: None,
        sort: None,
        direction: None,
        limit: None,
        offset: None,
    });
    if lead_query_is_default(&query) {
        let context = state.inner().cache.context(CACHE_LEADS, || {
            build_leads_context(state.inner(), query.clone())
        })?;
        return Ok(Template::render("leads", context));
    }

    let context = build_leads_context(state.inner(), query)?;
    Ok(Template::render("leads", context))
}

fn build_leads_default_context(state: &AppState) -> Result<Value, FrontendError> {
    build_leads_context(
        state,
        LeadQuery {
            status: None,
            severity: None,
            rule_id: None,
            entity: None,
            sort: None,
            direction: None,
            limit: None,
            offset: None,
        },
    )
}

fn build_leads_context(state: &AppState, query: LeadQuery) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let leads = list_intel_leads(
        &mut connection,
        query.status.as_deref(),
        query.severity.as_deref(),
        query.rule_id.as_deref(),
        query.entity.as_deref(),
        query.sort.as_deref(),
        query.direction.as_deref(),
        query.limit,
        query.offset,
    )
    .frontend_context("loading intel leads")?;
    let has_leads = !leads.items.is_empty();
    let extra_values = lead_query_extra_params(&query);
    let extra_refs = extra_values
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let pagination = pagination_context("/leads", &leads, &extra_refs);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;
    let sort_severity_url = lead_sort_url(&query, "severity");
    let sort_confidence_url = lead_sort_url(&query, "confidence");
    let sort_last_seen_url = lead_sort_url(&query, "last_seen");
    let sort_status_url = lead_sort_url(&query, "status");
    let sort_rule_url = lead_sort_url(&query, "rule");
    let sort_entity_url = lead_sort_url(&query, "entity");
    let rule_filter_options = lead_rule_filter_options(query.rule_id.as_deref());
    let status_filter_options = lead_filter_options(
        &["new", "triaged", "monitoring", "suppressed"],
        query.status.as_deref(),
    );
    let severity_filter_options = lead_filter_options(
        &["low", "medium", "high", "critical"],
        query.severity.as_deref(),
    );

    template_context(context! {
        title: "Intel Leads",
        description: "Local deterministic leads generated from crawl observations, entity reuse, page diffs, blacklist hits, and service fingerprints.",
        leads: leads.items,
        lead_count: leads.total_count,
        has_leads: has_leads,
        filters: query,
        pagination: pagination,
        has_pagination: has_pagination,
        sort_severity_url: sort_severity_url,
        sort_confidence_url: sort_confidence_url,
        sort_last_seen_url: sort_last_seen_url,
        sort_status_url: sort_status_url,
        sort_rule_url: sort_rule_url,
        sort_entity_url: sort_entity_url,
        rule_filter_options: rule_filter_options,
        status_filter_options: status_filter_options,
        severity_filter_options: severity_filter_options,
    })
}

#[get("/leads/<lead_id>")]
fn lead_detail(state: &State<AppState>, lead_id: i32) -> HtmlResult {
    let mut connection = state.inner().connection()?;
    let detail = get_intel_lead_detail(&mut connection, lead_id)
        .frontend_context("loading intel lead detail")?;
    let Some(detail) = detail else {
        return Err(FrontendError {
            status: Status::NotFound,
            title: "Lead Not Found",
            detail: format!("intel lead {lead_id} was not found"),
        });
    };
    let has_evidence = !detail.evidence.is_empty();
    let has_related_pages = !detail.related_pages.is_empty();
    let has_related_sites = !detail.related_sites.is_empty();
    let has_related_entities = !detail.related_entities.is_empty();

    Ok(Template::render(
        "lead_detail",
        context! {
            title: detail.lead.title.clone(),
            description: detail.lead.summary.clone(),
            detail: detail,
            has_evidence: has_evidence,
            has_related_pages: has_related_pages,
            has_related_sites: has_related_sites,
            has_related_entities: has_related_entities,
        },
    ))
}

#[post("/leads/<lead_id>/status", data = "<form>")]
fn lead_status_form(
    state: &State<AppState>,
    lead_id: i32,
    form: Form<LeadStatusForm>,
) -> Result<Redirect, FrontendError> {
    let mut connection = state.inner().connection()?;
    let updated = update_intel_lead_status(&mut connection, lead_id, &form.status)
        .frontend_context("updating intel lead status")?;
    if updated.is_none() {
        return Err(FrontendError {
            status: Status::NotFound,
            title: "Lead Not Found",
            detail: format!("intel lead {lead_id} was not found"),
        });
    }
    invalidate_lead_caches(state.inner());
    Ok(Redirect::to(format!("/leads/{lead_id}")))
}

#[get("/relationships?<query..>")]
fn relationships(state: &State<AppState>, query: Option<RelationshipQuery>) -> HtmlResult {
    let query = query.unwrap_or(RelationshipQuery {
        limit: None,
        offset: None,
        focus: None,
        depth: None,
    });
    if relationship_query_is_default(&query) {
        let context = state.inner().cache.context(CACHE_RELATIONSHIPS, || {
            build_relationships_context(state.inner(), query.clone())
        })?;
        return Ok(Template::render("relationships", context));
    }

    let context = build_relationships_context(state.inner(), query)?;
    Ok(Template::render("relationships", context))
}

fn build_relationships_default_context(state: &AppState) -> Result<Value, FrontendError> {
    build_relationships_context(
        state,
        RelationshipQuery {
            limit: None,
            offset: None,
            focus: None,
            depth: None,
        },
    )
}

fn build_relationships_context(
    state: &AppState,
    query: RelationshipQuery,
) -> Result<Value, FrontendError> {
    let relationship_focus = query.focus.unwrap_or_default();
    let relationship_focus = relationship_focus.trim().to_string();
    let relationship_depth = query.depth.unwrap_or(3).clamp(1, 4);

    // For initial page load without pagination params, skip the expensive table query
    // Users can use the graph visualization to explore relationships
    let (relationships, has_pagination) = if query.limit.is_none() && query.offset.is_none() {
        // Fast initial load - no database query
        (Vec::new(), false)
    } else {
        // User requested pagination - load table data
        let mut connection = state.connection()?;
        let relationships = list_site_relationships_fast(&mut connection, query.limit, query.offset)
            .frontend_context("loading site relationships")?;
        let relationship_depth_param = relationship_depth.to_string();
        let mut extra_params = Vec::new();
        if !relationship_focus.is_empty() {
            extra_params.push(("focus", relationship_focus.as_str()));
        }
        extra_params.push(("depth", relationship_depth_param.as_str()));
        let pagination = pagination_context("/relationships", &relationships, &extra_params);
        let has_pagination = pagination.has_previous_page || pagination.has_next_page;
        (relationships.items, has_pagination)
    };

    let has_relationships = !relationships.is_empty();
    let relationship_count = relationships.len() as i64;

    template_context(context! {
        title: "Site Relationships",
        description: "Host-level references observed while scanning pages.",
        relationships: relationships,
        relationship_count: relationship_count,
        has_relationships: has_relationships,
        pagination: PaginationView {
            total_count: 0,
            limit: 50,
            offset: 0,
            has_previous_page: false,
            has_next_page: false,
            previous_page_url: String::new(),
            next_page_url: String::new(),
        },
        has_pagination: has_pagination,
        relationship_focus: relationship_focus,
        relationship_depth: relationship_depth,
    })
}

#[get("/entities/emails?<query..>")]
fn email_entities(state: &State<AppState>, query: Option<EmailQuery>) -> HtmlResult {
    let query = query.unwrap_or(EmailQuery {
        value: None,
        limit: None,
        offset: None,
    });
    if email_query_is_default(&query) {
        let context = state.inner().cache.context(CACHE_EMAILS, || {
            build_email_entities_context(state.inner(), query.clone())
        })?;
        return Ok(Template::render("emails", context));
    }

    let context = build_email_entities_context(state.inner(), query)?;
    Ok(Template::render("emails", context))
}

fn build_email_entities_default_context(state: &AppState) -> Result<Value, FrontendError> {
    build_email_entities_context(
        state,
        EmailQuery {
            value: None,
            limit: None,
            offset: None,
        },
    )
}

fn build_email_entities_context(
    state: &AppState,
    query: EmailQuery,
) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let entities = list_email_entities(&mut connection, query.limit, query.offset)
        .frontend_context("loading email entities")?;
    let selected = match query.value.clone() {
        Some(value) => get_email_entity_detail(&mut connection, &value)
            .frontend_context("loading email entity detail")?,
        None => None,
    };
    let has_entities = !entities.items.is_empty();
    let has_selected = selected.is_some();
    let selected_page_count = selected.as_ref().map(|item| item.pages.len()).unwrap_or(0);
    let extra_params = query
        .value
        .as_ref()
        .map(|value| vec![("value", value.as_str())])
        .unwrap_or_default();
    let pagination = pagination_context("/entities/emails", &entities, &extra_params);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;

    template_context(context! {
        title: "Shared Emails",
        description: "See which email addresses appear across multiple scanned sites.",
        entities: entities.items,
        selected: selected,
        has_entities: has_entities,
        has_selected: has_selected,
        entity_count: entities.total_count,
        selected_page_count: selected_page_count,
        pagination: pagination,
        has_pagination: has_pagination,
    })
}

#[get("/entities/crypto?<query..>")]
fn crypto_entities(state: &State<AppState>, query: Option<CryptoQuery>) -> HtmlResult {
    let query = query.unwrap_or(CryptoQuery {
        asset_type: None,
        reference: None,
        limit: None,
        offset: None,
    });
    if crypto_query_is_default(&query) {
        let context = state.inner().cache.context(CACHE_CRYPTO, || {
            build_crypto_entities_context(state.inner(), query.clone())
        })?;
        return Ok(Template::render("crypto", context));
    }

    let context = build_crypto_entities_context(state.inner(), query)?;
    Ok(Template::render("crypto", context))
}

fn build_crypto_entities_default_context(state: &AppState) -> Result<Value, FrontendError> {
    build_crypto_entities_context(
        state,
        CryptoQuery {
            asset_type: None,
            reference: None,
            limit: None,
            offset: None,
        },
    )
}

fn build_crypto_entities_context(
    state: &AppState,
    query: CryptoQuery,
) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let entities = list_crypto_entities(&mut connection, query.limit, query.offset)
        .frontend_context("loading crypto entities")?;
    let selected = match (query.asset_type.clone(), query.reference.clone()) {
        (Some(asset_type), Some(reference)) => {
            get_crypto_entity_detail(&mut connection, &asset_type, &reference)
                .frontend_context("loading crypto entity detail")?
        }
        _ => None,
    };
    let has_entities = !entities.items.is_empty();
    let has_selected = selected.is_some();
    let selected_page_count = selected.as_ref().map(|item| item.pages.len()).unwrap_or(0);
    let mut extra_params = Vec::new();
    if let Some(asset_type) = query.asset_type.as_ref() {
        extra_params.push(("asset_type", asset_type.as_str()));
    }
    if let Some(reference) = query.reference.as_ref() {
        extra_params.push(("reference", reference.as_str()));
    }
    let pagination = pagination_context("/entities/crypto", &entities, &extra_params);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;

    template_context(context! {
        title: "Shared Crypto References",
        description: "Review wallet or payment references that appear on more than one site.",
        entities: entities.items,
        selected: selected,
        has_entities: has_entities,
        has_selected: has_selected,
        entity_count: entities.total_count,
        selected_page_count: selected_page_count,
        pagination: pagination,
        has_pagination: has_pagination,
    })
}

#[get("/entities/ssh?<query..>")]
fn ssh_entities(state: &State<AppState>, query: Option<SshQuery>) -> HtmlResult {
    let query = query.unwrap_or(SshQuery {
        algorithm: None,
        fingerprint: None,
        limit: None,
        offset: None,
    });
    if ssh_query_is_default(&query) {
        let context = state.inner().cache.context(CACHE_SSH, || {
            build_ssh_entities_context(state.inner(), query.clone())
        })?;
        return Ok(Template::render("ssh", context));
    }

    let context = build_ssh_entities_context(state.inner(), query)?;
    Ok(Template::render("ssh", context))
}

fn build_ssh_entities_default_context(state: &AppState) -> Result<Value, FrontendError> {
    build_ssh_entities_context(
        state,
        SshQuery {
            algorithm: None,
            fingerprint: None,
            limit: None,
            offset: None,
        },
    )
}

fn build_ssh_entities_context(state: &AppState, query: SshQuery) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let entities = list_ssh_host_keys(&mut connection, query.limit, query.offset)
        .frontend_context("loading ssh host key entities")?;
    let selected = match (query.algorithm.clone(), query.fingerprint.clone()) {
        (Some(algorithm), Some(fingerprint)) => {
            get_ssh_host_key_detail(&mut connection, &algorithm, &fingerprint)
                .frontend_context("loading ssh host key detail")?
        }
        _ => None,
    };
    let has_entities = !entities.items.is_empty();
    let has_selected = selected.is_some();
    let selected_host_count = selected.as_ref().map(|item| item.host_count).unwrap_or(0);
    let selected_endpoint_count = selected
        .as_ref()
        .map(|item| item.endpoint_count)
        .unwrap_or(0);
    let mut extra_params = Vec::new();
    if let Some(algorithm) = query.algorithm.as_ref() {
        extra_params.push(("algorithm", algorithm.as_str()));
    }
    if let Some(fingerprint) = query.fingerprint.as_ref() {
        extra_params.push(("fingerprint", fingerprint.as_str()));
    }
    let pagination = pagination_context("/entities/ssh", &entities, &extra_params);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;

    template_context(context! {
        title: "Shared SSH Host Keys",
        description: "Correlate SSH host keys across recently reachable hosts and identify infrastructure reused by multiple sites.",
        entities: entities.items,
        selected: selected,
        has_entities: has_entities,
        has_selected: has_selected,
        entity_count: entities.total_count,
        selected_host_count: selected_host_count,
        selected_endpoint_count: selected_endpoint_count,
        pagination: pagination,
        has_pagination: has_pagination,
    })
}

#[get("/entities/http?<query..>")]
fn http_entities(state: &State<AppState>, query: Option<HttpQuery>) -> HtmlResult {
    let query = query.unwrap_or(HttpQuery {
        host: None,
        scheme: None,
        port: None,
        limit: None,
        offset: None,
    });
    if http_query_is_default(&query) {
        let context = state.inner().cache.context(CACHE_HTTP, || {
            build_http_entities_context(state.inner(), query.clone())
        })?;
        return Ok(Template::render("http", context));
    }

    let context = build_http_entities_context(state.inner(), query)?;
    Ok(Template::render("http", context))
}

fn build_http_entities_default_context(state: &AppState) -> Result<Value, FrontendError> {
    build_http_entities_context(
        state,
        HttpQuery {
            host: None,
            scheme: None,
            port: None,
            limit: None,
            offset: None,
        },
    )
}

fn build_http_entities_context(state: &AppState, query: HttpQuery) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let entities = list_host_http_observations(&mut connection, query.limit, query.offset)
        .frontend_context("loading host http observations")?;
    let selected = match (query.host.clone(), query.scheme.clone(), query.port) {
        (Some(host), Some(scheme), Some(port)) => {
            get_host_http_observation_detail(&mut connection, &host, &scheme, port)
                .frontend_context("loading host http observation detail")?
        }
        _ => None,
    };
    let has_entities = !entities.items.is_empty();
    let has_selected = selected.is_some();
    let mut extra_params = Vec::new();
    if let Some(host) = query.host.as_ref() {
        extra_params.push(("host", host.as_str()));
    }
    if let Some(scheme) = query.scheme.as_ref() {
        extra_params.push(("scheme", scheme.as_str()));
    }
    let port_param = query.port.map(|value| value.to_string());
    if let Some(port) = port_param.as_deref() {
        extra_params.push(("port", port));
    }
    let pagination = pagination_context("/entities/http", &entities, &extra_params);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;

    template_context(context! {
        title: "HTTP Fingerprints",
        description: "Inspect host-level HTTP headers, stack hints, exposed resource probes, redirect targets, and any captured TLS certificate fingerprints.",
        entities: entities.items,
        selected: selected,
        has_entities: has_entities,
        has_selected: has_selected,
        entity_count: entities.total_count,
        pagination: pagination,
        has_pagination: has_pagination,
    })
}

#[get("/entities/services?<query..>")]
fn service_entities(state: &State<AppState>, query: Option<ServiceQuery>) -> HtmlResult {
    let query = query.unwrap_or(ServiceQuery {
        host: None,
        service: None,
        port: None,
        limit: None,
        offset: None,
    });
    if service_query_is_default(&query) {
        let context = state.inner().cache.context(CACHE_SERVICES, || {
            build_service_entities_context(state.inner(), query.clone())
        })?;
        return Ok(Template::render("services", context));
    }

    let context = build_service_entities_context(state.inner(), query)?;
    Ok(Template::render("services", context))
}

fn build_service_entities_default_context(state: &AppState) -> Result<Value, FrontendError> {
    build_service_entities_context(
        state,
        ServiceQuery {
            host: None,
            service: None,
            port: None,
            limit: None,
            offset: None,
        },
    )
}

fn build_service_entities_context(
    state: &AppState,
    query: ServiceQuery,
) -> Result<Value, FrontendError> {
    let mut connection = state.connection()?;
    let entities = list_host_service_observations(&mut connection, query.limit, query.offset)
        .frontend_context("loading host service observations")?;
    let selected = match (query.host.clone(), query.service.clone(), query.port) {
        (Some(host), Some(service), Some(port)) => {
            get_host_service_observation_detail(&mut connection, &host, &service, port)
                .frontend_context("loading host service observation detail")?
        }
        _ => None,
    };
    let has_entities = !entities.items.is_empty();
    let has_selected = selected.is_some();
    let mut extra_params = Vec::new();
    if let Some(host) = query.host.as_ref() {
        extra_params.push(("host", host.as_str()));
    }
    if let Some(service) = query.service.as_ref() {
        extra_params.push(("service", service.as_str()));
    }
    let port_param = query.port.map(|value| value.to_string());
    if let Some(port) = port_param.as_deref() {
        extra_params.push(("port", port));
    }
    let pagination = pagination_context("/entities/services", &entities, &extra_params);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;

    template_context(context! {
        title: "Other Network Services",
        description: "Inspect host-level IRC and FTP banners captured from recently reachable hosts, alongside any auxiliary web ports already visible under HTTP.",
        entities: entities.items,
        selected: selected,
        has_entities: has_entities,
        has_selected: has_selected,
        entity_count: entities.total_count,
        pagination: pagination,
        has_pagination: has_pagination,
    })
}

#[get("/search?<search..>")]
fn search_page(state: &State<AppState>, search: Option<SearchQuery>) -> HtmlResult {
    let search = search.unwrap_or(SearchQuery {
        query: None,
        limit: None,
        offset: None,
    });
    let query = search.query.unwrap_or_default();
    let limit = search.limit.unwrap_or(20).clamp(1, 50);
    let offset = search.offset.unwrap_or(0).max(0);

    let results = if query.trim().is_empty() {
        PaginatedResult {
            items: Vec::new(),
            total_count: 0,
            limit,
            offset,
        }
    } else {
        let mut connection = state.inner().connection()?;
        search_pages(&mut connection, &query, Some(limit), Some(offset))
            .frontend_context("searching pages")?
    };
    let has_query = !query.trim().is_empty();
    let has_results = !results.items.is_empty();
    let result_count = results.total_count;
    let extra_params = vec![("query", query.trim())];
    let pagination = pagination_context("/search", &results, &extra_params);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;

    Ok(Template::render(
        "search",
        context! {
            title: "Search",
            description: "Search titles, URLs, languages, emails, crypto references, and keyword-tagged sites.",
            query: query.trim(),
            limit: limit,
            results: results.items,
            has_query: has_query,
            has_results: has_results,
            result_count: result_count,
            pagination: pagination,
            has_pagination: has_pagination,
        },
    ))
}

#[get("/api/stats")]
fn api_stats(state: &State<AppState>) -> Result<Json<ApiResponse<spyder::models::Stats>>, Status> {
    let mut connection = api_connection(state)?;
    let stats = collect_stats(&mut connection).map_err(|_| Status::InternalServerError)?;

    Ok(Json(ApiResponse {
        success: true,
        data: stats,
    }))
}

#[get("/api/metrics")]
fn api_metrics(state: &State<AppState>) -> Json<spyder::metrics::MetricsSnapshot> {
    Json(state.metrics.snapshot())
}

#[get("/api/search?<search..>")]
fn api_search(
    state: &State<AppState>,
    search: Option<SearchQuery>,
) -> Result<Json<ApiResponse<PaginatedResult<spyder::models::SearchResult>>>, Status> {
    let search = search.unwrap_or(SearchQuery {
        query: None,
        limit: None,
        offset: None,
    });
    let query = search.query.unwrap_or_default();
    let mut connection = api_connection(state)?;
    let results = search_pages(&mut connection, &query, search.limit, search.offset)
        .map_err(|_| Status::InternalServerError)?;

    Ok(Json(ApiResponse {
        success: true,
        data: results,
    }))
}

#[get("/api/relationships/graph?<query..>")]
fn api_relationship_graph(
    state: &State<AppState>,
    query: Option<RelationshipQuery>,
) -> Result<Json<ApiResponse<spyder::models::SiteRelationshipGraph>>, Status> {
    let query = query.unwrap_or(RelationshipQuery {
        limit: None,
        offset: None,
        focus: None,
        depth: None,
    });
    let mut connection = api_connection(state)?;
    let graph = get_site_relationship_graph(
        &mut connection,
        query.focus.as_deref(),
        query.depth,
        query.limit,
    )
    .map_err(|_| Status::InternalServerError)?;

    Ok(Json(ApiResponse {
        success: true,
        data: graph,
    }))
}

#[get("/api/blacklist")]
fn api_blacklist(
    state: &State<AppState>,
) -> Result<Json<ApiResponse<Vec<spyder::models::DomainBlacklistSummary>>>, Status> {
    let mut connection = api_connection(state)?;
    let entries = list_domain_blacklist_summaries(&mut connection)
        .map_err(|_| Status::InternalServerError)?;

    Ok(Json(ApiResponse {
        success: true,
        data: entries,
    }))
}

#[get("/api/blacklist/auto")]
fn api_auto_blacklist(
    state: &State<AppState>,
) -> Result<Json<ApiResponse<spyder::models::AutoBlacklistConfig>>, Status> {
    let mut connection = api_connection(state)?;
    let config =
        get_auto_blacklist_config(&mut connection).map_err(|_| Status::InternalServerError)?;

    Ok(Json(ApiResponse {
        success: true,
        data: config,
    }))
}

#[get("/api/sites?<list_query..>")]
fn api_sites(
    state: &State<AppState>,
    list_query: Option<ListQuery>,
) -> Result<Json<ApiResponse<PaginatedResult<spyder::models::SiteProfileSummary>>>, Status> {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    let mut connection = api_connection(state)?;
    let sites = list_site_profiles(&mut connection, list_query.limit, list_query.offset)
        .map_err(|_| Status::InternalServerError)?;

    Ok(Json(ApiResponse {
        success: true,
        data: sites,
    }))
}

#[get("/api/pages/<page_id>/history")]
fn api_page_history(
    state: &State<AppState>,
    page_id: i32,
) -> Result<Json<ApiResponse<Vec<spyder::models::PageScanSummary>>>, Status> {
    let mut connection = api_connection(state)?;
    let page =
        get_page_detail(&mut connection, page_id).map_err(|_| Status::InternalServerError)?;
    if page.is_none() {
        return Err(Status::NotFound);
    }
    let scans = list_page_scan_summaries(&mut connection, page_id)
        .map_err(|_| Status::InternalServerError)?;

    Ok(Json(ApiResponse {
        success: true,
        data: scans,
    }))
}

#[get("/api/pages/<page_id>/history/<scan_id>")]
fn api_page_scan_detail(
    state: &State<AppState>,
    page_id: i32,
    scan_id: i32,
) -> Result<Json<ApiResponse<spyder::models::PageScanDetail>>, Status> {
    let mut connection = api_connection(state)?;
    let detail = get_page_scan_detail(&mut connection, page_id, scan_id)
        .map_err(|_| Status::InternalServerError)?;
    let Some(detail) = detail else {
        return Err(Status::NotFound);
    };

    Ok(Json(ApiResponse {
        success: true,
        data: detail,
    }))
}

#[get("/api/leads?<query..>")]
fn api_leads(
    state: &State<AppState>,
    query: Option<LeadQuery>,
) -> Result<Json<ApiResponse<PaginatedResult<spyder::models::IntelLeadSummary>>>, Status> {
    let query = query.unwrap_or(LeadQuery {
        status: None,
        severity: None,
        rule_id: None,
        entity: None,
        sort: None,
        direction: None,
        limit: None,
        offset: None,
    });
    let mut connection = api_connection(state)?;
    let leads = list_intel_leads(
        &mut connection,
        query.status.as_deref(),
        query.severity.as_deref(),
        query.rule_id.as_deref(),
        query.entity.as_deref(),
        query.sort.as_deref(),
        query.direction.as_deref(),
        query.limit,
        query.offset,
    )
    .map_err(|_| Status::InternalServerError)?;

    Ok(Json(ApiResponse {
        success: true,
        data: leads,
    }))
}

#[get("/api/leads/<lead_id>")]
fn api_lead_detail(
    state: &State<AppState>,
    lead_id: i32,
) -> Result<Json<ApiResponse<spyder::models::IntelLeadDetail>>, Status> {
    let mut connection = api_connection(state)?;
    let detail =
        get_intel_lead_detail(&mut connection, lead_id).map_err(|_| Status::InternalServerError)?;
    let Some(detail) = detail else {
        return Err(Status::NotFound);
    };

    Ok(Json(ApiResponse {
        success: true,
        data: detail,
    }))
}

#[post("/api/leads/<lead_id>/status", data = "<request>")]
fn api_lead_status(
    state: &State<AppState>,
    lead_id: i32,
    request: Json<LeadStatusRequest>,
) -> Result<Json<ApiResponse<spyder::models::IntelLeadDetail>>, Status> {
    let mut connection = api_connection(state)?;
    let detail = update_intel_lead_status(&mut connection, lead_id, &request.status)
        .map_err(|_| Status::BadRequest)?;
    let Some(detail) = detail else {
        return Err(Status::NotFound);
    };
    invalidate_lead_caches(state.inner());

    Ok(Json(ApiResponse {
        success: true,
        data: detail,
    }))
}

fn api_connection(state: &State<AppState>) -> Result<DbConnection, Status> {
    state
        .inner()
        .connection()
        .map_err(|_| Status::InternalServerError)
}

struct CachedRoute {
    key: &'static str,
    label: &'static str,
    build: fn(&AppState) -> Result<Value, FrontendError>,
}

const CACHED_ROUTES: &[CachedRoute] = &[
    CachedRoute {
        key: CACHE_DASHBOARD,
        label: "/",
        build: build_dashboard_context,
    },
    CachedRoute {
        key: CACHE_PAGES,
        label: "/pages",
        build: build_pages_default_context,
    },
    CachedRoute {
        key: CACHE_ANALYTICS,
        label: "/analytics",
        build: build_analytics_context,
    },
    CachedRoute {
        key: CACHE_SITES,
        label: "/sites",
        build: build_sites_default_context,
    },
    CachedRoute {
        key: CACHE_WATCHLISTS,
        label: "/watchlists",
        build: build_watchlists_context,
    },
    CachedRoute {
        key: CACHE_LEADS,
        label: "/leads",
        build: build_leads_default_context,
    },
    CachedRoute {
        key: CACHE_RELATIONSHIPS,
        label: "/relationships",
        build: build_relationships_default_context,
    },
    CachedRoute {
        key: CACHE_EMAILS,
        label: "/entities/emails",
        build: build_email_entities_default_context,
    },
    CachedRoute {
        key: CACHE_CRYPTO,
        label: "/entities/crypto",
        build: build_crypto_entities_default_context,
    },
    CachedRoute {
        key: CACHE_HTTP,
        label: "/entities/http",
        build: build_http_entities_default_context,
    },
    CachedRoute {
        key: CACHE_SERVICES,
        label: "/entities/services",
        build: build_service_entities_default_context,
    },
    CachedRoute {
        key: CACHE_SSH,
        label: "/entities/ssh",
        build: build_ssh_entities_default_context,
    },
    CachedRoute {
        key: CACHE_BLACKLIST,
        label: "/blacklist",
        build: build_blacklist_context,
    },
    CachedRoute {
        key: CACHE_WORK,
        label: "/work",
        build: build_work_default_context,
    },
    CachedRoute {
        key: CACHE_TOP,
        label: "/top",
        build: build_top_context,
    },
];

fn cache_warmer_fairing() -> AdHoc {
    AdHoc::on_liftoff("frontend default-page cache warmer", |rocket| {
        Box::pin(async move {
            let Some(state) = rocket.state::<AppState>() else {
                warn!("AppState is not managed, skipping cache warmer");
                return;
            };
            let state = (*state).clone();
            let interval = if state.cache.ttl() < Duration::from_secs(1) {
                Duration::from_secs(1)
            } else {
                state.cache.ttl()
            };

            if let Err(error) = thread::Builder::new()
                .name("spyder-frontend-cache-warmer".to_string())
                .spawn(move || loop {
                    warm_default_caches(&state);
                    thread::sleep(interval);
                })
            {
                error!(error = %error, "Cache warmer failed to start");
            }
        })
    })
}

fn warm_default_caches(state: &AppState) {
    for route in CACHED_ROUTES {
        if !state.should_warm_cache_route(route.key) {
            continue;
        }
        if !state.cache.begin_refresh(route.key) {
            continue;
        }
        let started_at = Instant::now();
        let result = state
            .cache
            .refresh_context(route.key, || (route.build)(state));
        let elapsed = started_at.elapsed();
        state.cache.finish_refresh(route.key);
        if let Err(error) = result {
            error!(
                route = route.label,
                duration_secs = elapsed.as_secs_f64(),
                error = %error.detail,
                "Cache warmer failed to refresh"
            );
        } else {
            state.log_cache_route_duration(route.label, elapsed);
        }
    }
}

fn invalidate_blacklist_caches(state: &AppState) {
    state.cache.invalidate_many(&[
        CACHE_BLACKLIST,
        CACHE_DASHBOARD,
        CACHE_RELATIONSHIPS,
        CACHE_SITES,
    ]);
}

fn invalidate_watchlist_caches(state: &AppState) {
    state
        .cache
        .invalidate_many(&[CACHE_WATCHLISTS, CACHE_LEADS, CACHE_DASHBOARD]);
}

fn invalidate_lead_caches(state: &AppState) {
    state.cache.invalidate_many(&[CACHE_LEADS, CACHE_DASHBOARD]);
}

fn lead_query_is_default(query: &LeadQuery) -> bool {
    string_query_value_is_default(query.status.as_deref())
        && string_query_value_is_default(query.severity.as_deref())
        && string_query_value_is_default(query.rule_id.as_deref())
        && string_query_value_is_default(query.entity.as_deref())
        && string_query_value_is_default(query.sort.as_deref())
        && string_query_value_is_default(query.direction.as_deref())
        && query.limit.is_none()
        && query.offset.unwrap_or(0) == 0
}

fn relationship_query_is_default(query: &RelationshipQuery) -> bool {
    query.limit.is_none()
        && query.offset.unwrap_or(0) == 0
        && string_query_value_is_default(query.focus.as_deref())
        && query.depth.unwrap_or(3) == 3
}

fn email_query_is_default(query: &EmailQuery) -> bool {
    string_query_value_is_default(query.value.as_deref())
        && query.limit.is_none()
        && query.offset.unwrap_or(0) == 0
}

fn crypto_query_is_default(query: &CryptoQuery) -> bool {
    string_query_value_is_default(query.asset_type.as_deref())
        && string_query_value_is_default(query.reference.as_deref())
        && query.limit.is_none()
        && query.offset.unwrap_or(0) == 0
}

fn ssh_query_is_default(query: &SshQuery) -> bool {
    string_query_value_is_default(query.algorithm.as_deref())
        && string_query_value_is_default(query.fingerprint.as_deref())
        && query.limit.is_none()
        && query.offset.unwrap_or(0) == 0
}

fn http_query_is_default(query: &HttpQuery) -> bool {
    string_query_value_is_default(query.host.as_deref())
        && string_query_value_is_default(query.scheme.as_deref())
        && query.port.is_none()
        && query.limit.is_none()
        && query.offset.unwrap_or(0) == 0
}

fn service_query_is_default(query: &ServiceQuery) -> bool {
    string_query_value_is_default(query.host.as_deref())
        && string_query_value_is_default(query.service.as_deref())
        && query.port.is_none()
        && query.limit.is_none()
        && query.offset.unwrap_or(0) == 0
}

fn string_query_value_is_default(value: Option<&str>) -> bool {
    value.map(str::trim).unwrap_or_default().is_empty()
}

fn lead_query_extra_params(query: &LeadQuery) -> Vec<(String, String)> {
    let mut params = Vec::new();
    push_optional_param(&mut params, "status", query.status.as_deref());
    push_optional_param(&mut params, "severity", query.severity.as_deref());
    push_optional_param(&mut params, "rule_id", query.rule_id.as_deref());
    push_optional_param(&mut params, "entity", query.entity.as_deref());
    push_optional_param(&mut params, "sort", query.sort.as_deref());
    push_optional_param(&mut params, "direction", query.direction.as_deref());
    params
}

fn lead_filter_options(values: &[&str], selected: Option<&str>) -> Vec<LeadFilterOption> {
    let selected = selected.map(str::trim).unwrap_or_default();
    values
        .iter()
        .map(|value| LeadFilterOption {
            value: value.to_string(),
            selected: *value == selected,
        })
        .collect()
}

fn lead_rule_filter_options(selected: Option<&str>) -> Vec<LeadFilterOption> {
    let selected = selected.map(str::trim).unwrap_or_default();
    intel_lead_rule_ids()
        .into_iter()
        .map(|rule_id| LeadFilterOption {
            value: rule_id.to_string(),
            selected: rule_id == selected,
        })
        .collect()
}

fn watchlist_type_options() -> Vec<WatchlistTypeOption> {
    valid_watchlist_item_types()
        .into_iter()
        .map(|value| WatchlistTypeOption {
            value: value.to_string(),
            label: watchlist_type_label(value).to_string(),
        })
        .collect()
}

fn watchlist_type_label(value: &str) -> &'static str {
    match value {
        "domain" => "Domain",
        "url" => "URL",
        "email" => "Email",
        "crypto" => "Crypto",
        "keyword" => "Keyword",
        "ssh_fingerprint" => "SSH fingerprint",
        "http_fingerprint" => "HTTP fingerprint",
        "favicon_hash" => "Favicon hash",
        _ => "Item",
    }
}

fn push_optional_param(params: &mut Vec<(String, String)>, key: &str, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        params.push((key.to_string(), value.to_string()));
    }
}

fn lead_sort_url(query: &LeadQuery, sort: &str) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    push_sort_query_pair(&mut serializer, "status", query.status.as_deref());
    push_sort_query_pair(&mut serializer, "severity", query.severity.as_deref());
    push_sort_query_pair(&mut serializer, "rule_id", query.rule_id.as_deref());
    push_sort_query_pair(&mut serializer, "entity", query.entity.as_deref());
    serializer.append_pair("sort", sort);
    let next_direction = if query.sort.as_deref() == Some(sort)
        && query.direction.as_deref().unwrap_or("desc") == "desc"
    {
        "asc"
    } else {
        "desc"
    };
    serializer.append_pair("direction", next_direction);
    format!("/leads?{}", serializer.finish())
}

fn push_sort_query_pair(
    serializer: &mut form_urlencoded::Serializer<'_, String>,
    key: &str,
    value: Option<&str>,
) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        serializer.append_pair(key, value);
    }
}

fn pagination_context<T>(
    base_path: &str,
    page: &PaginatedResult<T>,
    extra_params: &[(&str, &str)],
) -> PaginationView {
    let has_previous_page = page.offset > 0;
    let has_next_page = page.offset + page.limit < page.total_count;
    let previous_offset = (page.offset - page.limit).max(0);
    let next_offset = page.offset + page.limit;

    PaginationView {
        total_count: page.total_count,
        limit: page.limit,
        offset: page.offset,
        has_previous_page,
        has_next_page,
        previous_page_url: build_list_url(base_path, page.limit, previous_offset, extra_params),
        next_page_url: build_list_url(base_path, page.limit, next_offset, extra_params),
    }
}

fn work_unit_view_from_model(
    workunit: &spyder::models::WorkUnit,
    blacklist_domains: &[String],
) -> WorkUnitView {
    let host = Url::parse(&workunit.url)
        .ok()
        .and_then(|url| url.host_str().map(|value| value.to_ascii_lowercase()))
        .unwrap_or_default();
    let blacklist_match_domain = find_matching_blacklist_domain(&host, blacklist_domains);

    WorkUnitView {
        id: workunit.id,
        url: workunit.url.clone(),
        status: workunit.status.clone(),
        retry_count: workunit.retry_count,
        next_attempt_at: workunit.next_attempt_at.clone(),
        last_attempt_at: workunit.last_attempt_at.clone(),
        last_error: workunit.last_error.clone(),
        created_at: workunit.created_at.clone(),
        is_blacklisted: blacklist_match_domain.is_some(),
        blacklist_match_domain,
    }
}

fn build_list_url(
    base_path: &str,
    limit: i64,
    offset: i64,
    extra_params: &[(&str, &str)],
) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("limit", &limit.to_string());
    serializer.append_pair("offset", &offset.to_string());
    for (key, value) in extra_params {
        serializer.append_pair(key, value);
    }
    format!("{base_path}?{}", serializer.finish())
}

fn build_category_legend_items(
    distribution: &[CategoryDistributionEntry],
    count_label: &str,
) -> Vec<CategoryLegendView> {
    let total_hosts = distribution
        .iter()
        .map(|entry| entry.host_count)
        .sum::<usize>()
        .max(1);
    distribution
        .iter()
        .map(|entry| CategoryLegendView {
            category: entry.category.clone(),
            label: entry.label.clone(),
            color: analytics_chart_color(&entry.category).to_string(),
            host_count: entry.host_count,
            count_label: count_label.to_string(),
            percentage_label: format!(
                "{:.1}%",
                (entry.host_count as f64 / total_hosts as f64) * 100.0
            ),
        })
        .collect()
}

fn analytics_chart_color(series_key: &str) -> &'static str {
    match series_key {
        "search-engine" => "#00ff88",
        "forum" => "#00d4ff",
        "market" => "#ff00ff",
        "directory" => "#d7ff00",
        "wiki" => "#8b5cf6",
        "blog" => "#ff3366",
        "escrow" => "#ffb000",
        "shop" => "#f97316",
        "vendor-page" => "#f472b6",
        "docs" => "#7dd3fc",
        "indexer" => "#66f2ff",
        "content" => "#98a2b3",
        "seo-spam" => "#ef4444",
        _ => fallback_chart_color(series_key),
    }
}

fn fallback_chart_color(series_key: &str) -> &'static str {
    const PALETTE: [&str; 12] = [
        "#00ff88", "#00d4ff", "#ff00ff", "#d7ff00", "#8b5cf6", "#ff3366", "#ffb000", "#f97316",
        "#f472b6", "#7dd3fc", "#66f2ff", "#98a2b3",
    ];

    let hash = series_key.bytes().fold(0_usize, |acc, byte| {
        acc.wrapping_mul(33).wrapping_add(byte as usize)
    });
    PALETTE[hash % PALETTE.len()]
}

fn render_distribution_pie_chart(
    distribution: &[CategoryDistributionEntry],
    center_label: &str,
    count_label: &str,
    aria_label: &str,
    empty_title: &str,
    empty_subtitle: &str,
) -> String {
    if distribution.is_empty() {
        return format!(
            r##"
<svg class="chart-svg" viewBox="0 0 320 320" role="img" aria-label="{aria_label}">
  <circle cx="160" cy="160" r="96" fill="none" stroke="#2a2a3a" stroke-width="46"></circle>
  <text x="160" y="154" text-anchor="middle" font-size="18" font-weight="700" fill="#e0e0e0">{empty_title}</text>
  <text x="160" y="176" text-anchor="middle" font-size="12" fill="#98a2b3">{empty_subtitle}</text>
</svg>
"##,
            aria_label = aria_label,
            empty_title = empty_title,
            empty_subtitle = empty_subtitle,
        )
        .trim()
        .to_string();
    }

    let total_hosts = distribution
        .iter()
        .map(|entry| entry.host_count)
        .sum::<usize>()
        .max(1);
    let radius = 96.0_f64;
    let circumference = 2.0 * std::f64::consts::PI * radius;
    let mut offset = 0.0_f64;
    let mut slices = String::new();

    for entry in distribution {
        let slice_len = circumference * (entry.host_count as f64 / total_hosts as f64);
        let color = analytics_chart_color(&entry.category);
        let percentage = (entry.host_count as f64 / total_hosts as f64) * 100.0;
        slices.push_str(&format!(
            r##"<circle cx="160" cy="160" r="{radius}" fill="none" stroke="{color}" stroke-width="46" stroke-dasharray="{slice_len:.3} {remaining:.3}" stroke-dashoffset="{dashoffset:.3}" transform="rotate(-90 160 160)"><title>{label}: {count} {count_label} ({percentage:.1}%)</title></circle>"##,
            radius = radius,
            color = color,
            slice_len = slice_len,
            remaining = (circumference - slice_len).max(0.0),
            dashoffset = -offset,
            label = entry.label,
            count = entry.host_count,
            count_label = count_label,
            percentage = percentage,
        ));
        offset += slice_len;
    }

    format!(
        r##"
<svg class="chart-svg" viewBox="0 0 320 320" role="img" aria-label="{aria_label}">
  <circle cx="160" cy="160" r="{radius}" fill="none" stroke="#242438" stroke-width="46"></circle>
  {slices}
  <circle cx="160" cy="160" r="62" fill="#0a0a0f" stroke="#00ff88" stroke-width="1.5"></circle>
  <text x="160" y="150" text-anchor="middle" font-size="14" fill="#98a2b3">{center_label}</text>
  <text x="160" y="176" text-anchor="middle" font-size="34" font-weight="700" fill="#e0e0e0">{total_hosts}</text>
</svg>
"##,
        aria_label = aria_label,
        radius = radius,
        slices = slices,
        center_label = center_label,
        total_hosts = total_hosts,
    )
    .trim()
    .to_string()
}

fn render_timeline_histogram(
    distribution: &[CategoryDistributionEntry],
    timeline: &[CategoryTimelinePoint],
    count_label: &str,
    aria_label: &str,
    empty_title: &str,
    empty_subtitle: &str,
) -> String {
    if timeline.is_empty() {
        return format!(
            r##"
<svg class="chart-svg" viewBox="0 0 920 320" role="img" aria-label="{aria_label}">
  <rect x="0" y="0" width="920" height="320" rx="4" fill="#101018"></rect>
  <text x="460" y="150" text-anchor="middle" font-size="18" font-weight="700" fill="#e0e0e0">{empty_title}</text>
  <text x="460" y="176" text-anchor="middle" font-size="12" fill="#98a2b3">{empty_subtitle}</text>
</svg>
"##,
            aria_label = aria_label,
            empty_title = empty_title,
            empty_subtitle = empty_subtitle,
        )
        .trim()
        .to_string();
    }

    let category_order = distribution
        .iter()
        .map(|entry| entry.category.clone())
        .collect::<Vec<_>>();
    let category_labels = distribution
        .iter()
        .map(|entry| (entry.category.clone(), entry.label.clone()))
        .collect::<HashMap<_, _>>();
    let mut by_day = BTreeMap::<String, HashMap<String, usize>>::new();
    for point in timeline {
        by_day
            .entry(point.day.clone())
            .or_default()
            .insert(point.category.clone(), point.host_count);
    }

    let buckets = by_day.into_iter().collect::<Vec<_>>();
    let max_total = buckets
        .iter()
        .map(|(_, counts)| counts.values().sum::<usize>())
        .max()
        .unwrap_or(1)
        .max(1);

    let left = 56.0_f64;
    let right = 16.0_f64;
    let top = 20.0_f64;
    let bottom = 40.0_f64;
    let chart_height = 220.0_f64;
    let bar_width = 18.0_f64;
    let gap = 12.0_f64;
    let plot_width = buckets.len() as f64 * (bar_width + gap);
    let width = (left + right + plot_width).max(920.0);
    let height = top + chart_height + bottom;
    let step = if buckets.len() > 14 {
        ((buckets.len() as f64) / 14.0).ceil() as usize
    } else {
        1
    };

    let mut grid = String::new();
    for tick in 0..=4 {
        let value = ((max_total as f64) * (tick as f64) / 4.0).round() as usize;
        let y = top + chart_height - ((value as f64 / max_total as f64) * chart_height);
        grid.push_str(&format!(
            r##"<line x1="{left:.1}" y1="{y:.1}" x2="{right_x:.1}" y2="{y:.1}" stroke="#2a2a3a" stroke-width="1"></line><text x="{label_x:.1}" y="{label_y:.1}" text-anchor="end" font-size="11" fill="#98a2b3">{value}</text>"##,
            left = left,
            y = y,
            right_x = width - right,
            label_x = left - 8.0,
            label_y = y + 4.0,
            value = value,
        ));
    }

    let mut bars = String::new();
    for (index, (day, counts)) in buckets.iter().enumerate() {
        let x = left + index as f64 * (bar_width + gap);
        let mut y_cursor = top + chart_height;
        let total = counts.values().sum::<usize>();

        for category in &category_order {
            let count = counts.get(category).copied().unwrap_or_default();
            if count == 0 {
                continue;
            }
            let segment_height = (count as f64 / max_total as f64) * chart_height;
            y_cursor -= segment_height;
            let color = analytics_chart_color(category);
            let label = category_labels
                .get(category)
                .cloned()
                .unwrap_or_else(|| category.clone());
            bars.push_str(&format!(
                r##"<rect x="{x:.1}" y="{y:.1}" width="{width:.1}" height="{height:.1}" rx="3" fill="{color}"><title>{day} · {label}: {count} {count_label}</title></rect>"##,
                x = x,
                y = y_cursor,
                width = bar_width,
                height = segment_height.max(1.5),
                color = color,
                day = day,
                label = label,
                count = count,
                count_label = count_label,
            ));
        }

        bars.push_str(&format!(
            r##"<text x="{x:.1}" y="{y:.1}" text-anchor="middle" font-size="10" fill="#98a2b3">{label}</text>"##,
            x = x + (bar_width / 2.0),
            y = top + chart_height - ((total as f64 / max_total as f64) * chart_height) - 6.0,
            label = total,
        ));

        if index % step == 0 || index + 1 == buckets.len() {
            bars.push_str(&format!(
                r##"<text x="{x:.1}" y="{y:.1}" text-anchor="middle" font-size="11" fill="#98a2b3">{label}</text>"##,
                x = x + (bar_width / 2.0),
                y = height - 12.0,
                label = short_day_label(day),
            ));
        }
    }

    format!(
        r##"
<svg class="chart-svg chart-svg-wide" viewBox="0 0 {width:.1} {height:.1}" role="img" aria-label="{aria_label}">
  <rect x="0" y="0" width="{width:.1}" height="{height:.1}" rx="4" fill="#101018"></rect>
  {grid}
  <line x1="{left:.1}" y1="{axis_y:.1}" x2="{axis_right:.1}" y2="{axis_y:.1}" stroke="#00d4ff" stroke-width="1.2"></line>
  {bars}
</svg>
"##,
        aria_label = aria_label,
        width = width,
        height = height,
        grid = grid,
        left = left,
        axis_y = top + chart_height,
        axis_right = width - right,
        bars = bars,
    )
    .trim()
    .to_string()
}

fn short_day_label(value: &str) -> String {
    value
        .get(5..10)
        .map(ToString::to_string)
        .unwrap_or_else(|| value.to_string())
}

#[launch]
fn rocket() -> _ {
    build_rocket()
}

fn build_rocket() -> Rocket<Build> {
    // Initialize structured logging
    spyder::logging::init_tracing();

    let state = build_app_state()
        .unwrap_or_else(|error| panic!("failed to initialize frontend state: {}", error.detail));

    rocket::build()
        .manage(state)
        .attach(RequestTimer)
        .attach(cache_warmer_fairing())
        .attach(Template::fairing())
        .mount(
            "/",
            routes![
                index,
                data,
                pages,
                page_detail,
                page_history,
                page_scan_detail,
                list_work,
                blacklist,
                add_blacklist_auto_rule,
                enable_blacklist_auto_rule,
                disable_blacklist_auto_rule,
                delete_blacklist_auto_rule,
                top,
                analytics,
                sites,
                watchlists,
                add_watchlist,
                delete_watchlist_item,
                leads,
                lead_detail,
                lead_status_form,
                relationships,
                email_entities,
                crypto_entities,
                http_entities,
                service_entities,
                ssh_entities,
                search_page,
                api_stats,
                api_metrics,
                api_search,
                api_relationship_graph,
                api_blacklist,
                api_auto_blacklist,
                api_sites,
                api_page_history,
                api_page_scan_detail,
                api_leads,
                api_lead_detail,
                api_lead_status
            ],
        )
        .mount("/static", FileServer::from(relative!("static")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::connection::SimpleConnection;
    use diesel::Connection;
    use rocket::http::Status;
    use rocket::local::blocking::Client;
    use spyder::models::{
        CategoryHint, ClassificationSignals, CryptoReference, LanguageDetection, LinkObservation,
        NewHostSshObservation, PageSnapshot,
    };
    use spyder::{
        add_forum_keyword_rule, save_host_ssh_observation, save_page_info, SSH_STATUS_SUCCESS,
    };
    use std::env;
    use std::fs;
    use std::process;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn timed_cache_returns_cached_value_within_ttl() {
        let cache = TimedCache::new(Duration::from_secs(30));
        let started_at = Instant::now();

        let first = cache
            .get_or_refresh_at("key", started_at, || Ok::<_, &'static str>(1))
            .expect("initial refresh");
        let second = cache
            .get_or_refresh_at("key", started_at + Duration::from_secs(1), || {
                Ok::<_, &'static str>(2)
            })
            .expect("cached read");

        assert_eq!(first, CacheRead::Refreshed(1));
        assert_eq!(second, CacheRead::Fresh(1));
    }

    #[test]
    fn timed_cache_refreshes_after_ttl() {
        let cache = TimedCache::new(Duration::from_secs(30));
        let started_at = Instant::now();

        cache
            .get_or_refresh_at("key", started_at, || Ok::<_, &'static str>(1))
            .expect("initial refresh");
        let refreshed = cache
            .get_or_refresh_at("key", started_at + Duration::from_secs(31), || {
                Ok::<_, &'static str>(2)
            })
            .expect("expired refresh");

        assert_eq!(refreshed, CacheRead::Refreshed(2));
    }

    #[test]
    fn timed_cache_keeps_stale_value_when_refresh_fails() {
        let cache = TimedCache::new(Duration::from_secs(30));
        let started_at = Instant::now();

        cache
            .get_or_refresh_at("key", started_at, || Ok::<_, &'static str>(1))
            .expect("initial refresh");
        let stale = cache
            .get_or_refresh_at("key", started_at + Duration::from_secs(31), || {
                Err::<i32, _>("refresh failed")
            })
            .expect("stale read");

        assert_eq!(
            stale,
            CacheRead::Stale {
                value: 1,
                error: "refresh failed",
            }
        );
    }

    #[test]
    fn timed_cache_returns_error_without_stale_value() {
        let cache = TimedCache::<i32>::new(Duration::from_secs(30));
        let result =
            cache.get_or_refresh_at("key", Instant::now(), || Err::<i32, _>("refresh failed"));

        assert_eq!(result, Err("refresh failed"));
    }

    fn setup_test_database() -> String {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let database_path =
            env::temp_dir().join(format!("spyder-frontend-{}-{unique}.sqlite", process::id()));
        let database_url = database_path.to_string_lossy().into_owned();

        let mut conn =
            diesel::sqlite::SqliteConnection::establish(&database_url).expect("sqlite file");
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
        add_forum_keyword_rule(&mut conn, "Acme Corp", "acme corp").expect("seed keyword rule");

        let market_snapshot = PageSnapshot {
            title: "Alpha Market".to_string(),
            url: "http://alpha.onion".to_string(),
            language: "English".to_string(),
            language_detection: LanguageDetection::unknown(),
            keyword_corpus: "http://alpha.onion\nAlpha Market\nmarketplace listings".to_string(),
            links: vec![LinkObservation {
                target_url: "http://beta.onion".to_string(),
                target_host: "beta.onion".to_string(),
            }],
            emails: vec!["ops@alpha.onion".to_string()],
            crypto_refs: vec![CryptoReference {
                asset_type: "bitcoin".to_string(),
                reference: "bc1qalpha000000000000000000000000000000000".to_string(),
            }],
            classification_signals: ClassificationSignals {
                word_count: 180,
                hints: vec![CategoryHint {
                    category: "market".to_string(),
                    evidence: "title:market".to_string(),
                    weight: 6,
                }],
                ..ClassificationSignals::default()
            },
            topic_observations: Vec::new(),
        };
        let forum_snapshot = PageSnapshot {
            title: "Beta Forum".to_string(),
            url: "http://beta.onion".to_string(),
            language: "French".to_string(),
            language_detection: LanguageDetection::unknown(),
            keyword_corpus: "http://beta.onion\nBeta Forum\nthread about acme corp".to_string(),
            links: vec![LinkObservation {
                target_url: "http://alpha.onion".to_string(),
                target_host: "alpha.onion".to_string(),
            }],
            emails: vec!["team@shared.test".to_string()],
            crypto_refs: vec![CryptoReference {
                asset_type: "bitcoin".to_string(),
                reference: "bc1qalpha000000000000000000000000000000000".to_string(),
            }],
            classification_signals: ClassificationSignals {
                word_count: 220,
                password_form_count: 1,
                hints: vec![
                    CategoryHint {
                        category: "forum".to_string(),
                        evidence: "title:forum".to_string(),
                        weight: 6,
                    },
                    CategoryHint {
                        category: "forum".to_string(),
                        evidence: "text:thread".to_string(),
                        weight: 4,
                    },
                ],
                ..ClassificationSignals::default()
            },
            topic_observations: Vec::new(),
        };
        let directory_snapshot = PageSnapshot {
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
                    category: "directory".to_string(),
                    evidence: "title:directory".to_string(),
                    weight: 6,
                }],
                ..ClassificationSignals::default()
            },
            topic_observations: Vec::new(),
        };
        save_page_info(&mut conn, &market_snapshot).expect("seed alpha page");
        save_page_info(&mut conn, &forum_snapshot).expect("seed beta page");
        save_page_info(&mut conn, &directory_snapshot).expect("seed gamma page");
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
        save_host_ssh_observation(
            &mut conn,
            &NewHostSshObservation {
                host: "beta.onion".to_string(),
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
        .expect("seed ssh host key");

        database_url
    }

    #[test]
    fn search_page_renders_results_container_and_matching_results() {
        let _guard = TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("test lock");
        let database_url = setup_test_database();
        env::set_var("DATABASE_URL", &database_url);

        let client = Client::tracked(build_rocket()).expect("rocket client");
        let response = client.get("/search?query=forum&limit=5").dispatch();
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().expect("response body");
        assert!(body.contains("data-api-search"));
        assert!(body.contains("data-results-target=\"#search-results\""));
        assert!(body.contains("id=\"search-results\""));
        assert!(body.contains("Beta Forum"));
        assert!(body.contains("search-results-grid"));

        fs::remove_file(&database_url).expect("remove test database");
    }

    #[test]
    fn search_page_supports_keyword_tag_queries() {
        let _guard = TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("test lock");
        let database_url = setup_test_database();
        env::set_var("DATABASE_URL", &database_url);

        let client = Client::tracked(build_rocket()).expect("rocket client");
        let response = client.get("/search?query=keyword:acme&limit=5").dispatch();
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().expect("response body");
        assert!(body.contains("Beta Forum"));
        assert!(body.contains("beta.onion"));
        assert!(body.contains("keyword:cisco"));

        fs::remove_file(&database_url).expect("remove test database");
    }

    #[test]
    fn search_page_renders_pagination_controls() {
        let _guard = TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("test lock");
        let database_url = setup_test_database();
        env::set_var("DATABASE_URL", &database_url);

        let client = Client::tracked(build_rocket()).expect("rocket client");
        let response = client
            .get("/search?query=onion&limit=1&offset=0")
            .dispatch();
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().expect("response body");
        assert!(body.contains("3 matches"));
        assert!(body.contains("href=\"/search?limit=1&amp;offset=1&amp;query=onion\""));
        assert!(!body.contains("href=\"/search?limit=1&amp;offset=-1&amp;query=onion\""));

        fs::remove_file(&database_url).expect("remove test database");
    }

    #[test]
    fn pages_page_renders_pagination_controls() {
        let _guard = TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("test lock");
        let database_url = setup_test_database();
        env::set_var("DATABASE_URL", &database_url);

        let client = Client::tracked(build_rocket()).expect("rocket client");
        let response = client.get("/pages?limit=1&offset=1").dispatch();
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().expect("response body");
        assert!(body.contains("3 records"));
        assert!(body.contains("href=\"/pages?limit=1&amp;offset=0\""));
        assert!(body.contains("href=\"/pages?limit=1&amp;offset=2\""));

        fs::remove_file(&database_url).expect("remove test database");
    }

    #[test]
    fn ssh_page_renders_host_key_entities() {
        let _guard = TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("test lock");
        let database_url = setup_test_database();
        env::set_var("DATABASE_URL", &database_url);

        let client = Client::tracked(build_rocket()).expect("rocket client");
        let response = client
            .get("/entities/ssh?algorithm=ssh-ed25519&fingerprint=sha256%3Afeedbeef")
            .dispatch();
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().expect("response body");
        assert!(body.contains("Shared SSH Host Keys"));
        assert!(body.contains("sha256:feedbeef"));
        assert!(body.contains("beta.onion:22"));

        fs::remove_file(&database_url).expect("remove test database");
    }

    #[test]
    fn top_page_renders_all_leaderboard_sections() {
        let _guard = TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("test lock");
        let database_url = setup_test_database();
        env::set_var("DATABASE_URL", &database_url);

        let client = Client::tracked(build_rocket()).expect("rocket client");
        let response = client.get("/top").dispatch();
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().expect("response body");
        assert!(body.contains("Most Email Refs"));
        assert!(body.contains("Most Crypto Refs"));
        assert!(body.contains("Most Outgoing Links"));
        assert!(body.contains("Most Referenced Sites"));
        assert!(body.contains("gamma.onion"));
        assert!(body.contains("beta.onion"));

        fs::remove_file(&database_url).expect("remove test database");
    }

    #[test]
    fn analytics_page_renders_keyword_breakdown_sections() {
        let _guard = TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("test lock");
        let database_url = setup_test_database();
        env::set_var("DATABASE_URL", &database_url);

        let client = Client::tracked(build_rocket()).expect("rocket client");
        let response = client.get("/analytics").dispatch();
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().expect("response body");
        assert!(body.contains("Current Category Mix"));
        assert!(body.contains("Current Keyword Mix"));
        assert!(body.contains("Keyword Timeline"));
        assert!(body.contains("keyword:acme corp"));

        fs::remove_file(&database_url).expect("remove test database");
    }

    #[test]
    fn sites_page_renders_recent_first_with_last_scan_column() {
        let _guard = TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("test lock");
        let database_url = setup_test_database();
        env::set_var("DATABASE_URL", &database_url);

        let client = Client::tracked(build_rocket()).expect("rocket client");
        let response = client.get("/sites").dispatch();
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().expect("response body");
        assert!(body.contains("<th>Last Scan</th>"));
        assert!(body.contains("keyword:acme corp"));

        let beta_index = body.find("beta.onion").expect("beta host present");
        let alpha_index = body.find("alpha.onion").expect("alpha host present");
        let gamma_index = body.find("gamma.onion").expect("gamma host present");
        assert!(beta_index < alpha_index);
        assert!(alpha_index < gamma_index);

        fs::remove_file(&database_url).expect("remove test database");
    }

    #[test]
    fn page_detail_and_scan_detail_render_forum_keyword_tags() {
        let _guard = TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("test lock");
        let database_url = setup_test_database();
        env::set_var("DATABASE_URL", &database_url);

        let client = Client::tracked(build_rocket()).expect("rocket client");

        let page_response = client.get("/pages/2").dispatch();
        assert_eq!(page_response.status(), Status::Ok);
        let page_body = page_response.into_string().expect("page detail body");
        assert!(page_body.contains("Site Classification"));
        assert!(page_body.contains("keyword:acme corp"));

        let scan_response = client.get("/pages/2/history/2").dispatch();
        assert_eq!(scan_response.status(), Status::Ok);
        let scan_body = scan_response.into_string().expect("scan detail body");
        assert!(scan_body.contains("Site Classification"));
        assert!(scan_body.contains("keyword:acme corp"));

        fs::remove_file(&database_url).expect("remove test database");
    }
}
