use diesel::pg::PgConnection;
use rocket::form::FromForm;
use rocket::fs::{relative, FileServer};
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{self, Responder};
use rocket::serde::{json::Json, Serialize};
use rocket::{get, launch, routes};
use rocket_dyn_templates::{context, Template};
use spyder::models::{
    CategoryDistributionEntry, CategoryTimelinePoint, PaginatedResult, TopSiteSection,
};
use spyder::{
    collect_stats, establish_connection, find_matching_blacklist_domain, get_crypto_entity_detail,
    get_email_entity_detail, get_host_http_observation_detail, get_page_detail,
    get_page_scan_detail, get_ssh_host_key_detail, list_crypto_entities,
    list_domain_blacklist_rules, list_domain_blacklist_summaries, list_email_entities,
    list_host_http_observations, list_page_scan_summaries, list_page_summaries,
    list_site_category_distribution, list_site_category_timeline, list_site_keyword_distribution,
    list_site_keyword_timeline, list_site_profiles, list_site_relationships, list_ssh_host_keys,
    list_top_referenced_sites, list_top_sites_by_crypto_refs, list_top_sites_by_email_refs,
    list_top_sites_by_outgoing_links, list_work_units, search_pages,
};
use std::collections::{BTreeMap, HashMap};
use url::form_urlencoded;
use url::Url;

type HtmlResult = Result<Template, FrontendError>;

#[derive(Debug)]
struct FrontendError {
    status: Status,
    title: &'static str,
    detail: String,
}

impl FrontendError {
    fn internal(context: &'static str, error: anyhow::Error) -> Self {
        let detail = format!("{context}: {error:#}");
        eprintln!("FRONTEND ERROR: {detail}");
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

fn open_connection() -> Result<PgConnection, FrontendError> {
    establish_connection().frontend_context("opening database connection")
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
    percentage_label: String,
}

#[derive(FromForm, Clone)]
struct ListQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(FromForm)]
struct SearchQuery {
    query: Option<String>,
    limit: Option<i64>,
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

fn render_pages(title: &str, description: &str, list_query: Option<ListQuery>) -> HtmlResult {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    let mut connection = open_connection()?;
    let pages = list_page_summaries(&mut connection, list_query.limit, list_query.offset)
        .frontend_context("loading page summaries")?;
    let has_pages = !pages.items.is_empty();
    let pagination = pagination_context("/pages", &pages, &[]);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;

    Ok(Template::render(
        "pages",
        context! {
            title: title,
            description: description,
            pages: pages.items,
            has_pages: has_pages,
            page_count: pages.total_count,
            pagination: pagination,
            has_pagination: has_pagination,
        },
    ))
}

#[get("/")]
fn index() -> HtmlResult {
    let mut connection = open_connection()?;
    let stats = collect_stats(&mut connection).frontend_context("loading dashboard stats")?;

    let pages = list_page_summaries(&mut connection, Some(8), Some(0))
        .frontend_context("loading dashboard pages")?;
    let email_entities = list_email_entities(&mut connection, Some(8), Some(0))
        .frontend_context("loading dashboard email entities")?;
    let crypto_entities = list_crypto_entities(&mut connection, Some(8), Some(0))
        .frontend_context("loading dashboard crypto entities")?;
    let relationships = list_site_relationships(&mut connection, Some(8), Some(0))
        .frontend_context("loading dashboard relationships")?;
    let ssh_host_keys = list_ssh_host_keys(&mut connection, Some(8), Some(0))
        .frontend_context("loading dashboard ssh host keys")?;

    Ok(Template::render(
        "dashboard",
        context! {
            title: "Spyder Dashboard",
            description: "Track scanned pages, shared entities, retries, and site-to-site references across clearnet and Tor targets.",
            stats: stats,
            pages: pages.items,
            email_entities: email_entities.items,
            crypto_entities: crypto_entities.items,
            relationships: relationships.items,
            ssh_host_keys: ssh_host_keys.items,
            has_pages: pages.total_count > 0,
            has_email_entities: email_entities.total_count > 0,
            has_crypto_entities: crypto_entities.total_count > 0,
            has_relationships: relationships.total_count > 0,
            has_ssh_host_keys: ssh_host_keys.total_count > 0,
        },
    ))
}

#[get("/data?<list_query..>")]
fn data(list_query: Option<ListQuery>) -> HtmlResult {
    render_pages(
        "Scanned Pages",
        "Browse indexed pages with scan time, language, and extracted-entity counts.",
        list_query,
    )
}

#[get("/pages?<list_query..>")]
fn pages(list_query: Option<ListQuery>) -> HtmlResult {
    render_pages(
        "Scanned Pages",
        "Browse indexed pages with scan time, language, and extracted-entity counts.",
        list_query,
    )
}

#[get("/pages/<page_id>")]
fn page_detail(page_id: i32) -> HtmlResult {
    let mut connection = open_connection()?;
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
    let outgoing_link_count = page.outgoing_links.len();
    let incoming_link_count = page.incoming_links.len();
    let email_count = page.emails.len();
    let crypto_ref_count = page.crypto_refs.len();
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
            outgoing_link_count: outgoing_link_count,
            incoming_link_count: incoming_link_count,
            email_count: email_count,
            crypto_ref_count: crypto_ref_count,
        },
    ))
}

#[get("/pages/<page_id>/history")]
fn page_history(page_id: i32) -> HtmlResult {
    let mut connection = open_connection()?;
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
fn page_scan_detail(page_id: i32, scan_id: i32) -> HtmlResult {
    let mut connection = open_connection()?;
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
fn list_work(list_query: Option<ListQuery>) -> HtmlResult {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    let mut connection = open_connection()?;
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

    Ok(Template::render(
        "work",
        context! {
            title: "Queue",
            description: "Inspect queued, completed, retried, and terminally failed URLs.",
            workunits: workunit_views,
            has_workunits: has_workunits,
            workunit_count: workunits.total_count,
            pagination: pagination,
            has_pagination: has_pagination,
        },
    ))
}

#[get("/blacklist")]
fn blacklist() -> HtmlResult {
    let mut connection = open_connection()?;
    let entries = list_domain_blacklist_summaries(&mut connection)
        .frontend_context("loading blacklist summaries")?;
    let has_entries = !entries.is_empty();
    let entry_count = entries.len();

    Ok(Template::render(
        "blacklist",
        context! {
            title: "Domain Blacklist",
            description: "Review domains blocked from discovered-link queueing and see how often they appear in stored links.",
            entries: entries,
            has_entries: has_entries,
            entry_count: entry_count,
        },
    ))
}

#[get("/top")]
fn top() -> HtmlResult {
    let mut connection = open_connection()?;
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

    Ok(Template::render(
        "top",
        context! {
            title: "Top Sites",
            description: "Host-level leaderboards for the most active and most referenced sites in the current index.",
            sections: sections,
            has_sections: has_sections,
        },
    ))
}

#[get("/analytics")]
fn analytics() -> HtmlResult {
    let mut connection = open_connection()?;
    let category_distribution = list_site_category_distribution(&mut connection)
        .frontend_context("loading site category distribution")?;
    let category_timeline = list_site_category_timeline(&mut connection)
        .frontend_context("loading site category timeline")?;
    let keyword_distribution = list_site_keyword_distribution(&mut connection)
        .frontend_context("loading site keyword distribution")?;
    let keyword_timeline = list_site_keyword_timeline(&mut connection)
        .frontend_context("loading site keyword timeline")?;
    let total_hosts = category_distribution
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
            value: first_day.clone(),
            label: "First Classified Day".to_string(),
        },
        CategoryMetricView {
            value: last_day.clone(),
            label: "Latest Classified Day".to_string(),
        },
    ];
    let category_legend = build_category_legend_items(&category_distribution);
    let category_pie_svg = render_distribution_pie_chart(
        &category_distribution,
        "Classified",
        "Current site category distribution",
        "No data",
        "Run more scans to classify hosts",
    );
    let category_histogram_svg = render_timeline_histogram(
        &category_distribution,
        &category_timeline,
        "Daily histogram of newly classified hosts by category",
        "No timeline yet",
        "Newly classified hosts will appear here over time",
    );
    let keyword_legend = build_category_legend_items(&keyword_distribution);
    let keyword_pie_svg = render_distribution_pie_chart(
        &keyword_distribution,
        "Keyword Tags",
        "Current site keyword tag distribution",
        "No keyword data",
        "Tagged forum hosts will appear here after keyword matches are found",
    );
    let keyword_histogram_svg = render_timeline_histogram(
        &keyword_distribution,
        &keyword_timeline,
        "Daily histogram of first observed host keyword tags",
        "No keyword timeline yet",
        "Newly tagged forum hosts will appear here over time",
    );
    let has_distribution = !category_distribution.is_empty();
    let has_timeline = !category_timeline.is_empty();
    let has_keyword_distribution = !keyword_distribution.is_empty();
    let has_keyword_timeline = !keyword_timeline.is_empty();

    Ok(Template::render(
        "analytics",
        context! {
            title: "Site Analytics",
            description: "See current category and keyword-tag breakdowns, plus when those classifications and tags first appeared in the index.",
            metrics: metrics,
            category_legend: category_legend,
            category_pie_svg: category_pie_svg,
            category_histogram_svg: category_histogram_svg,
            keyword_legend: keyword_legend,
            keyword_pie_svg: keyword_pie_svg,
            keyword_histogram_svg: keyword_histogram_svg,
            has_distribution: has_distribution,
            has_timeline: has_timeline,
            has_keyword_distribution: has_keyword_distribution,
            has_keyword_timeline: has_keyword_timeline,
            total_hosts: total_hosts,
            first_day: first_day,
            last_day: last_day,
            keyword_first_day: keyword_first_day,
            keyword_last_day: keyword_last_day,
        },
    ))
}

#[get("/sites?<list_query..>")]
fn sites(list_query: Option<ListQuery>) -> HtmlResult {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    let mut connection = open_connection()?;
    let sites = list_site_profiles(&mut connection, list_query.limit, list_query.offset)
        .frontend_context("loading site profiles")?;
    let has_sites = !sites.items.is_empty();
    let pagination = pagination_context("/sites", &sites, &[]);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;

    Ok(Template::render(
        "sites",
        context! {
            title: "Site Profiles",
            description: "Heuristic host categorization derived from crawled page content and structure.",
            sites: sites.items,
            site_count: sites.total_count,
            has_sites: has_sites,
            pagination: pagination,
            has_pagination: has_pagination,
        },
    ))
}

#[get("/relationships?<list_query..>")]
fn relationships(list_query: Option<ListQuery>) -> HtmlResult {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    let mut connection = open_connection()?;
    let relationships =
        list_site_relationships(&mut connection, list_query.limit, list_query.offset)
            .frontend_context("loading site relationships")?;
    let has_relationships = !relationships.items.is_empty();
    let pagination = pagination_context("/relationships", &relationships, &[]);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;

    Ok(Template::render(
        "relationships",
        context! {
            title: "Site Relationships",
            description: "Host-level references observed while scanning pages.",
            relationships: relationships.items,
            relationship_count: relationships.total_count,
            has_relationships: has_relationships,
            pagination: pagination,
            has_pagination: has_pagination,
        },
    ))
}

#[get("/entities/emails?<query..>")]
fn email_entities(query: Option<EmailQuery>) -> HtmlResult {
    let query = query.unwrap_or(EmailQuery {
        value: None,
        limit: None,
        offset: None,
    });
    let mut connection = open_connection()?;
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

    Ok(Template::render(
        "emails",
        context! {
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
        },
    ))
}

#[get("/entities/crypto?<query..>")]
fn crypto_entities(query: Option<CryptoQuery>) -> HtmlResult {
    let query = query.unwrap_or(CryptoQuery {
        asset_type: None,
        reference: None,
        limit: None,
        offset: None,
    });
    let mut connection = open_connection()?;
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

    Ok(Template::render(
        "crypto",
        context! {
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
        },
    ))
}

#[get("/entities/ssh?<query..>")]
fn ssh_entities(query: Option<SshQuery>) -> HtmlResult {
    let query = query.unwrap_or(SshQuery {
        algorithm: None,
        fingerprint: None,
        limit: None,
        offset: None,
    });
    let mut connection = open_connection()?;
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

    Ok(Template::render(
        "ssh",
        context! {
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
        },
    ))
}

#[get("/entities/http?<query..>")]
fn http_entities(query: Option<HttpQuery>) -> HtmlResult {
    let query = query.unwrap_or(HttpQuery {
        host: None,
        scheme: None,
        port: None,
        limit: None,
        offset: None,
    });
    let mut connection = open_connection()?;
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

    Ok(Template::render(
        "http",
        context! {
            title: "HTTP Fingerprints",
            description: "Inspect host-level HTTP headers, favicon hashes, redirect targets, and any captured TLS certificate fingerprints.",
            entities: entities.items,
            selected: selected,
            has_entities: has_entities,
            has_selected: has_selected,
            entity_count: entities.total_count,
            pagination: pagination,
            has_pagination: has_pagination,
        },
    ))
}

#[get("/search?<search..>")]
fn search_page(search: Option<SearchQuery>) -> HtmlResult {
    let search = search.unwrap_or(SearchQuery {
        query: None,
        limit: None,
    });
    let query = search.query.unwrap_or_default();
    let limit = search.limit.unwrap_or(20).clamp(1, 50);

    let mut connection = open_connection()?;
    let results = if query.trim().is_empty() {
        Vec::new()
    } else {
        search_pages(&mut connection, &query, Some(limit)).frontend_context("searching pages")?
    };
    let has_query = !query.trim().is_empty();
    let has_results = !results.is_empty();
    let result_count = results.len();

    Ok(Template::render(
        "search",
        context! {
            title: "Search",
            description: "Search titles, URLs, languages, emails, crypto references, and keyword-tagged sites.",
            query: query.trim(),
            limit: limit,
            results: results,
            has_query: has_query,
            has_results: has_results,
            result_count: result_count,
        },
    ))
}

#[get("/api/stats")]
fn api_stats() -> Result<Json<ApiResponse<spyder::models::Stats>>, Status> {
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let stats = collect_stats(&mut connection).map_err(|_| Status::InternalServerError)?;

    Ok(Json(ApiResponse {
        success: true,
        data: stats,
    }))
}

#[get("/api/search?<search..>")]
fn api_search(
    search: Option<SearchQuery>,
) -> Result<Json<ApiResponse<Vec<spyder::models::SearchResult>>>, Status> {
    let search = search.unwrap_or(SearchQuery {
        query: None,
        limit: None,
    });
    let query = search.query.unwrap_or_default();
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let results = search_pages(&mut connection, &query, search.limit)
        .map_err(|_| Status::InternalServerError)?;

    Ok(Json(ApiResponse {
        success: true,
        data: results,
    }))
}

#[get("/api/blacklist")]
fn api_blacklist() -> Result<Json<ApiResponse<Vec<spyder::models::DomainBlacklistSummary>>>, Status>
{
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let entries = list_domain_blacklist_summaries(&mut connection)
        .map_err(|_| Status::InternalServerError)?;

    Ok(Json(ApiResponse {
        success: true,
        data: entries,
    }))
}

#[get("/api/sites?<list_query..>")]
fn api_sites(
    list_query: Option<ListQuery>,
) -> Result<Json<ApiResponse<PaginatedResult<spyder::models::SiteProfileSummary>>>, Status> {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let sites = list_site_profiles(&mut connection, list_query.limit, list_query.offset)
        .map_err(|_| Status::InternalServerError)?;

    Ok(Json(ApiResponse {
        success: true,
        data: sites,
    }))
}

#[get("/api/pages/<page_id>/history")]
fn api_page_history(
    page_id: i32,
) -> Result<Json<ApiResponse<Vec<spyder::models::PageScanSummary>>>, Status> {
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
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
    page_id: i32,
    scan_id: i32,
) -> Result<Json<ApiResponse<spyder::models::PageScanDetail>>, Status> {
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
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
            percentage_label: format!(
                "{:.1}%",
                (entry.host_count as f64 / total_hosts as f64) * 100.0
            ),
        })
        .collect()
}

fn analytics_chart_color(series_key: &str) -> &'static str {
    match series_key {
        "search-engine" => "#1f6c5c",
        "forum" => "#0f5b73",
        "market" => "#a35d10",
        "directory" => "#3f6c1f",
        "wiki" => "#4c5f9c",
        "blog" => "#b24f6b",
        "escrow" => "#7f4db8",
        "shop" => "#b2492c",
        "vendor-page" => "#8b5e34",
        "docs" => "#366b87",
        "indexer" => "#6e4d34",
        "content" => "#677a74",
        _ => fallback_chart_color(series_key),
    }
}

fn fallback_chart_color(series_key: &str) -> &'static str {
    const PALETTE: [&str; 12] = [
        "#1f6c5c", "#0f5b73", "#a35d10", "#3f6c1f", "#4c5f9c", "#b24f6b", "#7f4db8", "#b2492c",
        "#8b5e34", "#366b87", "#6e4d34", "#677a74",
    ];

    let hash = series_key.bytes().fold(0_usize, |acc, byte| {
        acc.wrapping_mul(33).wrapping_add(byte as usize)
    });
    PALETTE[hash % PALETTE.len()]
}

fn render_distribution_pie_chart(
    distribution: &[CategoryDistributionEntry],
    center_label: &str,
    aria_label: &str,
    empty_title: &str,
    empty_subtitle: &str,
) -> String {
    if distribution.is_empty() {
        return format!(
            r##"
<svg class="chart-svg" viewBox="0 0 320 320" role="img" aria-label="{aria_label}">
  <circle cx="160" cy="160" r="96" fill="none" stroke="#d8e0d2" stroke-width="46"></circle>
  <text x="160" y="154" text-anchor="middle" font-size="18" font-weight="700" fill="#5c6c68">{empty_title}</text>
  <text x="160" y="176" text-anchor="middle" font-size="12" fill="#5c6c68">{empty_subtitle}</text>
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
            r##"<circle cx="160" cy="160" r="{radius}" fill="none" stroke="{color}" stroke-width="46" stroke-dasharray="{slice_len:.3} {remaining:.3}" stroke-dashoffset="{dashoffset:.3}" transform="rotate(-90 160 160)"><title>{label}: {count} hosts ({percentage:.1}%)</title></circle>"##,
            radius = radius,
            color = color,
            slice_len = slice_len,
            remaining = (circumference - slice_len).max(0.0),
            dashoffset = -offset,
            label = entry.label,
            count = entry.host_count,
            percentage = percentage,
        ));
        offset += slice_len;
    }

    format!(
        r##"
<svg class="chart-svg" viewBox="0 0 320 320" role="img" aria-label="{aria_label}">
  <circle cx="160" cy="160" r="{radius}" fill="none" stroke="#e8ece2" stroke-width="46"></circle>
  {slices}
  <circle cx="160" cy="160" r="62" fill="#fbfcf8" stroke="#d8e0d2" stroke-width="1.5"></circle>
  <text x="160" y="150" text-anchor="middle" font-size="14" fill="#5c6c68">{center_label}</text>
  <text x="160" y="176" text-anchor="middle" font-size="34" font-weight="700" fill="#17211f">{total_hosts}</text>
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
    aria_label: &str,
    empty_title: &str,
    empty_subtitle: &str,
) -> String {
    if timeline.is_empty() {
        return format!(
            r##"
<svg class="chart-svg" viewBox="0 0 920 320" role="img" aria-label="{aria_label}">
  <rect x="0" y="0" width="920" height="320" rx="18" fill="#fbfcf8"></rect>
  <text x="460" y="150" text-anchor="middle" font-size="18" font-weight="700" fill="#5c6c68">{empty_title}</text>
  <text x="460" y="176" text-anchor="middle" font-size="12" fill="#5c6c68">{empty_subtitle}</text>
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
            r##"<line x1="{left:.1}" y1="{y:.1}" x2="{right_x:.1}" y2="{y:.1}" stroke="#d8e0d2" stroke-width="1"></line><text x="{label_x:.1}" y="{label_y:.1}" text-anchor="end" font-size="11" fill="#5c6c68">{value}</text>"##,
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
                r##"<rect x="{x:.1}" y="{y:.1}" width="{width:.1}" height="{height:.1}" rx="3" fill="{color}"><title>{day} · {label}: {count} hosts</title></rect>"##,
                x = x,
                y = y_cursor,
                width = bar_width,
                height = segment_height.max(1.5),
                color = color,
                day = day,
                label = label,
                count = count,
            ));
        }

        bars.push_str(&format!(
            r##"<text x="{x:.1}" y="{y:.1}" text-anchor="middle" font-size="10" fill="#5c6c68">{label}</text>"##,
            x = x + (bar_width / 2.0),
            y = top + chart_height - ((total as f64 / max_total as f64) * chart_height) - 6.0,
            label = total,
        ));

        if index % step == 0 || index + 1 == buckets.len() {
            bars.push_str(&format!(
                r##"<text x="{x:.1}" y="{y:.1}" text-anchor="middle" font-size="11" fill="#5c6c68">{label}</text>"##,
                x = x + (bar_width / 2.0),
                y = height - 12.0,
                label = short_day_label(day),
            ));
        }
    }

    format!(
        r##"
<svg class="chart-svg chart-svg-wide" viewBox="0 0 {width:.1} {height:.1}" role="img" aria-label="{aria_label}">
  <rect x="0" y="0" width="{width:.1}" height="{height:.1}" rx="18" fill="#fbfcf8"></rect>
  {grid}
  <line x1="{left:.1}" y1="{axis_y:.1}" x2="{axis_right:.1}" y2="{axis_y:.1}" stroke="#9fb0aa" stroke-width="1.2"></line>
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

fn build_rocket() -> rocket::Rocket<rocket::Build> {
    rocket::build()
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
                top,
                analytics,
                sites,
                relationships,
                email_entities,
                crypto_entities,
                http_entities,
                ssh_entities,
                search_page,
                api_stats,
                api_search,
                api_blacklist,
                api_sites,
                api_page_history,
                api_page_scan_detail
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
        CategoryHint, ClassificationSignals, CryptoReference, LinkObservation,
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
        };
        let forum_snapshot = PageSnapshot {
            title: "Beta Forum".to_string(),
            url: "http://beta.onion".to_string(),
            language: "French".to_string(),
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
        };
        let directory_snapshot = PageSnapshot {
            title: "Gamma Directory".to_string(),
            url: "http://gamma.onion".to_string(),
            language: "German".to_string(),
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
        };
        save_page_info(&mut conn, &market_snapshot).expect("seed alpha page");
        save_page_info(&mut conn, &forum_snapshot).expect("seed beta page");
        save_page_info(&mut conn, &directory_snapshot).expect("seed gamma page");
        conn.batch_execute(
            "
            UPDATE page SET last_scanned_at = '2026-05-02 08:00:00' WHERE url = 'http://alpha.onion';
            UPDATE page SET last_scanned_at = '2026-05-03 09:00:00' WHERE url = 'http://beta.onion';
            UPDATE page SET last_scanned_at = '2026-05-01 07:00:00' WHERE url = 'http://gamma.onion';
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
