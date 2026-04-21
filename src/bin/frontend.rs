use rocket::form::FromForm;
use rocket::fs::{relative, FileServer};
use rocket::http::Status;
use rocket::serde::{json::Json, Serialize};
use rocket::{get, launch, routes};
use rocket_dyn_templates::{context, Template};
use spyder::models::PaginatedResult;
use spyder::{
    collect_stats, establish_connection, find_matching_blacklist_domain, get_crypto_entity_detail,
    get_email_entity_detail, get_page_detail, get_page_scan_detail, list_crypto_entities,
    list_domain_blacklist_rules, list_domain_blacklist_summaries, list_email_entities,
    list_page_scan_summaries, list_page_summaries, list_site_profiles, list_site_relationships,
    list_work_units, search_pages,
};
use url::form_urlencoded;
use url::Url;

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

fn render_pages(
    title: &str,
    description: &str,
    list_query: Option<ListQuery>,
) -> Result<Template, Status> {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let pages = list_page_summaries(&mut connection, list_query.limit, list_query.offset)
        .map_err(|_| Status::InternalServerError)?;
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
fn index() -> Result<Template, Status> {
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let stats = collect_stats(&mut connection).map_err(|_| Status::InternalServerError)?;

    let pages = list_page_summaries(&mut connection, Some(8), Some(0))
        .map_err(|_| Status::InternalServerError)?;
    let email_entities = list_email_entities(&mut connection, Some(8), Some(0))
        .map_err(|_| Status::InternalServerError)?;
    let crypto_entities = list_crypto_entities(&mut connection, Some(8), Some(0))
        .map_err(|_| Status::InternalServerError)?;
    let relationships = list_site_relationships(&mut connection, Some(8), Some(0))
        .map_err(|_| Status::InternalServerError)?;

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
            has_pages: pages.total_count > 0,
            has_email_entities: email_entities.total_count > 0,
            has_crypto_entities: crypto_entities.total_count > 0,
            has_relationships: relationships.total_count > 0,
        },
    ))
}

#[get("/data?<list_query..>")]
fn data(list_query: Option<ListQuery>) -> Result<Template, Status> {
    render_pages(
        "Scanned Pages",
        "Browse indexed pages with scan time, language, and extracted-entity counts.",
        list_query,
    )
}

#[get("/pages?<list_query..>")]
fn pages(list_query: Option<ListQuery>) -> Result<Template, Status> {
    render_pages(
        "Scanned Pages",
        "Browse indexed pages with scan time, language, and extracted-entity counts.",
        list_query,
    )
}

#[get("/pages/<page_id>")]
fn page_detail(page_id: i32) -> Result<Template, Status> {
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let page =
        get_page_detail(&mut connection, page_id).map_err(|_| Status::InternalServerError)?;
    let Some(page) = page else {
        return Err(Status::NotFound);
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
        .map_err(|_| Status::InternalServerError)?;
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
fn page_history(page_id: i32) -> Result<Template, Status> {
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let page =
        get_page_detail(&mut connection, page_id).map_err(|_| Status::InternalServerError)?;
    let Some(page) = page else {
        return Err(Status::NotFound);
    };
    let scans = list_page_scan_summaries(&mut connection, page_id)
        .map_err(|_| Status::InternalServerError)?;
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
fn page_scan_detail(page_id: i32, scan_id: i32) -> Result<Template, Status> {
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let detail = get_page_scan_detail(&mut connection, page_id, scan_id)
        .map_err(|_| Status::InternalServerError)?;
    let Some(detail) = detail else {
        return Err(Status::NotFound);
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
fn list_work(list_query: Option<ListQuery>) -> Result<Template, Status> {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let workunits = list_work_units(&mut connection, list_query.limit, list_query.offset)
        .map_err(|_| Status::InternalServerError)?;
    let blacklist_domains = list_domain_blacklist_rules(&mut connection)
        .map_err(|_| Status::InternalServerError)?
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
fn blacklist() -> Result<Template, Status> {
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let entries = list_domain_blacklist_summaries(&mut connection)
        .map_err(|_| Status::InternalServerError)?;
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

#[get("/sites?<list_query..>")]
fn sites(list_query: Option<ListQuery>) -> Result<Template, Status> {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let sites = list_site_profiles(&mut connection, list_query.limit, list_query.offset)
        .map_err(|_| Status::InternalServerError)?;
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
fn relationships(list_query: Option<ListQuery>) -> Result<Template, Status> {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let relationships =
        list_site_relationships(&mut connection, list_query.limit, list_query.offset)
            .map_err(|_| Status::InternalServerError)?;
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
fn email_entities(query: Option<EmailQuery>) -> Result<Template, Status> {
    let query = query.unwrap_or(EmailQuery {
        value: None,
        limit: None,
        offset: None,
    });
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let entities = list_email_entities(&mut connection, query.limit, query.offset)
        .map_err(|_| Status::InternalServerError)?;
    let selected = match query.value.clone() {
        Some(value) => get_email_entity_detail(&mut connection, &value)
            .map_err(|_| Status::InternalServerError)?,
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
fn crypto_entities(query: Option<CryptoQuery>) -> Result<Template, Status> {
    let query = query.unwrap_or(CryptoQuery {
        asset_type: None,
        reference: None,
        limit: None,
        offset: None,
    });
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let entities = list_crypto_entities(&mut connection, query.limit, query.offset)
        .map_err(|_| Status::InternalServerError)?;
    let selected = match (query.asset_type.clone(), query.reference.clone()) {
        (Some(asset_type), Some(reference)) => {
            get_crypto_entity_detail(&mut connection, &asset_type, &reference)
                .map_err(|_| Status::InternalServerError)?
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

#[get("/search?<search..>")]
fn search_page(search: Option<SearchQuery>) -> Result<Template, Status> {
    let search = search.unwrap_or(SearchQuery {
        query: None,
        limit: None,
    });
    let query = search.query.unwrap_or_default();
    let limit = search.limit.unwrap_or(20).clamp(1, 50);

    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let results = if query.trim().is_empty() {
        Vec::new()
    } else {
        search_pages(&mut connection, &query, Some(limit))
            .map_err(|_| Status::InternalServerError)?
    };
    let has_query = !query.trim().is_empty();
    let has_results = !results.is_empty();
    let result_count = results.len();

    Ok(Template::render(
        "search",
        context! {
            title: "Search",
            description: "Search titles, URLs, languages, emails, and crypto references.",
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
                sites,
                relationships,
                email_entities,
                crypto_entities,
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
        CategoryHint, ClassificationSignals, CryptoReference, LinkObservation, PageSnapshot,
    };
    use spyder::save_page_info;
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

        let forum_snapshot = PageSnapshot {
            title: "Beta Forum".to_string(),
            url: "http://beta.onion".to_string(),
            language: "French".to_string(),
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
        save_page_info(&mut conn, &forum_snapshot).expect("seed page");

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
}
