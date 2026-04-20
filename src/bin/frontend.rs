use rocket::form::FromForm;
use rocket::fs::{relative, FileServer};
use rocket::http::Status;
use rocket::serde::{json::Json, Serialize};
use rocket::{get, launch, routes};
use rocket_dyn_templates::{context, Template};
use spyder::models::PaginatedResult;
use spyder::{
    collect_stats, establish_connection, get_crypto_entity_detail, get_email_entity_detail,
    get_page_detail, list_crypto_entities, list_email_entities, list_page_summaries,
    list_site_relationships, list_work_units, search_pages,
};
use url::form_urlencoded;

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

    Ok(Template::render(
        "page_detail",
        context! {
            title: page.title.clone(),
            description: "Page detail with outbound links, inbound references, emails, wallets, and scan metadata.",
            page: page,
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

#[get("/work?<list_query..>")]
fn list_work(list_query: Option<ListQuery>) -> Result<Template, Status> {
    let list_query = list_query.unwrap_or(ListQuery {
        limit: None,
        offset: None,
    });
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let workunits = list_work_units(&mut connection, list_query.limit, list_query.offset)
        .map_err(|_| Status::InternalServerError)?;
    let has_workunits = !workunits.items.is_empty();
    let pagination = pagination_context("/work", &workunits, &[]);
    let has_pagination = pagination.has_previous_page || pagination.has_next_page;

    Ok(Template::render(
        "work",
        context! {
            title: "Queue",
            description: "Inspect queued, completed, retried, and terminally failed URLs.",
            workunits: workunits.items,
            has_workunits: has_workunits,
            workunit_count: workunits.total_count,
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
    rocket::build()
        .attach(Template::fairing())
        .mount(
            "/",
            routes![
                index,
                data,
                pages,
                page_detail,
                list_work,
                relationships,
                email_entities,
                crypto_entities,
                search_page,
                api_stats,
                api_search
            ],
        )
        .mount("/static", FileServer::from(relative!("static")))
}
