use rocket::form::FromForm;
use rocket::fs::{relative, FileServer};
use rocket::http::Status;
use rocket::serde::{json::Json, Serialize};
use rocket::{get, launch, routes};
use rocket_dyn_templates::{context, Template};
use spyder::{collect_stats, establish_connection, list_pages, list_work_units, search_pages};

#[derive(Serialize)]
#[serde(crate = "rocket::serde")]
struct ApiResponse<T> {
    success: bool,
    data: T,
}

#[derive(FromForm)]
struct SearchQuery {
    query: Option<String>,
    limit: Option<i64>,
}

fn render_dashboard(
    title: &str,
    description: &str,
    include_pages: bool,
) -> Result<Template, Status> {
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let pages = if include_pages {
        list_pages(&mut connection).map_err(|_| Status::InternalServerError)?
    } else {
        Vec::new()
    };
    let workunits = list_work_units(&mut connection).map_err(|_| Status::InternalServerError)?;

    Ok(Template::render(
        "index",
        context! {
            title: title,
            description: description,
            pages: pages,
            workunits: workunits,
        },
    ))
}

fn render_search_page(query: &str, limit: Option<i64>) -> Result<Template, Status> {
    let trimmed = query.trim();
    let mut connection = establish_connection().map_err(|_| Status::InternalServerError)?;
    let results = if trimmed.is_empty() {
        Vec::new()
    } else {
        search_pages(&mut connection, trimmed, limit).map_err(|_| Status::InternalServerError)?
    };
    let has_results = !results.is_empty();

    Ok(Template::render(
        "search",
        context! {
            title: "Recherche",
            description: "Recherche simple dans les pages indexées.",
            query: trimmed,
            results: results,
            has_results: has_results,
            has_query: !trimmed.is_empty(),
        },
    ))
}

#[get("/")]
fn index() -> Result<Template, Status> {
    render_dashboard("page principale", "Ici la page principale", true)
}

#[get("/data")]
fn data() -> Result<Template, Status> {
    render_dashboard("Données", "Pages analysées et éléments extraits.", true)
}

#[get("/work")]
fn list_work() -> Result<Template, Status> {
    render_dashboard(
        "Liste des work units",
        "Liste des liens à visiter et leur état.",
        false,
    )
}

#[get("/search?<search..>")]
fn search_page(search: Option<SearchQuery>) -> Result<Template, Status> {
    let search = search.unwrap_or(SearchQuery {
        query: None,
        limit: None,
    });

    render_search_page(&search.query.unwrap_or_default(), search.limit)
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

#[launch]
fn rocket() -> _ {
    rocket::build()
        .attach(Template::fairing())
        .mount(
            "/",
            routes![index, data, list_work, search_page, api_stats, api_search],
        )
        .mount("/static", FileServer::from(relative!("static")))
}
