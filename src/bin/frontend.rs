use spyder::establish_connection;

use rocket::fs::{relative, FileServer};
use rocket::{get, launch, routes};
use rocket_dyn_templates::{context, Template};

use spyder::models::*;

#[get("/work")]
fn list_work() -> Template {
    use diesel::query_dsl::QueryDsl;
    use diesel::RunQueryDsl;
    use diesel::SelectableHelper;
    use spyder::schema::work_unit;

    let connection = &mut establish_connection();
    let work_units = work_unit::table
        .select(WorkUnit::as_select())
        .load(connection)
        .expect("Error querying for work");
    return Template::render(
        "index",
        context! { title: "Liste des work units",
        description: "liste des liens a visiter et leur etat.",
        workunits: work_units},
    );
}

fn list_pages() -> Vec<Page> {
    use diesel::query_dsl::QueryDsl;
    use diesel::RunQueryDsl;
    use diesel::SelectableHelper;
    use spyder::schema::page;

    let connection = &mut establish_connection();
    page::table
        .select(Page::as_select())
        .load(connection)
        .expect("Error querying for pages")
}

#[get("/")]
fn index() -> Template {
    let pages = list_pages();
    Template::render(
        "index",
        context! { title: "page principale", description: "Ici la page principale",  pages: pages },
    )
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .attach(Template::fairing())
        .mount("/", routes![index, list_work])
        .mount("/static", FileServer::from(relative!("static")))
}
