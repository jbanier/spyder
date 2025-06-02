use spyder::{establish_connection};

use rocket::{get, routes, launch};
use rocket_dyn_templates::{Template, context};
use rocket::fs::{FileServer, relative};

use spyder::models::*;


#[get("/work")]
fn list_work() -> Template {
    use diesel::query_dsl::QueryDsl;
    use diesel::RunQueryDsl;
    use diesel::SelectableHelper;
    use spyder::schema::work_unit;

    let mut html_table = Vec::new();

    let connection = &mut establish_connection();
    let work_units = work_unit::table
        .select(WorkUnit::as_select())
        .load(connection)
        .expect("Error querying for work");
    for w in work_units {
        html_table.push(w.url.clone());
    }
    return Template::render("index", context! { title: "Liste des work units", description: "liste des liens a visiter et leur etat."})
}

#[get("/")]
fn index() -> Template {
    Template::render("index", context! { title: "page principale", description: "Ici la page principale" })
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .attach(Template::fairing())
        .mount("/", routes![index, list_work])
        .mount("/static", FileServer::from(relative!("static")))
}
