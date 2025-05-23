pub mod models;
pub mod schema;

use diesel::prelude::*;
use dotenvy::dotenv;
use models::*;
use std::env;

pub fn establish_connection() -> SqliteConnection {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    SqliteConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url))
}

pub fn create_work_unit(conn: &mut SqliteConnection, url: &String) -> WorkUnit {
    let work_unit = NewUnit { url };

    diesel::insert_into(crate::schema::work_unit::table)
        .values(work_unit)
        .returning(WorkUnit::as_returning())
        .get_result(conn)
        .expect("Error saving new post")
}

pub fn save_page_info(conn: &mut SqliteConnection, p: &Page) -> Page {
    diesel::insert_into(crate::schema::page::table)
        .values(p)
        .returning(Page::as_returning())
        .get_result(conn)
        .expect("Error saving page")
}

pub fn mark_work_unit_as_processed(conn: &mut SqliteConnection, wu: &WorkUnit) -> WorkUnit {
    use crate::schema::work_unit::dsl::*;
    use diesel::query_dsl::QueryDsl;
    use diesel::ExpressionMethods;
    use diesel::RunQueryDsl;

    diesel::update(crate::schema::work_unit::table.filter(id.eq(wu.id)))
        .set(processed.eq(true))
        .get_result(conn)
        .expect("Error updating work unit")
}
