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

    diesel::insert_into(crate::schema::WorkUnit::table)
        .values(work_unit)
        .returning(WorkUnit::as_returning())
        .get_result(conn)
        .expect("Error saving new post")
}
