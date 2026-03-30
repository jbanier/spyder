use diesel::prelude::*;
use rocket::serde::Serialize;

#[derive(Selectable, Queryable, Serialize)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::page)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Page {
    pub id: i32,
    pub title: String,
    pub url: String,
    pub links: String,
    pub emails: String,
    pub coins: String,
    pub created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::page)]
pub struct NewPage {
    pub title: String,
    pub url: String,
    pub links: String,
    pub emails: String,
    pub coins: String,
}

#[derive(Selectable, Queryable, Serialize)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::work_unit)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct WorkUnit {
    pub id: i32,
    pub url: String,
    pub status: String,
    pub retry_count: i32,
    pub last_error: Option<String>,
    pub created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::work_unit)]
pub struct NewUnit<'a> {
    pub url: &'a str,
    pub status: &'a str,
}

#[derive(Serialize)]
#[serde(crate = "rocket::serde")]
pub struct Stats {
    pub total_pages: i64,
    pub total_domains: usize,
    pub pending_work_units: i64,
    pub failed_work_units: i64,
    pub last_scrape: String,
}

#[derive(Serialize)]
#[serde(crate = "rocket::serde")]
pub struct SearchResult {
    pub title: String,
    pub url: String,
    pub scraped_at: String,
}
