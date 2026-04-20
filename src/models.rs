use diesel::prelude::*;
use rocket::serde::Serialize;

#[derive(Selectable, Queryable, Serialize, Clone)]
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
    pub language: String,
    pub last_scanned_at: String,
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
    pub language: String,
}

#[derive(Selectable, Queryable, Clone)]
#[diesel(table_name = crate::schema::page_link)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PageLink {
    pub id: i32,
    pub source_page_id: i32,
    pub target_url: String,
    pub target_host: String,
    pub created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::page_link)]
pub struct NewPageLink {
    pub source_page_id: i32,
    pub target_url: String,
    pub target_host: String,
}

#[derive(Selectable, Queryable, Clone)]
#[diesel(table_name = crate::schema::page_email)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PageEmail {
    pub id: i32,
    pub page_id: i32,
    pub email: String,
    pub created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::page_email)]
pub struct NewPageEmail {
    pub page_id: i32,
    pub email: String,
}

#[derive(Selectable, Queryable, Clone)]
#[diesel(table_name = crate::schema::page_crypto)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PageCrypto {
    pub id: i32,
    pub page_id: i32,
    pub asset_type: String,
    pub reference: String,
    pub created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::page_crypto)]
pub struct NewPageCrypto {
    pub page_id: i32,
    pub asset_type: String,
    pub reference: String,
}

#[derive(Selectable, Queryable, Serialize, Clone)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::work_unit)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct WorkUnit {
    pub id: i32,
    pub url: String,
    pub status: String,
    pub retry_count: i32,
    pub next_attempt_at: String,
    pub last_attempt_at: Option<String>,
    pub last_error: Option<String>,
    pub created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::work_unit)]
pub struct NewUnit<'a> {
    pub url: &'a str,
    pub status: &'a str,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq, Hash)]
#[serde(crate = "rocket::serde")]
pub struct CryptoReference {
    pub asset_type: String,
    pub reference: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct LinkObservation {
    pub target_url: String,
    pub target_host: String,
}

#[derive(Clone, Debug)]
pub struct PageSnapshot {
    pub title: String,
    pub url: String,
    pub language: String,
    pub links: Vec<LinkObservation>,
    pub emails: Vec<String>,
    pub crypto_refs: Vec<CryptoReference>,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "rocket::serde")]
pub struct EmailObservation {
    pub value: String,
    pub detail_url: String,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "rocket::serde")]
pub struct CryptoObservation {
    pub asset_type: String,
    pub reference: String,
    pub detail_url: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct Stats {
    pub total_pages: i64,
    pub total_domains: i64,
    pub pending_work_units: i64,
    pub failed_work_units: i64,
    pub last_scrape: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct SearchResult {
    pub page_id: i32,
    pub title: String,
    pub url: String,
    pub language: String,
    pub scraped_at: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct PageSummary {
    pub id: i32,
    pub title: String,
    pub url: String,
    pub host: String,
    pub language: String,
    pub last_scanned_at: String,
    pub outbound_link_count: usize,
    pub email_count: usize,
    pub crypto_count: usize,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct PageReference {
    pub id: i32,
    pub title: String,
    pub url: String,
    pub language: String,
    pub last_scanned_at: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct LinkReference {
    pub target_url: String,
    pub target_host: String,
    pub target_page_id: Option<i32>,
    pub target_page_title: Option<String>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct IncomingReference {
    pub source_page_id: i32,
    pub source_title: String,
    pub source_url: String,
    pub source_host: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct PageDetail {
    pub id: i32,
    pub title: String,
    pub url: String,
    pub host: String,
    pub language: String,
    pub created_at: String,
    pub last_scanned_at: String,
    pub outgoing_links: Vec<LinkReference>,
    pub incoming_links: Vec<IncomingReference>,
    pub emails: Vec<EmailObservation>,
    pub crypto_refs: Vec<CryptoObservation>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct EmailEntitySummary {
    pub value: String,
    pub page_count: usize,
    pub detail_url: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct EmailEntityDetail {
    pub value: String,
    pub pages: Vec<PageReference>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct CryptoEntitySummary {
    pub asset_type: String,
    pub reference: String,
    pub page_count: usize,
    pub detail_url: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct CryptoEntityDetail {
    pub asset_type: String,
    pub reference: String,
    pub pages: Vec<PageReference>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct SiteRelationship {
    pub source_host: String,
    pub target_host: String,
    pub reference_count: usize,
}

#[derive(Clone, Debug)]
pub struct PaginatedResult<T> {
    pub items: Vec<T>,
    pub total_count: i64,
    pub limit: i64,
    pub offset: i64,
}
