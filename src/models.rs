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

#[derive(Selectable, Queryable, Serialize, Clone)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::domain_blacklist)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DomainBlacklistRule {
    pub id: i32,
    pub domain: String,
    pub created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::domain_blacklist)]
pub struct NewDomainBlacklist<'a> {
    pub domain: &'a str,
}

#[derive(Selectable, Queryable, Clone)]
#[diesel(table_name = crate::schema::page_classification)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PageClassificationRecord {
    pub id: i32,
    pub page_id: i32,
    pub host: String,
    pub category: String,
    pub confidence: String,
    pub score: i32,
    pub evidence: String,
    pub last_classified_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::page_classification)]
pub struct NewPageClassification {
    pub page_id: i32,
    pub host: String,
    pub category: String,
    pub confidence: String,
    pub score: i32,
    pub evidence: String,
}

#[derive(Selectable, Queryable, Serialize, Clone)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::site_profile)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct SiteProfileRecord {
    pub id: i32,
    pub host: String,
    pub category: String,
    pub confidence: String,
    pub score: i32,
    pub page_count: i32,
    pub evidence: String,
    pub source_page_id: Option<i32>,
    pub last_classified_at: String,
    pub created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::site_profile)]
pub struct NewSiteProfile {
    pub host: String,
    pub category: String,
    pub confidence: String,
    pub score: i32,
    pub page_count: i32,
    pub evidence: String,
    pub source_page_id: Option<i32>,
}

#[derive(Selectable, Queryable, Serialize, Clone)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::page_scan)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PageScan {
    pub id: i32,
    pub page_id: i32,
    pub title: String,
    pub language: String,
    pub scanned_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::page_scan)]
pub struct NewPageScan {
    pub page_id: i32,
    pub title: String,
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
#[diesel(table_name = crate::schema::page_scan_link)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PageScanLink {
    pub id: i32,
    pub scan_id: i32,
    pub target_url: String,
    pub target_host: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::page_scan_link)]
pub struct NewPageScanLink {
    pub scan_id: i32,
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
#[diesel(table_name = crate::schema::page_scan_email)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PageScanEmail {
    pub id: i32,
    pub scan_id: i32,
    pub email: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::page_scan_email)]
pub struct NewPageScanEmail {
    pub scan_id: i32,
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

#[derive(Selectable, Queryable, Clone)]
#[diesel(table_name = crate::schema::page_scan_crypto)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PageScanCrypto {
    pub id: i32,
    pub scan_id: i32,
    pub asset_type: String,
    pub reference: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::page_scan_crypto)]
pub struct NewPageScanCrypto {
    pub scan_id: i32,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CategoryHint {
    pub category: String,
    pub evidence: String,
    pub weight: i32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClassificationSignals {
    pub word_count: usize,
    pub total_form_count: usize,
    pub search_form_count: usize,
    pub password_form_count: usize,
    pub hints: Vec<CategoryHint>,
}

#[derive(Clone, Debug)]
pub struct PageSnapshot {
    pub title: String,
    pub url: String,
    pub language: String,
    pub links: Vec<LinkObservation>,
    pub emails: Vec<String>,
    pub crypto_refs: Vec<CryptoReference>,
    pub classification_signals: ClassificationSignals,
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
    pub host: String,
    pub language: String,
    pub scraped_at: String,
    pub site_category: Option<SiteCategoryBadge>,
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
    pub site_category: Option<SiteCategoryBadge>,
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
    pub is_blacklisted: bool,
    pub blacklist_match_domain: Option<String>,
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
    pub site_profile: Option<SiteProfileSummary>,
}

#[derive(Serialize, Clone, Debug, Default, Eq, PartialEq)]
#[serde(crate = "rocket::serde")]
pub struct PageScanChangeSummary {
    pub added_links: usize,
    pub removed_links: usize,
    pub added_emails: usize,
    pub removed_emails: usize,
    pub added_crypto_refs: usize,
    pub removed_crypto_refs: usize,
    pub title_changed: bool,
    pub language_changed: bool,
    pub has_changes: bool,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct PageScanSummary {
    pub id: i32,
    pub page_id: i32,
    pub title: String,
    pub language: String,
    pub scanned_at: String,
    pub outbound_link_count: usize,
    pub email_count: usize,
    pub crypto_count: usize,
    pub change_summary: Option<PageScanChangeSummary>,
    pub detail_url: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct PageScanDiff {
    pub has_previous_scan: bool,
    pub previous_scan_id: Option<i32>,
    pub previous_scanned_at: Option<String>,
    pub title_before: Option<String>,
    pub title_after: String,
    pub language_before: Option<String>,
    pub language_after: String,
    pub change_summary: PageScanChangeSummary,
    pub added_links: Vec<LinkReference>,
    pub removed_links: Vec<LinkReference>,
    pub added_emails: Vec<EmailObservation>,
    pub removed_emails: Vec<EmailObservation>,
    pub added_crypto_refs: Vec<CryptoObservation>,
    pub removed_crypto_refs: Vec<CryptoObservation>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct PageScanDetail {
    pub page_id: i32,
    pub page_title: String,
    pub page_url: String,
    pub page_host: String,
    pub scan: PageScanSummary,
    pub previous_scan: Option<PageScanSummary>,
    pub outgoing_links: Vec<LinkReference>,
    pub emails: Vec<EmailObservation>,
    pub crypto_refs: Vec<CryptoObservation>,
    pub diff: PageScanDiff,
    pub site_profile: Option<SiteProfileSummary>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct SiteCategoryBadge {
    pub category: String,
    pub label: String,
    pub confidence: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct SiteProfileSummary {
    pub host: String,
    pub category: String,
    pub label: String,
    pub confidence: String,
    pub evidence: Vec<String>,
    pub page_count: usize,
    pub source_page_id: Option<i32>,
    pub source_page_title: Option<String>,
    pub source_page_url: Option<String>,
    pub last_classified_at: String,
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
    pub is_blacklisted: bool,
    pub blacklist_match_domain: Option<String>,
    pub source_site_category: Option<SiteCategoryBadge>,
    pub target_site_category: Option<SiteCategoryBadge>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct DomainBlacklistSummary {
    pub id: i32,
    pub domain: String,
    pub created_at: String,
    pub page_link_count: usize,
    pub page_scan_link_count: usize,
}

#[derive(Serialize, Clone, Debug)]
#[serde(crate = "rocket::serde")]
pub struct PaginatedResult<T> {
    pub items: Vec<T>,
    pub total_count: i64,
    pub limit: i64,
    pub offset: i64,
}
