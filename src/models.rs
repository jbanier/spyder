use diesel::prelude::*;
use rocket::serde::Serialize;

#[derive(Selectable, Queryable, Serialize, Clone)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::page)]
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

#[derive(Selectable, Queryable, Serialize, Clone)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::forum_keyword_rule)]
pub struct ForumKeywordRule {
    pub id: i32,
    pub label: String,
    pub pattern: String,
    pub created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::forum_keyword_rule)]
pub struct NewForumKeywordRule<'a> {
    pub label: &'a str,
    pub pattern: &'a str,
}

#[derive(Selectable, Queryable, Serialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::watchlist_item)]
pub struct WatchlistItem {
    pub id: i32,
    pub item_type: String,
    pub value: String,
    pub label: String,
    pub enabled: bool,
    pub created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::watchlist_item)]
pub struct NewWatchlistItem<'a> {
    pub item_type: &'a str,
    pub value: &'a str,
    pub label: &'a str,
}

#[derive(Selectable, Queryable, Serialize, Clone, Debug)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::intel_lead)]
pub struct IntelLeadRecord {
    pub id: i32,
    pub rule_id: String,
    pub lead_key: String,
    pub title: String,
    pub summary: String,
    pub severity: String,
    pub confidence: i32,
    pub score: i32,
    pub status: String,
    pub primary_entity_type: String,
    pub primary_entity_value: String,
    pub related_entity_type: Option<String>,
    pub related_entity_value: Option<String>,
    pub first_seen_at: String,
    pub last_seen_at: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Insertable, Clone)]
#[diesel(table_name = crate::schema::intel_lead)]
pub struct NewIntelLead {
    pub rule_id: String,
    pub lead_key: String,
    pub title: String,
    pub summary: String,
    pub severity: String,
    pub confidence: i32,
    pub score: i32,
    pub status: String,
    pub primary_entity_type: String,
    pub primary_entity_value: String,
    pub related_entity_type: Option<String>,
    pub related_entity_value: Option<String>,
    pub first_seen_at: String,
    pub last_seen_at: String,
}

#[derive(Selectable, Queryable, Serialize, Clone, Debug)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::intel_lead_evidence)]
pub struct IntelLeadEvidenceRecord {
    pub id: i32,
    pub lead_id: i32,
    pub source_type: String,
    pub source_id: i32,
    pub source_key: String,
    pub evidence_text: String,
    pub observed_at: String,
    pub created_at: String,
}

#[derive(Insertable, Clone)]
#[diesel(table_name = crate::schema::intel_lead_evidence)]
pub struct NewIntelLeadEvidence {
    pub lead_id: i32,
    pub source_type: String,
    pub source_id: i32,
    pub source_key: String,
    pub evidence_text: String,
    pub observed_at: String,
}

#[derive(Selectable, Queryable, Serialize, Clone)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::host_ssh_observation)]
pub struct HostSshObservationRecord {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub status: String,
    pub host_key_algorithm: Option<String>,
    pub host_key: Option<String>,
    pub host_key_fingerprint: Option<String>,
    pub server_banner: Option<String>,
    pub last_error: Option<String>,
    pub last_attempt_at: String,
    pub last_success_at: Option<String>,
    pub created_at: String,
}

#[derive(Insertable, Clone)]
#[diesel(table_name = crate::schema::host_ssh_observation)]
pub struct NewHostSshObservation {
    pub host: String,
    pub port: i32,
    pub status: String,
    pub host_key_algorithm: Option<String>,
    pub host_key: Option<String>,
    pub host_key_fingerprint: Option<String>,
    pub server_banner: Option<String>,
    pub last_error: Option<String>,
    pub last_attempt_at: String,
    pub last_success_at: Option<String>,
}

#[derive(Selectable, Queryable, Serialize, Clone)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::host_http_observation)]
pub struct HostHttpObservationRecord {
    pub id: i32,
    pub host: String,
    pub scheme: String,
    pub port: i32,
    pub status: String,
    pub http_status_code: Option<i32>,
    pub final_url: Option<String>,
    pub server_header: Option<String>,
    pub powered_by_header: Option<String>,
    pub content_type_header: Option<String>,
    pub location_header: Option<String>,
    pub via_header: Option<String>,
    pub alt_svc_header: Option<String>,
    pub www_authenticate_header: Option<String>,
    pub set_cookie_names: Option<String>,
    pub response_headers: Option<String>,
    pub header_fingerprint: Option<String>,
    pub favicon_url: Option<String>,
    pub favicon_hash: Option<String>,
    pub stack_versions: Option<String>,
    pub exposed_resources: Option<String>,
    pub last_error: Option<String>,
    pub last_attempt_at: String,
    pub last_success_at: Option<String>,
    pub created_at: String,
}

#[derive(Selectable, Queryable, Serialize, Clone)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::host_service_observation)]
pub struct HostServiceObservationRecord {
    pub id: i32,
    pub host: String,
    pub service: String,
    pub port: i32,
    pub status: String,
    pub banner: Option<String>,
    pub banner_fingerprint: Option<String>,
    pub last_error: Option<String>,
    pub last_attempt_at: String,
    pub last_success_at: Option<String>,
    pub created_at: String,
}

#[derive(Insertable, Clone)]
#[diesel(table_name = crate::schema::host_service_observation)]
pub struct NewHostServiceObservation {
    pub host: String,
    pub service: String,
    pub port: i32,
    pub status: String,
    pub banner: Option<String>,
    pub banner_fingerprint: Option<String>,
    pub last_error: Option<String>,
    pub last_attempt_at: String,
    pub last_success_at: Option<String>,
}

#[derive(Insertable, Clone)]
#[diesel(table_name = crate::schema::host_http_observation)]
pub struct NewHostHttpObservation {
    pub host: String,
    pub scheme: String,
    pub port: i32,
    pub status: String,
    pub http_status_code: Option<i32>,
    pub final_url: Option<String>,
    pub server_header: Option<String>,
    pub powered_by_header: Option<String>,
    pub content_type_header: Option<String>,
    pub location_header: Option<String>,
    pub via_header: Option<String>,
    pub alt_svc_header: Option<String>,
    pub www_authenticate_header: Option<String>,
    pub set_cookie_names: Option<String>,
    pub response_headers: Option<String>,
    pub header_fingerprint: Option<String>,
    pub favicon_url: Option<String>,
    pub favicon_hash: Option<String>,
    pub stack_versions: Option<String>,
    pub exposed_resources: Option<String>,
    pub last_error: Option<String>,
    pub last_attempt_at: String,
    pub last_success_at: Option<String>,
}

#[derive(Selectable, Queryable, Serialize, Clone)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::host_tls_observation)]
pub struct HostTlsObservationRecord {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub status: String,
    pub certificate_sha256: Option<String>,
    pub last_error: Option<String>,
    pub last_attempt_at: String,
    pub last_success_at: Option<String>,
    pub created_at: String,
}

#[derive(Insertable, Clone)]
#[diesel(table_name = crate::schema::host_tls_observation)]
pub struct NewHostTlsObservation {
    pub host: String,
    pub port: i32,
    pub status: String,
    pub certificate_sha256: Option<String>,
    pub last_error: Option<String>,
    pub last_attempt_at: String,
    pub last_success_at: Option<String>,
}

#[derive(Selectable, Queryable, Clone)]
#[diesel(table_name = crate::schema::page_classification)]
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
pub struct SiteProfileRecord {
    pub id: i32,
    pub host: String,
    pub category: String,
    pub confidence: String,
    pub score: i32,
    pub page_count: i32,
    pub first_found_at: String,
    pub last_scanned_at: String,
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
    pub first_found_at: String,
    pub last_scanned_at: String,
    pub evidence: String,
    pub source_page_id: Option<i32>,
}

#[derive(Selectable, Queryable, Serialize, Clone)]
#[serde(crate = "rocket::serde")]
#[diesel(table_name = crate::schema::page_scan)]
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
#[diesel(table_name = crate::schema::page_keyword_tag)]
pub struct PageKeywordTag {
    pub id: i32,
    pub page_id: i32,
    pub tag: String,
    pub created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::page_keyword_tag)]
pub struct NewPageKeywordTag {
    pub page_id: i32,
    pub tag: String,
    pub created_at: String,
}

#[derive(Selectable, Queryable, Clone)]
#[diesel(table_name = crate::schema::page_language_detection)]
pub struct PageLanguageDetectionRecord {
    pub id: i32,
    pub page_id: i32,
    pub language_code: String,
    pub language_name: String,
    pub confidence: i32,
    pub source: String,
    pub evidence: String,
    pub updated_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::page_language_detection)]
pub struct NewPageLanguageDetection {
    pub page_id: i32,
    pub language_code: String,
    pub language_name: String,
    pub confidence: i32,
    pub source: String,
    pub evidence: String,
}

#[derive(Selectable, Queryable, Clone)]
#[diesel(table_name = crate::schema::page_topic_tag)]
pub struct PageTopicTagRecord {
    pub id: i32,
    pub page_id: i32,
    pub topic: String,
    pub score: i32,
    pub confidence: String,
    pub evidence: String,
    pub created_at: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::page_topic_tag)]
pub struct NewPageTopicTag {
    pub page_id: i32,
    pub topic: String,
    pub score: i32,
    pub confidence: String,
    pub evidence: String,
}

#[derive(Selectable, Queryable, Clone)]
#[diesel(table_name = crate::schema::page_scan_email)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LanguageDetection {
    pub code: String,
    pub name: String,
    pub confidence: i32,
    pub source: String,
    pub evidence: String,
}

impl LanguageDetection {
    pub fn unknown() -> Self {
        Self {
            code: String::new(),
            name: "Unknown".to_string(),
            confidence: 0,
            source: "none".to_string(),
            evidence: "signals:insufficient".to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TopicObservation {
    pub topic: String,
    pub score: i32,
    pub confidence: String,
    pub evidence: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct PageSnapshot {
    pub title: String,
    pub url: String,
    pub language: String,
    pub language_detection: LanguageDetection,
    pub keyword_corpus: String,
    pub links: Vec<LinkObservation>,
    pub emails: Vec<String>,
    pub crypto_refs: Vec<CryptoReference>,
    pub classification_signals: ClassificationSignals,
    pub topic_observations: Vec<TopicObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecentHostCandidate {
    pub host: String,
    pub last_scanned_at: String,
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
pub struct LanguageDetectionSummary {
    pub language_code: String,
    pub language_name: String,
    pub confidence: i32,
    pub source: String,
    pub evidence: String,
    pub updated_at: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct PageTopicSummary {
    pub topic: String,
    pub label: String,
    pub score: i32,
    pub confidence: String,
    pub evidence: Vec<String>,
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
    pub language_detection: Option<LanguageDetectionSummary>,
    pub topic_tags: Vec<PageTopicSummary>,
    pub site_profile: Option<SiteProfileSummary>,
    pub host_http_observation: Option<HostHttpObservationDetail>,
    pub intel_leads: Vec<IntelLeadBadge>,
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
    pub keyword_tags: Vec<String>,
    pub page_count: usize,
    pub first_found_at: String,
    pub source_page_id: Option<i32>,
    pub source_page_title: Option<String>,
    pub source_page_url: Option<String>,
    pub last_scanned_at: String,
    pub last_classified_at: String,
    pub intel_leads: Vec<IntelLeadBadge>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct HostHttpObservationSummary {
    pub host: String,
    pub scheme: String,
    pub port: i32,
    pub endpoint_url: String,
    pub status: String,
    pub http_status_code: Option<i32>,
    pub final_url: Option<String>,
    pub server_header: Option<String>,
    pub header_fingerprint: Option<String>,
    pub favicon_hash: Option<String>,
    pub stack_versions: Option<String>,
    pub last_success_at: Option<String>,
    pub detail_url: String,
    pub site_category: Option<SiteCategoryBadge>,
    pub source_page_id: Option<i32>,
    pub source_page_title: Option<String>,
    pub source_page_url: Option<String>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct HostHttpObservationDetail {
    pub host: String,
    pub scheme: String,
    pub port: i32,
    pub endpoint_url: String,
    pub status: String,
    pub http_status_code: Option<i32>,
    pub final_url: Option<String>,
    pub server_header: Option<String>,
    pub powered_by_header: Option<String>,
    pub content_type_header: Option<String>,
    pub location_header: Option<String>,
    pub via_header: Option<String>,
    pub alt_svc_header: Option<String>,
    pub www_authenticate_header: Option<String>,
    pub set_cookie_names: Option<String>,
    pub response_headers: Option<String>,
    pub header_fingerprint: Option<String>,
    pub favicon_url: Option<String>,
    pub favicon_hash: Option<String>,
    pub stack_versions: Option<String>,
    pub exposed_resources: Option<String>,
    pub last_error: Option<String>,
    pub last_attempt_at: String,
    pub last_success_at: Option<String>,
    pub detail_url: String,
    pub site_category: Option<SiteCategoryBadge>,
    pub source_page_id: Option<i32>,
    pub source_page_title: Option<String>,
    pub source_page_url: Option<String>,
    pub tls_endpoint_url: Option<String>,
    pub tls_observation: Option<HostTlsObservationRecord>,
    pub intel_leads: Vec<IntelLeadBadge>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct HostServiceObservationSummary {
    pub host: String,
    pub service: String,
    pub port: i32,
    pub endpoint_url: String,
    pub status: String,
    pub banner: Option<String>,
    pub banner_fingerprint: Option<String>,
    pub last_success_at: Option<String>,
    pub detail_url: String,
    pub site_category: Option<SiteCategoryBadge>,
    pub source_page_id: Option<i32>,
    pub source_page_title: Option<String>,
    pub source_page_url: Option<String>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct HostServiceObservationDetail {
    pub host: String,
    pub service: String,
    pub port: i32,
    pub endpoint_url: String,
    pub status: String,
    pub banner: Option<String>,
    pub banner_fingerprint: Option<String>,
    pub last_error: Option<String>,
    pub last_attempt_at: String,
    pub last_success_at: Option<String>,
    pub detail_url: String,
    pub site_category: Option<SiteCategoryBadge>,
    pub source_page_id: Option<i32>,
    pub source_page_title: Option<String>,
    pub source_page_url: Option<String>,
    pub intel_leads: Vec<IntelLeadBadge>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct TopSiteEntry {
    pub host: String,
    pub count: usize,
    pub last_scanned_at: Option<String>,
    pub page_id: Option<i32>,
    pub page_title: Option<String>,
    pub page_url: Option<String>,
    pub site_category: Option<SiteCategoryBadge>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct TopSiteSection {
    pub title: String,
    pub description: String,
    pub count_label: String,
    pub has_items: bool,
    pub items: Vec<TopSiteEntry>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct CategoryDistributionEntry {
    pub category: String,
    pub label: String,
    pub host_count: usize,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct CategoryTimelinePoint {
    pub day: String,
    pub category: String,
    pub label: String,
    pub host_count: usize,
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
    pub intel_leads: Vec<IntelLeadBadge>,
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
    pub intel_leads: Vec<IntelLeadBadge>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct SshHostKeySummary {
    pub algorithm: String,
    pub fingerprint: String,
    pub host_count: usize,
    pub endpoint_count: usize,
    pub last_success_at: String,
    pub detail_url: String,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct SshHostKeyDetail {
    pub algorithm: String,
    pub fingerprint: String,
    pub host_count: usize,
    pub endpoint_count: usize,
    pub endpoints: Vec<SshHostKeyEndpoint>,
    pub intel_leads: Vec<IntelLeadBadge>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct SshHostKeyEndpoint {
    pub host: String,
    pub port: i32,
    pub status: String,
    pub last_error: Option<String>,
    pub last_attempt_at: String,
    pub last_success_at: Option<String>,
    pub server_banner: Option<String>,
    pub host_key: Option<String>,
    pub site_category: Option<SiteCategoryBadge>,
    pub source_page_id: Option<i32>,
    pub source_page_title: Option<String>,
    pub source_page_url: Option<String>,
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
    pub intel_leads: Vec<IntelLeadBadge>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct SiteRelationshipGraph {
    pub mode: String,
    pub focus_host: Option<String>,
    pub depth: usize,
    pub nodes: Vec<SiteRelationshipGraphNode>,
    pub edges: Vec<SiteRelationshipGraphEdge>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct SiteRelationshipGraphNode {
    pub host: String,
    pub site_category: Option<SiteCategoryBadge>,
    pub incoming_count: usize,
    pub outgoing_count: usize,
    pub is_focus: bool,
    pub is_blacklisted: bool,
    pub blacklist_match_domain: Option<String>,
    pub depth: usize,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct SiteRelationshipGraphEdge {
    pub source_host: String,
    pub target_host: String,
    pub reference_count: usize,
    pub depth: usize,
    pub relationship_key: String,
    pub is_blacklisted: bool,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "rocket::serde")]
pub struct IntelLeadBadge {
    pub id: i32,
    pub rule_id: String,
    pub title: String,
    pub severity: String,
    pub confidence: i32,
    pub score: i32,
    pub status: String,
    pub detail_url: String,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "rocket::serde")]
pub struct IntelLeadSummary {
    pub id: i32,
    pub rule_id: String,
    pub lead_key: String,
    pub title: String,
    pub summary: String,
    pub severity: String,
    pub confidence: i32,
    pub score: i32,
    pub status: String,
    pub primary_entity_type: String,
    pub primary_entity_value: String,
    pub related_entity_type: Option<String>,
    pub related_entity_value: Option<String>,
    pub first_seen_at: String,
    pub last_seen_at: String,
    pub updated_at: String,
    pub evidence_count: usize,
    pub detail_url: String,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "rocket::serde")]
pub struct IntelLeadEvidenceView {
    pub id: i32,
    pub source_type: String,
    pub source_id: i32,
    pub source_key: String,
    pub evidence_text: String,
    pub observed_at: String,
    pub source_url: Option<String>,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "rocket::serde")]
pub struct IntelLeadEntityReference {
    pub entity_type: String,
    pub entity_value: String,
    pub detail_url: Option<String>,
}

#[derive(Serialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct IntelLeadDetail {
    pub lead: IntelLeadSummary,
    pub evidence: Vec<IntelLeadEvidenceView>,
    pub related_pages: Vec<PageReference>,
    pub related_sites: Vec<String>,
    pub related_entities: Vec<IntelLeadEntityReference>,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "rocket::serde")]
pub struct IntelLeadRecomputeSummary {
    pub candidate_count: usize,
    pub created_count: usize,
    pub updated_count: usize,
    pub evidence_count: usize,
    pub rule_summaries: Vec<IntelLeadRuleRecomputeSummary>,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "rocket::serde")]
pub struct IntelLeadRuleRecomputeSummary {
    pub rule_id: String,
    pub candidate_count: usize,
    pub created_count: usize,
    pub updated_count: usize,
    pub evidence_count: usize,
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
