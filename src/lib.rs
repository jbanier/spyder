pub mod extraction;
pub mod models;
pub mod schema;

use anyhow::{Context, Result};
use diesel::deserialize::QueryableByName;
use diesel::dsl::{count_star, sql};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Bool, Nullable, Text};
use diesel::upsert::excluded;
use dotenvy::dotenv;
use models::*;
use std::env;
use url::form_urlencoded;
use url::Url;

pub const STATUS_PENDING: &str = "pending";
pub const STATUS_DONE: &str = "done";
pub const STATUS_FAILED: &str = "failed";
pub const MAX_RETRY_ATTEMPTS: i32 = 5;
const DEFAULT_PAGE_LIMIT: i64 = 50;
const MAX_PAGE_LIMIT: i64 = 200;

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(QueryableByName)]
struct NullableTextRow {
    #[diesel(sql_type = Nullable<Text>)]
    value: Option<String>,
}

#[derive(QueryableByName)]
struct PageSummaryRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = Text)]
    title: String,
    #[diesel(sql_type = Text)]
    url: String,
    #[diesel(sql_type = Text)]
    host: String,
    #[diesel(sql_type = Text)]
    language: String,
    #[diesel(sql_type = Text)]
    last_scanned_at: String,
    #[diesel(sql_type = BigInt)]
    outbound_link_count: i64,
    #[diesel(sql_type = BigInt)]
    email_count: i64,
    #[diesel(sql_type = BigInt)]
    crypto_count: i64,
}

#[derive(QueryableByName)]
struct SearchResultRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    page_id: i32,
    #[diesel(sql_type = Text)]
    title: String,
    #[diesel(sql_type = Text)]
    url: String,
    #[diesel(sql_type = Text)]
    language: String,
    #[diesel(sql_type = Text)]
    scraped_at: String,
}

#[derive(QueryableByName)]
struct EmailEntitySummaryRow {
    #[diesel(sql_type = Text)]
    value: String,
    #[diesel(sql_type = BigInt)]
    page_count: i64,
}

#[derive(QueryableByName)]
struct CryptoEntitySummaryRow {
    #[diesel(sql_type = Text)]
    asset_type: String,
    #[diesel(sql_type = Text)]
    reference: String,
    #[diesel(sql_type = BigInt)]
    page_count: i64,
}

#[derive(QueryableByName)]
struct SiteRelationshipRow {
    #[diesel(sql_type = Text)]
    source_host: String,
    #[diesel(sql_type = Text)]
    target_host: String,
    #[diesel(sql_type = BigInt)]
    reference_count: i64,
}

#[derive(Clone, Copy)]
struct PaginationInput {
    limit: i64,
    offset: i64,
}

pub fn establish_connection() -> Result<SqliteConnection> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    SqliteConnection::establish(&database_url)
        .with_context(|| format!("error connecting to {database_url}"))
}

pub fn create_work_unit(conn: &mut SqliteConnection, url: &str) -> Result<()> {
    let work_unit = NewUnit {
        url,
        status: STATUS_PENDING,
    };

    diesel::insert_into(crate::schema::work_unit::table)
        .values(work_unit)
        .on_conflict(crate::schema::work_unit::url)
        .do_nothing()
        .execute(conn)
        .context("error saving work unit")?;

    Ok(())
}

pub fn save_page_info(conn: &mut SqliteConnection, snapshot: &PageSnapshot) -> Result<()> {
    use crate::schema::page::dsl::{
        coins as page_coins, emails as page_emails, language as page_language,
        last_scanned_at as page_last_scanned_at, links as page_links, title as page_title,
        url as page_url,
    };
    use crate::schema::{page_crypto, page_email, page_link};

    let new_page = NewPage {
        title: snapshot.title.clone(),
        url: snapshot.url.clone(),
        links: snapshot
            .links
            .iter()
            .map(|item| item.target_url.clone())
            .collect::<Vec<_>>()
            .join(","),
        emails: snapshot.emails.join(","),
        coins: snapshot
            .crypto_refs
            .iter()
            .map(|item| format!("{}:{}", item.asset_type, item.reference))
            .collect::<Vec<_>>()
            .join(","),
        language: snapshot.language.clone(),
    };

    conn.transaction::<_, anyhow::Error, _>(|conn| {
        diesel::insert_into(crate::schema::page::table)
            .values(&new_page)
            .on_conflict(page_url)
            .do_update()
            .set((
                page_title.eq(excluded(page_title)),
                page_links.eq(excluded(page_links)),
                page_emails.eq(excluded(page_emails)),
                page_coins.eq(excluded(page_coins)),
                page_language.eq(excluded(page_language)),
                page_last_scanned_at.eq(sql::<Text>("CURRENT_TIMESTAMP")),
            ))
            .execute(conn)
            .context("error saving page")?;

        let stored_page_id = crate::schema::page::table
            .filter(page_url.eq(&snapshot.url))
            .select(crate::schema::page::id)
            .first::<i32>(conn)
            .context("error loading saved page id")?;

        diesel::delete(page_link::table.filter(page_link::source_page_id.eq(stored_page_id)))
            .execute(conn)
            .context("error clearing saved page links")?;
        diesel::delete(page_email::table.filter(page_email::page_id.eq(stored_page_id)))
            .execute(conn)
            .context("error clearing saved page emails")?;
        diesel::delete(page_crypto::table.filter(page_crypto::page_id.eq(stored_page_id)))
            .execute(conn)
            .context("error clearing saved page crypto refs")?;

        let link_rows = snapshot
            .links
            .iter()
            .map(|item| NewPageLink {
                source_page_id: stored_page_id,
                target_url: item.target_url.clone(),
                target_host: item.target_host.clone(),
            })
            .collect::<Vec<_>>();
        if !link_rows.is_empty() {
            diesel::insert_into(page_link::table)
                .values(&link_rows)
                .execute(conn)
                .context("error saving page links")?;
        }

        let email_rows = snapshot
            .emails
            .iter()
            .map(|email| NewPageEmail {
                page_id: stored_page_id,
                email: email.clone(),
            })
            .collect::<Vec<_>>();
        if !email_rows.is_empty() {
            diesel::insert_into(page_email::table)
                .values(&email_rows)
                .execute(conn)
                .context("error saving page emails")?;
        }

        let crypto_rows = snapshot
            .crypto_refs
            .iter()
            .map(|item| NewPageCrypto {
                page_id: stored_page_id,
                asset_type: item.asset_type.clone(),
                reference: item.reference.clone(),
            })
            .collect::<Vec<_>>();
        if !crypto_rows.is_empty() {
            diesel::insert_into(page_crypto::table)
                .values(&crypto_rows)
                .execute(conn)
                .context("error saving page crypto references")?;
        }

        Ok(())
    })?;

    Ok(())
}

pub fn mark_work_unit_as_done(conn: &mut SqliteConnection, work_unit_id: i32) -> Result<()> {
    use crate::schema::work_unit::dsl::*;

    diesel::update(crate::schema::work_unit::table.filter(id.eq(work_unit_id)))
        .set((
            status.eq(STATUS_DONE),
            next_attempt_at.eq(sql::<Text>("CURRENT_TIMESTAMP")),
            last_attempt_at.eq(sql::<Nullable<Text>>("CURRENT_TIMESTAMP")),
            last_error.eq::<Option<String>>(None),
        ))
        .execute(conn)
        .context("error updating work unit status")?;

    Ok(())
}

pub fn record_work_unit_failure(
    conn: &mut SqliteConnection,
    work_unit_id: i32,
    error_message: &str,
    retriable: bool,
) -> Result<()> {
    use crate::schema::work_unit::dsl::*;

    let existing_work_unit = crate::schema::work_unit::table
        .filter(id.eq(work_unit_id))
        .select(WorkUnit::as_select())
        .first::<WorkUnit>(conn)
        .context("error loading work unit before retry update")?;
    let next_retry_count = existing_work_unit.retry_count + 1;
    let bounded_error = truncate(error_message, 500);
    let should_retry = retriable && next_retry_count < MAX_RETRY_ATTEMPTS;

    if should_retry {
        let backoff_minutes = retry_backoff_minutes(next_retry_count);
        diesel::update(crate::schema::work_unit::table.filter(id.eq(work_unit_id)))
            .set((
                status.eq(STATUS_PENDING),
                retry_count.eq(next_retry_count),
                last_error.eq(Some(bounded_error)),
                last_attempt_at.eq(sql::<Nullable<Text>>("CURRENT_TIMESTAMP")),
                next_attempt_at.eq(sql::<Text>(&format!(
                    "datetime(CURRENT_TIMESTAMP, '+{} minutes')",
                    backoff_minutes
                ))),
            ))
            .execute(conn)
            .context("error scheduling work unit retry")?;
    } else {
        diesel::update(crate::schema::work_unit::table.filter(id.eq(work_unit_id)))
            .set((
                status.eq(STATUS_FAILED),
                retry_count.eq(next_retry_count),
                last_error.eq(Some(bounded_error)),
                last_attempt_at.eq(sql::<Nullable<Text>>("CURRENT_TIMESTAMP")),
            ))
            .execute(conn)
            .context("error marking work unit as failed")?;
    }

    Ok(())
}

pub fn get_pending_work_units(conn: &mut SqliteConnection) -> Result<Vec<WorkUnit>> {
    use crate::schema::work_unit::dsl::*;

    crate::schema::work_unit::table
        .filter(status.eq(STATUS_PENDING))
        .filter(sql::<Bool>("next_attempt_at <= CURRENT_TIMESTAMP"))
        .order(next_attempt_at.asc())
        .then_order_by(id.asc())
        .select(WorkUnit::as_select())
        .load(conn)
        .context("error querying pending work units")
}

pub fn list_work_units(
    conn: &mut SqliteConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<WorkUnit>> {
    use crate::schema::work_unit::dsl::*;

    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = crate::schema::work_unit::table
        .select(count_star())
        .first(conn)
        .context("error counting work units")?;
    let items = crate::schema::work_unit::table
        .order(created_at.desc())
        .then_order_by(id.desc())
        .limit(pagination.limit)
        .offset(pagination.offset)
        .select(WorkUnit::as_select())
        .load(conn)
        .context("error querying work units")?;

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn list_page_summaries(
    conn: &mut SqliteConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<PageSummary>> {
    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = scalar_count(conn, "SELECT COUNT(*) AS count FROM page")
        .context("error counting pages for summary")?;
    let host_expr = sql_host_expr("p.url");
    let query = format!(
        "
        SELECT
            p.id,
            p.title,
            p.url,
            {host_expr} AS host,
            p.language,
            p.last_scanned_at,
            COALESCE(pl.link_count, 0) AS outbound_link_count,
            COALESCE(pe.email_count, 0) AS email_count,
            COALESCE(pc.crypto_count, 0) AS crypto_count
        FROM page p
        LEFT JOIN (
            SELECT source_page_id, COUNT(*) AS link_count
            FROM page_link
            GROUP BY source_page_id
        ) pl ON pl.source_page_id = p.id
        LEFT JOIN (
            SELECT page_id, COUNT(*) AS email_count
            FROM page_email
            GROUP BY page_id
        ) pe ON pe.page_id = p.id
        LEFT JOIN (
            SELECT page_id, COUNT(*) AS crypto_count
            FROM page_crypto
            GROUP BY page_id
        ) pc ON pc.page_id = p.id
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT ? OFFSET ?
        "
    );
    let rows = sql_query(query)
        .bind::<BigInt, _>(pagination.limit)
        .bind::<BigInt, _>(pagination.offset)
        .load::<PageSummaryRow>(conn)
        .context("error querying page summaries")?;
    let items = rows
        .into_iter()
        .map(|row| PageSummary {
            id: row.id,
            title: row.title,
            url: row.url,
            host: row.host,
            language: row.language,
            last_scanned_at: row.last_scanned_at,
            outbound_link_count: row.outbound_link_count.max(0) as usize,
            email_count: row.email_count.max(0) as usize,
            crypto_count: row.crypto_count.max(0) as usize,
        })
        .collect();

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn get_page_detail(conn: &mut SqliteConnection, page_id: i32) -> Result<Option<PageDetail>> {
    use crate::schema::page::dsl as page_dsl;
    use crate::schema::page_crypto::dsl as crypto_dsl;
    use crate::schema::page_email::dsl as email_dsl;
    use crate::schema::page_link::dsl as link_dsl;

    let page = page_dsl::page
        .filter(page_dsl::id.eq(page_id))
        .select(Page::as_select())
        .first::<Page>(conn)
        .optional()
        .context("error loading page detail")?;

    let Some(page) = page else {
        return Ok(None);
    };

    let outgoing_rows = link_dsl::page_link
        .filter(link_dsl::source_page_id.eq(page.id))
        .select(PageLink::as_select())
        .load::<PageLink>(conn)
        .context("error loading outgoing links")?;
    let target_urls = outgoing_rows
        .iter()
        .map(|row| row.target_url.clone())
        .collect::<Vec<_>>();
    let known_targets = if target_urls.is_empty() {
        std::collections::HashMap::new()
    } else {
        page_dsl::page
            .filter(page_dsl::url.eq_any(&target_urls))
            .select(Page::as_select())
            .load::<Page>(conn)
            .context("error loading known target pages")?
            .into_iter()
            .map(|item| (item.url.clone(), item))
            .collect::<std::collections::HashMap<_, _>>()
    };

    let mut outgoing_links = outgoing_rows
        .into_iter()
        .map(|row| {
            let known_target = known_targets.get(&row.target_url);
            LinkReference {
                target_url: row.target_url,
                target_host: row.target_host,
                target_page_id: known_target.map(|item| item.id),
                target_page_title: known_target.map(|item| item.title.clone()),
            }
        })
        .collect::<Vec<_>>();
    outgoing_links.sort_by(|left, right| {
        left.target_host
            .cmp(&right.target_host)
            .then_with(|| left.target_url.cmp(&right.target_url))
    });

    let inbound_rows = link_dsl::page_link
        .filter(link_dsl::target_url.eq(&page.url))
        .select(PageLink::as_select())
        .load::<PageLink>(conn)
        .context("error loading inbound links")?;
    let source_ids = inbound_rows
        .iter()
        .map(|row| row.source_page_id)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let source_pages = load_pages_by_ids(conn, &source_ids)?
        .into_iter()
        .map(|item| (item.id, item))
        .collect::<std::collections::HashMap<_, _>>();

    let mut incoming_links = inbound_rows
        .into_iter()
        .filter_map(|row| source_pages.get(&row.source_page_id))
        .map(|source_page| IncomingReference {
            source_page_id: source_page.id,
            source_title: source_page.title.clone(),
            source_url: source_page.url.clone(),
            source_host: host_from_url(&source_page.url),
        })
        .collect::<Vec<_>>();
    incoming_links.sort_by(|left, right| {
        left.source_host
            .cmp(&right.source_host)
            .then_with(|| left.source_url.cmp(&right.source_url))
    });

    let mut emails = email_dsl::page_email
        .filter(email_dsl::page_id.eq(page.id))
        .select(PageEmail::as_select())
        .load::<PageEmail>(conn)
        .context("error loading page emails")?
        .into_iter()
        .map(|row| EmailObservation {
            detail_url: build_query_url("/entities/emails", &[("value", &row.email)]),
            value: row.email,
        })
        .collect::<Vec<_>>();
    emails.sort_by(|left, right| left.value.cmp(&right.value));

    let mut crypto_refs = crypto_dsl::page_crypto
        .filter(crypto_dsl::page_id.eq(page.id))
        .select(PageCrypto::as_select())
        .load::<PageCrypto>(conn)
        .context("error loading page crypto references")?
        .into_iter()
        .map(|row| CryptoObservation {
            detail_url: build_query_url(
                "/entities/crypto",
                &[
                    ("asset_type", &row.asset_type),
                    ("reference", &row.reference),
                ],
            ),
            asset_type: row.asset_type,
            reference: row.reference,
        })
        .collect::<Vec<_>>();
    crypto_refs.sort_by(|left, right| {
        left.asset_type
            .cmp(&right.asset_type)
            .then_with(|| left.reference.cmp(&right.reference))
    });

    Ok(Some(PageDetail {
        id: page.id,
        title: page.title,
        url: page.url.clone(),
        host: host_from_url(&page.url),
        language: page.language,
        created_at: page.created_at,
        last_scanned_at: page.last_scanned_at,
        outgoing_links,
        incoming_links,
        emails,
        crypto_refs,
    }))
}

pub fn collect_stats(conn: &mut SqliteConnection) -> Result<Stats> {
    use crate::schema::work_unit::dsl as work_dsl;

    let total_pages =
        scalar_count(conn, "SELECT COUNT(*) AS count FROM page").context("error counting pages")?;
    let pending_work_units = work_dsl::work_unit
        .filter(work_dsl::status.eq(STATUS_PENDING))
        .select(count_star())
        .first(conn)
        .context("error counting pending work units")?;
    let failed_work_units = work_dsl::work_unit
        .filter(work_dsl::status.eq(STATUS_FAILED))
        .select(count_star())
        .first(conn)
        .context("error counting failed work units")?;

    let host_expr = sql_host_expr("url");
    let total_domains = scalar_count(
        conn,
        &format!(
            "SELECT COUNT(*) AS count FROM (SELECT DISTINCT {host_expr} AS host FROM page WHERE {host_expr} != '')"
        ),
    )
    .context("error counting distinct domains")?;
    let last_scrape = scalar_nullable_text(conn, "SELECT MAX(last_scanned_at) AS value FROM page")
        .context("error loading last scrape")?
        .unwrap_or_else(|| "Never".to_string());

    Ok(Stats {
        total_pages,
        total_domains,
        pending_work_units,
        failed_work_units,
        last_scrape,
    })
}

pub fn search_pages(
    conn: &mut SqliteConnection,
    query: &str,
    requested_limit: Option<i64>,
) -> Result<Vec<SearchResult>> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let limit = requested_limit.unwrap_or(10).clamp(1, 50);
    let pattern = format!("%{}%", escape_like(trimmed));
    let sql = "
        SELECT
            p.id AS page_id,
            p.title,
            p.url,
            p.language,
            p.last_scanned_at AS scraped_at
        FROM page p
        WHERE p.title LIKE ? ESCAPE '\\' COLLATE NOCASE
            OR p.url LIKE ? ESCAPE '\\' COLLATE NOCASE
            OR p.language LIKE ? ESCAPE '\\' COLLATE NOCASE
            OR EXISTS (
                SELECT 1
                FROM page_email pe
                WHERE pe.page_id = p.id
                    AND pe.email LIKE ? ESCAPE '\\' COLLATE NOCASE
            )
            OR EXISTS (
                SELECT 1
                FROM page_crypto pc
                WHERE pc.page_id = p.id
                    AND (pc.asset_type || ':' || pc.reference) LIKE ? ESCAPE '\\' COLLATE NOCASE
            )
        ORDER BY p.last_scanned_at DESC, p.id DESC
        LIMIT ?
    ";
    let rows = sql_query(sql)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<Text, _>(&pattern)
        .bind::<BigInt, _>(limit)
        .load::<SearchResultRow>(conn)
        .context("error searching pages")?;

    Ok(rows
        .into_iter()
        .map(|row| SearchResult {
            page_id: row.page_id,
            title: row.title,
            url: row.url,
            language: row.language,
            scraped_at: row.scraped_at,
        })
        .collect())
}

pub fn list_email_entities(
    conn: &mut SqliteConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<EmailEntitySummary>> {
    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = scalar_count(
        conn,
        "SELECT COUNT(*) AS count FROM (SELECT email FROM page_email GROUP BY email)",
    )
    .context("error counting email entities")?;
    let rows = sql_query(
        "
        SELECT
            email AS value,
            COUNT(*) AS page_count
        FROM page_email
        GROUP BY email
        ORDER BY page_count DESC, value ASC
        LIMIT ? OFFSET ?
        ",
    )
    .bind::<BigInt, _>(pagination.limit)
    .bind::<BigInt, _>(pagination.offset)
    .load::<EmailEntitySummaryRow>(conn)
    .context("error loading email entities")?;
    let items = rows
        .into_iter()
        .map(|row| EmailEntitySummary {
            detail_url: build_query_url("/entities/emails", &[("value", &row.value)]),
            value: row.value,
            page_count: row.page_count.max(0) as usize,
        })
        .collect();

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn get_email_entity_detail(
    conn: &mut SqliteConnection,
    value: &str,
) -> Result<Option<EmailEntityDetail>> {
    use crate::schema::page_email::dsl as email_dsl;

    let page_ids = email_dsl::page_email
        .filter(email_dsl::email.eq(value))
        .select(email_dsl::page_id)
        .load::<i32>(conn)
        .context("error loading pages for email entity")?;
    if page_ids.is_empty() {
        return Ok(None);
    }

    let mut pages = load_pages_by_ids(conn, &page_ids)?
        .into_iter()
        .map(page_reference_from_page)
        .collect::<Vec<_>>();
    pages.sort_by(|left, right| {
        right
            .last_scanned_at
            .cmp(&left.last_scanned_at)
            .then_with(|| left.url.cmp(&right.url))
    });

    Ok(Some(EmailEntityDetail {
        value: value.to_string(),
        pages,
    }))
}

pub fn list_crypto_entities(
    conn: &mut SqliteConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<CryptoEntitySummary>> {
    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let total_count = scalar_count(
        conn,
        "SELECT COUNT(*) AS count FROM (SELECT asset_type, reference FROM page_crypto GROUP BY asset_type, reference)",
    )
    .context("error counting crypto entities")?;
    let rows = sql_query(
        "
        SELECT
            asset_type,
            reference,
            COUNT(*) AS page_count
        FROM page_crypto
        GROUP BY asset_type, reference
        ORDER BY page_count DESC, asset_type ASC, reference ASC
        LIMIT ? OFFSET ?
        ",
    )
    .bind::<BigInt, _>(pagination.limit)
    .bind::<BigInt, _>(pagination.offset)
    .load::<CryptoEntitySummaryRow>(conn)
    .context("error loading crypto entities")?;
    let items = rows
        .into_iter()
        .map(|row| CryptoEntitySummary {
            detail_url: build_query_url(
                "/entities/crypto",
                &[
                    ("asset_type", &row.asset_type),
                    ("reference", &row.reference),
                ],
            ),
            asset_type: row.asset_type,
            reference: row.reference,
            page_count: row.page_count.max(0) as usize,
        })
        .collect();

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

pub fn get_crypto_entity_detail(
    conn: &mut SqliteConnection,
    asset_type: &str,
    reference: &str,
) -> Result<Option<CryptoEntityDetail>> {
    use crate::schema::page_crypto::dsl as crypto_dsl;

    let page_ids = crypto_dsl::page_crypto
        .filter(crypto_dsl::asset_type.eq(asset_type))
        .filter(crypto_dsl::reference.eq(reference))
        .select(crypto_dsl::page_id)
        .load::<i32>(conn)
        .context("error loading pages for crypto entity")?;
    if page_ids.is_empty() {
        return Ok(None);
    }

    let mut pages = load_pages_by_ids(conn, &page_ids)?
        .into_iter()
        .map(page_reference_from_page)
        .collect::<Vec<_>>();
    pages.sort_by(|left, right| {
        right
            .last_scanned_at
            .cmp(&left.last_scanned_at)
            .then_with(|| left.url.cmp(&right.url))
    });

    Ok(Some(CryptoEntityDetail {
        asset_type: asset_type.to_string(),
        reference: reference.to_string(),
        pages,
    }))
}

pub fn list_site_relationships(
    conn: &mut SqliteConnection,
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
) -> Result<PaginatedResult<SiteRelationship>> {
    let pagination = normalize_pagination(
        requested_limit,
        requested_offset,
        DEFAULT_PAGE_LIMIT,
        MAX_PAGE_LIMIT,
    );
    let source_host_expr = sql_host_expr("p.url");
    let total_count = scalar_count(
        conn,
        &format!(
            "
            SELECT COUNT(*) AS count
            FROM (
                SELECT 1
                FROM page_link pl
                JOIN page p ON p.id = pl.source_page_id
                WHERE pl.target_host != ''
                    AND {source_host_expr} != ''
                    AND {source_host_expr} != pl.target_host
                GROUP BY {source_host_expr}, pl.target_host
            )
            "
        ),
    )
    .context("error counting site relationships")?;
    let query = format!(
        "
        SELECT
            {source_host_expr} AS source_host,
            pl.target_host,
            COUNT(*) AS reference_count
        FROM page_link pl
        JOIN page p ON p.id = pl.source_page_id
        WHERE pl.target_host != ''
            AND {source_host_expr} != ''
            AND {source_host_expr} != pl.target_host
        GROUP BY {source_host_expr}, pl.target_host
        ORDER BY reference_count DESC, source_host ASC, pl.target_host ASC
        LIMIT ? OFFSET ?
        "
    );
    let rows = sql_query(query)
        .bind::<BigInt, _>(pagination.limit)
        .bind::<BigInt, _>(pagination.offset)
        .load::<SiteRelationshipRow>(conn)
        .context("error loading site relationships")?;
    let items = rows
        .into_iter()
        .map(|row| SiteRelationship {
            source_host: row.source_host,
            target_host: row.target_host,
            reference_count: row.reference_count.max(0) as usize,
        })
        .collect();

    Ok(PaginatedResult {
        items,
        total_count,
        limit: pagination.limit,
        offset: pagination.offset,
    })
}

fn load_pages_by_ids(conn: &mut SqliteConnection, page_ids: &[i32]) -> Result<Vec<Page>> {
    use crate::schema::page::dsl as page_dsl;

    if page_ids.is_empty() {
        return Ok(Vec::new());
    }

    page_dsl::page
        .filter(page_dsl::id.eq_any(page_ids))
        .select(Page::as_select())
        .load(conn)
        .context("error loading pages by id")
}

fn page_reference_from_page(page: Page) -> PageReference {
    PageReference {
        id: page.id,
        title: page.title,
        url: page.url,
        language: page.language,
        last_scanned_at: page.last_scanned_at,
    }
}

fn scalar_count(conn: &mut SqliteConnection, query: &str) -> Result<i64> {
    Ok(sql_query(query)
        .get_result::<CountRow>(conn)
        .context("error loading count result")?
        .count)
}

fn scalar_nullable_text(conn: &mut SqliteConnection, query: &str) -> Result<Option<String>> {
    Ok(sql_query(query)
        .get_result::<NullableTextRow>(conn)
        .context("error loading text result")?
        .value)
}

fn host_from_url(value: &str) -> String {
    Url::parse(value)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_string()))
        .unwrap_or_default()
}

fn build_query_url(base: &str, params: &[(&str, &str)]) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, value);
    }
    format!("{base}?{}", serializer.finish())
}

fn escape_like(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn normalize_pagination(
    requested_limit: Option<i64>,
    requested_offset: Option<i64>,
    default_limit: i64,
    max_limit: i64,
) -> PaginationInput {
    PaginationInput {
        limit: requested_limit.unwrap_or(default_limit).clamp(1, max_limit),
        offset: requested_offset.unwrap_or(0).max(0),
    }
}

fn retry_backoff_minutes(next_retry_count: i32) -> i32 {
    let exponent = next_retry_count.saturating_sub(1) as u32;
    2_i32.pow(exponent).min(60)
}

fn sql_host_expr(column: &str) -> String {
    format!(
        "
        CASE
            WHEN instr({column}, '://') > 0 THEN
                CASE
                    WHEN instr(substr({column}, instr({column}, '://') + 3), '/') > 0 THEN
                        substr(
                            substr({column}, instr({column}, '://') + 3),
                            1,
                            instr(substr({column}, instr({column}, '://') + 3), '/') - 1
                        )
                    ELSE substr({column}, instr({column}, '://') + 3)
                END
            ELSE ''
        END
        "
    )
}

fn truncate(input: &str, max_len: usize) -> String {
    input.chars().take(max_len).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::connection::SimpleConnection;

    fn setup_connection() -> SqliteConnection {
        let mut conn = SqliteConnection::establish(":memory:").expect("in-memory sqlite");
        conn.batch_execute(
            "
            CREATE TABLE work_unit(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              url VARCHAR NOT NULL UNIQUE,
              status VARCHAR NOT NULL DEFAULT 'pending',
              retry_count INTEGER NOT NULL DEFAULT 0,
              next_attempt_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              last_attempt_at VARCHAR,
              last_error VARCHAR,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX idx_work_unit_status_next_attempt_at ON work_unit(status, next_attempt_at);
            CREATE TABLE page(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              title VARCHAR NOT NULL,
              url VARCHAR NOT NULL UNIQUE,
              links VARCHAR NOT NULL,
              emails VARCHAR NOT NULL,
              coins VARCHAR NOT NULL,
              language VARCHAR NOT NULL DEFAULT '',
              last_scanned_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE page_link(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              source_page_id INTEGER NOT NULL,
              target_url VARCHAR NOT NULL,
              target_host VARCHAR NOT NULL DEFAULT '',
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(source_page_id, target_url)
            );
            CREATE TABLE page_email(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              page_id INTEGER NOT NULL,
              email VARCHAR NOT NULL,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(page_id, email)
            );
            CREATE TABLE page_crypto(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              page_id INTEGER NOT NULL,
              asset_type VARCHAR NOT NULL,
              reference VARCHAR NOT NULL,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(page_id, asset_type, reference)
            );
            ",
        )
        .expect("schema setup");
        conn
    }

    fn alpha_snapshot() -> PageSnapshot {
        PageSnapshot {
            title: "Alpha Market".to_string(),
            url: "http://alpha.onion".to_string(),
            language: "English".to_string(),
            links: vec![LinkObservation {
                target_url: "http://beta.onion".to_string(),
                target_host: "beta.onion".to_string(),
            }],
            emails: vec!["team@shared.test".to_string()],
            crypto_refs: vec![CryptoReference {
                asset_type: "bitcoin".to_string(),
                reference: "bc1qalpha000000000000000000000000000000000".to_string(),
            }],
        }
    }

    fn beta_snapshot() -> PageSnapshot {
        PageSnapshot {
            title: "Beta Forum".to_string(),
            url: "http://beta.onion".to_string(),
            language: "French".to_string(),
            links: vec![LinkObservation {
                target_url: "http://alpha.onion".to_string(),
                target_host: "alpha.onion".to_string(),
            }],
            emails: vec!["team@shared.test".to_string()],
            crypto_refs: vec![
                CryptoReference {
                    asset_type: "bitcoin".to_string(),
                    reference: "bc1qalpha000000000000000000000000000000000".to_string(),
                },
                CryptoReference {
                    asset_type: "ethereum".to_string(),
                    reference: "0x2222222222222222222222222222222222222222".to_string(),
                },
            ],
        }
    }

    #[test]
    fn work_units_are_inserted_idempotently() {
        let mut conn = setup_connection();

        create_work_unit(&mut conn, "https://example.com").expect("first insert");
        create_work_unit(&mut conn, "https://example.com").expect("duplicate insert");

        let work_units = list_work_units(&mut conn, None, None).expect("load work units");
        assert_eq!(work_units.items.len(), 1);
        assert_eq!(work_units.items[0].status, STATUS_PENDING);
    }

    #[test]
    fn transient_failures_are_rescheduled_then_exhausted() {
        let mut conn = setup_connection();

        create_work_unit(&mut conn, "https://broken.example").expect("insert work unit");
        let work_unit = list_work_units(&mut conn, None, None)
            .expect("load work units")
            .items
            .remove(0);
        record_work_unit_failure(&mut conn, work_unit.id, "network timeout", true)
            .expect("retryable failure");

        let updated = list_work_units(&mut conn, None, None)
            .expect("reload work units")
            .items
            .remove(0);
        assert_eq!(updated.status, STATUS_PENDING);
        assert_eq!(updated.retry_count, 1);
        assert_eq!(updated.last_error.as_deref(), Some("network timeout"));
        assert!(get_pending_work_units(&mut conn)
            .expect("due work units")
            .is_empty());

        for _ in 0..(MAX_RETRY_ATTEMPTS - 1) {
            record_work_unit_failure(&mut conn, work_unit.id, "network timeout", true)
                .expect("subsequent retryable failure");
        }

        let exhausted = list_work_units(&mut conn, None, None)
            .expect("reload exhausted work unit")
            .items
            .remove(0);
        assert_eq!(exhausted.status, STATUS_FAILED);
        assert_eq!(exhausted.retry_count, MAX_RETRY_ATTEMPTS);
    }

    #[test]
    fn permanent_failures_are_terminal() {
        let mut conn = setup_connection();

        create_work_unit(&mut conn, "notaurl").expect("insert work unit");
        let work_unit = list_work_units(&mut conn, None, None)
            .expect("load work units")
            .items
            .remove(0);
        record_work_unit_failure(&mut conn, work_unit.id, "invalid url", false)
            .expect("terminal failure");

        let updated = list_work_units(&mut conn, None, None)
            .expect("reload work units")
            .items
            .remove(0);
        assert_eq!(updated.status, STATUS_FAILED);
        assert_eq!(updated.retry_count, 1);
    }

    #[test]
    fn page_relations_entities_search_and_pagination_are_available() {
        let mut conn = setup_connection();

        save_page_info(&mut conn, &alpha_snapshot()).expect("save alpha");
        save_page_info(&mut conn, &beta_snapshot()).expect("save beta");
        create_work_unit(&mut conn, "http://pending.onion").expect("insert work unit");

        let summaries = list_page_summaries(&mut conn, Some(1), Some(1)).expect("page summaries");
        assert_eq!(summaries.total_count, 2);
        assert_eq!(summaries.items.len(), 1);

        let all_summaries = list_page_summaries(&mut conn, None, None).expect("full summaries");
        let alpha = all_summaries
            .items
            .iter()
            .find(|item| item.url == "http://alpha.onion")
            .expect("alpha summary");
        assert_eq!(alpha.outbound_link_count, 1);
        assert_eq!(alpha.email_count, 1);
        assert_eq!(alpha.crypto_count, 1);

        let detail = get_page_detail(&mut conn, alpha.id)
            .expect("page detail")
            .expect("alpha detail");
        assert_eq!(detail.outgoing_links.len(), 1);
        assert_eq!(detail.incoming_links.len(), 1);
        assert_eq!(detail.emails[0].value, "team@shared.test");
        assert_eq!(detail.crypto_refs.len(), 1);

        let email_entities = list_email_entities(&mut conn, None, None).expect("email entities");
        assert_eq!(email_entities.items[0].page_count, 2);

        let email_detail = get_email_entity_detail(&mut conn, "team@shared.test")
            .expect("email detail")
            .expect("email detail exists");
        assert_eq!(email_detail.pages.len(), 2);

        let crypto_detail = get_crypto_entity_detail(
            &mut conn,
            "bitcoin",
            "bc1qalpha000000000000000000000000000000000",
        )
        .expect("crypto detail")
        .expect("crypto detail exists");
        assert_eq!(crypto_detail.pages.len(), 2);

        let relationships = list_site_relationships(&mut conn, None, None).expect("relationships");
        assert_eq!(relationships.items.len(), 2);

        let stats = collect_stats(&mut conn).expect("collect stats");
        assert_eq!(stats.total_pages, 2);
        assert_eq!(stats.total_domains, 2);
        assert_eq!(stats.pending_work_units, 1);
        assert_eq!(stats.failed_work_units, 0);
        assert_ne!(stats.last_scrape, "Never");

        let search_results = search_pages(
            &mut conn,
            "0x2222222222222222222222222222222222222222",
            Some(5),
        )
        .expect("search pages");
        assert_eq!(search_results.len(), 1);
        assert_eq!(search_results[0].title, "Beta Forum");
    }

    #[test]
    fn page_detail_links_are_url_encoded() {
        let mut conn = setup_connection();

        let mut snapshot = alpha_snapshot();
        snapshot.emails = vec!["ops+intel@alpha.onion".to_string()];
        save_page_info(&mut conn, &snapshot).expect("save alpha");

        let summary = list_page_summaries(&mut conn, None, None)
            .expect("summaries")
            .items
            .remove(0);
        let detail = get_page_detail(&mut conn, summary.id)
            .expect("detail")
            .expect("detail exists");

        assert_eq!(
            detail.emails[0].detail_url,
            "/entities/emails?value=ops%2Bintel%40alpha.onion"
        );
    }

    #[test]
    fn rescanning_a_page_replaces_child_observations() {
        let mut conn = setup_connection();

        save_page_info(&mut conn, &alpha_snapshot()).expect("save alpha");

        let mut rescanned = alpha_snapshot();
        rescanned.emails = vec!["ops@alpha.onion".to_string()];
        rescanned.crypto_refs = vec![CryptoReference {
            asset_type: "ethereum".to_string(),
            reference: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        }];
        rescanned.links = vec![LinkObservation {
            target_url: "http://gamma.onion".to_string(),
            target_host: "gamma.onion".to_string(),
        }];
        save_page_info(&mut conn, &rescanned).expect("resave alpha");

        let summary = list_page_summaries(&mut conn, None, None)
            .expect("summaries")
            .items
            .remove(0);
        let detail = get_page_detail(&mut conn, summary.id)
            .expect("detail")
            .expect("detail exists");

        assert_eq!(detail.emails[0].value, "ops@alpha.onion");
        assert_eq!(detail.crypto_refs.len(), 1);
        assert_eq!(detail.crypto_refs[0].asset_type, "ethereum");
        assert_eq!(detail.outgoing_links.len(), 1);

        assert!(get_email_entity_detail(&mut conn, "team@shared.test")
            .expect("old email detail")
            .is_none());
        assert!(get_crypto_entity_detail(
            &mut conn,
            "bitcoin",
            "bc1qalpha000000000000000000000000000000000",
        )
        .expect("old crypto detail")
        .is_none());
    }

    #[test]
    fn retry_backfill_migration_populates_relationship_tables() {
        let mut conn = SqliteConnection::establish(":memory:").expect("in-memory sqlite");
        conn.batch_execute(
            "
            CREATE TABLE work_unit(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              url VARCHAR NOT NULL UNIQUE,
              status VARCHAR NOT NULL DEFAULT 'pending',
              retry_count INTEGER NOT NULL DEFAULT 0,
              last_error VARCHAR,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE page(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              title VARCHAR NOT NULL,
              url VARCHAR NOT NULL UNIQUE,
              links VARCHAR NOT NULL,
              emails VARCHAR NOT NULL,
              coins VARCHAR NOT NULL,
              language VARCHAR NOT NULL DEFAULT '',
              last_scanned_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE page_link(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              source_page_id INTEGER NOT NULL,
              target_url VARCHAR NOT NULL,
              target_host VARCHAR NOT NULL DEFAULT '',
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(source_page_id, target_url)
            );
            CREATE TABLE page_email(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              page_id INTEGER NOT NULL,
              email VARCHAR NOT NULL,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(page_id, email)
            );
            CREATE TABLE page_crypto(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              page_id INTEGER NOT NULL,
              asset_type VARCHAR NOT NULL,
              reference VARCHAR NOT NULL,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(page_id, asset_type, reference)
            );
            INSERT INTO work_unit(url, status) VALUES ('http://legacy.onion', 'pending');
            INSERT INTO page(title, url, links, emails, coins, language, last_scanned_at, created_at)
            VALUES (
              'Legacy Page',
              'http://legacy.onion',
              'http://beta.onion/about,http://gamma.onion',
              'intel+ops@legacy.onion,team@legacy.onion',
              'bitcoin:bc1qlegacy00000000000000000000000000000000,ethereum:0x3333333333333333333333333333333333333333',
              '',
              CURRENT_TIMESTAMP,
              CURRENT_TIMESTAMP
            );
            ",
        )
        .expect("legacy schema setup");

        conn.batch_execute(include_str!(
            "../migrations/2026-04-20-130000_retry_queue_and_backfill/up.sql"
        ))
        .expect("retry/backfill migration");

        let link_count =
            scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_link").expect("link count");
        let email_count = scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_email")
            .expect("email count");
        let crypto_count = scalar_count(&mut conn, "SELECT COUNT(*) AS count FROM page_crypto")
            .expect("crypto count");

        assert_eq!(link_count, 2);
        assert_eq!(email_count, 2);
        assert_eq!(crypto_count, 2);

        let migrated_work_unit = list_work_units(&mut conn, None, None)
            .expect("work units")
            .items
            .remove(0);
        assert_eq!(
            migrated_work_unit.next_attempt_at,
            migrated_work_unit.created_at
        );
        assert!(migrated_work_unit.last_attempt_at.is_none());
    }
}
