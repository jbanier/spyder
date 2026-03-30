pub mod models;
pub mod schema;

use anyhow::{Context, Result};
use diesel::dsl::count_star;
use diesel::prelude::*;
use diesel::upsert::excluded;
use dotenvy::dotenv;
use models::*;
use reqwest::Url;
use std::collections::HashSet;
use std::env;

pub const STATUS_PENDING: &str = "pending";
pub const STATUS_DONE: &str = "done";
pub const STATUS_FAILED: &str = "failed";

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

pub fn save_page_info(conn: &mut SqliteConnection, new_page: &NewPage) -> Result<()> {
    use crate::schema::page::dsl::{
        coins as page_coins, emails as page_emails, links as page_links, title as page_title,
        url as page_url,
    };

    diesel::insert_into(crate::schema::page::table)
        .values(new_page)
        .on_conflict(page_url)
        .do_update()
        .set((
            page_title.eq(excluded(page_title)),
            page_links.eq(excluded(page_links)),
            page_emails.eq(excluded(page_emails)),
            page_coins.eq(excluded(page_coins)),
        ))
        .execute(conn)
        .context("error saving page")?;

    Ok(())
}

pub fn mark_work_unit_as_done(conn: &mut SqliteConnection, work_unit_id: i32) -> Result<()> {
    use crate::schema::work_unit::dsl::*;

    diesel::update(crate::schema::work_unit::table.filter(id.eq(work_unit_id)))
        .set((
            status.eq(STATUS_DONE),
            last_error.eq::<Option<String>>(None),
        ))
        .execute(conn)
        .context("error updating work unit status")?;

    Ok(())
}

pub fn mark_work_unit_as_failed(
    conn: &mut SqliteConnection,
    work_unit_id: i32,
    error_message: &str,
) -> Result<()> {
    use crate::schema::work_unit::dsl::*;

    let bounded_error = truncate(error_message, 500);
    diesel::update(crate::schema::work_unit::table.filter(id.eq(work_unit_id)))
        .set((
            status.eq(STATUS_FAILED),
            retry_count.eq(retry_count + 1),
            last_error.eq(Some(bounded_error)),
        ))
        .execute(conn)
        .context("error marking work unit as failed")?;

    Ok(())
}

pub fn get_pending_work_units(conn: &mut SqliteConnection) -> Result<Vec<WorkUnit>> {
    use crate::schema::work_unit::dsl::*;

    crate::schema::work_unit::table
        .filter(status.eq(STATUS_PENDING))
        .select(WorkUnit::as_select())
        .load(conn)
        .context("error querying pending work units")
}

pub fn list_work_units(conn: &mut SqliteConnection) -> Result<Vec<WorkUnit>> {
    crate::schema::work_unit::table
        .order(crate::schema::work_unit::id.desc())
        .select(WorkUnit::as_select())
        .load(conn)
        .context("error querying work units")
}

pub fn list_pages(conn: &mut SqliteConnection) -> Result<Vec<Page>> {
    crate::schema::page::table
        .order(crate::schema::page::id.desc())
        .select(Page::as_select())
        .load(conn)
        .context("error querying pages")
}

pub fn collect_stats(conn: &mut SqliteConnection) -> Result<Stats> {
    use crate::schema::page::dsl as page_dsl;
    use crate::schema::work_unit::dsl as work_dsl;

    let total_pages = page_dsl::page
        .select(count_star())
        .first(conn)
        .context("error counting pages")?;

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

    let pages = page_dsl::page
        .order(page_dsl::id.desc())
        .select(Page::as_select())
        .load(conn)
        .context("error loading pages for stats")?;

    let mut domains = HashSet::new();
    for page in &pages {
        if let Ok(parsed) = Url::parse(&page.url) {
            if let Some(host) = parsed.host_str() {
                domains.insert(host.to_string());
            }
        }
    }

    let last_scrape = pages
        .first()
        .map(|page| page.created_at.clone())
        .unwrap_or_else(|| "Never".to_string());

    Ok(Stats {
        total_pages,
        total_domains: domains.len(),
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
    use crate::schema::page::dsl::*;

    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let pattern = format!("%{}%", trimmed.replace('%', "\\%").replace('_', "\\_"));
    let search_limit = requested_limit.unwrap_or(10).clamp(1, 50);

    let results = crate::schema::page::table
        .filter(
            title
                .like(&pattern)
                .or(url.like(&pattern))
                .or(emails.like(&pattern)),
        )
        .order(id.desc())
        .limit(search_limit)
        .select(Page::as_select())
        .load(conn)
        .context("error searching pages")?;

    Ok(results
        .into_iter()
        .map(|item| SearchResult {
            title: item.title,
            url: item.url,
            scraped_at: item.created_at,
        })
        .collect())
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
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            ",
        )
        .expect("schema setup");
        conn
    }

    #[test]
    fn work_units_are_inserted_idempotently() {
        let mut conn = setup_connection();

        create_work_unit(&mut conn, "https://example.com").expect("first insert");
        create_work_unit(&mut conn, "https://example.com").expect("duplicate insert");

        let work_units = list_work_units(&mut conn).expect("load work units");
        assert_eq!(work_units.len(), 1);
        assert_eq!(work_units[0].status, STATUS_PENDING);
    }

    #[test]
    fn failed_work_unit_tracks_error_and_retries() {
        let mut conn = setup_connection();

        create_work_unit(&mut conn, "https://broken.example").expect("insert work unit");
        let work_unit = list_work_units(&mut conn)
            .expect("load work units")
            .remove(0);
        mark_work_unit_as_failed(&mut conn, work_unit.id, "network timeout").expect("mark failed");

        let updated = list_work_units(&mut conn)
            .expect("reload work units")
            .remove(0);
        assert_eq!(updated.status, STATUS_FAILED);
        assert_eq!(updated.retry_count, 1);
        assert_eq!(updated.last_error.as_deref(), Some("network timeout"));
    }

    #[test]
    fn stats_and_search_reflect_stored_pages() {
        let mut conn = setup_connection();

        save_page_info(
            &mut conn,
            &NewPage {
                title: "Example Domain".to_string(),
                url: "https://example.com".to_string(),
                links: "https://example.com/about".to_string(),
                emails: "team@example.com".to_string(),
                coins: "".to_string(),
            },
        )
        .expect("save page");
        save_page_info(
            &mut conn,
            &NewPage {
                title: "Rust Lang".to_string(),
                url: "https://www.rust-lang.org".to_string(),
                links: "https://www.rust-lang.org/learn".to_string(),
                emails: "community@rust-lang.org".to_string(),
                coins: "".to_string(),
            },
        )
        .expect("save page");
        create_work_unit(&mut conn, "https://pending.example").expect("insert work unit");

        let stats = collect_stats(&mut conn).expect("collect stats");
        assert_eq!(stats.total_pages, 2);
        assert_eq!(stats.total_domains, 2);
        assert_eq!(stats.pending_work_units, 1);
        assert_eq!(stats.failed_work_units, 0);
        assert_ne!(stats.last_scrape, "Never");

        let search_results = search_pages(&mut conn, "rust", Some(5)).expect("search pages");
        assert_eq!(search_results.len(), 1);
        assert_eq!(search_results[0].title, "Rust Lang");
    }
}
