use anyhow::{Context, Result};
use reqwest::blocking::Client;
use reqwest::{Proxy, StatusCode};
use spyder::extraction::extract_page_snapshot;
use spyder::{
    create_work_unit, establish_connection, get_pending_work_units, mark_work_unit_as_done,
    record_work_unit_failure, save_page_info,
};
use std::env;
use std::time::Duration;
use url::Url;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailureKind {
    Retriable,
    Permanent,
}

struct CrawlFailure {
    error: anyhow::Error,
    kind: FailureKind,
}

impl CrawlFailure {
    fn retriable(error: anyhow::Error) -> Self {
        Self {
            error,
            kind: FailureKind::Retriable,
        }
    }

    fn permanent(error: anyhow::Error) -> Self {
        Self {
            error,
            kind: FailureKind::Permanent,
        }
    }
}

fn fetch_page_snapshot(
    client: &Client,
    url: &str,
) -> std::result::Result<spyder::models::PageSnapshot, CrawlFailure> {
    let body = fetch_body(client, url)?;
    extract_page_snapshot(url, &body)
        .map_err(|error| CrawlFailure::permanent(error.context(format!("failed to parse {url}"))))
}

fn fetch_body(client: &Client, url: &str) -> std::result::Result<String, CrawlFailure> {
    let response = client.get(url).send().map_err(|error| {
        let wrapped = anyhow::Error::new(error).context(format!("request failed for {url}"));
        if is_retriable_request_error(&wrapped) {
            CrawlFailure::retriable(wrapped)
        } else {
            CrawlFailure::permanent(wrapped)
        }
    })?;
    let status = response.status();
    if !status.is_success() {
        let error = anyhow::anyhow!("non-success status {} for {}", status.as_u16(), url);
        return if is_retriable_status(status) {
            Err(CrawlFailure::retriable(error))
        } else {
            Err(CrawlFailure::permanent(error))
        };
    }

    response.text().map_err(|error| {
        let wrapped =
            anyhow::Error::new(error).context(format!("failed to read response body for {url}"));
        CrawlFailure::retriable(wrapped)
    })
}

fn enqueue_seed_and_links(client: &Client, url: &str) -> Result<usize> {
    let mut connection = establish_connection()?;
    create_work_unit(&mut connection, url)?;

    Url::parse(url).with_context(|| format!("invalid url: {url}"))?;
    let snapshot = fetch_page_snapshot(client, url)
        .map_err(|failure| failure.error)
        .with_context(|| format!("unable to discover links for seed {url}"))?;

    let mut inserted = 1;
    for discovered_url in snapshot
        .links
        .into_iter()
        .map(|item| item.target_url)
        .collect::<std::collections::HashSet<_>>()
    {
        let parsed_discovered = Url::parse(&discovered_url)
            .with_context(|| format!("invalid discovered url from {url}: {discovered_url}"))?;
        match parsed_discovered.scheme() {
            "http" | "https" => {
                create_work_unit(&mut connection, parsed_discovered.as_str())?;
                inserted += 1;
            }
            _ => {}
        }
    }

    Ok(inserted)
}

fn work_queue(client: &Client) -> Result<()> {
    let mut connection = establish_connection()?;
    let work_units = get_pending_work_units(&mut connection)?;

    println!("Working with {} pending work units", work_units.len());
    for work_unit in work_units {
        println!("Processing {}", work_unit.url);

        match fetch_page_snapshot(client, &work_unit.url) {
            Ok(page_snapshot) => {
                save_page_info(&mut connection, &page_snapshot)?;
                let discovered_count = enqueue_discovered_links(&mut connection, &page_snapshot)?;
                mark_work_unit_as_done(&mut connection, work_unit.id)?;
                println!(
                    "Stored page and queued {} discovered URLs",
                    discovered_count
                );
            }
            Err(failure) => {
                eprintln!(
                    "ERROR: couldn't extract page information: {:?}",
                    failure.error
                );
                record_work_unit_failure(
                    &mut connection,
                    work_unit.id,
                    &failure.error.to_string(),
                    failure.kind == FailureKind::Retriable,
                )?;
            }
        }
    }

    Ok(())
}

fn enqueue_discovered_links(
    connection: &mut diesel::sqlite::SqliteConnection,
    snapshot: &spyder::models::PageSnapshot,
) -> Result<usize> {
    let mut created = 0;
    for link in &snapshot.links {
        let parsed = Url::parse(&link.target_url)
            .with_context(|| format!("invalid discovered url: {}", link.target_url))?;
        match parsed.scheme() {
            "http" | "https" => {
                create_work_unit(connection, parsed.as_str())?;
                created += 1;
            }
            _ => {}
        }
    }
    Ok(created)
}

fn usage(program: &str) {
    eprintln!("Usage: {program} [SUBCOMMAND] [OPTIONS]");
    eprintln!("Subcommands:");
    eprintln!("    add <url>      enqueue the seed page and discovered links.");
    eprintln!("    work           process pending work units and store page metadata.");
}

fn print_error(error: &anyhow::Error) {
    eprintln!("ERROR: {error:?}");

    if error
        .chain()
        .any(|cause| cause.to_string().contains("no such table:"))
    {
        eprintln!(
            "HINT: database schema is missing. Run `diesel setup` and `diesel migration run`."
        );
    }
}

fn build_http_client() -> Result<Client> {
    let mut builder = Client::builder()
        .timeout(Duration::from_secs(15))
        .no_proxy();
    if let Some(proxy_url) = configured_proxy_url() {
        builder = builder.proxy(
            Proxy::all(&proxy_url).with_context(|| format!("invalid proxy url: {proxy_url}"))?,
        );
    }

    builder.build().context("http client should build")
}

fn configured_proxy_url() -> Option<String> {
    configured_proxy_url_from_values(env::var("ALL_PROXY").ok(), env::var("all_proxy").ok())
}

fn configured_proxy_url_from_values(
    upper: Option<String>,
    lower: Option<String>,
) -> Option<String> {
    upper
        .into_iter()
        .chain(lower)
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

fn is_retriable_request_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(reqwest_error) = cause.downcast_ref::<reqwest::Error>() {
            reqwest_error.is_timeout() || reqwest_error.is_connect() || reqwest_error.is_request()
        } else {
            false
        }
    })
}

fn is_retriable_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn main() {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "spyder".to_string());
    let client = match build_http_client() {
        Ok(client) => client,
        Err(error) => {
            print_error(&error);
            std::process::exit(1);
        }
    };

    let result = match args.next().as_deref() {
        Some("add") => match args.next() {
            Some(url) => enqueue_seed_and_links(&client, &url).map(|count| {
                println!("Enqueued {count} URLs");
            }),
            None => {
                usage(&program);
                Err(anyhow::anyhow!("no url is provided"))
            }
        },
        Some("work") => work_queue(&client),
        Some(_) | None => {
            usage(&program);
            Err(anyhow::anyhow!("invalid or missing subcommand"))
        }
    };

    if let Err(error) = result {
        print_error(&error);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_prefers_uppercase_then_lowercase() {
        assert_eq!(
            configured_proxy_url_from_values(
                Some("socks5h://upper:9050".to_string()),
                Some("socks5h://lower:9050".to_string()),
            ),
            Some("socks5h://upper:9050".to_string())
        );
        assert_eq!(
            configured_proxy_url_from_values(None, Some(" socks5h://lower:9050 ".to_string())),
            Some("socks5h://lower:9050".to_string())
        );
        assert_eq!(
            configured_proxy_url_from_values(None, Some("   ".to_string())),
            None
        );
    }

    #[test]
    fn retryable_statuses_match_transient_http_failures() {
        assert!(is_retriable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retriable_status(StatusCode::BAD_GATEWAY));
        assert!(!is_retriable_status(StatusCode::NOT_FOUND));
    }
}
