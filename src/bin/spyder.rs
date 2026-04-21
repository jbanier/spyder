use anyhow::{Context, Result};
use reqwest::blocking::Client;
use reqwest::{Proxy, StatusCode};
use spyder::extraction::extract_page_snapshot;
use spyder::{
    add_domain_blacklist_entry, create_work_unit, establish_connection,
    find_matching_blacklist_domain, get_pending_work_units, list_domain_blacklist_rules,
    mark_work_unit_as_done, record_work_unit_failure, remove_domain_blacklist_entry,
    save_page_info,
};
use std::env;
use std::fmt::Display;
use std::time::Duration;
use url::Url;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailureKind {
    Retriable,
    Permanent,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct WorkOptions {
    onion_only: bool,
}

struct CrawlFailure {
    error: anyhow::Error,
    kind: FailureKind,
}

struct DiscoveryEnqueueOutcome {
    queued_count: usize,
    skipped_blacklisted_count: usize,
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

fn print_status(message: impl Display) {
    println!("==> {message}");
}

fn print_progress(current: usize, total: usize, message: impl Display) {
    println!("[{current}/{total}] {message}");
}

fn count_label(count: usize, singular: &str, plural: &str) -> String {
    format!("{} {}", count, if count == 1 { singular } else { plural })
}

fn compact_for_terminal(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return normalized;
    }

    let truncated = normalized.chars().take(max_chars).collect::<String>();
    if normalized.chars().count() > max_chars {
        format!("{truncated}...")
    } else {
        normalized
    }
}

fn summarize_page_snapshot(snapshot: &spyder::models::PageSnapshot) -> String {
    let mut parts = vec![
        count_label(snapshot.links.len(), "link", "links"),
        count_label(snapshot.emails.len(), "email", "emails"),
        count_label(snapshot.crypto_refs.len(), "crypto ref", "crypto refs"),
    ];
    let language = compact_for_terminal(&snapshot.language, 24);
    if !language.is_empty() {
        parts.push(format!("language {language}"));
    }

    let title = compact_for_terminal(&snapshot.title, 48);
    if title.is_empty() {
        parts.join(", ")
    } else {
        format!("title \"{title}\", {}", parts.join(", "))
    }
}

fn failure_kind_label(kind: FailureKind) -> &'static str {
    match kind {
        FailureKind::Retriable => "retriable",
        FailureKind::Permanent => "permanent",
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
    print_status(format!("Queueing seed URL {url}"));
    create_work_unit(&mut connection, url)?;

    Url::parse(url).with_context(|| format!("invalid url: {url}"))?;
    print_status(format!("Fetching seed page {url}"));
    let snapshot = fetch_page_snapshot(client, url)
        .map_err(|failure| failure.error)
        .with_context(|| format!("unable to discover links for seed {url}"))?;
    print_status(format!("Extracted {}", summarize_page_snapshot(&snapshot)));
    print_status("Queueing discovered links from the seed page");
    let outcome = enqueue_discovered_links(&mut connection, &snapshot)?;
    println!(
        "Queued {} discovered URLs from the seed page",
        outcome.queued_count
    );
    if outcome.skipped_blacklisted_count > 0 {
        println!(
            "Skipped {} blacklisted discovered URLs",
            outcome.skipped_blacklisted_count
        );
    }

    Ok(1 + outcome.queued_count)
}

fn work_queue(client: &Client, options: WorkOptions) -> Result<()> {
    let mut connection = establish_connection()?;
    print_status("Loading pending work units");
    let pending_work_units = get_pending_work_units(&mut connection)?;
    let pending_count = pending_work_units.len();
    let work_units = select_work_units_for_processing(pending_work_units, options);

    if work_units.is_empty() {
        if options.onion_only {
            println!(
                "No pending .onion work units to process ({} non-onion pending URLs skipped)",
                pending_count
            );
        } else {
            println!("No pending work units to process");
        }
        return Ok(());
    }

    let total = work_units.len();
    if options.onion_only {
        let skipped_count = pending_count - work_units.len();
        println!(
            "Working with {} pending work units whose host ends in .onion ({} skipped)",
            work_units.len(),
            skipped_count
        );
    } else {
        println!("Working with {} pending work units", work_units.len());
    }
    for (index, work_unit) in work_units.into_iter().enumerate() {
        let current = index + 1;
        print_progress(current, total, format!("Fetching {}", work_unit.url));

        match fetch_page_snapshot(client, &work_unit.url) {
            Ok(page_snapshot) => {
                print_progress(
                    current,
                    total,
                    format!("Extracted {}", summarize_page_snapshot(&page_snapshot)),
                );
                save_page_info(&mut connection, &page_snapshot)?;
                let discovery_outcome = enqueue_discovered_links(&mut connection, &page_snapshot)?;
                mark_work_unit_as_done(&mut connection, work_unit.id)?;
                print_progress(
                    current,
                    total,
                    format!(
                        "Stored page and queued {} discovered URLs",
                        discovery_outcome.queued_count
                    ),
                );
                if discovery_outcome.skipped_blacklisted_count > 0 {
                    print_progress(
                        current,
                        total,
                        format!(
                            "Skipped {} blacklisted discovered URLs",
                            discovery_outcome.skipped_blacklisted_count
                        ),
                    );
                }
            }
            Err(failure) => {
                eprintln!(
                    "[{current}/{total}] Failed to process {} ({})",
                    work_unit.url,
                    failure_kind_label(failure.kind)
                );
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
                eprintln!(
                    "[{current}/{total}] Recorded failure state for {}",
                    work_unit.url
                );
            }
        }
    }

    Ok(())
}

fn select_work_units_for_processing(
    work_units: Vec<spyder::models::WorkUnit>,
    options: WorkOptions,
) -> Vec<spyder::models::WorkUnit> {
    work_units
        .into_iter()
        .filter(|work_unit| !options.onion_only || url_targets_onion(&work_unit.url))
        .collect()
}

fn url_targets_onion(url: &str) -> bool {
    Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed
                .host_str()
                .map(|host| host.trim_end_matches('.').to_string())
        })
        .map(|host| host.to_ascii_lowercase().ends_with(".onion"))
        .unwrap_or(false)
}

fn enqueue_discovered_links(
    connection: &mut diesel::sqlite::SqliteConnection,
    snapshot: &spyder::models::PageSnapshot,
) -> Result<DiscoveryEnqueueOutcome> {
    let blacklist_domains = list_domain_blacklist_rules(connection)?
        .into_iter()
        .map(|rule| rule.domain)
        .collect::<Vec<_>>();
    let mut outcome = DiscoveryEnqueueOutcome {
        queued_count: 0,
        skipped_blacklisted_count: 0,
    };

    for link in &snapshot.links {
        let parsed = Url::parse(&link.target_url)
            .with_context(|| format!("invalid discovered url: {}", link.target_url))?;
        match parsed.scheme() {
            "http" | "https" => {
                let link_host = parsed
                    .host_str()
                    .map(|value| value.to_ascii_lowercase())
                    .unwrap_or_else(|| link.target_host.to_ascii_lowercase());
                if find_matching_blacklist_domain(&link_host, &blacklist_domains).is_some() {
                    outcome.skipped_blacklisted_count += 1;
                    continue;
                }
                create_work_unit(connection, parsed.as_str())?;
                outcome.queued_count += 1;
            }
            _ => {}
        }
    }
    Ok(outcome)
}

fn list_blacklist() -> Result<()> {
    let mut connection = establish_connection()?;
    let entries = list_domain_blacklist_rules(&mut connection)?;

    if entries.is_empty() {
        println!("No blacklisted domains configured");
        return Ok(());
    }

    for entry in entries {
        println!("{}", entry.domain);
    }
    Ok(())
}

fn add_blacklist_domain(raw_domain: &str) -> Result<()> {
    let mut connection = establish_connection()?;
    let entry = add_domain_blacklist_entry(&mut connection, raw_domain)?;
    println!("Blacklisted {}", entry.domain);
    Ok(())
}

fn remove_blacklist_domain(raw_domain: &str) -> Result<()> {
    let mut connection = establish_connection()?;
    let domain = remove_domain_blacklist_entry(&mut connection, raw_domain)?;
    println!("Removed blacklist entry {}", domain);
    Ok(())
}

fn usage(program: &str) {
    eprintln!("Usage: {program} [SUBCOMMAND] [OPTIONS]");
    eprintln!("Subcommands:");
    eprintln!("    add <url>      enqueue the seed page and discovered links.");
    eprintln!("    blacklist list");
    eprintln!("    blacklist add <domain>");
    eprintln!("    blacklist remove <domain>");
    eprintln!("    work [--onion-only] process pending work units and store page metadata.");
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
    // Crawl targets may present self-signed, expired, or otherwise invalid certificates.
    let mut builder = Client::builder()
        .timeout(Duration::from_secs(15))
        .danger_accept_invalid_certs(true)
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

fn parse_work_options(args: impl IntoIterator<Item = String>) -> Result<WorkOptions> {
    let mut options = WorkOptions::default();

    for arg in args {
        match arg.as_str() {
            "--onion-only" => options.onion_only = true,
            _ => anyhow::bail!("invalid work option: {arg}"),
        }
    }

    Ok(options)
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
        Some("blacklist") => match args.next().as_deref() {
            Some("list") => list_blacklist(),
            Some("add") => match args.next() {
                Some(domain) => add_blacklist_domain(&domain),
                None => {
                    usage(&program);
                    Err(anyhow::anyhow!("no blacklist domain is provided"))
                }
            },
            Some("remove") => match args.next() {
                Some(domain) => remove_blacklist_domain(&domain),
                None => {
                    usage(&program);
                    Err(anyhow::anyhow!("no blacklist domain is provided"))
                }
            },
            Some(_) | None => {
                usage(&program);
                Err(anyhow::anyhow!("invalid or missing blacklist subcommand"))
            }
        },
        Some("work") => match parse_work_options(args) {
            Ok(options) => work_queue(&client, options),
            Err(error) => {
                usage(&program);
                Err(error)
            }
        },
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
    use diesel::connection::SimpleConnection;
    use diesel::Connection;

    fn setup_connection() -> diesel::sqlite::SqliteConnection {
        let mut conn =
            diesel::sqlite::SqliteConnection::establish(":memory:").expect("in-memory sqlite");
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
            CREATE TABLE domain_blacklist(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              domain VARCHAR NOT NULL UNIQUE,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            ",
        )
        .expect("schema setup");
        conn
    }

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

    #[test]
    fn compact_for_terminal_normalizes_whitespace_and_truncates() {
        assert_eq!(
            compact_for_terminal("  Alpha   Beta\tGamma  ", 10),
            "Alpha Beta..."
        );
    }

    #[test]
    fn snapshot_summary_includes_title_and_counts() {
        let snapshot = spyder::models::PageSnapshot {
            title: "Alpha Market".to_string(),
            url: "http://alpha.onion".to_string(),
            language: "English".to_string(),
            links: vec![spyder::models::LinkObservation {
                target_url: "http://beta.onion".to_string(),
                target_host: "beta.onion".to_string(),
            }],
            emails: vec!["ops@alpha.onion".to_string()],
            crypto_refs: vec![spyder::models::CryptoReference {
                asset_type: "btc".to_string(),
                reference: "bc1test".to_string(),
            }],
            classification_signals: spyder::models::ClassificationSignals::default(),
        };

        assert_eq!(
            summarize_page_snapshot(&snapshot),
            "title \"Alpha Market\", 1 link, 1 email, 1 crypto ref, language English"
        );
    }

    #[test]
    fn work_options_accept_onion_only_flag() {
        let options = parse_work_options(vec!["--onion-only".to_string()]).expect("work options");
        assert_eq!(options, WorkOptions { onion_only: true });
    }

    #[test]
    fn work_options_reject_unknown_flags() {
        let error = parse_work_options(vec!["--bogus".to_string()]).expect_err("invalid option");
        assert_eq!(error.to_string(), "invalid work option: --bogus");
    }

    #[test]
    fn onion_only_work_selection_skips_non_onion_urls() {
        let mut conn = setup_connection();
        create_work_unit(&mut conn, "http://alpha.onion").expect("insert onion work unit");
        create_work_unit(&mut conn, "https://example.com").expect("insert clearnet work unit");
        create_work_unit(&mut conn, "notaurl").expect("insert invalid work unit");

        let selected = select_work_units_for_processing(
            get_pending_work_units(&mut conn).expect("pending work units"),
            WorkOptions { onion_only: true },
        );

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].url, "http://alpha.onion");
    }

    #[test]
    fn discovered_blacklisted_links_are_not_queued() {
        let mut conn = setup_connection();
        add_domain_blacklist_entry(&mut conn, "blocked.onion").expect("add blacklist");

        let snapshot = spyder::models::PageSnapshot {
            title: "Seed".to_string(),
            url: "http://seed.onion".to_string(),
            language: "English".to_string(),
            links: vec![
                spyder::models::LinkObservation {
                    target_url: "http://allowed.onion".to_string(),
                    target_host: "allowed.onion".to_string(),
                },
                spyder::models::LinkObservation {
                    target_url: "http://sub.blocked.onion".to_string(),
                    target_host: "sub.blocked.onion".to_string(),
                },
            ],
            emails: Vec::new(),
            crypto_refs: Vec::new(),
            classification_signals: spyder::models::ClassificationSignals::default(),
        };

        let outcome = enqueue_discovered_links(&mut conn, &snapshot).expect("enqueue links");
        assert_eq!(outcome.queued_count, 1);
        assert_eq!(outcome.skipped_blacklisted_count, 1);

        let pending = get_pending_work_units(&mut conn).expect("pending work units");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].url, "http://allowed.onion/");
    }
}
