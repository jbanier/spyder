use anyhow::{Context, Result};
use regex::Regex;
use reqwest::blocking::Client;
use reqwest::Url;
use scraper::{Html, Selector};
use spyder::models::NewPage;
use spyder::{
    create_work_unit, establish_connection, get_pending_work_units, mark_work_unit_as_done,
    mark_work_unit_as_failed, save_page_info,
};
use std::collections::HashSet;
use std::env;
use std::time::Duration;

fn extract_links(body: &str, base_url: &Url) -> Result<HashSet<String>> {
    let document = Html::parse_document(body);
    let selector = Selector::parse("a[href]").expect("valid selector");
    let mut discovered = HashSet::new();

    for element in document.select(&selector) {
        if let Some(raw_href) = element.value().attr("href") {
            if let Ok(url) = base_url.join(raw_href) {
                match url.scheme() {
                    "http" | "https" => {
                        discovered.insert(url.to_string());
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(discovered)
}

fn build_page(url: &str, body: &str) -> Result<NewPage> {
    let document = Html::parse_document(body);
    let selector = Selector::parse("title").expect("valid selector");

    let title_text = document
        .select(&selector)
        .next()
        .map(|title| title.text().collect::<Vec<_>>().join(" "))
        .filter(|title| !title.trim().is_empty())
        .unwrap_or_else(|| "no title".to_string());

    let email_regex = Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")?;
    let crypto_address_regex = Regex::new(r"(bitcoin|ethereum):[a-zA-Z0-9]+")?;
    let base_url = Url::parse(url).with_context(|| format!("invalid url: {url}"))?;

    let mut emails = HashSet::new();
    let mut coins = HashSet::new();
    let mut links = extract_links(body, &base_url)?;

    for caps in email_regex.captures_iter(body) {
        emails.insert(caps[0].to_string());
    }

    for caps in crypto_address_regex.captures_iter(body) {
        coins.insert(caps[0].to_string());
    }

    let mut link_list = links.drain().collect::<Vec<_>>();
    link_list.sort();

    let mut email_list = emails.into_iter().collect::<Vec<_>>();
    email_list.sort();

    let mut coin_list = coins.into_iter().collect::<Vec<_>>();
    coin_list.sort();

    Ok(NewPage {
        title: title_text,
        url: url.to_string(),
        links: link_list.join(","),
        emails: email_list.join(","),
        coins: coin_list.join(","),
    })
}

fn fetch_body(client: &Client, url: &str) -> Result<String> {
    client
        .get(url)
        .send()
        .with_context(|| format!("request failed for {url}"))?
        .error_for_status()
        .with_context(|| format!("non-success status for {url}"))?
        .text()
        .with_context(|| format!("failed to read response body for {url}"))
}

fn enqueue_seed_and_links(client: &Client, url: &str) -> Result<usize> {
    let mut connection = establish_connection()?;
    create_work_unit(&mut connection, url)?;

    let parsed = Url::parse(url).with_context(|| format!("invalid url: {url}"))?;
    let body = fetch_body(client, url)?;
    let links = extract_links(&body, &parsed)?;

    let mut inserted = 1;
    for discovered_url in links {
        create_work_unit(&mut connection, &discovered_url)?;
        inserted += 1;
    }

    Ok(inserted)
}

fn work_queue(client: &Client) -> Result<()> {
    let mut connection = establish_connection()?;
    let work_units = get_pending_work_units(&mut connection)?;

    println!("Working with {} pending work units", work_units.len());
    for work_unit in work_units {
        println!("Processing {}", work_unit.url);

        match fetch_body(client, &work_unit.url).and_then(|body| build_page(&work_unit.url, &body))
        {
            Ok(page) => {
                save_page_info(&mut connection, &page)?;
                mark_work_unit_as_done(&mut connection, work_unit.id)?;
            }
            Err(error) => {
                eprintln!("ERROR: couldn't extract page information: {error:?}");
                mark_work_unit_as_failed(&mut connection, work_unit.id, &error.to_string())?;
            }
        }
    }

    Ok(())
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

fn build_http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(15))
        // Avoid macOS system proxy discovery, which can panic in restricted or
        // misconfigured environments before the request is even attempted.
        .no_proxy()
        .build()
        .expect("http client should build")
}

fn main() {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "spyder".to_string());
    let client = build_http_client();

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
