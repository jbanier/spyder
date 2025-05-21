use regex::Regex;
use spyder::models::*;
use spyder::{create_work_unit, establish_connection};
use std::collections::HashSet;
use std::env;

fn extract_links(body: &str, page: Page) -> anyhow::Result<HashSet<std::string::String>> {
    // Define regular expressions for email and cryptocurrency addresses
    let link_address_regex = Regex::new(r"https://[\w+\.-/]+").unwrap();
    let mut url_work = HashSet::new();
    let mut links = Vec::new();

    for caps in link_address_regex.captures_iter(&body) {
        links.push(String::from(&caps[0]));
        url_work.insert(String::from(&caps[0]));
    }
    println!("[*] Links count {:?}", links.len());
    return Ok(url_work);
}

fn extract_data_from_page(url: String) -> anyhow::Result<Page> {
    use scraper::{Html, Selector};

    let body = reqwest::blocking::get(url.clone())?.text()?;

    let document = Html::parse_document(body.as_str());
    let selector = Selector::parse("title").unwrap();

    let title_element = document.select(&selector).next().unwrap();
    let title_text = title_element.text().collect::<Vec<_>>();

    // Define regular expressions for email and cryptocurrency addresses
    let email_regex = Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}").unwrap();
    let crypto_address_regex = Regex::new(r"(bitcoin|ethereum):[a-zA-Z0-9]+").unwrap();
    let link_address_regex = Regex::new(r"https://[\w+\.-/]+").unwrap();
    let mut url_work = HashSet::new();
    let mut emails = Vec::new();
    let mut coins = Vec::new();
    let mut links = Vec::new();

    for caps in email_regex.captures_iter(&body) {
        emails.push(String::from(&caps[0]));
    }

    for caps in crypto_address_regex.captures_iter(&body) {
        coins.push(String::from(&caps[0]));
    }

    for caps in link_address_regex.captures_iter(&body) {
        links.push(String::from(&caps[0]));
        url_work.insert(String::from(&caps[0]));
    }

    println!("Page title {:?}", title_text);
    println!("email count {:?}", emails.len());
    println!("coins count {:?}", coins.len());

    let page = Page {
        title: title_text.join(" "),
        url: String::from(url.clone()),
        emails: emails.join(","),
        coins: coins.join(","),
        links: links.join(","),
    };
    Ok(page)
}

fn fetch_page(url: String) -> anyhow::Result<HashSet<std::string::String>> {
    let page = Page {
        title: String::from("title"),
        url: String::from(url.clone()),
        emails: String::new(),
        coins: String::new(),
        links: String::new(),
    };
    let body = reqwest::blocking::get(url)?.text()?;
    let workqueue = extract_links(&body, page).unwrap();

    Ok(workqueue)
}

fn usage(program: &str) {
    eprintln!("Usage: {program} [SUBCOMMAND] [OPTIONS]");
    eprintln!("Subcommands:");
    eprintln!("    add <url>      start crawling the page and adding links to the work queue.");
    eprintln!("    work           start crawling the work queue to extract meta data from pages.");
}

fn main() {
    let mut args = env::args();
    let program = args.next().expect("path to program is provided");

    let subcommand = args.next().ok_or_else(|| {
        usage(&program);
        eprintln!("ERROR: no subcommand is provided");
    });

    match subcommand.expect("subcommand missing?").as_str() {
        "add" => {
            let url_to_add = args
                .next()
                .ok_or_else(|| {
                    usage(&program);
                    eprintln!("ERROR: no url is provided subcommand");
                })
                .unwrap();
            let connection = &mut establish_connection();
            let workqueue = fetch_page(url_to_add);

            for work in workqueue {
                for url in work {
                    println!("# Adding {:?} to queue", url);
                    create_work_unit(connection, &url);
                }
            }
        }
        "work" => {
            let _ = extract_data_from_page("https://slashdot.org".to_string());
        }
        &_ => {
            usage(&program);
        }
    }
}
