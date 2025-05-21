use anyhow::Result;
use regex::Regex;
use reqwest::blocking;
use std::error::Error;
use std::collections::HashSet;
use dotenvy::dotenv;
use std::env;
use diesel::prelude::*;
use spyder::models::*;
use spyder::{create_work_unit, establish_connection};

fn parse_page(body: &str, mut page: Page) -> anyhow::Result<HashSet<std::string::String>> {
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
    println!("page count {:?}", links.len());
    println!("email count {:?}", emails.len());
    println!("coins count {:?}", coins.len());

    page.emails = emails.join(",");
    page.coins = coins.join(",");
    page.links = links.join(",");
    return Ok(url_work);
}

fn fetch_page(url: &str) -> anyhow::Result<HashSet<std::string::String>> {
    let mut page = Page {
        title: String::from("title"),
        url: String::from(url),
        emails: String::new(),
        coins: String::new(),
        links: String::new(),
    };
    let body = reqwest::blocking::get(url)?.text()?;
    let workqueue = parse_page(&body, page).unwrap();

    Ok(workqueue)
}

fn main() {
    let connection = &mut establish_connection();
    let mut workqueue = fetch_page("https://slashdot.org");

    for work in workqueue { 
        for url in work {
            println!("# Adding {:?} to queue", url);
            create_work_unit(connection, &url);
        }
    } 
}
