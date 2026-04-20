use crate::models::{CryptoReference, LinkObservation, PageSnapshot};
use anyhow::{Context, Result};
use regex::Regex;
use scraper::{Html, Selector};
use std::collections::HashSet;
use std::sync::OnceLock;
use url::Url;
use whatlang::detect;

pub fn extract_page_snapshot(url: &str, body: &str) -> Result<PageSnapshot> {
    let document = Html::parse_document(body);
    let base_url = Url::parse(url).with_context(|| format!("invalid url: {url}"))?;
    let text = extract_document_text(&document);

    Ok(PageSnapshot {
        title: extract_title(&document),
        url: url.to_string(),
        language: detect_primary_language(&text),
        links: extract_links(&document, &base_url),
        emails: extract_emails(body),
        crypto_refs: extract_crypto_refs(body),
    })
}

fn extract_title(document: &Html) -> String {
    let selector = Selector::parse("title").expect("valid selector");
    document
        .select(&selector)
        .next()
        .map(|title| title.text().collect::<Vec<_>>().join(" "))
        .map(|title| title.trim().to_string())
        .filter(|title| !title.is_empty())
        .unwrap_or_else(|| "no title".to_string())
}

fn extract_document_text(document: &Html) -> String {
    document.root_element().text().collect::<Vec<_>>().join(" ")
}

fn extract_links(document: &Html, base_url: &Url) -> Vec<LinkObservation> {
    let selector = Selector::parse("a[href]").expect("valid selector");
    let mut discovered = HashSet::new();

    for element in document.select(&selector) {
        if let Some(raw_href) = element.value().attr("href") {
            if let Ok(url) = base_url.join(raw_href) {
                match url.scheme() {
                    "http" | "https" => {
                        let target_url = url.to_string();
                        let target_host = url.host_str().unwrap_or_default().to_string();
                        discovered.insert(LinkObservation {
                            target_url,
                            target_host,
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    let mut links = discovered.into_iter().collect::<Vec<_>>();
    links.sort_by(|left, right| left.target_url.cmp(&right.target_url));
    links
}

fn extract_emails(body: &str) -> Vec<String> {
    let mut emails = HashSet::new();

    for captures in email_regex().captures_iter(body) {
        emails.insert(captures[0].to_ascii_lowercase());
    }

    let mut values = emails.into_iter().collect::<Vec<_>>();
    values.sort();
    values
}

fn extract_crypto_refs(body: &str) -> Vec<CryptoReference> {
    let mut references = HashSet::new();

    for captures in bitcoin_uri_regex().captures_iter(body) {
        references.insert(CryptoReference {
            asset_type: "bitcoin".to_string(),
            reference: captures[1].to_string(),
        });
    }

    for captures in bitcoin_address_regex().captures_iter(body) {
        references.insert(CryptoReference {
            asset_type: "bitcoin".to_string(),
            reference: captures[0].to_string(),
        });
    }

    for captures in ethereum_uri_regex().captures_iter(body) {
        references.insert(CryptoReference {
            asset_type: "ethereum".to_string(),
            reference: captures[1].to_ascii_lowercase(),
        });
    }

    for captures in ethereum_address_regex().captures_iter(body) {
        references.insert(CryptoReference {
            asset_type: "ethereum".to_string(),
            reference: captures[0].to_ascii_lowercase(),
        });
    }

    let mut values = references.into_iter().collect::<Vec<_>>();
    values.sort_by(|left, right| {
        left.asset_type
            .cmp(&right.asset_type)
            .then_with(|| left.reference.cmp(&right.reference))
    });
    values
}

fn detect_primary_language(text: &str) -> String {
    let sample = text.chars().take(5_000).collect::<String>();
    detect(&sample)
        .map(|info| info.lang().eng_name().to_string())
        .unwrap_or_else(|| "Unknown".to_string())
}

fn email_regex() -> &'static Regex {
    static EMAIL: OnceLock<Regex> = OnceLock::new();
    EMAIL.get_or_init(|| {
        Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}").expect("valid email regex")
    })
}

fn bitcoin_uri_regex() -> &'static Regex {
    static BITCOIN_URI: OnceLock<Regex> = OnceLock::new();
    BITCOIN_URI.get_or_init(|| {
        Regex::new(
            r"(?i)\bbitcoin:((?:bc1[ac-hj-np-z02-9]{11,71})|(?:[13][a-km-zA-HJ-NP-Z1-9]{25,34}))",
        )
        .expect("valid bitcoin uri regex")
    })
}

fn bitcoin_address_regex() -> &'static Regex {
    static BITCOIN: OnceLock<Regex> = OnceLock::new();
    BITCOIN.get_or_init(|| {
        Regex::new(r"\b(?:bc1[ac-hj-np-z02-9]{11,71}|[13][a-km-zA-HJ-NP-Z1-9]{25,34})\b")
            .expect("valid bitcoin address regex")
    })
}

fn ethereum_uri_regex() -> &'static Regex {
    static ETHEREUM_URI: OnceLock<Regex> = OnceLock::new();
    ETHEREUM_URI.get_or_init(|| {
        Regex::new(r"(?i)\bethereum:(0x[a-fA-F0-9]{40})\b").expect("valid ethereum uri regex")
    })
}

fn ethereum_address_regex() -> &'static Regex {
    static ETHEREUM: OnceLock<Regex> = OnceLock::new();
    ETHEREUM
        .get_or_init(|| Regex::new(r"\b0x[a-fA-F0-9]{40}\b").expect("valid ethereum address regex"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_extracts_links_entities_and_language() {
        let snapshot = extract_page_snapshot(
            "https://example.com/base/",
            r#"
            <html>
                <head><title>Sample Page</title></head>
                <body>
                    <p>Hello there, this page is written in English and contains enough text for detection.</p>
                    <a href="/about">About</a>
                    <a href="https://alpha.onion/">Alpha</a>
                    contact: Team@Example.com
                    btc: bitcoin:bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080
                    eth: 0x1111111111111111111111111111111111111111
                </body>
            </html>
            "#,
        )
        .expect("snapshot");

        assert_eq!(snapshot.title, "Sample Page");
        assert_eq!(snapshot.language, "English");
        assert_eq!(snapshot.emails, vec!["team@example.com".to_string()]);
        assert_eq!(snapshot.links.len(), 2);
        assert!(snapshot
            .links
            .iter()
            .any(|link| link.target_url == "https://example.com/about"));
        assert!(snapshot
            .links
            .iter()
            .any(|link| link.target_url == "https://alpha.onion/"));
        assert!(snapshot.crypto_refs.iter().any(|item| {
            item.asset_type == "bitcoin"
                && item.reference == "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080"
        }));
        assert!(snapshot.crypto_refs.iter().any(|item| {
            item.asset_type == "ethereum"
                && item.reference == "0x1111111111111111111111111111111111111111"
        }));
    }
}
