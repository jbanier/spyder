use crate::models::{
    CategoryHint, ClassificationSignals, CryptoReference, LinkObservation, PageSnapshot,
};
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
    let title = extract_title(&document);
    let text = extract_document_text(&document);
    let links = extract_links(&document, &base_url);
    let classification_signals =
        extract_classification_signals(&document, &base_url, &title, &text, &links);

    Ok(PageSnapshot {
        title,
        url: url.to_string(),
        language: detect_primary_language(&text),
        links,
        emails: extract_emails(body),
        crypto_refs: extract_crypto_refs(body),
        classification_signals,
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

fn extract_classification_signals(
    document: &Html,
    base_url: &Url,
    title: &str,
    text: &str,
    links: &[LinkObservation],
) -> ClassificationSignals {
    let mut signals = ClassificationSignals {
        word_count: text.split_whitespace().count(),
        ..ClassificationSignals::default()
    };
    let lower_title = title.to_ascii_lowercase();
    let lower_text = text.to_ascii_lowercase();
    let current_path = base_url.path().to_ascii_lowercase();

    add_keyword_hints(
        &lower_title,
        &[
            ("search-engine", "search engine", "title:search-engine", 7),
            ("search-engine", "search", "title:search", 5),
            ("forum", "forum", "title:forum", 6),
            ("forum", "discussion", "title:discussion", 4),
            ("market", "marketplace", "title:marketplace", 7),
            ("market", "market", "title:market", 5),
            ("directory", "directory", "title:directory", 6),
            ("wiki", "wiki", "title:wiki", 6),
            ("blog", "blog", "title:blog", 6),
            ("escrow", "escrow", "title:escrow", 7),
            ("shop", "shop", "title:shop", 6),
            ("shop", "store", "title:store", 5),
            ("vendor-page", "vendor", "title:vendor", 6),
            ("docs", "documentation", "title:documentation", 7),
            ("docs", "docs", "title:docs", 6),
            ("docs", "reference", "title:reference", 5),
            ("indexer", "indexer", "title:indexer", 7),
            ("indexer", "onion list", "title:onion-list", 6),
        ],
        &mut signals,
    );
    add_keyword_hints(
        &lower_text,
        &[
            (
                "search-engine",
                "advanced search",
                "text:advanced-search",
                5,
            ),
            ("search-engine", "search results", "text:search-results", 5),
            ("forum", "reply", "text:reply", 3),
            ("forum", "thread", "text:thread", 4),
            ("forum", "topic", "text:topic", 4),
            ("forum", "posted by", "text:posted-by", 3),
            ("market", "listing", "text:listing", 4),
            ("market", "checkout", "text:checkout", 4),
            ("market", "shopping cart", "text:shopping-cart", 4),
            ("market", "price", "text:price", 2),
            ("market", "product", "text:product", 3),
            ("directory", "categories", "text:categories", 4),
            ("directory", "resources", "text:resources", 3),
            ("directory", "link list", "text:link-list", 4),
            ("wiki", "main page", "text:main-page", 4),
            ("wiki", "revision", "text:revision", 4),
            ("wiki", "edit this", "text:edit-this", 3),
            ("blog", "posted on", "text:posted-on", 3),
            ("blog", "comments", "text:comments", 3),
            ("blog", "archive", "text:archive", 3),
            ("escrow", "buyer protection", "text:buyer-protection", 5),
            ("escrow", "release funds", "text:release-funds", 5),
            ("escrow", "dispute", "text:dispute", 4),
            ("shop", "buy now", "text:buy-now", 5),
            ("shop", "add to cart", "text:add-to-cart", 5),
            ("shop", "storefront", "text:storefront", 4),
            ("vendor-page", "seller", "text:seller", 4),
            ("vendor-page", "feedback", "text:feedback", 3),
            ("vendor-page", "pgp", "text:pgp", 3),
            ("docs", "api reference", "text:api-reference", 5),
            ("docs", "installation", "text:installation", 4),
            ("docs", "configuration", "text:configuration", 4),
            ("docs", "manual", "text:manual", 4),
            ("indexer", "mirror status", "text:mirror-status", 5),
            ("indexer", "discovery", "text:discovery", 4),
            ("indexer", "indexed", "text:indexed", 4),
            ("indexer", "crawl", "text:crawl", 3),
        ],
        &mut signals,
    );
    add_path_hints(
        &current_path,
        &[
            ("search-engine", "/search", "path:search", 4),
            ("forum", "/forum", "path:forum", 4),
            ("forum", "/thread", "path:thread", 4),
            ("forum", "/topic", "path:topic", 4),
            ("directory", "/directory", "path:directory", 4),
            ("directory", "/links", "path:links", 3),
            ("wiki", "/wiki", "path:wiki", 5),
            ("blog", "/blog", "path:blog", 4),
            ("blog", "/posts", "path:posts", 3),
            ("escrow", "/escrow", "path:escrow", 5),
            ("shop", "/shop", "path:shop", 4),
            ("shop", "/store", "path:store", 4),
            ("vendor-page", "/vendor", "path:vendor", 5),
            ("vendor-page", "/seller", "path:seller", 4),
            ("docs", "/docs", "path:docs", 5),
            ("docs", "/api", "path:api", 4),
            ("docs", "/guide", "path:guide", 4),
            ("indexer", "/index", "path:index", 4),
            ("indexer", "/mirror", "path:mirror", 4),
            ("indexer", "/status", "path:status", 3),
        ],
        &mut signals,
    );

    let form_selector = Selector::parse("form").expect("valid selector");
    let control_selector =
        Selector::parse("input, textarea, select, button").expect("valid selector");
    for form in document.select(&form_selector) {
        signals.total_form_count += 1;
        let mut is_search_form = false;
        let mut has_password = false;
        let form_text = form
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        for control in form.select(&control_selector) {
            let input_type = control
                .value()
                .attr("type")
                .unwrap_or_default()
                .to_ascii_lowercase();
            let input_name = control
                .value()
                .attr("name")
                .unwrap_or_default()
                .to_ascii_lowercase();
            let input_id = control
                .value()
                .attr("id")
                .unwrap_or_default()
                .to_ascii_lowercase();
            let placeholder = control
                .value()
                .attr("placeholder")
                .unwrap_or_default()
                .to_ascii_lowercase();
            let input_value = control
                .value()
                .attr("value")
                .unwrap_or_default()
                .to_ascii_lowercase();
            let attrs = [
                input_type.as_str(),
                input_name.as_str(),
                input_id.as_str(),
                placeholder.as_str(),
                input_value.as_str(),
            ]
            .join(" ");
            if input_type == "password"
                || input_name.contains("password")
                || input_id.contains("password")
                || placeholder.contains("password")
            {
                has_password = true;
            }
            if input_type == "search"
                || input_name == "q"
                || input_name.contains("search")
                || input_name.contains("query")
                || input_id.contains("search")
                || placeholder.contains("search")
                || input_value.contains("search")
                || attrs.contains("search query")
                || form_text.contains("search")
            {
                is_search_form = true;
            }
        }

        if is_search_form {
            signals.search_form_count += 1;
            push_hint(&mut signals, "search-engine", "form:search", 7);
            if links.len() >= 10 {
                push_hint(&mut signals, "indexer", "form:search-many-links", 3);
            }
        }
        if has_password {
            signals.password_form_count += 1;
            push_hint(&mut signals, "forum", "form:password", 2);
            push_hint(&mut signals, "market", "form:password", 3);
            push_hint(&mut signals, "vendor-page", "form:password", 2);
            push_hint(&mut signals, "shop", "form:password", 2);
        }
    }

    if links.len() >= 12 {
        push_hint(&mut signals, "directory", "links:many-outbound", 3);
        push_hint(&mut signals, "indexer", "links:many-outbound", 2);
    }

    for link in links {
        if let Ok(parsed) = Url::parse(&link.target_url) {
            add_path_hints(
                &parsed.path().to_ascii_lowercase(),
                &[
                    ("search-engine", "/search", "link-path:search", 2),
                    ("forum", "/thread", "link-path:thread", 2),
                    ("forum", "/topic", "link-path:topic", 2),
                    ("forum", "/forum", "link-path:forum", 2),
                    ("market", "/listing", "link-path:listing", 2),
                    ("shop", "/product", "link-path:product", 3),
                    ("shop", "/cart", "link-path:cart", 3),
                    ("vendor-page", "/vendor", "link-path:vendor", 3),
                    ("vendor-page", "/seller", "link-path:seller", 2),
                    ("wiki", "/wiki", "link-path:wiki", 2),
                    ("blog", "/blog", "link-path:blog", 2),
                    ("blog", "/post", "link-path:post", 2),
                    ("docs", "/docs", "link-path:docs", 2),
                    ("docs", "/reference", "link-path:reference", 2),
                    ("directory", "/directory", "link-path:directory", 2),
                    ("directory", "/links", "link-path:links", 2),
                    ("indexer", "/mirror", "link-path:mirror", 2),
                ],
                &mut signals,
            );
        }
    }

    signals
}

fn add_keyword_hints(
    haystack: &str,
    rules: &[(&str, &str, &str, i32)],
    signals: &mut ClassificationSignals,
) {
    for (category, needle, evidence, weight) in rules {
        if haystack.contains(needle) {
            push_hint(signals, category, evidence, *weight);
        }
    }
}

fn add_path_hints(
    path: &str,
    rules: &[(&str, &str, &str, i32)],
    signals: &mut ClassificationSignals,
) {
    for (category, needle, evidence, weight) in rules {
        if path.contains(needle) {
            push_hint(signals, category, evidence, *weight);
        }
    }
}

fn push_hint(signals: &mut ClassificationSignals, category: &str, evidence: &str, weight: i32) {
    if signals
        .hints
        .iter()
        .any(|hint| hint.category == category && hint.evidence == evidence)
    {
        return;
    }

    signals.hints.push(CategoryHint {
        category: category.to_string(),
        evidence: evidence.to_string(),
        weight,
    });
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
    fn snapshot_extracts_links_entities_language_and_search_signals() {
        let snapshot = extract_page_snapshot(
            "https://example.com/base/",
            r#"
            <html>
                <head><title>Search Engine Hub</title></head>
                <body>
                    <p>Hello there, this page is written in English and contains enough text for detection.</p>
                    <form action="/search">
                        <input type="search" name="q" placeholder="Search">
                        <button type="submit">Search</button>
                    </form>
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

        assert_eq!(snapshot.title, "Search Engine Hub");
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
        assert_eq!(snapshot.classification_signals.search_form_count, 1);
        assert!(snapshot
            .classification_signals
            .hints
            .iter()
            .any(|hint| hint.category == "search-engine"));
    }

    #[test]
    fn snapshot_detects_forum_and_market_hints() {
        let snapshot = extract_page_snapshot(
            "https://market.example/forum/thread/42",
            r#"
            <html>
                <head><title>Vendor Market Forum</title></head>
                <body>
                    <form action="/login">
                        <input type="text" name="username">
                        <input type="password" name="password">
                    </form>
                    <p>Reply in this thread to contact the seller and review the product listing.</p>
                    <a href="/vendor/acme">Vendor</a>
                    <a href="/product/alpha">Product</a>
                </body>
            </html>
            "#,
        )
        .expect("snapshot");

        assert_eq!(snapshot.classification_signals.password_form_count, 1);
        assert!(snapshot
            .classification_signals
            .hints
            .iter()
            .any(|hint| hint.category == "forum"));
        assert!(snapshot
            .classification_signals
            .hints
            .iter()
            .any(|hint| hint.category == "market"));
    }
}
