use crate::models::{
    CategoryHint, ClassificationSignals, CryptoReference, LanguageDetection, LinkObservation,
    PageSnapshot, TopicObservation,
};
use anyhow::{Context, Result};
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use url::Url;
use whatlang::detect;

pub fn extract_page_snapshot(url: &str, body: &str) -> Result<PageSnapshot> {
    let normalized_url = crate::normalize_crawl_url(url);
    let document = Html::parse_document(body);
    let base_url = Url::parse(&crate::strip_url_fragment(url))
        .with_context(|| format!("invalid url: {url}"))?;
    let title = extract_title(&document);
    let text = extract_document_text(&document);
    let links = extract_links(&document, &base_url);
    let classification_signals =
        extract_classification_signals(&document, &base_url, &title, &text, &links);
    let language_detection = detect_page_language(&document, &text);
    let topic_observations =
        extract_topic_observations(&document, &base_url, &title, &text, &links);
    let keyword_corpus = build_keyword_corpus(&normalized_url, &title, &text, &links);

    Ok(PageSnapshot {
        title,
        url: normalized_url,
        language: language_detection.name.clone(),
        language_detection,
        keyword_corpus,
        links,
        emails: extract_emails(body),
        crypto_refs: extract_crypto_refs(body),
        classification_signals,
        topic_observations,
    })
}

pub fn extract_favicon_url(url: &str, body: &str) -> Option<String> {
    let document = Html::parse_document(body);
    let base_url = Url::parse(&crate::strip_url_fragment(url)).ok()?;
    let selector = Selector::parse("link[rel][href]").expect("valid selector");

    for element in document.select(&selector) {
        let rel = element
            .value()
            .attr("rel")
            .unwrap_or_default()
            .to_ascii_lowercase();
        if !rel.contains("icon") {
            continue;
        }

        let href = element.value().attr("href")?;
        if href.starts_with("data:") {
            continue;
        }

        if let Ok(favicon_url) = base_url.join(href) {
            return Some(favicon_url.to_string());
        }
    }

    let host = base_url.host_str()?;
    let mut fallback = format!("{}://{}", base_url.scheme(), host);
    if let Some(port) = base_url.port() {
        fallback.push(':');
        fallback.push_str(&port.to_string());
    }
    fallback.push_str("/favicon.ico");
    Some(fallback)
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

fn build_keyword_corpus(url: &str, title: &str, text: &str, links: &[LinkObservation]) -> String {
    let mut segments = vec![url.to_string(), title.to_string(), text.to_string()];
    segments.extend(links.iter().map(|link| link.target_url.clone()));
    segments.join("\n")
}

fn extract_links(document: &Html, base_url: &Url) -> Vec<LinkObservation> {
    let selector = Selector::parse("a[href]").expect("valid selector");
    let mut discovered = HashSet::new();

    for element in document.select(&selector) {
        if let Some(raw_href) = element.value().attr("href") {
            if let Ok(url) = base_url.join(raw_href) {
                match url.scheme() {
                    "http" | "https" => {
                        let target_url = crate::normalize_crawl_url(url.as_str());
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

    add_seo_spam_hints(document, base_url, &mut signals);

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

fn add_seo_spam_hints(document: &Html, base_url: &Url, signals: &mut ClassificationSignals) {
    let meta_keyword_contents = meta_keyword_contents(document);
    if !meta_keyword_contents.is_empty() {
        let keyword_count = meta_keyword_count(&meta_keyword_contents);
        if keyword_count >= 75 {
            push_hint(
                signals,
                "seo-spam",
                &format!("meta-keywords:massive:{keyword_count}"),
                7,
            );
        } else if keyword_count >= 35 {
            push_hint(
                signals,
                "seo-spam",
                &format!("meta-keywords:large:{keyword_count}"),
                4,
            );
        }

        let language_count = meta_keyword_language_count(&meta_keyword_contents);
        if language_count >= 10 {
            push_hint(
                signals,
                "seo-spam",
                &format!("meta-keywords:many-languages:{language_count}"),
                8,
            );
        } else if language_count >= 6 {
            push_hint(
                signals,
                "seo-spam",
                &format!("meta-keywords:multi-language:{language_count}"),
                5,
            );
        }
    }

    if meta_robots_allows_index_follow(document) {
        push_hint(signals, "seo-spam", "meta-robots:index-follow", 2);
    }

    let link_profile = link_visibility_profile(document, base_url);
    if link_profile.hidden_link_count >= 3 {
        push_hint(
            signals,
            "seo-spam",
            &format!("links:many-hidden:{}", link_profile.hidden_link_count),
            6,
        );
    } else if link_profile.hidden_link_count > 0 {
        push_hint(signals, "seo-spam", "links:hidden", 3);
    }

    if link_profile.visible_http_link_count >= 3
        && link_profile.internal_visible_link_count == 0
        && link_profile.external_hosts.len() == 1
    {
        let weight = if link_profile.visible_http_link_count >= 5 {
            6
        } else {
            4
        };
        if let Some(host) = link_profile.external_hosts.iter().next() {
            push_hint(
                signals,
                "seo-spam",
                &format!("links:single-external-visible-host:{host}"),
                weight,
            );
        }
    }
}

fn meta_keyword_contents(document: &Html) -> Vec<String> {
    let selector = Selector::parse("meta[name][content]").expect("valid selector");
    document
        .select(&selector)
        .filter_map(|element| {
            let name = element.value().attr("name")?.trim().to_ascii_lowercase();
            if name == "keywords" || name == "news_keywords" || name.contains("keyword") {
                element
                    .value()
                    .attr("content")
                    .map(str::trim)
                    .filter(|content| !content.is_empty())
                    .map(str::to_string)
            } else {
                None
            }
        })
        .collect()
}

fn meta_keyword_count(contents: &[String]) -> usize {
    contents
        .iter()
        .flat_map(|content| keyword_terms(content))
        .collect::<HashSet<_>>()
        .len()
}

fn keyword_terms(content: &str) -> Vec<String> {
    let delimited = content
        .split(|character| matches!(character, ',' | ';' | '|' | '\n' | '\r' | '\t'))
        .collect::<Vec<_>>();
    let raw_terms = if delimited.len() > 1 {
        delimited
    } else {
        content.split_whitespace().collect::<Vec<_>>()
    };

    raw_terms
        .into_iter()
        .map(normalize_keyword_term)
        .filter(|term| {
            term.chars()
                .filter(|character| character.is_alphanumeric())
                .count()
                >= 2
        })
        .collect()
}

fn normalize_keyword_term(term: &str) -> String {
    term.trim()
        .trim_matches(|character: char| !character.is_alphanumeric())
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn meta_keyword_language_count(contents: &[String]) -> usize {
    let mut languages = HashSet::new();
    for content in contents {
        add_script_language_hints(content, &mut languages);
        add_latin_keyword_language_hints(content, &mut languages);
        for chunk in keyword_language_detection_chunks(content) {
            if let Some(info) = detect(&chunk) {
                if info.confidence() >= 0.45 {
                    languages.insert(info.lang().code().to_string());
                }
            }
        }
    }
    languages.len()
}

fn keyword_language_detection_chunks(content: &str) -> Vec<String> {
    let terms = keyword_terms(content)
        .into_iter()
        .filter(|term| alphabetic_count(term) >= 4)
        .collect::<Vec<_>>();
    let mut chunks = terms
        .iter()
        .filter(|term| alphabetic_count(term) >= 12)
        .cloned()
        .collect::<Vec<_>>();
    for group in terms.chunks(8) {
        let chunk = group.join(" ");
        if alphabetic_count(&chunk) >= 24 {
            chunks.push(chunk);
        }
    }
    chunks
}

fn alphabetic_count(value: &str) -> usize {
    value
        .chars()
        .filter(|character| character.is_alphabetic())
        .count()
}

fn add_latin_keyword_language_hints(content: &str, languages: &mut HashSet<String>) {
    const MARKERS: &[(&str, &[&str])] = &[
        ("en", &[" cheap ", " free ", " best online "]),
        ("es", &[" comprar ", " mejor ", " barato "]),
        ("fr", &[" acheter ", " meilleur ", " pas cher "]),
        ("de", &[" kaufen ", " beste ", " guenstig "]),
        ("it", &[" comprare ", " migliore ", " economico "]),
        ("pt", &[" comprar ", " melhor ", " barato "]),
        ("nl", &[" kopen ", " goedkoop ", " beste "]),
        ("pl", &[" kupic ", " darmowe ", " najlepsze "]),
        ("tr", &[" satin al ", " ucretsiz ", " en iyi "]),
        ("id", &[" beli ", " murah ", " terbaik "]),
        ("vi", &[" mua ", " mien phi ", " tot nhat "]),
    ];

    let haystack = language_marker_haystack(content);
    for (language, markers) in MARKERS {
        if markers.iter().any(|marker| haystack.contains(marker)) {
            languages.insert((*language).to_string());
        }
    }
}

fn language_marker_haystack(content: &str) -> String {
    let mut normalized = String::with_capacity(content.len() + 2);
    normalized.push(' ');
    for character in content.to_ascii_lowercase().chars() {
        if character.is_ascii_alphanumeric() {
            normalized.push(character);
        } else {
            normalized.push(' ');
        }
    }
    normalized.push(' ');
    let compact = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    format!(" {compact} ")
}

fn add_script_language_hints(content: &str, languages: &mut HashSet<String>) {
    let mut script_counts = HashMap::<&'static str, usize>::new();
    for character in content.chars() {
        let codepoint = character as u32;
        let script = if (0x0400..=0x052F).contains(&codepoint) {
            Some("script:cyrillic")
        } else if (0x0600..=0x06FF).contains(&codepoint) {
            Some("script:arabic")
        } else if (0x0590..=0x05FF).contains(&codepoint) {
            Some("script:hebrew")
        } else if (0x0370..=0x03FF).contains(&codepoint) {
            Some("script:greek")
        } else if (0x0900..=0x097F).contains(&codepoint) {
            Some("script:devanagari")
        } else if (0x0E00..=0x0E7F).contains(&codepoint) {
            Some("script:thai")
        } else if (0x3040..=0x30FF).contains(&codepoint) {
            Some("script:kana")
        } else if (0x4E00..=0x9FFF).contains(&codepoint) {
            Some("script:han")
        } else if (0xAC00..=0xD7AF).contains(&codepoint) {
            Some("script:hangul")
        } else {
            None
        };
        if let Some(script) = script {
            *script_counts.entry(script).or_default() += 1;
        }
    }

    for (script, count) in script_counts {
        if count >= 2 {
            languages.insert(script.to_string());
        }
    }
}

fn meta_robots_allows_index_follow(document: &Html) -> bool {
    let selector = Selector::parse("meta[name][content]").expect("valid selector");
    document.select(&selector).any(|element| {
        let name = element
            .value()
            .attr("name")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if !matches!(name.as_str(), "robots" | "googlebot" | "bingbot") {
            return false;
        }
        let content = element
            .value()
            .attr("content")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let directives = content
            .split(|character: char| {
                character == ',' || character == ';' || character.is_whitespace()
            })
            .map(str::trim)
            .filter(|directive| !directive.is_empty())
            .collect::<HashSet<_>>();
        directives.contains("index")
            && directives.contains("follow")
            && !directives.contains("noindex")
            && !directives.contains("nofollow")
    })
}

#[derive(Default)]
struct LinkVisibilityProfile {
    visible_http_link_count: usize,
    internal_visible_link_count: usize,
    hidden_link_count: usize,
    external_hosts: HashSet<String>,
}

fn link_visibility_profile(document: &Html, base_url: &Url) -> LinkVisibilityProfile {
    let selector = Selector::parse("a[href]").expect("valid selector");
    let base_host = base_url.host_str().unwrap_or_default().to_ascii_lowercase();
    let mut profile = LinkVisibilityProfile::default();

    for element in document.select(&selector) {
        let Some(raw_href) = element.value().attr("href") else {
            continue;
        };
        let Ok(target_url) = base_url.join(raw_href) else {
            continue;
        };
        if !matches!(target_url.scheme(), "http" | "https") {
            continue;
        }

        if link_element_is_hidden(&element) {
            profile.hidden_link_count += 1;
            continue;
        }

        let target_host = target_url
            .host_str()
            .unwrap_or_default()
            .trim()
            .trim_end_matches('.')
            .to_ascii_lowercase();
        if target_host.is_empty() {
            continue;
        }

        profile.visible_http_link_count += 1;
        if target_host == base_host {
            profile.internal_visible_link_count += 1;
        } else {
            profile.external_hosts.insert(target_host);
        }
    }

    profile
}

fn link_element_is_hidden(element: &ElementRef<'_>) -> bool {
    let value = element.value();
    if value.attr("hidden").is_some() {
        return true;
    }
    if value
        .attr("aria-hidden")
        .map(|attr| attr.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        return true;
    }
    if value
        .attr("style")
        .map(inline_style_marks_hidden)
        .unwrap_or(false)
    {
        return true;
    }
    value
        .attr("class")
        .map(class_or_id_marks_hidden)
        .unwrap_or(false)
        || value
            .attr("id")
            .map(class_or_id_marks_hidden)
            .unwrap_or(false)
}

fn inline_style_marks_hidden(style: &str) -> bool {
    let compact = style
        .to_ascii_lowercase()
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    compact.contains("display:none")
        || compact.contains("visibility:hidden")
        || compact.contains("opacity:0")
        || compact.contains("font-size:0")
        || compact.contains("width:0")
        || compact.contains("height:0")
        || compact.contains("max-width:0")
        || compact.contains("max-height:0")
}

fn class_or_id_marks_hidden(value: &str) -> bool {
    value
        .to_ascii_lowercase()
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '-'))
        .any(|token| {
            matches!(
                token,
                "hidden" | "invisible" | "visually-hidden" | "sr-only" | "d-none" | "u-hidden"
            )
        })
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

fn detect_page_language(document: &Html, text: &str) -> LanguageDetection {
    let declared = declared_language(document);
    let detected = detect_language_from_text(text);

    match (declared, detected) {
        (Some(mut declared), Some(detected)) => {
            if language_codes_agree(&declared.code, &detected.code) {
                declared.confidence = declared.confidence.max(detected.confidence).max(90);
                declared.evidence = format!("{}; {}", declared.evidence, detected.evidence);
                declared
            } else if detected.confidence >= 90 {
                detected
            } else {
                declared
            }
        }
        (Some(declared), None) => declared,
        (None, Some(detected)) => detected,
        (None, None) => LanguageDetection::unknown(),
    }
}

fn declared_language(document: &Html) -> Option<LanguageDetection> {
    if let Some(value) = select_first_attr(document, "html[lang]", "lang") {
        return language_detection_from_declared_tag(&value, "html-lang", 88);
    }

    let selector = Selector::parse("meta").expect("valid selector");
    for element in document.select(&selector) {
        let name = element
            .value()
            .attr("name")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let property = element
            .value()
            .attr("property")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let http_equiv = element
            .value()
            .attr("http-equiv")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let content = element.value().attr("content").unwrap_or_default().trim();
        if content.is_empty() {
            continue;
        }
        let source = if name == "language" {
            Some("meta-language")
        } else if property == "og:locale" {
            Some("meta-og-locale")
        } else if http_equiv == "content-language" {
            Some("meta-content-language")
        } else {
            None
        };
        if let Some(source) = source {
            if let Some(detection) = language_detection_from_declared_tag(content, source, 82) {
                return Some(detection);
            }
        }
    }

    None
}

fn select_first_attr(document: &Html, selector: &str, attr: &str) -> Option<String> {
    let selector = Selector::parse(selector).ok()?;
    document
        .select(&selector)
        .find_map(|element| element.value().attr(attr))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn language_detection_from_declared_tag(
    raw_value: &str,
    source: &str,
    confidence: i32,
) -> Option<LanguageDetection> {
    let raw_language = raw_value
        .split(',')
        .next()
        .unwrap_or(raw_value)
        .trim()
        .split(';')
        .next()
        .unwrap_or(raw_value)
        .trim();
    let primary = raw_language
        .replace('_', "-")
        .split('-')
        .next()
        .unwrap_or(raw_language)
        .trim()
        .to_ascii_lowercase();
    if primary.is_empty() {
        return None;
    }
    let name = language_name_for_code(&primary)
        .unwrap_or(raw_language)
        .to_string();
    Some(LanguageDetection {
        code: primary,
        name,
        confidence,
        source: source.to_string(),
        evidence: format!("{source}:{raw_language}"),
    })
}

fn detect_language_from_text(text: &str) -> Option<LanguageDetection> {
    let sample = text.chars().take(12_000).collect::<String>();
    detect(&sample).map(|info| LanguageDetection {
        code: info.lang().code().to_string(),
        name: info.lang().eng_name().to_string(),
        confidence: (info.confidence() * 100.0).round().clamp(0.0, 100.0) as i32,
        source: "whatlang".to_string(),
        evidence: format!(
            "whatlang:{}:{:.2}",
            format!("{:?}", info.script()).to_ascii_lowercase(),
            info.confidence()
        ),
    })
}

fn language_codes_agree(left: &str, right: &str) -> bool {
    let left = left.trim().to_ascii_lowercase();
    let right = right.trim().to_ascii_lowercase();
    if left.is_empty() || right.is_empty() {
        return false;
    }
    left == right
        || language_name_for_code(&left) == language_name_for_code(&right)
        || iso2_to_iso3(&left)
            .map(|code| code == right)
            .unwrap_or(false)
        || iso2_to_iso3(&right)
            .map(|code| code == left)
            .unwrap_or(false)
}

fn language_name_for_code(code: &str) -> Option<&'static str> {
    match code.to_ascii_lowercase().as_str() {
        "ar" | "ara" => Some("Arabic"),
        "de" | "deu" | "ger" => Some("German"),
        "en" | "eng" => Some("English"),
        "es" | "spa" => Some("Spanish"),
        "fr" | "fra" | "fre" => Some("French"),
        "it" | "ita" => Some("Italian"),
        "nl" | "nld" | "dut" => Some("Dutch"),
        "pl" | "pol" => Some("Polish"),
        "pt" | "por" => Some("Portuguese"),
        "ru" | "rus" => Some("Russian"),
        "tr" | "tur" => Some("Turkish"),
        "uk" | "ukr" => Some("Ukrainian"),
        "zh" | "cmn" | "zho" | "chi" => Some("Chinese"),
        _ => None,
    }
}

fn iso2_to_iso3(code: &str) -> Option<&'static str> {
    match code {
        "ar" => Some("ara"),
        "de" => Some("deu"),
        "en" => Some("eng"),
        "es" => Some("spa"),
        "fr" => Some("fra"),
        "it" => Some("ita"),
        "nl" => Some("nld"),
        "pl" => Some("pol"),
        "pt" => Some("por"),
        "ru" => Some("rus"),
        "tr" => Some("tur"),
        "uk" => Some("ukr"),
        "zh" => Some("cmn"),
        _ => None,
    }
}

fn extract_topic_observations(
    document: &Html,
    base_url: &Url,
    title: &str,
    text: &str,
    links: &[LinkObservation],
) -> Vec<TopicObservation> {
    let mut scores = HashMap::<String, i32>::new();
    let mut evidence = HashMap::<String, Vec<(i32, String)>>::new();

    let title_text = title.to_ascii_lowercase();
    let heading_text = collect_selector_text(document, "h1, h2, h3, h4, h5, h6");
    let meta_text = collect_meta_topic_text(document);
    let body_text = text.to_ascii_lowercase();
    let url_text = base_url.as_str().to_ascii_lowercase();
    let link_text = links
        .iter()
        .map(|link| link.target_url.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join("\n");

    apply_topic_rules(
        &title_text,
        "title",
        3,
        topic_keyword_rules(),
        &mut scores,
        &mut evidence,
    );
    apply_topic_rules(
        &heading_text,
        "heading",
        2,
        topic_keyword_rules(),
        &mut scores,
        &mut evidence,
    );
    apply_topic_rules(
        &meta_text,
        "meta",
        2,
        topic_keyword_rules(),
        &mut scores,
        &mut evidence,
    );
    apply_topic_rules(
        &body_text,
        "text",
        1,
        topic_keyword_rules(),
        &mut scores,
        &mut evidence,
    );
    apply_topic_rules(
        &url_text,
        "url",
        2,
        topic_path_rules(),
        &mut scores,
        &mut evidence,
    );
    apply_topic_rules(
        &link_text,
        "link",
        1,
        topic_path_rules(),
        &mut scores,
        &mut evidence,
    );

    let mut topics = scores
        .into_iter()
        .filter(|(_, score)| *score >= 4)
        .map(|(topic, score)| {
            let mut topic_evidence = evidence.remove(&topic).unwrap_or_default();
            topic_evidence
                .sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
            let evidence = topic_evidence
                .into_iter()
                .map(|(_, evidence)| evidence)
                .take(8)
                .collect::<Vec<_>>();
            TopicObservation {
                topic,
                score: score.clamp(0, 100),
                confidence: topic_confidence(score).to_string(),
                evidence,
            }
        })
        .collect::<Vec<_>>();
    topics.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.topic.cmp(&right.topic))
    });
    topics.truncate(8);
    topics
}

fn collect_selector_text(document: &Html, selector: &str) -> String {
    let selector = Selector::parse(selector).expect("valid selector");
    document
        .select(&selector)
        .flat_map(|element| element.text())
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn collect_meta_topic_text(document: &Html) -> String {
    let selector = Selector::parse("meta").expect("valid selector");
    let mut values = Vec::new();
    for element in document.select(&selector) {
        let name = element
            .value()
            .attr("name")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let property = element
            .value()
            .attr("property")
            .unwrap_or_default()
            .to_ascii_lowercase();
        if matches!(
            name.as_str(),
            "description" | "keywords" | "subject" | "topic"
        ) || matches!(property.as_str(), "og:title" | "og:description")
        {
            if let Some(content) = element.value().attr("content") {
                values.push(content);
            }
        }
    }
    values.join(" ").to_ascii_lowercase()
}

fn apply_topic_rules(
    haystack: &str,
    source: &str,
    source_multiplier: i32,
    rules: &[(&str, &str, i32)],
    scores: &mut HashMap<String, i32>,
    evidence: &mut HashMap<String, Vec<(i32, String)>>,
) {
    for (topic, needle, weight) in rules {
        if topic_rule_matches(haystack, needle) {
            let weighted_score = weight * source_multiplier;
            *scores.entry((*topic).to_string()).or_default() += weighted_score;
            evidence
                .entry((*topic).to_string())
                .or_default()
                .push((weighted_score, format!("{source}:{needle}")));
        }
    }
}

fn topic_rule_matches(haystack: &str, needle: &str) -> bool {
    if needle.starts_with('/') {
        return haystack.contains(needle);
    }

    let starts_with_word = needle
        .chars()
        .next()
        .map(|character| character.is_ascii_alphanumeric())
        .unwrap_or(false);
    let ends_with_word = needle
        .chars()
        .next_back()
        .map(|character| character.is_ascii_alphanumeric())
        .unwrap_or(false);
    let mut offset = 0;
    while let Some(relative_index) = haystack[offset..].find(needle) {
        let start = offset + relative_index;
        let end = start + needle.len();
        let before_is_boundary = !starts_with_word
            || start == 0
            || haystack[..start]
                .chars()
                .next_back()
                .map(|character| !character.is_ascii_alphanumeric())
                .unwrap_or(true);
        let after_is_boundary = !ends_with_word
            || end >= haystack.len()
            || haystack[end..]
                .chars()
                .next()
                .map(|character| !character.is_ascii_alphanumeric())
                .unwrap_or(true);
        if before_is_boundary && after_is_boundary {
            return true;
        }
        offset = end.max(offset + 1);
    }
    false
}

fn topic_confidence(score: i32) -> &'static str {
    if score >= 18 {
        "high"
    } else if score >= 9 {
        "medium"
    } else {
        "low"
    }
}

fn topic_keyword_rules() -> &'static [(&'static str, &'static str, i32)] {
    &[
        ("marketplace", "marketplace", 5),
        ("marketplace", "market", 3),
        ("marketplace", "vendor", 4),
        ("marketplace", "seller", 3),
        ("marketplace", "escrow", 4),
        ("marketplace", "checkout", 3),
        ("marketplace", "shopping cart", 3),
        ("marketplace", "add to cart", 4),
        ("forum", "forum", 5),
        ("forum", "thread", 4),
        ("forum", "topic", 3),
        ("forum", "reply", 3),
        ("forum", "posted by", 3),
        ("directory", "directory", 5),
        ("directory", "link list", 4),
        ("directory", "onion list", 4),
        ("directory", "hidden wiki", 5),
        ("directory", "mirror", 3),
        ("search", "search engine", 5),
        ("search", "advanced search", 4),
        ("search", "search results", 4),
        ("documentation", "documentation", 5),
        ("documentation", "api reference", 4),
        ("documentation", "manual", 4),
        ("documentation", "installation", 3),
        ("crypto", "bitcoin", 4),
        ("crypto", "monero", 4),
        ("crypto", "ethereum", 4),
        ("crypto", "wallet", 3),
        ("crypto", "btc", 2),
        ("crypto", "xmr", 2),
        ("credentials", "credential", 5),
        ("credentials", "password", 4),
        ("credentials", "combo list", 5),
        ("credentials", "account dump", 5),
        ("credentials", "stealer log", 5),
        ("data-leak", "data leak", 5),
        ("data-leak", "database dump", 5),
        ("data-leak", "breach", 4),
        ("data-leak", "leaked", 3),
        ("data-leak", "confidential", 3),
        ("malware", "malware", 5),
        ("malware", "ransomware", 5),
        ("malware", "botnet", 5),
        ("malware", "stealer", 4),
        ("malware", "loader", 3),
        ("malware", "command and control", 5),
        ("phishing", "phishing", 5),
        ("phishing", "spoof", 3),
        ("phishing", "webmail login", 4),
        ("exploit", "exploit", 5),
        ("exploit", "vulnerability", 4),
        ("exploit", "cve-", 5),
        ("exploit", "rce", 4),
        ("exploit", "zero day", 5),
        ("infrastructure", "proxy", 3),
        ("infrastructure", "socks", 3),
        ("infrastructure", "vpn", 3),
        ("infrastructure", "ssh", 3),
        ("infrastructure", "server", 2),
        ("infrastructure", "admin panel", 4),
    ]
}

fn topic_path_rules() -> &'static [(&'static str, &'static str, i32)] {
    &[
        ("marketplace", "/market", 4),
        ("marketplace", "/vendor", 4),
        ("marketplace", "/seller", 3),
        ("marketplace", "/listing", 4),
        ("marketplace", "/product", 3),
        ("forum", "/forum", 4),
        ("forum", "/thread", 4),
        ("forum", "/topic", 4),
        ("directory", "/directory", 4),
        ("directory", "/links", 3),
        ("directory", "/mirror", 3),
        ("search", "/search", 4),
        ("documentation", "/docs", 4),
        ("documentation", "/api", 3),
        ("documentation", "/guide", 3),
        ("credentials", "/logs", 3),
        ("credentials", "/accounts", 3),
        ("data-leak", "/leaks", 4),
        ("data-leak", "/dump", 4),
        ("malware", "/malware", 4),
        ("malware", "/botnet", 4),
        ("phishing", "/phish", 4),
        ("exploit", "/exploit", 4),
        ("exploit", "/cve", 4),
        ("infrastructure", "/proxy", 3),
        ("infrastructure", "/vpn", 3),
    ]
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
        assert_eq!(snapshot.language_detection.name, "English");
        assert!(snapshot.language_detection.confidence > 0);
        assert!(snapshot
            .topic_observations
            .iter()
            .any(|topic| topic.topic == "search" && topic.score >= 9));
        assert_eq!(snapshot.emails, vec!["team@example.com".to_string()]);
        assert_eq!(snapshot.links.len(), 2);
        assert!(snapshot
            .links
            .iter()
            .any(|link| link.target_url == "https://example.com"));
        assert!(snapshot
            .links
            .iter()
            .any(|link| link.target_url == "https://alpha.onion"));
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
        assert!(snapshot
            .topic_observations
            .iter()
            .any(|topic| topic.topic == "forum"));
        assert!(snapshot
            .topic_observations
            .iter()
            .any(|topic| topic.topic == "marketplace"));
    }

    #[test]
    fn snapshot_detects_seo_spam_signals() {
        let snapshot = extract_page_snapshot(
            "https://doorway.example/",
            r#"
            <html>
                <head>
                    <title>Random Promo 8f31</title>
                    <meta name="robots" content="index, follow">
                    <meta name="keywords" content="
                        cheap free best online, comprar mejor barato, acheter meilleur pas cher,
                        kaufen beste guenstig, comprare migliore economico, comprar melhor barato,
                        kopen goedkoop beste, kupic darmowe najlepsze, satin al ucretsiz en iyi,
                        beli murah terbaik, mua mien phi tot nhat,
                        alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron
                    ">
                </head>
                <body>
                    <a href="https://money.example/a">Offer A</a>
                    <a href="https://money.example/b">Offer B</a>
                    <a href="https://money.example/c">Offer C</a>
                    <a href="https://money.example/d">Offer D</a>
                    <a href="https://money.example/e">Offer E</a>
                    <a href="https://hidden.example/a" style="display:none">Hidden A</a>
                    <a href="https://hidden.example/b" class="hidden">Hidden B</a>
                    <a href="https://hidden.example/c" aria-hidden="true">Hidden C</a>
                </body>
            </html>
            "#,
        )
        .expect("snapshot");

        let seo_hints = snapshot
            .classification_signals
            .hints
            .iter()
            .filter(|hint| hint.category == "seo-spam")
            .map(|hint| hint.evidence.as_str())
            .collect::<Vec<_>>();
        assert!(seo_hints
            .iter()
            .any(|evidence| evidence.starts_with("meta-keywords:many-languages")));
        assert!(seo_hints
            .iter()
            .any(|evidence| evidence == &"meta-robots:index-follow"));
        assert!(seo_hints
            .iter()
            .any(|evidence| evidence.starts_with("links:many-hidden")));
        assert!(seo_hints.iter().any(
            |evidence| evidence.starts_with("links:single-external-visible-host:money.example")
        ));
    }

    #[test]
    fn topic_keyword_matching_uses_word_boundaries() {
        assert!(topic_rule_matches("open market listings", "market"));
        assert!(!topic_rule_matches("marketing page", "market"));
        assert!(topic_rule_matches("cve-2026-0001 details", "cve-"));
    }

    #[test]
    fn snapshot_ignores_url_fragments() {
        let snapshot = extract_page_snapshot(
            "https://example.com/docs/page#overview",
            r##"
            <html>
                <head><title>Docs</title></head>
                <body>
                    <a href="#faq">FAQ</a>
                    <a href="/docs/page#returns">Returns</a>
                    <a href="/docs/next#intro">Next</a>
                </body>
            </html>
            "##,
        )
        .expect("snapshot");

        assert_eq!(snapshot.url, "https://example.com");
        assert_eq!(snapshot.links.len(), 1);
        assert!(snapshot
            .links
            .iter()
            .any(|link| link.target_url == "https://example.com"));
    }
}
