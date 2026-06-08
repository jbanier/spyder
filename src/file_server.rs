use regex::Regex;
use reqwest::blocking::Client;
use scraper::{Html, Selector};
use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::Duration;
use url::Url;

/// Parse human-readable size string to bytes (1024-based units)
pub fn parse_size(size_str: &str) -> Option<u64> {
    static SIZE_REGEX: OnceLock<Regex> = OnceLock::new();
    let regex = SIZE_REGEX.get_or_init(|| {
        Regex::new(r"^(\d+(?:\.\d+)?)\s*([KMGT]?B?)$").expect("valid regex")
    });

    let trimmed = size_str.trim();

    // Try plain number first
    if let Ok(bytes) = trimmed.parse::<u64>() {
        return Some(bytes);
    }

    // Try with units
    let captures = regex.captures(trimmed)?;
    let number: f64 = captures.get(1)?.as_str().parse().ok()?;
    let unit = captures.get(2)?.as_str();

    let multiplier: u64 = match unit {
        "K" | "KB" => 1024,
        "M" | "MB" => 1024 * 1024,
        "G" | "GB" => 1024 * 1024 * 1024,
        "T" | "TB" => 1024 * 1024 * 1024 * 1024,
        _ => return None,
    };

    // Check for overflow before casting to u64
    let result = number * multiplier as f64;
    if result > u64::MAX as f64 || result < 0.0 {
        return None;
    }

    Some(result as u64)
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileEntry {
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DirectoryListing {
    pub files: Vec<FileEntry>,
    pub directories: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FileServerMetrics {
    pub total_files: u32,
    pub total_size: u64,
    pub depth_scanned: u32,
    pub skipped_count: u32,
    pub skipped_paths: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ScanResult {
    pub file_count: u32,
    pub total_size: u64,
    pub errors: Vec<String>,
}

/// Parse directory listing HTML to extract files and subdirectories
pub fn parse_directory_listing(html: &str) -> DirectoryListing {
    let document = Html::parse_document(html);
    let selector = Selector::parse("a[href]").expect("valid selector");

    let mut files = Vec::new();
    let mut directories = Vec::new();

    for element in document.select(&selector) {
        let href = match element.value().attr("href") {
            Some(h) => h,
            None => continue,
        };

        // Skip parent directory links
        if href == "../" {
            continue;
        }

        let name = element.text().collect::<String>().trim().to_string();

        // Directories end with /
        if href.ends_with('/') {
            directories.push(href.to_string());
            continue;
        }

        // Extract size from surrounding text
        if let Some(size) = extract_size_from_context(&element) {
            files.push(FileEntry {
                name: name.clone(),
                size,
            });
        }
    }

    DirectoryListing { files, directories }
}

/// Extract file size from text near the link element
fn extract_size_from_context(element: &scraper::ElementRef) -> Option<u64> {
    // Collect text from siblings following the anchor element
    let mut text = String::new();
    let mut current = element.next_sibling();

    // Gather text from the next few text nodes after the link
    while let Some(node) = current {
        if let Some(t) = node.value().as_text() {
            text.push_str(t);
            // Stop when we have enough text or hit a newline after getting some content
            if text.len() > 100 || (text.len() > 10 && text.contains('\n')) {
                break;
            }
        }
        current = node.next_sibling();
    }

    // Look for size patterns in the text immediately following the link
    static SIZE_IN_CONTEXT: OnceLock<Regex> = OnceLock::new();
    let regex = SIZE_IN_CONTEXT.get_or_init(|| {
        Regex::new(r"\s+(\d+(?:\.\d+)?\s*[KMGT]?B?)\s*(?:\s|\n|$)").expect("valid regex")
    });

    if let Some(captures) = regex.captures(&text) {
        if let Some(size_str) = captures.get(1) {
            return parse_size(size_str.as_str().trim());
        }
    }

    None
}

const MAX_DIRECTORIES: usize = 100;
const FETCH_TIMEOUT_SECS: u64 = 10;

fn get_max_dirs() -> usize {
    MAX_DIRECTORIES
}

fn get_fetch_timeout_secs() -> u64 {
    FETCH_TIMEOUT_SECS
}

fn fetch_with_timeout(url: &str, client: &Client) -> anyhow::Result<String> {
    let timeout_secs = get_fetch_timeout_secs();
    let response = client
        .get(url)
        .timeout(Duration::from_secs(timeout_secs))
        .send()?;

    let body = response.text()?;
    Ok(body)
}

/// Recursively scan directory listings up to max_depth
pub fn scan_recursive(
    url: &str,
    current_depth: u32,
    max_depth: u32,
    visited: &mut HashSet<String>,
    client: &Client,
) -> ScanResult {
    let mut result = ScanResult {
        file_count: 0,
        total_size: 0,
        errors: Vec::new(),
    };

    // Check depth limit
    if current_depth > max_depth {
        return result;
    }

    // Check visited
    if visited.contains(url) {
        return result;
    }

    // Check directory count limit
    let max_dirs = get_max_dirs();
    if visited.len() >= max_dirs {
        result.errors.push(format!("max directories limit reached"));
        return result;
    }

    visited.insert(url.to_string());

    // Fetch directory listing
    let html = match fetch_with_timeout(url, client) {
        Ok(body) => body,
        Err(e) => {
            result.errors.push(format!("{}: {}", url, e));
            return result;
        }
    };

    // Parse listing
    let listing = parse_directory_listing(&html);

    // Accumulate files
    for file in &listing.files {
        result.file_count += 1;
        result.total_size += file.size;
    }

    // Recurse into subdirectories if not at max depth
    if current_depth < max_depth {
        let base_url = match Url::parse(url) {
            Ok(u) => u,
            Err(e) => {
                result.errors.push(format!("invalid base URL {}: {}", url, e));
                return result;
            }
        };

        for dir in &listing.directories {
            let subdir_url = match base_url.join(dir) {
                Ok(u) => u.to_string(),
                Err(e) => {
                    result.errors.push(format!("invalid subdir URL {}: {}", dir, e));
                    continue;
                }
            };

            let sub_result = scan_recursive(
                &subdir_url,
                current_depth + 1,
                max_depth,
                visited,
                client,
            );

            result.file_count += sub_result.file_count;
            result.total_size += sub_result.total_size;
            result.errors.extend(sub_result.errors);
        }
    }

    result
}
