use regex::Regex;
use scraper::{Html, Selector};
use std::sync::OnceLock;

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
struct ScanResult {
    file_count: u32,
    total_size: u64,
    errors: Vec<String>,
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
