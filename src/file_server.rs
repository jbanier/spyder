use regex::Regex;
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
