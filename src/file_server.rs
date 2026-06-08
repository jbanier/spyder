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
        "" => 1,
        "K" | "KB" => 1024,
        "M" | "MB" => 1024 * 1024,
        "G" | "GB" => 1024 * 1024 * 1024,
        "T" | "TB" => 1024 * 1024 * 1024 * 1024,
        _ => return None,
    };

    Some((number * multiplier as f64) as u64)
}
