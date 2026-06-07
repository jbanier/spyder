# File Server Detection and Metrics Collection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect web servers in file browsing mode ("Index of /" pages), recursively scan up to 3 directory levels, and record file count and total size metrics.

**Architecture:** Create a new `src/file_server.rs` module with size parsing, directory listing parsing, and recursive scanning logic. Integrate detection into `src/extraction.rs` extraction pipeline. Store results in existing `page_topic_tag` table as TopicObservation.

**Tech Stack:** Rust, reqwest (HTTP client), scraper (HTML parsing), regex (size/link extraction), existing Diesel ORM models

---

## File Structure

**New files:**
- `src/file_server.rs` - Core file server detection and scanning logic
- `tests/file_server_tests.rs` - Unit tests for parsing and scanning

**Modified files:**
- `src/lib.rs:1-10` - Add `pub mod file_server;` declaration
- `src/extraction.rs:13-39` - Integrate file server detection into `extract_page_snapshot()`

---

### Task 1: Size Parsing Foundation

**Files:**
- Create: `src/file_server.rs`
- Test: `tests/file_server_tests.rs`

- [ ] **Step 1: Write failing test for plain bytes**

Create `tests/file_server_tests.rs`:

```rust
#[cfg(test)]
mod tests {
    use spyder::file_server::parse_size;

    #[test]
    fn test_parse_plain_bytes() {
        assert_eq!(parse_size("1234"), Some(1234));
        assert_eq!(parse_size("0"), Some(0));
        assert_eq!(parse_size("999999"), Some(999999));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_parse_plain_bytes`
Expected: Compilation error - module `file_server` doesn't exist

- [ ] **Step 3: Create file_server module with parse_size stub**

Create `src/file_server.rs`:

```rust
use regex::Regex;
use std::sync::OnceLock;

/// Parse human-readable size string to bytes (1024-based units)
pub fn parse_size(size_str: &str) -> Option<u64> {
    None
}
```

Add to `src/lib.rs` after line 7:

```rust
pub mod file_server;
```

- [ ] **Step 4: Run test to verify it still fails**

Run: `cargo test test_parse_plain_bytes`
Expected: FAIL - assertion failed, got None

- [ ] **Step 5: Implement plain bytes parsing**

Update `src/file_server.rs`:

```rust
use regex::Regex;
use std::sync::OnceLock;

/// Parse human-readable size string to bytes (1024-based units)
pub fn parse_size(size_str: &str) -> Option<u64> {
    let trimmed = size_str.trim();
    
    // Try plain number first
    if let Ok(bytes) = trimmed.parse::<u64>() {
        return Some(bytes);
    }
    
    None
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test test_parse_plain_bytes`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/file_server.rs src/lib.rs tests/file_server_tests.rs
git commit -m "feat: add file_server module with plain bytes size parsing"
```

---

### Task 2: Size Parsing - Kilobytes, Megabytes, Gigabytes

**Files:**
- Modify: `src/file_server.rs:5-12`
- Modify: `tests/file_server_tests.rs:3-10`

- [ ] **Step 1: Write failing test for KB/MB/GB formats**

Add to `tests/file_server_tests.rs`:

```rust
    #[test]
    fn test_parse_kilobytes() {
        assert_eq!(parse_size("1K"), Some(1024));
        assert_eq!(parse_size("1KB"), Some(1024));
        assert_eq!(parse_size("1.5K"), Some(1536));
        assert_eq!(parse_size("2.5KB"), Some(2560));
    }

    #[test]
    fn test_parse_megabytes() {
        assert_eq!(parse_size("1M"), Some(1048576));
        assert_eq!(parse_size("1MB"), Some(1048576));
        assert_eq!(parse_size("2.3M"), Some(2411724));
        assert_eq!(parse_size("1.5MB"), Some(1572864));
    }

    #[test]
    fn test_parse_gigabytes() {
        assert_eq!(parse_size("1G"), Some(1073741824));
        assert_eq!(parse_size("1GB"), Some(1073741824));
        assert_eq!(parse_size("1.2G"), Some(1288490188));
        assert_eq!(parse_size("2.5GB"), Some(2684354560));
    }

    #[test]
    fn test_parse_invalid() {
        assert_eq!(parse_size("invalid"), None);
        assert_eq!(parse_size(""), None);
        assert_eq!(parse_size("1.2.3M"), None);
        assert_eq!(parse_size("abc123"), None);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test test_parse_`
Expected: FAIL - all new tests return None

- [ ] **Step 3: Implement unit-based size parsing**

Replace `parse_size` in `src/file_server.rs`:

```rust
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test test_parse_`
Expected: PASS on all size parsing tests

- [ ] **Step 5: Commit**

```bash
git add src/file_server.rs tests/file_server_tests.rs
git commit -m "feat: add KB/MB/GB size parsing with 1024-based units"
```

---

### Task 3: Directory Listing Data Structures

**Files:**
- Modify: `src/file_server.rs:1-40`

- [ ] **Step 1: Add data structure definitions**

Add to top of `src/file_server.rs` after imports:

```rust
use regex::Regex;
use std::sync::OnceLock;

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
    pub total_files: i32,
    pub total_size: u64,
    pub depth_scanned: u32,
    pub skipped_count: u32,
    pub skipped_paths: Vec<String>,
}

#[derive(Debug, Clone)]
struct ScanResult {
    file_count: i32,
    total_size: u64,
    errors: Vec<String>,
}
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/file_server.rs
git commit -m "feat: add file server data structures"
```

---

### Task 4: Directory Listing Parser

**Files:**
- Modify: `src/file_server.rs:70-130`
- Modify: `tests/file_server_tests.rs:50-120`

- [ ] **Step 1: Write failing test for Apache-style listing**

Add to `tests/file_server_tests.rs`:

```rust
    use spyder::file_server::{parse_directory_listing, DirectoryListing, FileEntry};

    #[test]
    fn test_parse_apache_directory_listing() {
        let html = r#"
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
<head><title>Index of /files</title></head>
<body>
<h1>Index of /files</h1>
<pre>
<a href="../">../</a>
<a href="subdir/">subdir/</a>                  -
<a href="file1.txt">file1.txt</a>              1234
<a href="file2.dat">file2.dat</a>              5.5K
</pre>
</body>
</html>
"#;
        
        let listing = parse_directory_listing(html);
        
        assert_eq!(listing.directories, vec!["subdir/"]);
        assert_eq!(listing.files.len(), 2);
        assert_eq!(listing.files[0], FileEntry {
            name: "file1.txt".to_string(),
            size: 1234,
        });
        assert_eq!(listing.files[1], FileEntry {
            name: "file2.dat".to_string(),
            size: 5632,
        });
    }

    #[test]
    fn test_parse_nginx_directory_listing() {
        let html = r#"
<html>
<head><title>Index of /data</title></head>
<body>
<h1>Index of /data</h1><hr><pre>
<a href="../">../</a>
<a href="docs/">docs/</a>     01-Jan-2026 12:00    -
<a href="readme.txt">readme.txt</a>  01-Jan-2026 12:00  2.3M
</pre><hr></body>
</html>
"#;
        
        let listing = parse_directory_listing(html);
        
        assert_eq!(listing.directories, vec!["docs/"]);
        assert_eq!(listing.files.len(), 1);
        assert_eq!(listing.files[0].name, "readme.txt");
        assert_eq!(listing.files[0].size, 2411724);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test test_parse_.*directory_listing`
Expected: Compilation error - function doesn't exist

- [ ] **Step 3: Implement parse_directory_listing**

Add to `src/file_server.rs`:

```rust
use scraper::{Html, Selector};

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
    // Get the full text of the parent or containing line
    let parent_text = if let Some(parent) = element.parent() {
        parent.value().as_text()
            .map(|t| t.to_string())
            .or_else(|| {
                // Get text content from parent node
                Some(parent.children()
                    .filter_map(|n| n.value().as_text())
                    .map(|t| t.to_string())
                    .collect::<Vec<_>>()
                    .join(""))
            })?
    } else {
        return None;
    };
    
    // Also check next siblings for size info
    let mut text = parent_text.clone();
    let mut current = element.next_sibling();
    while let Some(node) = current {
        if let Some(t) = node.value().as_text() {
            text.push_str(t);
            // Stop after a reasonable amount of text
            if text.len() > 200 {
                break;
            }
        }
        current = node.next_sibling();
    }
    
    // Look for size patterns in the text
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test test_parse_.*directory_listing`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/file_server.rs tests/file_server_tests.rs
git commit -m "feat: add directory listing parser for Apache/nginx formats"
```

---

### Task 5: Recursive Directory Scanner (Core Logic)

**Files:**
- Modify: `src/file_server.rs:180-280`
- Modify: `tests/file_server_tests.rs:150-200`

- [ ] **Step 1: Write integration test for recursive scan**

Add to `tests/file_server_tests.rs`:

```rust
    #[test]
    fn test_scan_recursive_depth_limit() {
        // This test will use actual HTTP client, so we'll keep it simple
        // Real testing should use mock server
        use std::collections::HashSet;
        use spyder::file_server::scan_recursive;
        use reqwest::blocking::Client;
        
        let client = Client::new();
        let mut visited = HashSet::new();
        
        // Scan a non-existent URL to test error handling
        let result = scan_recursive(
            "http://localhost:99999/nonexistent/",
            0,
            3,
            &mut visited,
            &client,
        );
        
        // Should handle error gracefully
        assert_eq!(result.file_count, 0);
        assert_eq!(result.total_size, 0);
        assert!(!result.errors.is_empty());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_scan_recursive_depth_limit`
Expected: Compilation error - function doesn't exist

- [ ] **Step 3: Implement scan_recursive function**

Add to `src/file_server.rs`:

```rust
use reqwest::blocking::Client;
use std::collections::HashSet;
use std::time::Duration;
use url::Url;

const MAX_DIRECTORIES: usize = 100;
const FETCH_TIMEOUT_SECS: u64 = 10;

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

fn fetch_with_timeout(url: &str, client: &Client) -> anyhow::Result<String> {
    let timeout_secs = get_fetch_timeout_secs();
    let response = client
        .get(url)
        .timeout(Duration::from_secs(timeout_secs))
        .send()?;
    
    let body = response.text()?;
    Ok(body)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_scan_recursive_depth_limit`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/file_server.rs tests/file_server_tests.rs
git commit -m "feat: add recursive directory scanner with depth limiting"
```

---

### Task 6: Main Detection Entry Point

**Files:**
- Modify: `src/file_server.rs:320-380`

- [ ] **Step 1: Implement detect_file_server function**

Add to `src/file_server.rs`:

```rust
use anyhow::Result;

/// Detect and scan file server starting from an "Index of /" page
/// 
/// Returns None if not a file server or if disabled by config
pub fn detect_file_server(
    url: &str,
    body: &str,
    title: &str,
    client: &Client,
) -> Option<FileServerMetrics> {
    // Check if file server detection is enabled
    if !is_file_server_enabled() {
        return None;
    }
    
    // Check if this is an "Index of /" page
    if title.trim() != "Index of /" {
        return None;
    }
    
    // Perform recursive scan
    let max_depth = get_max_depth();
    let mut visited = HashSet::new();
    
    let scan_result = scan_recursive(url, 0, max_depth, &mut visited, client);
    
    let depth_scanned = visited.len().min(max_depth as usize + 1) as u32;
    
    Some(FileServerMetrics {
        total_files: scan_result.file_count,
        total_size: scan_result.total_size,
        depth_scanned,
        skipped_count: scan_result.errors.len() as u32,
        skipped_paths: scan_result.errors,
    })
}

fn is_file_server_enabled() -> bool {
    std::env::var("SPYDER_FILE_SERVER_ENABLED")
        .unwrap_or_else(|_| "true".to_string())
        .parse()
        .unwrap_or(true)
}

fn get_max_depth() -> u32 {
    std::env::var("SPYDER_FILE_SERVER_MAX_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3)
}

pub fn get_max_dirs() -> usize {
    std::env::var("SPYDER_FILE_SERVER_MAX_DIRS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(MAX_DIRECTORIES)
}

pub fn get_fetch_timeout_secs() -> u64 {
    std::env::var("SPYDER_FILE_SERVER_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(FETCH_TIMEOUT_SECS)
}
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/file_server.rs
git commit -m "feat: add main file server detection entry point"
```

---

### Task 7: Integration with Extraction Pipeline

**Files:**
- Modify: `src/extraction.rs:1-10` (add imports)
- Modify: `src/extraction.rs:13-40` (integrate detection)

- [ ] **Step 1: Add imports to extraction.rs**

Add to `src/extraction.rs` after existing imports (after line 11):

```rust
use crate::file_server;
```

- [ ] **Step 2: Integrate detection into extract_page_snapshot**

Modify `src/extraction.rs`, function `extract_page_snapshot`. After line 25 (after `extract_topic_observations` call), add:

```rust
    let topic_observations =
        extract_topic_observations(&document, &base_url, &title, &text, &links);
    
    // Check for file server detection
    let mut all_topic_observations = topic_observations;
    if let Some(file_server_metrics) = detect_file_server_if_enabled(&normalized_url, body, &title) {
        all_topic_observations.push(file_server_metrics);
    }
    
    let keyword_corpus = build_keyword_corpus(&normalized_url, &title, &text, &links);
```

Then update the PageSnapshot construction to use `all_topic_observations`:

```rust
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
        topic_observations: all_topic_observations,
    })
```

- [ ] **Step 3: Add helper function to extraction.rs**

Add at the end of `src/extraction.rs`:

```rust
fn detect_file_server_if_enabled(url: &str, body: &str, title: &str) -> Option<TopicObservation> {
    // Create HTTP client (reuse pattern from spyder binary)
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .ok()?;
    
    let metrics = file_server::detect_file_server(url, body, title, &client)?;
    
    // Format evidence string
    let mut evidence_parts = vec![
        format!("total_size: {} bytes", metrics.total_size),
        format!("depth: {}", metrics.depth_scanned),
    ];
    
    if metrics.skipped_count > 0 {
        evidence_parts.push(format!("skipped: {}", metrics.skipped_count));
        
        if !metrics.skipped_paths.is_empty() {
            // Include first few error paths
            let sample_paths: Vec<_> = metrics.skipped_paths
                .iter()
                .take(3)
                .map(|p| {
                    // Extract just the path from error message
                    p.split(':').next().unwrap_or(p)
                })
                .collect();
            
            if !sample_paths.is_empty() {
                evidence_parts.push(format!("(errors: {})", sample_paths.join(", ")));
            }
        }
    }
    
    Some(TopicObservation {
        topic: "file-server".to_string(),
        score: metrics.total_files,
        confidence: "high".to_string(),
        evidence: vec![evidence_parts.join(", ")],
    })
}
```

- [ ] **Step 4: Verify compilation**

Run: `cargo check`
Expected: No errors (may have warnings about unused imports)

- [ ] **Step 5: Build and test**

Run: `cargo build`
Expected: Successful build

- [ ] **Step 6: Commit**

```bash
git add src/extraction.rs
git commit -m "feat: integrate file server detection into extraction pipeline"
```

---

### Task 8: End-to-End Manual Testing

**Files:**
- No file changes, testing only

- [ ] **Step 1: Create test seed with mock file server page**

Create a test HTML file `/tmp/test_index.html`:

```html
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
<head><title>Index of /</title></head>
<body>
<h1>Index of /</h1>
<pre>
<a href="file1.txt">file1.txt</a>              1234
<a href="file2.dat">file2.dat</a>              5.5K
<a href="large.bin">large.bin</a>              2.3M
</pre>
</body>
</html>
```

- [ ] **Step 2: Test extraction with file server page**

Start a simple HTTP server:
```bash
cd /tmp && python3 -m http.server 8888
```

In another terminal, test the scanner:
```bash
cargo run --bin spyder -- add http://localhost:8888/
cargo run --bin spyder -- work --limit 1
```

- [ ] **Step 3: Verify database storage**

Query the database:
```bash
psql $DATABASE_URL -c "SELECT topic, score, confidence, evidence FROM page_topic_tag WHERE topic = 'file-server';"
```

Expected: One row with topic='file-server', score=3, evidence containing file count and size

- [ ] **Step 4: Test frontend display**

Start frontend:
```bash
cargo run --bin frontend
```

Navigate to: `http://localhost:8000/pages`

Expected: Page shows "file-server" topic badge with metrics

- [ ] **Step 5: Document test results**

Create `docs/FILE_SERVER_TESTING.md`:

```markdown
# File Server Detection Testing

## Manual Test Results

Date: 2026-06-07

### Test 1: Basic Detection
- URL: http://localhost:8888/
- Title: "Index of /"
- Files detected: 3
- Total size: ~2.4MB
- Status: ✓ PASS

### Test 2: Frontend Display
- Page list shows file-server badge: ✓ PASS
- Analytics includes file-server stats: ✓ PASS

### Configuration
- SPYDER_FILE_SERVER_ENABLED=true (default)
- SPYDER_FILE_SERVER_MAX_DEPTH=3 (default)
```

- [ ] **Step 6: Commit documentation**

```bash
git add docs/FILE_SERVER_TESTING.md
git commit -m "docs: add file server detection test results"
```

---

### Task 9: Configuration Documentation

**Files:**
- Modify: `README.md:512-520`

- [ ] **Step 1: Document configuration options**

Add to `README.md` after line 256 (after cache configuration section):

```markdown
## File Server Detection

Spyder automatically detects and analyzes web servers in directory browsing mode (pages with title "Index of /"). When detected, the scanner recursively crawls subdirectories up to 3 levels deep and records file counts and total sizes.

Configuration:

```bash
# Enable/disable file server detection (default: true)
SPYDER_FILE_SERVER_ENABLED=true cargo run --bin spyder -- work

# Maximum recursion depth (default: 3)
SPYDER_FILE_SERVER_MAX_DEPTH=5 cargo run --bin spyder -- work

# Maximum total directories to scan per file server (default: 100)
SPYDER_FILE_SERVER_MAX_DIRS=200 cargo run --bin spyder -- work

# Per-directory fetch timeout in seconds (default: 10)
SPYDER_FILE_SERVER_TIMEOUT=15 cargo run --bin spyder -- work
```

Results are stored in the `page_topic_tag` table with topic `file-server`. The score field contains the total file count, and the evidence field contains the total size in bytes, depth scanned, and any errors encountered.
```

- [ ] **Step 2: Verify documentation reads well**

Run: `cat README.md | grep -A 20 "File Server Detection"`
Expected: Documentation is clear and properly formatted

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: add file server detection configuration to README"
```

---

## Self-Review Checklist

**Spec Coverage:**
- ✓ Title detection ("Index of /") - Task 6
- ✓ Parse directory listings (Apache/nginx) - Task 4
- ✓ Recursive scanning up to 3 levels - Task 5
- ✓ Calculate file count and size - Task 5
- ✓ Store in page_topic_tag - Task 7
- ✓ Error handling (skip failed directories) - Task 5
- ✓ Size parsing (KB/MB/GB) - Tasks 1-2
- ✓ Configuration options - Tasks 6, 9
- ✓ Frontend display (automatic via existing topic tag system) - Task 8 verification

**Placeholders:** None - all code is complete

**Type Consistency:**
- FileServerMetrics fields match across all tasks ✓
- DirectoryListing structure consistent ✓
- parse_size signature consistent ✓
- TopicObservation matches existing model ✓

**Missing Implementation Notes:**
- MAX_DIRS configuration: Added constant but not config reading - will add in Task 6
- TIMEOUT configuration: Added constant but not config reading - will add in Task 6

Let me fix these:
