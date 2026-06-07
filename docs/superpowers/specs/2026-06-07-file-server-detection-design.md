# File Server Detection and Metrics Collection

## Overview

Extend the Spyder scanner to detect web servers in file browsing mode (directory listing pages with title "Index of /"), automatically tag them as "file-server", and collect metrics about the number of files and total size available by recursively scanning up to 3 directory levels.

## Requirements

- Detect pages with title exactly matching "Index of /"
- Parse directory listings to extract file information
- Recursively scan subdirectories up to 3 levels deep
- Calculate total file count and cumulative size across all scanned directories
- Store metrics in the existing `page_topic_tag` table
- Handle errors gracefully by skipping failed directories without aborting the entire scan

## Detection Logic

### Trigger Condition

During page extraction in `extract_page_snapshot()`, after extracting the page title, check if the title exactly matches "Index of /". This indicates a web server in directory browsing mode.

### Supported Formats

The parser will support common web server directory listing formats:
- Apache HTTP Server directory indexes
- nginx autoindex pages
- lighttpd directory listings

These formats typically render as HTML tables or pre-formatted text containing file/directory names, sizes, modification dates, and types.

## Directory Parsing

### HTML Parsing Strategy

Use regex patterns and HTML parsing to extract directory entries:

1. **Identify links:** Parse `<a href="...">` tags to find files and subdirectories
2. **Distinguish files from directories:** 
   - Directory links typically end with `/`
   - Exclude parent directory links (`../`)
3. **Extract sizes:** Parse human-readable sizes (e.g., "1.5M", "2.3K", "456 bytes")
4. **Convert to bytes:** Normalize all sizes to bytes for aggregation

### Size Parsing

Support common size formats:
- Plain numbers: "1234" → 1234 bytes
- Kilobytes: "1.5K", "1.5KB" → 1536 bytes (using 1024-based)
- Megabytes: "2.3M", "2.3MB" → 2411724 bytes (using 1024-based)
- Gigabytes: "1.2G", "1.2GB" → 1288490188 bytes (using 1024-based)

Use binary (1024-based) units throughout, as this matches common web server directory listing conventions (Apache, nginx use binary units).

## Recursive Crawling

### Traversal Algorithm

1. **Starting point:** Begin at the detected "Index of /" page (depth 0)
2. **Parse current directory:** Extract all files and subdirectories
3. **Accumulate file metrics:** Add file count and sizes to running totals
4. **Recurse into subdirectories:** For each subdirectory found:
   - Check if current depth < 3
   - Check if URL not already visited (loop prevention)
   - Fetch subdirectory listing
   - Parse and recurse
5. **Return aggregated metrics:** Total files and total size across all successfully scanned directories

### Depth Tracking

- Root "Index of /" page is depth 0
- First-level subdirectories are depth 1
- Second-level subdirectories are depth 2
- Third-level subdirectories are depth 3
- Do not recurse beyond depth 3 (i.e., recurse while current_depth < 3)

### Error Handling

**Per-directory failures:**
- Network errors (timeout, connection refused, DNS failure)
- HTTP errors (404, 403, 500)
- Parse errors (malformed HTML, unrecognized format)

**Handling strategy:**
- Log the failed URL and error type
- Include skipped paths in the evidence field
- Continue scanning sibling and parent directories
- Do not fail the entire file server scan

**Example evidence with errors:**
```
total_size: 1234567 bytes, depth: 3, skipped: 2 (errors: /protected/, /timeout/)
```

### Recursion Safeguards

**Infinite loop prevention:**
- Track all visited URLs in a HashSet
- Before fetching a directory, check if URL already visited
- Skip if already processed

**Resource limits:**
- Per-directory fetch timeout: 10 seconds
- Maximum total directories scanned per file server: 100
- If limits exceeded, record what was scanned and stop

**Network reuse:**
- Reuse existing HTTP client from the crawler
- Use same proxy settings (Tor SOCKS if configured)
- Respect existing timeout and retry configurations

## Data Storage

### Table and Fields

Store results in the existing `page_topic_tag` table with this mapping:

| Field | Value | Description |
|-------|-------|-------------|
| `page_id` | Integer | ID of the "Index of /" page |
| `topic` | `"file-server"` | Static identifier for this feature |
| `score` | Integer | Total number of files found |
| `confidence` | `"high"` | Static value (deterministic detection) |
| `evidence` | String | Structured metadata (see below) |
| `created_at` | Timestamp | When the scan occurred |

### Evidence Format

The `evidence` field contains structured text with these components:

**Success case:**
```
total_size: 2847392 bytes, depth: 3
```

**With skipped directories:**
```
total_size: 2847392 bytes, depth: 3, skipped: 2
```

**With error details:**
```
total_size: 2847392 bytes, depth: 2, skipped: 5 (errors: /admin/, /private/)
```

### Example Records

**Small file server:**
```sql
INSERT INTO page_topic_tag (page_id, topic, score, confidence, evidence, created_at)
VALUES (123, 'file-server', 42, 'high', 'total_size: 18432 bytes, depth: 1', '2026-06-07 21:30:00');
```

**Large file server with errors:**
```sql
INSERT INTO page_topic_tag (page_id, topic, score, confidence, evidence, created_at)
VALUES (456, 'file-server', 1247, 'high', 'total_size: 5497558144 bytes, depth: 3, skipped: 3', '2026-06-07 21:35:00');
```

## Integration Points

### Extraction Pipeline

The file server detection integrates into `src/extraction.rs` in the `extract_page_snapshot()` function:

1. Extract title (existing code)
2. **NEW:** Check if title == "Index of /"
3. **NEW:** If matched, perform recursive directory scan
4. **NEW:** Create TopicObservation for "file-server" with metrics
5. Extract other page elements (existing code)
6. **NEW:** Append file-server TopicObservation to topic_observations vector
7. Return PageSnapshot (existing code)

### New Module

Create a new module `src/file_server.rs` containing:

- `detect_file_server(url: &str, body: &str, http_client: &Client) -> Option<FileServerMetrics>`
- `parse_directory_listing(html: &str) -> DirectoryListing`
- `parse_size(size_str: &str) -> Option<u64>`
- `scan_recursive(url: &str, depth: u32, visited: &mut HashSet<String>, client: &Client) -> ScanResult`

### Data Structures

```rust
pub struct FileServerMetrics {
    pub total_files: i32,
    pub total_size: u64,
    pub depth_scanned: u32,
    pub skipped_count: u32,
    pub skipped_paths: Vec<String>,
}

struct DirectoryListing {
    files: Vec<FileEntry>,
    directories: Vec<String>,
}

struct FileEntry {
    name: String,
    size: u64,
}

struct ScanResult {
    file_count: i32,
    total_size: u64,
    errors: Vec<String>,
}
```

### Configuration

Add optional configuration to control scanning behavior:

- `SPYDER_FILE_SERVER_MAX_DEPTH`: Maximum recursion depth (default: 3)
- `SPYDER_FILE_SERVER_MAX_DIRS`: Maximum directories to scan (default: 100)
- `SPYDER_FILE_SERVER_TIMEOUT`: Per-directory timeout in seconds (default: 10)
- `SPYDER_FILE_SERVER_ENABLED`: Enable/disable feature (default: true)

## Frontend Display

The file server tags will automatically appear in existing views:

### Pages List
- Display "file-server" topic badge alongside other topic tags
- Show file count and size in tooltip or badge details

### Page Detail View
- Include file server metrics in topic tags section
- Format total size in human-readable units (KB/MB/GB)
- Display depth scanned and skipped count if present

### Analytics View
- Add file-server to topic statistics
- Show total number of file servers discovered
- Optionally aggregate total storage across all file servers

## Testing Strategy

### Unit Tests

1. **Title detection:** Verify "Index of /" triggers detection, variations don't
2. **Size parsing:** Test various size formats (K, M, G, KB, MB, GB, plain bytes)
3. **Directory parsing:** Parse sample Apache/nginx/lighttpd HTML
4. **Depth limiting:** Verify recursion stops at depth 3
5. **Loop prevention:** Verify circular directory links don't cause infinite loops
6. **Error handling:** Verify failed fetches don't abort the scan

### Integration Tests

1. Mock HTTP server serving directory listings
2. Test recursive scanning with nested directories
3. Test timeout handling
4. Test max-directory limit enforcement
5. Verify correct storage in page_topic_tag

### Manual Testing

1. Scan actual public file servers (archive.org, mirror sites)
2. Test with various directory listing formats
3. Verify metrics match expected counts
4. Check frontend display of results

## Performance Considerations

### Network Impact

Each file server scan may fetch multiple HTTP requests:
- 1 request for root (already fetched)
- Up to 100 additional directory requests
- Each request waits up to 10 seconds

**Worst case:** 100 directories × 10s = 16.6 minutes per file server

**Mitigation:**
- This is acceptable for background crawling
- File servers are typically a small percentage of pages
- Can be disabled via configuration if needed

### Database Impact

Minimal - adds one row to `page_topic_tag` per file server detected. No schema changes required.

### Memory Usage

Bounded by visited URL tracking:
- HashSet of up to 100 URLs
- Each URL ~200 bytes average
- Total: ~20KB per file server scan

## Future Enhancements

Potential improvements for future iterations:

1. **Configurable depth per scan:** Allow operator to specify depth
2. **File type analysis:** Track file extensions and types
3. **Suspicious file detection:** Flag potentially malicious files (.exe, .dll, etc.)
4. **Download tracking:** Record which files are accessible vs forbidden
5. **Change detection:** Re-scan file servers to detect new/removed files
6. **Parallel directory fetching:** Speed up scans with concurrent requests

## Security Considerations

### Resource Exhaustion

The feature includes safeguards against resource exhaustion:
- Maximum depth (3 levels)
- Maximum directories (100)
- Per-directory timeout (10 seconds)
- Loop detection via visited URL tracking

### Malicious Listings

File servers could contain malicious content designed to exploit crawlers:
- **Infinite redirects:** Prevented by visited URL tracking
- **Zip bombs in size parsing:** Size values validated before parsing
- **XSS in filenames:** HTML parsing doesn't execute scripts
- **Recursive symlinks:** Limited by depth and URL deduplication

### Privacy

The feature only collects publicly accessible directory information. No authentication is attempted, and no private content is accessed beyond what the crawler already handles.

## Summary

This design extends Spyder's page extraction to detect and analyze file server directory listings. It fits naturally into the existing extraction pipeline, uses current database structures, requires no schema changes, and provides valuable intelligence about exposed file servers encountered during crawling. The implementation is bounded by clear limits to prevent resource exhaustion while still providing useful metrics.
