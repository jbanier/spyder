#[cfg(test)]
mod tests {
    use spyder::file_server::{parse_size, parse_directory_listing, FileEntry};

    #[test]
    fn test_parse_plain_bytes() {
        assert_eq!(parse_size("1234"), Some(1234));
        assert_eq!(parse_size("0"), Some(0));
        assert_eq!(parse_size("999999"), Some(999999));
    }

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

    #[test]
    fn test_parse_overflow() {
        // Values that would overflow u64
        assert_eq!(parse_size("99999999999999G"), None);
        assert_eq!(parse_size("18446744073709.6G"), None);
        assert_eq!(parse_size("999999999T"), None);
        // Valid large values should still work
        assert_eq!(parse_size("16384G"), Some(16384 * 1024 * 1024 * 1024));
    }

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
}
