#[cfg(test)]
mod tests {
    use spyder::file_server::parse_size;

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
}
