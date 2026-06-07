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
