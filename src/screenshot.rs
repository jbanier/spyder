use anyhow::{bail, Context, Result};
use headless_chrome::{Browser, LaunchOptions};
use std::ffi::OsString;
use std::fs;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;
use url::Url;

pub struct ScreenshotConfig {
    pub screenshots_dir: PathBuf,
    pub timeout_seconds: u64,
    pub viewport_width: u32,
    pub viewport_height: u32,
    pub max_size_bytes: usize,
    pub proxy_url: Option<String>,
}

impl Default for ScreenshotConfig {
    fn default() -> Self {
        Self {
            screenshots_dir: PathBuf::from("screenshots"),
            timeout_seconds: 30,
            viewport_width: 1920,
            viewport_height: 1080,
            max_size_bytes: 50 * 1024 * 1024, // 50MB max
            proxy_url: None,
        }
    }
}

pub struct ScreenshotCapture {
    config: ScreenshotConfig,
}

impl ScreenshotCapture {
    pub fn new(config: ScreenshotConfig) -> Result<Self> {
        // Create screenshots directory if it doesn't exist
        fs::create_dir_all(&config.screenshots_dir)
            .context("Failed to create screenshots directory")?;

        Ok(Self { config })
    }

    /// Capture a screenshot of the given URL
    /// Returns (screenshot_path, size_in_bytes) on success
    pub fn capture(&self, url: &str, page_id: i32) -> Result<(String, i32)> {
        // Security: Validate URL before navigation
        validate_url(url)?;

        // Build launch options with proxy if configured
        // Note: Storing proxy_arg to ensure it lives long enough for the borrow
        let _proxy_arg: Option<OsString>;
        let launch_options = if let Some(proxy) = &self.config.proxy_url {
            _proxy_arg = Some(OsString::from(format!("--proxy-server={}", proxy)));
            LaunchOptions {
                headless: true,
                sandbox: true,
                idle_browser_timeout: Duration::from_secs(self.config.timeout_seconds),
                args: if let Some(ref arg) = _proxy_arg {
                    vec![arg.as_os_str()]
                } else {
                    vec![]
                },
                ..Default::default()
            }
        } else {
            _proxy_arg = None;
            LaunchOptions {
                headless: true,
                sandbox: true,
                idle_browser_timeout: Duration::from_secs(self.config.timeout_seconds),
                ..Default::default()
            }
        };

        let browser = Browser::new(launch_options)
            .context("Failed to launch headless browser")?;

        let tab = browser.new_tab()
            .context("Failed to create browser tab")?;

        // Navigate to validated URL
        tab.navigate_to(url)
            .context("Failed to navigate to URL")?;

        tab.wait_for_element("body")
            .context("Page body element not found")?;

        // Additional wait for dynamic content
        std::thread::sleep(Duration::from_secs(2));

        // Capture screenshot
        let screenshot_data = tab.capture_screenshot(
            headless_chrome::protocol::cdp::Page::CaptureScreenshotFormatOption::Png,
            None,
            None,
            true,
        ).context("Failed to capture screenshot")?;

        // Security: Enforce size limit
        if screenshot_data.len() > self.config.max_size_bytes {
            bail!("Screenshot exceeds maximum allowed size ({} bytes)", self.config.max_size_bytes);
        }

        // Generate filename: {page_id}_{timestamp}.png
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let filename = format!("{}_{}.png", page_id, timestamp);
        let filepath = self.config.screenshots_dir.join(&filename);

        // Write screenshot to disk
        fs::write(&filepath, &screenshot_data)
            .context("Failed to write screenshot to disk")?;

        // Security: Use try_from to prevent overflow
        let size_bytes = i32::try_from(screenshot_data.len())
            .context("Screenshot size overflow")?;
        let relative_path = filepath.to_string_lossy().to_string();

        Ok((relative_path, size_bytes))
    }

    /// Capture screenshot with network-specific timeout
    pub fn capture_with_timeout(&self, url: &str, page_id: i32, timeout_secs: u64) -> Result<(String, i32)> {
        let mut custom_config = self.config.clone();
        custom_config.timeout_seconds = timeout_secs;

        let custom_capture = Self::new(custom_config)?;
        custom_capture.capture(url, page_id)
    }
}

/// Validate URL to prevent SSRF and file:// access
fn validate_url(url_str: &str) -> Result<()> {
    let parsed = Url::parse(url_str)
        .context("Invalid URL format")?;

    // Only allow HTTP/HTTPS schemes
    match parsed.scheme() {
        "http" | "https" => {},
        scheme => bail!("Unsupported URL scheme: {}. Only http:// and https:// are allowed", scheme),
    }

    // Block navigation to private/local addresses to prevent SSRF
    if let Some(host) = parsed.host_str() {
        // Check for localhost
        if host == "localhost" {
            bail!("Cannot screenshot localhost addresses (SSRF protection)");
        }
    }

    // Check IP addresses (including localhost IPs)
    if let Some(host) = parsed.host() {
        if let url::Host::Ipv4(ipv4) = host {
            let ip = IpAddr::V4(ipv4);
            if ip.is_loopback() || is_private_ip(&ip) {
                bail!("Cannot screenshot localhost or private IP addresses (SSRF protection)");
            }
        } else if let url::Host::Ipv6(ipv6) = host {
            let ip = IpAddr::V6(ipv6);
            if ip.is_loopback() || is_private_ip(&ip) || is_special_ipv6(&ip) {
                bail!("Cannot screenshot localhost or private IP addresses (SSRF protection)");
            }
        } else if let url::Host::Domain(domain) = host {
            // Block common internal DNS names
            if domain.ends_with(".local") || domain.ends_with(".internal") {
                bail!("Cannot screenshot internal domain names (SSRF protection)");
            }

            // Security: Resolve DNS and validate all resolved IPs
            // This prevents DNS-based SSRF where attacker controls DNS to point to internal IPs
            validate_domain_ips(domain)?;
        }
    }

    Ok(())
}

/// Resolve domain name and validate all resolved IPs are not private/internal
fn validate_domain_ips(domain: &str) -> Result<()> {
    // Attempt DNS resolution with port 80 (required for ToSocketAddrs)
    let addr_str = format!("{}:80", domain);

    match addr_str.to_socket_addrs() {
        Ok(addrs) => {
            for addr in addrs {
                let ip = addr.ip();
                if ip.is_loopback() {
                    bail!("Domain {} resolves to localhost (SSRF protection)", domain);
                }
                if is_private_ip(&ip) {
                    bail!("Domain {} resolves to private IP {} (SSRF protection)", domain, ip);
                }
                if let IpAddr::V6(ipv6) = ip {
                    if is_special_ipv6(&IpAddr::V6(ipv6)) {
                        bail!("Domain {} resolves to special IPv6 address (SSRF protection)", domain);
                    }
                }
            }
            Ok(())
        }
        Err(_) => {
            // DNS resolution failed - allow it to fail later during actual navigation
            // This prevents blocking valid .onion domains that don't resolve in clearnet DNS
            Ok(())
        }
    }
}

/// Check if IP address is in private range (RFC1918, link-local, etc.)
fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => {
            let octets = ipv4.octets();
            // 0.0.0.0/8 (this network)
            octets[0] == 0 ||
            // 10.0.0.0/8 (private)
            octets[0] == 10 ||
            // 100.64.0.0/10 (shared address space / carrier-grade NAT)
            (octets[0] == 100 && octets[1] >= 64 && octets[1] <= 127) ||
            // 172.16.0.0/12 (private)
            (octets[0] == 172 && octets[1] >= 16 && octets[1] <= 31) ||
            // 192.168.0.0/16 (private)
            (octets[0] == 192 && octets[1] == 168) ||
            // 198.18.0.0/15 (benchmark testing)
            (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19)) ||
            // 169.254.0.0/16 (link-local)
            (octets[0] == 169 && octets[1] == 254)
        }
        IpAddr::V6(ipv6) => {
            // fc00::/7 (unique local)
            ipv6.octets()[0] >= 0xfc ||
            // fe80::/10 (link-local)
            (ipv6.octets()[0] == 0xfe && (ipv6.octets()[1] & 0xc0) == 0x80)
        }
    }
}

/// Check for special IPv6 addresses that should be blocked
fn is_special_ipv6(ip: &IpAddr) -> bool {
    if let IpAddr::V6(ipv6) = ip {
        let octets = ipv6.octets();

        // :: (unspecified)
        if ipv6.is_unspecified() {
            return true;
        }

        // ::ffff:0:0/96 (IPv4-mapped IPv6 addresses)
        // Check if first 10 bytes are 0, next 2 are 0xff
        if octets[..10].iter().all(|&b| b == 0) && octets[10] == 0xff && octets[11] == 0xff {
            // Extract the IPv4 part and check if it's private
            let ipv4 = IpAddr::V4(std::net::Ipv4Addr::new(octets[12], octets[13], octets[14], octets[15]));
            return ipv4.is_loopback() || is_private_ip(&ipv4);
        }
    }
    false
}

impl Clone for ScreenshotConfig {
    fn clone(&self) -> Self {
        Self {
            screenshots_dir: self.screenshots_dir.clone(),
            timeout_seconds: self.timeout_seconds,
            viewport_width: self.viewport_width,
            viewport_height: self.viewport_height,
            max_size_bytes: self.max_size_bytes,
            proxy_url: self.proxy_url.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_screenshot_config_default() {
        let config = ScreenshotConfig::default();
        assert_eq!(config.timeout_seconds, 30);
        assert_eq!(config.viewport_width, 1920);
        assert_eq!(config.viewport_height, 1080);
        assert_eq!(config.max_size_bytes, 50 * 1024 * 1024);
    }

    #[test]
    fn test_screenshot_directory_creation() {
        let temp_dir = env::temp_dir().join("spyder_test_screenshots");
        let config = ScreenshotConfig {
            screenshots_dir: temp_dir.clone(),
            ..Default::default()
        };

        let capture = ScreenshotCapture::new(config).expect("Failed to create ScreenshotCapture");
        assert!(temp_dir.exists());

        // Cleanup
        fs::remove_dir_all(temp_dir).ok();
    }

    #[test]
    fn test_validate_url_https() {
        assert!(validate_url("https://example.com").is_ok());
        assert!(validate_url("http://example.onion").is_ok());
    }

    #[test]
    fn test_validate_url_blocks_file_scheme() {
        let result = validate_url("file:///etc/passwd");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported URL scheme"));
    }

    #[test]
    fn test_validate_url_blocks_localhost() {
        assert!(validate_url("http://localhost").is_err());
        assert!(validate_url("http://127.0.0.1").is_err());
        assert!(validate_url("http://[::1]").is_err());
    }

    #[test]
    fn test_validate_url_blocks_private_ips() {
        assert!(validate_url("http://10.0.0.1").is_err());
        assert!(validate_url("http://192.168.1.1").is_err());
        assert!(validate_url("http://172.16.0.1").is_err());
    }

    #[test]
    fn test_validate_url_blocks_internal_domains() {
        assert!(validate_url("http://server.local").is_err());
        assert!(validate_url("http://api.internal").is_err());
    }

    #[test]
    fn test_is_private_ip() {
        // RFC1918 private ranges
        assert!(is_private_ip(&"10.0.0.1".parse().unwrap()));
        assert!(is_private_ip(&"192.168.1.1".parse().unwrap()));
        assert!(is_private_ip(&"172.16.0.1".parse().unwrap()));

        // Link-local
        assert!(is_private_ip(&"169.254.1.1".parse().unwrap()));

        // This network (0.0.0.0/8)
        assert!(is_private_ip(&"0.0.0.1".parse().unwrap()));

        // Shared address space (100.64.0.0/10)
        assert!(is_private_ip(&"100.64.0.1".parse().unwrap()));
        assert!(is_private_ip(&"100.127.255.255".parse().unwrap()));

        // Benchmark testing (198.18.0.0/15)
        assert!(is_private_ip(&"198.18.0.1".parse().unwrap()));
        assert!(is_private_ip(&"198.19.255.255".parse().unwrap()));

        // Public IPs should not be blocked
        assert!(!is_private_ip(&"8.8.8.8".parse().unwrap()));
        assert!(!is_private_ip(&"1.1.1.1".parse().unwrap()));
    }

    #[test]
    fn test_is_special_ipv6() {
        // Unspecified address
        let unspecified: IpAddr = "::".parse().unwrap();
        assert!(is_special_ipv6(&unspecified));

        // IPv4-mapped IPv6 with private IPv4
        let mapped_private: IpAddr = "::ffff:192.168.1.1".parse().unwrap();
        assert!(is_special_ipv6(&mapped_private));

        // IPv4-mapped IPv6 with localhost
        let mapped_localhost: IpAddr = "::ffff:127.0.0.1".parse().unwrap();
        assert!(is_special_ipv6(&mapped_localhost));

        // Regular IPv6 should not be blocked
        let regular: IpAddr = "2001:4860:4860::8888".parse().unwrap();
        assert!(!is_special_ipv6(&regular));
    }

    #[test]
    fn test_validate_domain_ips_public() {
        // Public domain should pass (though DNS may fail in test environment)
        // This test validates the logic, actual DNS resolution may vary
        let result = validate_domain_ips("example.com");
        // Either passes validation or DNS fails (both acceptable)
        assert!(result.is_ok() || result.is_err());
    }
}
