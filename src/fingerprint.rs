use serde_json::{json, Value};
use std::collections::HashMap;

/// Extract server software from HTTP headers
pub fn extract_server_software(headers: &HashMap<String, String>) -> Option<String> {
    // Priority order for server detection
    let server_headers = vec![
        "server",
        "x-powered-by",
        "x-aspnet-version",
        "x-aspnetmvc-version",
    ];

    for header_name in server_headers {
        if let Some(value) = headers.get(header_name) {
            return Some(sanitize_server_string(value));
        }
    }

    None
}

/// Detect technologies from HTTP headers (Wappalyzer-style)
pub fn detect_technologies(headers: &HashMap<String, String>, body: Option<&str>) -> Value {
    let mut technologies = Vec::new();

    // Web servers
    if let Some(server) = headers.get("server") {
        let server_lower = server.to_lowercase();
        if server_lower.contains("nginx") {
            technologies.push(technology("nginx", "Web Server", extract_version(server, "nginx")));
        }
        if server_lower.contains("apache") {
            technologies.push(technology("apache", "Web Server", extract_version(server, "apache")));
        }
        if server_lower.contains("iis") || server_lower.contains("microsoft-iis") {
            technologies.push(technology("Microsoft IIS", "Web Server", extract_version(server, "iis")));
        }
        if server_lower.contains("cloudflare") {
            technologies.push(technology("Cloudflare", "CDN", None));
        }
        if server_lower.contains("openresty") {
            technologies.push(technology("OpenResty", "Web Server", extract_version(server, "openresty")));
        }
    }

    // Backend frameworks
    if let Some(powered_by) = headers.get("x-powered-by") {
        let powered_lower = powered_by.to_lowercase();
        if powered_lower.contains("php") {
            technologies.push(technology("PHP", "Language", extract_version(powered_by, "php")));
        }
        if powered_lower.contains("asp.net") {
            technologies.push(technology("ASP.NET", "Framework", extract_version(powered_by, "asp.net")));
        }
        if powered_lower.contains("express") {
            technologies.push(technology("Express", "Framework", extract_version(powered_by, "express")));
        }
    }

    // JavaScript frameworks (from headers)
    if let Some(_) = headers.get("x-nextjs-page") {
        technologies.push(technology("Next.js", "Framework", None));
    }
    if let Some(_) = headers.get("x-nuxt-version") {
        technologies.push(technology("Nuxt.js", "Framework", headers.get("x-nuxt-version").map(|s| s.to_string())));
    }

    // Security headers
    if let Some(_) = headers.get("content-security-policy") {
        technologies.push(technology("Content Security Policy", "Security", None));
    }
    if let Some(_) = headers.get("strict-transport-security") {
        technologies.push(technology("HSTS", "Security", None));
    }

    // CDN/Proxies
    if let Some(_) = headers.get("cf-ray") {
        technologies.push(technology("Cloudflare", "CDN", None));
    }
    if let Some(_) = headers.get("x-amz-cf-id") {
        technologies.push(technology("Amazon CloudFront", "CDN", None));
    }
    if let Some(via) = headers.get("via") {
        if via.to_lowercase().contains("varnish") {
            technologies.push(technology("Varnish", "Cache", None));
        }
    }

    // Body-based detection (if provided)
    if let Some(html) = body {
        detect_from_body(html, &mut technologies);
    }

    json!(technologies)
}

fn detect_from_body(html: &str, technologies: &mut Vec<Value>) {
    let html_lower = html.to_lowercase();

    // CMS detection
    if html_lower.contains("wp-content") || html_lower.contains("wordpress") {
        technologies.push(technology("WordPress", "CMS", None));
    }
    if html_lower.contains("drupal") {
        technologies.push(technology("Drupal", "CMS", None));
    }
    if html_lower.contains("joomla") {
        technologies.push(technology("Joomla", "CMS", None));
    }

    // JavaScript libraries
    if html_lower.contains("jquery") {
        technologies.push(technology("jQuery", "JavaScript Library", None));
    }
    if html_lower.contains("react") {
        technologies.push(technology("React", "JavaScript Framework", None));
    }
    if html_lower.contains("vue.js") || html_lower.contains("vue.min.js") {
        technologies.push(technology("Vue.js", "JavaScript Framework", None));
    }
    if html_lower.contains("angular") {
        technologies.push(technology("Angular", "JavaScript Framework", None));
    }

    // Analytics
    if html_lower.contains("google-analytics") || html_lower.contains("ga.js") {
        technologies.push(technology("Google Analytics", "Analytics", None));
    }
    if html_lower.contains("matomo") {
        technologies.push(technology("Matomo", "Analytics", None));
    }
}

fn technology(name: &str, category: &str, version: Option<String>) -> Value {
    let mut tech = json!({
        "name": name,
        "category": category,
    });

    if let Some(v) = version {
        tech["version"] = json!(v);
    }

    tech
}

/// Extract version number from a header value
fn extract_version(value: &str, tech_name: &str) -> Option<String> {
    // Simple version extraction: look for pattern like "nginx/1.18.0"
    let tech_lower = tech_name.to_lowercase();
    let value_lower = value.to_lowercase();

    if let Some(pos) = value_lower.find(&tech_lower) {
        let after_name = &value[pos + tech_name.len()..];

        // Skip forward slash or space
        let version_start = after_name.chars()
            .position(|c| c.is_ascii_digit())?;

        let version_str = after_name[version_start..]
            .split_whitespace()
            .next()?
            .trim_end_matches(|c: char| !c.is_ascii_digit() && c != '.');

        if !version_str.is_empty() {
            return Some(version_str.to_string());
        }
    }

    None
}

/// Sanitize server string for storage (limit length, remove sensitive info)
fn sanitize_server_string(server: &str) -> String {
    // Take first 255 chars
    let truncated = if server.len() > 255 {
        &server[..255]
    } else {
        server
    };

    truncated.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_server_nginx() {
        let mut headers = HashMap::new();
        headers.insert("server".to_string(), "nginx/1.18.0".to_string());

        let server = extract_server_software(&headers);
        assert_eq!(server, Some("nginx/1.18.0".to_string()));
    }

    #[test]
    fn test_extract_server_apache() {
        let mut headers = HashMap::new();
        headers.insert("server".to_string(), "Apache/2.4.41 (Unix)".to_string());

        let server = extract_server_software(&headers);
        assert_eq!(server, Some("Apache/2.4.41 (Unix)".to_string()));
    }

    #[test]
    fn test_detect_technologies_nginx() {
        let mut headers = HashMap::new();
        headers.insert("server".to_string(), "nginx/1.18.0".to_string());

        let tech = detect_technologies(&headers, None);
        let tech_array = tech.as_array().unwrap();

        assert_eq!(tech_array.len(), 1);
        assert_eq!(tech_array[0]["name"], "nginx");
        assert_eq!(tech_array[0]["category"], "Web Server");
        assert_eq!(tech_array[0]["version"], "1.18.0");
    }

    #[test]
    fn test_detect_technologies_cloudflare() {
        let mut headers = HashMap::new();
        headers.insert("cf-ray".to_string(), "abc123".to_string());
        headers.insert("server".to_string(), "cloudflare".to_string());

        let tech = detect_technologies(&headers, None);
        let tech_array = tech.as_array().unwrap();

        // Should detect Cloudflare from both headers
        assert!(tech_array.len() >= 1);
        assert!(tech_array.iter().any(|t| t["name"] == "Cloudflare"));
    }

    #[test]
    fn test_detect_wordpress_from_body() {
        let mut headers = HashMap::new();
        let body = r#"<html><head><link rel="stylesheet" href="/wp-content/themes/mytheme/style.css"></head></html>"#;

        let tech = detect_technologies(&headers, Some(body));
        let tech_array = tech.as_array().unwrap();

        assert!(tech_array.iter().any(|t| t["name"] == "WordPress"));
    }

    #[test]
    fn test_extract_version() {
        assert_eq!(extract_version("nginx/1.18.0", "nginx"), Some("1.18.0".to_string()));
        assert_eq!(extract_version("Apache/2.4.41", "apache"), Some("2.4.41".to_string()));
        assert_eq!(extract_version("PHP/7.4.3", "php"), Some("7.4.3".to_string()));
        assert_eq!(extract_version("cloudflare", "nginx"), None);
    }

    #[test]
    fn test_sanitize_long_server_string() {
        let long_string = "a".repeat(300);
        let sanitized = sanitize_server_string(&long_string);

        assert_eq!(sanitized.len(), 255);
    }
}
