/// HTTP metadata capture for enhanced page intelligence
use reqwest::blocking::Response;
use reqwest::header::HeaderMap;
use serde_json::{json, Value};
use std::collections::HashMap;

/// Headers containing credentials that should be redacted from logs
const REDACTED_HEADERS: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "cookie",
    "x-api-key",
    "x-auth-token",
    "x-csrf-token",
    "x-session-token",
    "api-key",
    "apikey",
];

/// Extended HTTP capture with full metadata
pub struct EnhancedHttpCapture {
    pub status_code: i32,
    pub response_time_ms: i32,
    pub response_headers: Value,
    pub request_headers: Value,
    pub cookies: Value,
    pub redirect_chain: Value,
    pub server_software: Option<String>,
    pub detected_technologies: Value,
}

/// Capture HTTP metadata from request/response
pub fn capture_http_metadata(
    response: &Response,
    request_headers: &HeaderMap,
    elapsed_ms: u64,
    body: &str,
) -> EnhancedHttpCapture {
    let status_code = response.status().as_u16() as i32;
    let response_time_ms = elapsed_ms as i32;

    // Convert headers to HashMap for processing
    let response_headers_map = headers_to_hashmap(response.headers());
    let request_headers_map = headers_to_hashmap(request_headers);

    // Extract cookies from Set-Cookie headers
    let cookies = extract_cookies(response.headers());

    // Build redirect chain (if available from response)
    let redirect_chain = json!([]);  // TODO: Track redirects during fetch

    // Server fingerprinting
    let server_software = crate::fingerprint::extract_server_software(&response_headers_map);

    // Technology detection
    let detected_technologies =
        crate::fingerprint::detect_technologies(&response_headers_map, Some(body));

    EnhancedHttpCapture {
        status_code,
        response_time_ms,
        response_headers: json!(response_headers_map),
        request_headers: json!(request_headers_map),
        cookies,
        redirect_chain,
        server_software,
        detected_technologies,
    }
}

/// Convert HeaderMap to HashMap<String, String> for JSON serialization
/// Redacts sensitive headers containing credentials
fn headers_to_hashmap(headers: &HeaderMap) -> HashMap<String, String> {
    let mut map = HashMap::new();

    for (name, value) in headers.iter() {
        let key = name.as_str().to_lowercase();

        // Security: Redact sensitive headers to prevent credential leakage
        let val = if REDACTED_HEADERS.contains(&key.as_str()) {
            "[REDACTED]".to_string()
        } else {
            value.to_str().unwrap_or("<binary>").to_string()
        };

        // Combine multiple values with comma (HTTP standard)
        map.entry(key)
            .and_modify(|existing: &mut String| {
                existing.push_str(", ");
                existing.push_str(&val);
            })
            .or_insert(val);
    }

    map
}

/// Extract cookies from Set-Cookie headers
fn extract_cookies(headers: &HeaderMap) -> Value {
    let cookies: Vec<Value> = headers
        .get_all("set-cookie")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .map(|cookie_str| parse_cookie(cookie_str))
        .collect();

    json!(cookies)
}

/// Parse a Set-Cookie header value
/// Security: Masks cookie values to prevent credential leakage
fn parse_cookie(cookie_str: &str) -> Value {
    let parts: Vec<&str> = cookie_str.split(';').map(|s| s.trim()).collect();

    if parts.is_empty() {
        return json!({});
    }

    // First part is name=value
    let (name, value) = parts[0]
        .split_once('=')
        .unwrap_or((parts[0], ""));

    // Security: Mask cookie value to prevent session token leakage
    // Keep only structure for analysis (name + attributes), not sensitive data
    let masked_value = if value.is_empty() {
        "".to_string()
    } else if value.len() <= 4 {
        "***".to_string()
    } else {
        format!("{}***", &value[..4])
    };

    let mut cookie = json!({
        "name": name,
        "value": masked_value,
        "value_length": value.len(),
    });

    // Parse attributes (Path, Domain, Secure, HttpOnly, SameSite, etc.)
    for part in &parts[1..] {
        let (key, val) = part.split_once('=').unwrap_or((part, "true"));
        cookie[key.to_lowercase()] = json!(val);
    }

    cookie
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue, SET_COOKIE};

    #[test]
    fn test_headers_to_hashmap() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("text/html"));
        headers.insert("server", HeaderValue::from_static("nginx/1.18.0"));

        let map = headers_to_hashmap(&headers);

        assert_eq!(map.get("content-type"), Some(&"text/html".to_string()));
        assert_eq!(map.get("server"), Some(&"nginx/1.18.0".to_string()));
    }

    #[test]
    fn test_headers_to_hashmap_multiple_values() {
        let mut headers = HeaderMap::new();
        headers.append("cache-control", HeaderValue::from_static("no-cache"));
        headers.append("cache-control", HeaderValue::from_static("no-store"));

        let map = headers_to_hashmap(&headers);

        assert_eq!(map.get("cache-control"), Some(&"no-cache, no-store".to_string()));
    }

    #[test]
    fn test_parse_cookie() {
        let cookie_str = "session_id=abc123; Path=/; Secure; HttpOnly";
        let cookie = parse_cookie(cookie_str);

        assert_eq!(cookie["name"], "session_id");
        // Value is masked for security
        assert_eq!(cookie["value"], "abc1***");
        assert_eq!(cookie["value_length"], 6);
        assert_eq!(cookie["path"], "/");
        assert_eq!(cookie["secure"], "true");
        assert_eq!(cookie["httponly"], "true");
    }

    #[test]
    fn test_parse_cookie_short_value() {
        let cookie_str = "x=1";
        let cookie = parse_cookie(cookie_str);

        assert_eq!(cookie["name"], "x");
        assert_eq!(cookie["value"], "***");
        assert_eq!(cookie["value_length"], 1);
    }

    #[test]
    fn test_headers_redaction() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("text/html"));
        headers.insert("authorization", HeaderValue::from_static("Bearer secret-token-12345"));
        headers.insert("cookie", HeaderValue::from_static("session=abc123"));
        headers.insert("x-api-key", HeaderValue::from_static("key-12345"));

        let map = headers_to_hashmap(&headers);

        // Non-sensitive headers preserved
        assert_eq!(map.get("content-type"), Some(&"text/html".to_string()));

        // Sensitive headers redacted
        assert_eq!(map.get("authorization"), Some(&"[REDACTED]".to_string()));
        assert_eq!(map.get("cookie"), Some(&"[REDACTED]".to_string()));
        assert_eq!(map.get("x-api-key"), Some(&"[REDACTED]".to_string()));
    }

    #[test]
    fn test_extract_cookies() {
        let mut headers = HeaderMap::new();
        headers.insert(SET_COOKIE, HeaderValue::from_static("id=a3fWa; Expires=Thu, 21 Oct 2021 07:28:00 GMT"));
        headers.append(SET_COOKIE, HeaderValue::from_static("session=xyz; Path=/"));

        let cookies = extract_cookies(&headers);
        let cookies_array = cookies.as_array().unwrap();

        assert_eq!(cookies_array.len(), 2);
        assert_eq!(cookies_array[0]["name"], "id");
        assert_eq!(cookies_array[1]["name"], "session");
    }
}
