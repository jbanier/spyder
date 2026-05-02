use anyhow::{Context, Result};
use diesel::deserialize::QueryableByName;
use diesel::RunQueryDsl;
use reqwest::blocking::Client;
use reqwest::{Proxy, StatusCode};
use spyder::extraction::extract_page_snapshot;
use spyder::models::{HostSshObservationRecord, NewHostSshObservation};
use spyder::{
    add_domain_blacklist_entry, create_work_unit, establish_connection,
    find_matching_blacklist_domain, get_host_ssh_observation, get_pending_work_units,
    list_domain_blacklist_rules, list_recent_responding_hosts, mark_work_unit_as_done,
    normalize_crawl_url, record_work_unit_failure, remove_domain_blacklist_entry,
    save_host_ssh_observation, save_page_info, SSH_STATUS_SUCCESS,
};
use ssh2::{HashType, HostKeyType, Session};
use std::collections::HashSet;
use std::env;
use std::fmt::Display;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;
use url::Url;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailureKind {
    Retriable,
    Permanent,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct WorkOptions {
    onion_only: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SshScanOptions {
    recent_hours: i64,
    stale_hours: i64,
    limit: i64,
}

struct CrawlFailure {
    error: anyhow::Error,
    kind: FailureKind,
}

struct DiscoveryEnqueueOutcome {
    queued_count: usize,
    skipped_blacklisted_count: usize,
}

struct SocksProxyConfig {
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
}

struct SshHandshakeCapture {
    algorithm: String,
    host_key: String,
    fingerprint: String,
    server_banner: Option<String>,
}

#[derive(QueryableByName)]
struct NullableTextValueRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    value: Option<String>,
}

const SSH_PORTS: [u16; 2] = [22, 2222];
const DEFAULT_SSH_SCAN_RECENT_HOURS: i64 = 24 * 7;
const DEFAULT_SSH_SCAN_STALE_HOURS: i64 = 24;
const DEFAULT_SSH_SCAN_LIMIT: i64 = 200;
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_IO_TIMEOUT: Duration = Duration::from_secs(15);

impl Default for SshScanOptions {
    fn default() -> Self {
        Self {
            recent_hours: DEFAULT_SSH_SCAN_RECENT_HOURS,
            stale_hours: DEFAULT_SSH_SCAN_STALE_HOURS,
            limit: DEFAULT_SSH_SCAN_LIMIT,
        }
    }
}

impl CrawlFailure {
    fn retriable(error: anyhow::Error) -> Self {
        Self {
            error,
            kind: FailureKind::Retriable,
        }
    }

    fn permanent(error: anyhow::Error) -> Self {
        Self {
            error,
            kind: FailureKind::Permanent,
        }
    }
}

fn print_status(message: impl Display) {
    println!("==> {message}");
}

fn print_progress(current: usize, total: usize, message: impl Display) {
    println!("[{current}/{total}] {message}");
}

fn count_label(count: usize, singular: &str, plural: &str) -> String {
    format!("{} {}", count, if count == 1 { singular } else { plural })
}

fn compact_for_terminal(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return normalized;
    }

    let truncated = normalized.chars().take(max_chars).collect::<String>();
    if normalized.chars().count() > max_chars {
        format!("{truncated}...")
    } else {
        normalized
    }
}

fn summarize_page_snapshot(snapshot: &spyder::models::PageSnapshot) -> String {
    let mut parts = vec![
        count_label(snapshot.links.len(), "link", "links"),
        count_label(snapshot.emails.len(), "email", "emails"),
        count_label(snapshot.crypto_refs.len(), "crypto ref", "crypto refs"),
    ];
    let language = compact_for_terminal(&snapshot.language, 24);
    if !language.is_empty() {
        parts.push(format!("language {language}"));
    }

    let title = compact_for_terminal(&snapshot.title, 48);
    if title.is_empty() {
        parts.join(", ")
    } else {
        format!("title \"{title}\", {}", parts.join(", "))
    }
}

fn failure_kind_label(kind: FailureKind) -> &'static str {
    match kind {
        FailureKind::Retriable => "retriable",
        FailureKind::Permanent => "permanent",
    }
}

fn fetch_page_snapshot(
    client: &Client,
    url: &str,
) -> std::result::Result<spyder::models::PageSnapshot, CrawlFailure> {
    let body = fetch_body(client, url)?;
    extract_page_snapshot(url, &body)
        .map_err(|error| CrawlFailure::permanent(error.context(format!("failed to parse {url}"))))
}

fn fetch_body(client: &Client, url: &str) -> std::result::Result<String, CrawlFailure> {
    let response = client.get(url).send().map_err(|error| {
        let wrapped = anyhow::Error::new(error).context(format!("request failed for {url}"));
        if is_retriable_request_error(&wrapped) {
            CrawlFailure::retriable(wrapped)
        } else {
            CrawlFailure::permanent(wrapped)
        }
    })?;
    let status = response.status();
    if !status.is_success() {
        let error = anyhow::anyhow!("non-success status {} for {}", status.as_u16(), url);
        return if is_retriable_status(status) {
            Err(CrawlFailure::retriable(error))
        } else {
            Err(CrawlFailure::permanent(error))
        };
    }

    response.text().map_err(|error| {
        let wrapped =
            anyhow::Error::new(error).context(format!("failed to read response body for {url}"));
        CrawlFailure::retriable(wrapped)
    })
}

fn enqueue_seed_and_links(client: &Client, url: &str) -> Result<usize> {
    let normalized_url = normalize_crawl_url(url);
    let mut connection = establish_connection()?;
    print_status(format!("Queueing seed URL {normalized_url}"));
    create_work_unit(&mut connection, &normalized_url)?;

    Url::parse(&normalized_url).with_context(|| format!("invalid url: {normalized_url}"))?;
    print_status(format!("Fetching seed page {normalized_url}"));
    let snapshot = fetch_page_snapshot(client, &normalized_url)
        .map_err(|failure| failure.error)
        .with_context(|| format!("unable to discover links for seed {normalized_url}"))?;
    print_status(format!("Extracted {}", summarize_page_snapshot(&snapshot)));
    print_status("Queueing discovered links from the seed page");
    let outcome = enqueue_discovered_links(&mut connection, &snapshot)?;
    println!(
        "Queued {} discovered URLs from the seed page",
        outcome.queued_count
    );
    if outcome.skipped_blacklisted_count > 0 {
        println!(
            "Skipped {} blacklisted discovered URLs",
            outcome.skipped_blacklisted_count
        );
    }

    Ok(1 + outcome.queued_count)
}

fn work_queue(client: &Client, options: WorkOptions) -> Result<()> {
    let mut connection = establish_connection()?;
    print_status("Loading pending work units");
    let pending_work_units = get_pending_work_units(&mut connection)?;
    let pending_count = pending_work_units.len();
    let work_units = select_work_units_for_processing(pending_work_units, options);

    if work_units.is_empty() {
        if options.onion_only {
            println!(
                "No pending .onion work units to process ({} non-onion pending URLs skipped)",
                pending_count
            );
        } else {
            println!("No pending work units to process");
        }
        return Ok(());
    }

    let total = work_units.len();
    if options.onion_only {
        let skipped_count = pending_count - work_units.len();
        println!(
            "Working with {} pending work units whose host ends in .onion ({} skipped)",
            work_units.len(),
            skipped_count
        );
    } else {
        println!("Working with {} pending work units", work_units.len());
    }
    let mut processed_urls = HashSet::new();
    for (index, work_unit) in work_units.into_iter().enumerate() {
        let current = index + 1;
        let crawl_url = normalize_crawl_url(&work_unit.url);
        if processed_urls.contains(&crawl_url) {
            mark_work_unit_as_done(&mut connection, work_unit.id)?;
            print_progress(
                current,
                total,
                format!("Skipped duplicate URL {}", work_unit.url),
            );
            continue;
        }

        print_progress(current, total, format!("Fetching {crawl_url}"));

        match fetch_page_snapshot(client, &crawl_url) {
            Ok(page_snapshot) => {
                print_progress(
                    current,
                    total,
                    format!("Extracted {}", summarize_page_snapshot(&page_snapshot)),
                );
                save_page_info(&mut connection, &page_snapshot)?;
                let discovery_outcome = enqueue_discovered_links(&mut connection, &page_snapshot)?;
                mark_work_unit_as_done(&mut connection, work_unit.id)?;
                processed_urls.insert(crawl_url.clone());
                print_progress(
                    current,
                    total,
                    format!(
                        "Stored page and queued {} discovered URLs",
                        discovery_outcome.queued_count
                    ),
                );
                if discovery_outcome.skipped_blacklisted_count > 0 {
                    print_progress(
                        current,
                        total,
                        format!(
                            "Skipped {} blacklisted discovered URLs",
                            discovery_outcome.skipped_blacklisted_count
                        ),
                    );
                }
            }
            Err(failure) => {
                eprintln!(
                    "[{current}/{total}] Failed to process {} ({})",
                    crawl_url,
                    failure_kind_label(failure.kind)
                );
                eprintln!(
                    "ERROR: couldn't extract page information: {:?}",
                    failure.error
                );
                record_work_unit_failure(
                    &mut connection,
                    work_unit.id,
                    &failure.error.to_string(),
                    failure.kind == FailureKind::Retriable,
                )?;
                eprintln!(
                    "[{current}/{total}] Recorded failure state for {}",
                    work_unit.url
                );
            }
        }
    }

    Ok(())
}

fn ssh_scan_hosts(options: SshScanOptions) -> Result<()> {
    let mut connection = establish_connection()?;
    print_status(format!(
        "Loading hosts scanned in the last {} hours",
        options.recent_hours
    ));
    let candidates =
        list_recent_responding_hosts(&mut connection, options.recent_hours, Some(options.limit))?;
    if candidates.is_empty() {
        println!("No recently responding hosts to scan");
        return Ok(());
    }

    let stale_cutoff = ssh_stale_cutoff_timestamp(&mut connection, options.stale_hours)?;
    let proxy = load_socks_proxy_config()?;
    match proxy.as_ref() {
        Some(config) => print_status(format!(
            "Scanning SSH through SOCKS proxy {}",
            describe_socks_endpoint(config)
        )),
        None => print_status("No SOCKS proxy configured, scanning directly"),
    }

    let total_hosts = candidates.len();
    let mut attempted = 0usize;
    let mut skipped = 0usize;
    let mut successes = 0usize;
    let mut failures = 0usize;

    for (index, candidate) in candidates.into_iter().enumerate() {
        let current = index + 1;
        print_progress(
            current,
            total_hosts,
            format!("Scanning SSH endpoints for {}", candidate.host),
        );

        for port in SSH_PORTS {
            let existing =
                get_host_ssh_observation(&mut connection, &candidate.host, i32::from(port))?;
            if should_skip_ssh_attempt(existing.as_ref(), &candidate.last_scanned_at, &stale_cutoff)
            {
                skipped += 1;
                continue;
            }

            attempted += 1;
            match probe_ssh_endpoint(proxy.as_ref(), &candidate.host, port) {
                Ok(capture) => {
                    let fingerprint_preview = compact_for_terminal(&capture.fingerprint, 42);
                    save_host_ssh_observation(
                        &mut connection,
                        &NewHostSshObservation {
                            host: candidate.host.clone(),
                            port: i32::from(port),
                            status: SSH_STATUS_SUCCESS.to_string(),
                            host_key_algorithm: Some(capture.algorithm.clone()),
                            host_key: Some(capture.host_key),
                            host_key_fingerprint: Some(capture.fingerprint.clone()),
                            server_banner: capture.server_banner,
                            last_error: None,
                            last_attempt_at: String::new(),
                            last_success_at: None,
                        },
                    )?;
                    successes += 1;
                    print_progress(
                        current,
                        total_hosts,
                        format!(
                            "Saved {} {} for {}:{}",
                            capture.algorithm, fingerprint_preview, candidate.host, port
                        ),
                    );
                }
                Err(error) => {
                    let status = classify_ssh_probe_error(&error);
                    save_host_ssh_observation(
                        &mut connection,
                        &NewHostSshObservation {
                            host: candidate.host.clone(),
                            port: i32::from(port),
                            status: status.to_string(),
                            host_key_algorithm: None,
                            host_key: None,
                            host_key_fingerprint: None,
                            server_banner: None,
                            last_error: Some(truncate_for_storage(&error.to_string(), 500)),
                            last_attempt_at: String::new(),
                            last_success_at: None,
                        },
                    )?;
                    failures += 1;
                    eprintln!(
                        "[{current}/{total_hosts}] SSH scan failed for {}:{} ({status})",
                        candidate.host, port
                    );
                    eprintln!("ERROR: {error:?}");
                }
            }
        }
    }

    println!(
        "Attempted {} SSH endpoints across {} hosts ({} successes, {} failures, {} skipped)",
        attempted, total_hosts, successes, failures, skipped
    );
    Ok(())
}

fn load_socks_proxy_config() -> Result<Option<SocksProxyConfig>> {
    configured_proxy_url()
        .map(|proxy_url| parse_socks_proxy_config(&proxy_url))
        .transpose()
}

fn parse_socks_proxy_config(proxy_url: &str) -> Result<SocksProxyConfig> {
    let parsed =
        Url::parse(proxy_url).with_context(|| format!("invalid proxy url: {proxy_url}"))?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "socks5" | "socks5h"),
        "ssh-scan requires a socks5 proxy url, got {}",
        parsed.scheme()
    );

    let host = parsed
        .host_str()
        .map(|value| value.to_string())
        .context("proxy url must include a host")?;
    let port = parsed
        .port_or_known_default()
        .context("proxy url must include a port")?;
    let username = if parsed.username().is_empty() {
        None
    } else {
        Some(parsed.username().to_string())
    };

    Ok(SocksProxyConfig {
        host,
        port,
        username,
        password: parsed.password().map(|value| value.to_string()),
    })
}

fn probe_ssh_endpoint(
    proxy: Option<&SocksProxyConfig>,
    host: &str,
    port: u16,
) -> Result<SshHandshakeCapture> {
    let stream = connect_tcp_endpoint(proxy, host, port)?;
    let mut session = Session::new().context("failed to create ssh session")?;
    session.set_timeout(TCP_IO_TIMEOUT.as_millis() as u32);
    session.set_tcp_stream(stream);
    session
        .handshake()
        .with_context(|| format!("ssh handshake failed for {host}:{port}"))?;

    let (host_key, host_key_type) = session
        .host_key()
        .context("ssh server completed handshake without a host key")?;
    let algorithm = ssh_host_key_algorithm_name(host_key_type).to_string();
    let fingerprint = session
        .host_key_hash(HashType::Sha256)
        .map(|bytes| format!("sha256:{}", hex_encode(bytes)))
        .unwrap_or_else(|| format!("sha256:{}", hex_encode(host_key)));
    let server_banner = session.banner().map(|value| value.to_string());
    let _ = session.disconnect(None, "done", None);

    Ok(SshHandshakeCapture {
        algorithm,
        host_key: hex_encode(host_key),
        fingerprint,
        server_banner,
    })
}

fn connect_tcp_endpoint(
    proxy: Option<&SocksProxyConfig>,
    host: &str,
    port: u16,
) -> Result<TcpStream> {
    match proxy {
        Some(config) => connect_via_socks_proxy(config, host, port),
        None => {
            let address = resolve_socket_addr(host, port)?;
            let stream = TcpStream::connect_timeout(&address, TCP_CONNECT_TIMEOUT)
                .with_context(|| format!("tcp connect failed for {host}:{port}"))?;
            apply_tcp_timeouts(&stream)?;
            Ok(stream)
        }
    }
}

fn connect_via_socks_proxy(
    proxy: &SocksProxyConfig,
    target_host: &str,
    target_port: u16,
) -> Result<TcpStream> {
    let proxy_address = resolve_socket_addr(&proxy.host, proxy.port)?;
    let mut stream =
        TcpStream::connect_timeout(&proxy_address, TCP_CONNECT_TIMEOUT).with_context(|| {
            format!(
                "tcp connect failed for SOCKS proxy {}",
                describe_socks_endpoint(proxy)
            )
        })?;
    apply_tcp_timeouts(&stream)?;

    let methods = if proxy.username.is_some() || proxy.password.is_some() {
        vec![0x00_u8, 0x02_u8]
    } else {
        vec![0x00_u8]
    };
    let mut greeting = vec![0x05_u8, methods.len() as u8];
    greeting.extend_from_slice(&methods);
    stream
        .write_all(&greeting)
        .context("SOCKS proxy greeting write failed")?;

    let mut greeting_response = [0_u8; 2];
    stream
        .read_exact(&mut greeting_response)
        .context("SOCKS proxy greeting response read failed")?;
    anyhow::ensure!(
        greeting_response[0] == 0x05,
        "SOCKS proxy replied with unsupported version {}",
        greeting_response[0]
    );
    match greeting_response[1] {
        0x00 => {}
        0x02 => perform_socks5_username_password_auth(&mut stream, proxy)?,
        0xFF => anyhow::bail!("SOCKS proxy rejected all authentication methods"),
        method => anyhow::bail!("SOCKS proxy selected unsupported auth method {method}"),
    }

    let mut request = vec![0x05_u8, 0x01_u8, 0x00_u8];
    if let Ok(ip) = target_host.parse::<IpAddr>() {
        match ip {
            IpAddr::V4(value) => {
                request.push(0x01);
                request.extend_from_slice(&value.octets());
            }
            IpAddr::V6(value) => {
                request.push(0x04);
                request.extend_from_slice(&value.octets());
            }
        }
    } else {
        let host_bytes = target_host.as_bytes();
        anyhow::ensure!(
            host_bytes.len() <= u8::MAX as usize,
            "target host is too long for SOCKS5: {target_host}"
        );
        request.push(0x03);
        request.push(host_bytes.len() as u8);
        request.extend_from_slice(host_bytes);
    }
    request.extend_from_slice(&target_port.to_be_bytes());

    stream.write_all(&request).with_context(|| {
        format!("SOCKS connect request write failed for {target_host}:{target_port}")
    })?;

    let mut response_header = [0_u8; 4];
    stream
        .read_exact(&mut response_header)
        .context("SOCKS proxy connect response read failed")?;
    anyhow::ensure!(
        response_header[0] == 0x05,
        "SOCKS proxy connect reply used unsupported version {}",
        response_header[0]
    );
    anyhow::ensure!(
        response_header[1] == 0x00,
        "SOCKS proxy connect failed for {}:{} ({})",
        target_host,
        target_port,
        socks_reply_label(response_header[1])
    );
    read_socks_reply_tail(&mut stream, response_header[3])?;

    Ok(stream)
}

fn perform_socks5_username_password_auth(
    stream: &mut TcpStream,
    proxy: &SocksProxyConfig,
) -> Result<()> {
    let username = proxy.username.as_deref().unwrap_or_default().as_bytes();
    let password = proxy.password.as_deref().unwrap_or_default().as_bytes();
    anyhow::ensure!(
        username.len() <= u8::MAX as usize && password.len() <= u8::MAX as usize,
        "SOCKS proxy credentials are too long"
    );

    let mut request = vec![0x01_u8, username.len() as u8];
    request.extend_from_slice(username);
    request.push(password.len() as u8);
    request.extend_from_slice(password);
    stream
        .write_all(&request)
        .context("SOCKS proxy auth request write failed")?;

    let mut response = [0_u8; 2];
    stream
        .read_exact(&mut response)
        .context("SOCKS proxy auth response read failed")?;
    anyhow::ensure!(
        response[1] == 0x00,
        "SOCKS proxy username/password authentication failed"
    );
    Ok(())
}

fn read_socks_reply_tail(stream: &mut TcpStream, address_type: u8) -> Result<()> {
    match address_type {
        0x01 => {
            let mut buffer = [0_u8; 4];
            stream.read_exact(&mut buffer)?;
        }
        0x03 => {
            let mut length = [0_u8; 1];
            stream.read_exact(&mut length)?;
            let mut buffer = vec![0_u8; length[0] as usize];
            stream.read_exact(&mut buffer)?;
        }
        0x04 => {
            let mut buffer = [0_u8; 16];
            stream.read_exact(&mut buffer)?;
        }
        other => anyhow::bail!("SOCKS proxy returned unsupported address type {other}"),
    }

    let mut port = [0_u8; 2];
    stream.read_exact(&mut port)?;
    Ok(())
}

fn resolve_socket_addr(host: &str, port: u16) -> Result<SocketAddr> {
    (host, port)
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve {host}:{port}"))?
        .next()
        .with_context(|| format!("no socket address found for {host}:{port}"))
}

fn apply_tcp_timeouts(stream: &TcpStream) -> Result<()> {
    stream
        .set_read_timeout(Some(TCP_IO_TIMEOUT))
        .context("failed to set tcp read timeout")?;
    stream
        .set_write_timeout(Some(TCP_IO_TIMEOUT))
        .context("failed to set tcp write timeout")?;
    Ok(())
}

fn should_skip_ssh_attempt(
    existing: Option<&HostSshObservationRecord>,
    host_last_scanned_at: &str,
    stale_cutoff: &str,
) -> bool {
    existing
        .map(|row| {
            let last_attempt_at = row.last_attempt_at.as_str();
            last_attempt_at >= stale_cutoff && last_attempt_at >= host_last_scanned_at
        })
        .unwrap_or(false)
}

fn ssh_stale_cutoff_timestamp(
    conn: &mut diesel::sqlite::SqliteConnection,
    stale_hours: i64,
) -> Result<String> {
    let stale_hours = stale_hours.clamp(1, 24 * 365);
    let query = format!("SELECT datetime(CURRENT_TIMESTAMP, '-{stale_hours} hours') AS value");
    diesel::sql_query(query)
        .get_result::<NullableTextValueRow>(conn)
        .context("error loading ssh stale cutoff timestamp")?
        .value
        .context("ssh stale cutoff query returned no value")
}

fn classify_ssh_probe_error(error: &anyhow::Error) -> &'static str {
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|io_error| io_error.kind() == std::io::ErrorKind::TimedOut)
            .unwrap_or(false)
    }) {
        return "timeout";
    }
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|io_error| io_error.kind() == std::io::ErrorKind::ConnectionRefused)
            .unwrap_or(false)
    }) {
        return "connection-refused";
    }

    let rendered = error.to_string().to_ascii_lowercase();
    if rendered.contains("socks") || rendered.contains("proxy") {
        "proxy-error"
    } else if error
        .chain()
        .any(|cause| cause.downcast_ref::<ssh2::Error>().is_some())
    {
        "handshake-failed"
    } else {
        "network-error"
    }
}

fn ssh_host_key_algorithm_name(key_type: HostKeyType) -> &'static str {
    match key_type {
        HostKeyType::Rsa => "ssh-rsa",
        HostKeyType::Dss => "ssh-dss",
        HostKeyType::Ecdsa256 => "ecdsa-sha2-nistp256",
        HostKeyType::Ecdsa384 => "ecdsa-sha2-nistp384",
        HostKeyType::Ecdsa521 => "ecdsa-sha2-nistp521",
        HostKeyType::Ed25519 => "ssh-ed25519",
        HostKeyType::Unknown => "unknown",
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn truncate_for_storage(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn describe_socks_endpoint(proxy: &SocksProxyConfig) -> String {
    format!("{}:{}", proxy.host, proxy.port)
}

fn socks_reply_label(code: u8) -> &'static str {
    match code {
        0x01 => "general failure",
        0x02 => "connection not allowed",
        0x03 => "network unreachable",
        0x04 => "host unreachable",
        0x05 => "connection refused",
        0x06 => "ttl expired",
        0x07 => "command not supported",
        0x08 => "address type not supported",
        _ => "unknown error",
    }
}

fn select_work_units_for_processing(
    work_units: Vec<spyder::models::WorkUnit>,
    options: WorkOptions,
) -> Vec<spyder::models::WorkUnit> {
    work_units
        .into_iter()
        .filter(|work_unit| !options.onion_only || url_targets_onion(&work_unit.url))
        .collect()
}

fn url_targets_onion(url: &str) -> bool {
    Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed
                .host_str()
                .map(|host| host.trim_end_matches('.').to_string())
        })
        .map(|host| host.to_ascii_lowercase().ends_with(".onion"))
        .unwrap_or(false)
}

fn enqueue_discovered_links(
    connection: &mut diesel::sqlite::SqliteConnection,
    snapshot: &spyder::models::PageSnapshot,
) -> Result<DiscoveryEnqueueOutcome> {
    let blacklist_domains = list_domain_blacklist_rules(connection)?
        .into_iter()
        .map(|rule| rule.domain)
        .collect::<Vec<_>>();
    let mut outcome = DiscoveryEnqueueOutcome {
        queued_count: 0,
        skipped_blacklisted_count: 0,
    };

    for link in &snapshot.links {
        let parsed = Url::parse(&link.target_url)
            .with_context(|| format!("invalid discovered url: {}", link.target_url))?;
        match parsed.scheme() {
            "http" | "https" => {
                let link_host = parsed
                    .host_str()
                    .map(|value| value.to_ascii_lowercase())
                    .unwrap_or_else(|| link.target_host.to_ascii_lowercase());
                if find_matching_blacklist_domain(&link_host, &blacklist_domains).is_some() {
                    outcome.skipped_blacklisted_count += 1;
                    continue;
                }
                create_work_unit(connection, parsed.as_str())?;
                outcome.queued_count += 1;
            }
            _ => {}
        }
    }
    Ok(outcome)
}

fn list_blacklist() -> Result<()> {
    let mut connection = establish_connection()?;
    let entries = list_domain_blacklist_rules(&mut connection)?;

    if entries.is_empty() {
        println!("No blacklisted domains configured");
        return Ok(());
    }

    for entry in entries {
        println!("{}", entry.domain);
    }
    Ok(())
}

fn add_blacklist_domain(raw_domain: &str) -> Result<()> {
    let mut connection = establish_connection()?;
    let entry = add_domain_blacklist_entry(&mut connection, raw_domain)?;
    println!("Blacklisted {}", entry.domain);
    Ok(())
}

fn remove_blacklist_domain(raw_domain: &str) -> Result<()> {
    let mut connection = establish_connection()?;
    let domain = remove_domain_blacklist_entry(&mut connection, raw_domain)?;
    println!("Removed blacklist entry {}", domain);
    Ok(())
}

fn usage(program: &str) {
    eprintln!("Usage: {program} [SUBCOMMAND] [OPTIONS]");
    eprintln!("Subcommands:");
    eprintln!("    add <url>      enqueue the seed page and discovered links.");
    eprintln!("    blacklist list");
    eprintln!("    blacklist add <domain>");
    eprintln!("    blacklist remove <domain>");
    eprintln!(
        "    ssh-scan [--recent-hours N] [--stale-hours N] [--limit N] scan recent hosts for SSH host keys."
    );
    eprintln!("    work [--onion-only] process pending work units and store page metadata.");
}

fn print_error(error: &anyhow::Error) {
    eprintln!("ERROR: {error:?}");

    if error
        .chain()
        .any(|cause| cause.to_string().contains("no such table:"))
    {
        eprintln!(
            "HINT: database schema is missing. Run `diesel setup` and `diesel migration run`."
        );
    }
}

fn build_http_client() -> Result<Client> {
    // Crawl targets may present self-signed, expired, or otherwise invalid certificates.
    let mut builder = Client::builder()
        .timeout(Duration::from_secs(15))
        .danger_accept_invalid_certs(true)
        .no_proxy();
    if let Some(proxy_url) = configured_proxy_url() {
        builder = builder.proxy(
            Proxy::all(&proxy_url).with_context(|| format!("invalid proxy url: {proxy_url}"))?,
        );
    }

    builder.build().context("http client should build")
}

fn configured_proxy_url() -> Option<String> {
    configured_proxy_url_from_values(env::var("ALL_PROXY").ok(), env::var("all_proxy").ok())
}

fn configured_proxy_url_from_values(
    upper: Option<String>,
    lower: Option<String>,
) -> Option<String> {
    upper
        .into_iter()
        .chain(lower)
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

fn parse_work_options(args: impl IntoIterator<Item = String>) -> Result<WorkOptions> {
    let mut options = WorkOptions::default();

    for arg in args {
        match arg.as_str() {
            "--onion-only" => options.onion_only = true,
            _ => anyhow::bail!("invalid work option: {arg}"),
        }
    }

    Ok(options)
}

fn parse_ssh_scan_options(args: impl IntoIterator<Item = String>) -> Result<SshScanOptions> {
    let mut options = SshScanOptions::default();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--recent-hours" => {
                options.recent_hours =
                    parse_i64_option_value(args.next(), "--recent-hours", 1, 24 * 365)?;
            }
            "--stale-hours" => {
                options.stale_hours =
                    parse_i64_option_value(args.next(), "--stale-hours", 1, 24 * 365)?;
            }
            "--limit" => {
                options.limit = parse_i64_option_value(args.next(), "--limit", 1, 2_000)?;
            }
            _ => anyhow::bail!("invalid ssh-scan option: {arg}"),
        }
    }

    Ok(options)
}

fn parse_i64_option_value(value: Option<String>, option: &str, min: i64, max: i64) -> Result<i64> {
    let raw = value.with_context(|| format!("missing value for {option}"))?;
    let parsed = raw
        .parse::<i64>()
        .with_context(|| format!("invalid integer value for {option}: {raw}"))?;
    anyhow::ensure!(
        parsed >= min && parsed <= max,
        "{option} must be between {min} and {max}"
    );
    Ok(parsed)
}

fn is_retriable_request_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(reqwest_error) = cause.downcast_ref::<reqwest::Error>() {
            reqwest_error.is_timeout() || reqwest_error.is_connect() || reqwest_error.is_request()
        } else {
            false
        }
    })
}

fn is_retriable_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn main() {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "spyder".to_string());
    let result = match args.next().as_deref() {
        Some("add") => match args.next() {
            Some(url) => build_http_client().and_then(|client| {
                enqueue_seed_and_links(&client, &url).map(|count| {
                    println!("Enqueued {count} URLs");
                })
            }),
            None => {
                usage(&program);
                Err(anyhow::anyhow!("no url is provided"))
            }
        },
        Some("blacklist") => match args.next().as_deref() {
            Some("list") => list_blacklist(),
            Some("add") => match args.next() {
                Some(domain) => add_blacklist_domain(&domain),
                None => {
                    usage(&program);
                    Err(anyhow::anyhow!("no blacklist domain is provided"))
                }
            },
            Some("remove") => match args.next() {
                Some(domain) => remove_blacklist_domain(&domain),
                None => {
                    usage(&program);
                    Err(anyhow::anyhow!("no blacklist domain is provided"))
                }
            },
            Some(_) | None => {
                usage(&program);
                Err(anyhow::anyhow!("invalid or missing blacklist subcommand"))
            }
        },
        Some("ssh-scan") => match parse_ssh_scan_options(args) {
            Ok(options) => ssh_scan_hosts(options),
            Err(error) => {
                usage(&program);
                Err(error)
            }
        },
        Some("work") => match parse_work_options(args) {
            Ok(options) => build_http_client().and_then(|client| work_queue(&client, options)),
            Err(error) => {
                usage(&program);
                Err(error)
            }
        },
        Some(_) | None => {
            usage(&program);
            Err(anyhow::anyhow!("invalid or missing subcommand"))
        }
    };

    if let Err(error) = result {
        print_error(&error);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::connection::SimpleConnection;
    use diesel::Connection;

    fn setup_connection() -> diesel::sqlite::SqliteConnection {
        let mut conn =
            diesel::sqlite::SqliteConnection::establish(":memory:").expect("in-memory sqlite");
        conn.batch_execute(
            "
            CREATE TABLE work_unit(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              url VARCHAR NOT NULL UNIQUE,
              status VARCHAR NOT NULL DEFAULT 'pending',
              retry_count INTEGER NOT NULL DEFAULT 0,
              next_attempt_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
              last_attempt_at VARCHAR,
              last_error VARCHAR,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE domain_blacklist(
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              domain VARCHAR NOT NULL UNIQUE,
              created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            ",
        )
        .expect("schema setup");
        conn
    }

    #[test]
    fn proxy_prefers_uppercase_then_lowercase() {
        assert_eq!(
            configured_proxy_url_from_values(
                Some("socks5h://upper:9050".to_string()),
                Some("socks5h://lower:9050".to_string()),
            ),
            Some("socks5h://upper:9050".to_string())
        );
        assert_eq!(
            configured_proxy_url_from_values(None, Some(" socks5h://lower:9050 ".to_string())),
            Some("socks5h://lower:9050".to_string())
        );
        assert_eq!(
            configured_proxy_url_from_values(None, Some("   ".to_string())),
            None
        );
    }

    #[test]
    fn retryable_statuses_match_transient_http_failures() {
        assert!(is_retriable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retriable_status(StatusCode::BAD_GATEWAY));
        assert!(!is_retriable_status(StatusCode::NOT_FOUND));
    }

    #[test]
    fn compact_for_terminal_normalizes_whitespace_and_truncates() {
        assert_eq!(
            compact_for_terminal("  Alpha   Beta\tGamma  ", 10),
            "Alpha Beta..."
        );
    }

    #[test]
    fn snapshot_summary_includes_title_and_counts() {
        let snapshot = spyder::models::PageSnapshot {
            title: "Alpha Market".to_string(),
            url: "http://alpha.onion".to_string(),
            language: "English".to_string(),
            links: vec![spyder::models::LinkObservation {
                target_url: "http://beta.onion".to_string(),
                target_host: "beta.onion".to_string(),
            }],
            emails: vec!["ops@alpha.onion".to_string()],
            crypto_refs: vec![spyder::models::CryptoReference {
                asset_type: "btc".to_string(),
                reference: "bc1test".to_string(),
            }],
            classification_signals: spyder::models::ClassificationSignals::default(),
        };

        assert_eq!(
            summarize_page_snapshot(&snapshot),
            "title \"Alpha Market\", 1 link, 1 email, 1 crypto ref, language English"
        );
    }

    #[test]
    fn work_options_accept_onion_only_flag() {
        let options = parse_work_options(vec!["--onion-only".to_string()]).expect("work options");
        assert_eq!(options, WorkOptions { onion_only: true });
    }

    #[test]
    fn work_options_reject_unknown_flags() {
        let error = parse_work_options(vec!["--bogus".to_string()]).expect_err("invalid option");
        assert_eq!(error.to_string(), "invalid work option: --bogus");
    }

    #[test]
    fn ssh_scan_options_use_defaults() {
        let options = parse_ssh_scan_options(Vec::<String>::new()).expect("ssh scan options");
        assert_eq!(
            options,
            SshScanOptions {
                recent_hours: DEFAULT_SSH_SCAN_RECENT_HOURS,
                stale_hours: DEFAULT_SSH_SCAN_STALE_HOURS,
                limit: DEFAULT_SSH_SCAN_LIMIT,
            }
        );
    }

    #[test]
    fn ssh_scan_options_parse_custom_values() {
        let options = parse_ssh_scan_options(vec![
            "--recent-hours".to_string(),
            "12".to_string(),
            "--stale-hours".to_string(),
            "4".to_string(),
            "--limit".to_string(),
            "32".to_string(),
        ])
        .expect("ssh scan options");
        assert_eq!(
            options,
            SshScanOptions {
                recent_hours: 12,
                stale_hours: 4,
                limit: 32,
            }
        );
    }

    #[test]
    fn onion_only_work_selection_skips_non_onion_urls() {
        let mut conn = setup_connection();
        create_work_unit(&mut conn, "http://alpha.onion").expect("insert onion work unit");
        create_work_unit(&mut conn, "https://example.com").expect("insert clearnet work unit");
        create_work_unit(&mut conn, "notaurl").expect("insert invalid work unit");

        let selected = select_work_units_for_processing(
            get_pending_work_units(&mut conn).expect("pending work units"),
            WorkOptions { onion_only: true },
        );

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].url, "http://alpha.onion");
    }

    #[test]
    fn discovered_blacklisted_links_are_not_queued() {
        let mut conn = setup_connection();
        add_domain_blacklist_entry(&mut conn, "blocked.onion").expect("add blacklist");

        let snapshot = spyder::models::PageSnapshot {
            title: "Seed".to_string(),
            url: "http://seed.onion".to_string(),
            language: "English".to_string(),
            links: vec![
                spyder::models::LinkObservation {
                    target_url: "http://allowed.onion".to_string(),
                    target_host: "allowed.onion".to_string(),
                },
                spyder::models::LinkObservation {
                    target_url: "http://sub.blocked.onion".to_string(),
                    target_host: "sub.blocked.onion".to_string(),
                },
            ],
            emails: Vec::new(),
            crypto_refs: Vec::new(),
            classification_signals: spyder::models::ClassificationSignals::default(),
        };

        let outcome = enqueue_discovered_links(&mut conn, &snapshot).expect("enqueue links");
        assert_eq!(outcome.queued_count, 1);
        assert_eq!(outcome.skipped_blacklisted_count, 1);

        let pending = get_pending_work_units(&mut conn).expect("pending work units");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].url, "http://allowed.onion");
    }

    #[test]
    fn discovered_fragment_links_share_one_work_unit() {
        let mut conn = setup_connection();

        let snapshot = spyder::models::PageSnapshot {
            title: "Seed".to_string(),
            url: "http://seed.onion".to_string(),
            language: "English".to_string(),
            links: vec![
                spyder::models::LinkObservation {
                    target_url: "http://allowed.onion/docs#faq".to_string(),
                    target_host: "allowed.onion".to_string(),
                },
                spyder::models::LinkObservation {
                    target_url: "http://allowed.onion/docs#pricing".to_string(),
                    target_host: "allowed.onion".to_string(),
                },
            ],
            emails: Vec::new(),
            crypto_refs: Vec::new(),
            classification_signals: spyder::models::ClassificationSignals::default(),
        };

        enqueue_discovered_links(&mut conn, &snapshot).expect("enqueue links");

        let pending = get_pending_work_units(&mut conn).expect("pending work units");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].url, "http://allowed.onion");
    }
}
