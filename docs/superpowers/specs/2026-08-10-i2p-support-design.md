# I2P Network Support for Spyder

**Date:** 2026-08-10  
**Status:** Design approved, ready for implementation  
**Approach:** Minimal Extension (Phase 1)

## Overview

Add I2P (Invisible Internet Project) network support to Spyder's web crawler, enabling discovery and indexing of .i2p eepsites alongside existing Tor (.onion) and clearnet capabilities. The implementation extends the current proxy-based architecture with intelligent TLD-based routing and network-aware metadata tracking to support cross-network intelligence correlation.

**Primary Goals:**
1. Crawl .i2p eepsites using existing i2pd HTTP proxy
2. Tag all pages with source network for cross-network queries
3. Enable cross-network entity correlation (emails, crypto, SSH fingerprints)
4. Adaptive timeout handling for I2P's slower tunnel establishment

## Architecture

### Network Detection Layer

Examines URL TLD to determine network type:
- `.onion` → Tor network
- `.i2p` → I2P network  
- Everything else → Clearnet

Detection happens per-request from URL alone (stateless). Implementation in `src/lib.rs`:

```rust
pub enum NetworkType {
    Clearnet,
    Tor,
    I2p,
}

pub fn detect_network_type(url: &str) -> NetworkType {
    if url.ends_with(".onion") || url.contains(".onion/") || url.contains(".onion:") {
        NetworkType::Tor
    } else if url.ends_with(".i2p") || url.contains(".i2p/") || url.contains(".i2p:") {
        NetworkType::I2p
    } else {
        NetworkType::Clearnet
    }
}
```

### Proxy Router

Configures `reqwest::Client` with appropriate proxy per network type:

**Environment variables:**
- `I2P_PROXY` → I2P HTTP proxy (default: `http://127.0.0.1:4444`)
- `TOR_PROXY` or `ALL_PROXY` → Tor SOCKS proxy (default: `socks5h://127.0.0.1:9050`)
- Clearnet → no proxy

**Implementation** (`src/bin/spyder.rs`):

```rust
fn build_http_client_for_network(network: NetworkType) -> Result<Client> {
    let mut builder = Client::builder()
        .timeout(get_timeout_for_network(network))
        .danger_accept_invalid_certs(true);
    
    if let Some(proxy_url) = get_proxy_for_network(network) {
        builder = builder.proxy(Proxy::all(&proxy_url)?);
    } else {
        builder = builder.no_proxy();
    }
    
    builder.build()
}

fn get_proxy_for_network(network: NetworkType) -> Option<String> {
    match network {
        NetworkType::I2p => env::var("I2P_PROXY")
            .ok()
            .or_else(|| Some("http://127.0.0.1:4444".to_string())),
        NetworkType::Tor => env::var("TOR_PROXY")
            .ok()
            .or_else(|| env::var("ALL_PROXY").ok())
            .or_else(|| Some("socks5h://127.0.0.1:9050".to_string())),
        NetworkType::Clearnet => None,
    }
}
```

### Adaptive Timeout Manager

Tracks per-network success rates and adjusts timeouts within configured bounds.

**Initial timeouts:**
- I2P: 45 seconds (conservative for tunnel building)
- Tor: 15 seconds (unchanged)
- Clearnet: 15 seconds (unchanged)

**Adaptive behavior (I2P only):**
- Track last 100 I2P requests (success/timeout/failure)
- If success rate > 80% over 50+ requests → reduce timeout to 30s
- If success rate drops below 60% → revert to 45s

**Implementation** (`src/config.rs` or inline in `spyder.rs`):

```rust
use lazy_static::lazy_static;
use std::sync::Mutex;

lazy_static! {
    static ref I2P_TIMEOUT_TRACKER: Mutex<TimeoutTracker> = 
        Mutex::new(TimeoutTracker::new(100));
}

struct TimeoutTracker {
    outcomes: VecDeque<bool>, // true = success, false = timeout/error
    max_size: usize,
}

fn get_timeout_for_network(network: NetworkType) -> Duration {
    match network {
        NetworkType::I2p => {
            let tracker = I2P_TIMEOUT_TRACKER.lock().unwrap();
            if tracker.success_rate() > 0.8 && tracker.count() >= 50 {
                Duration::from_secs(30)
            } else {
                Duration::from_secs(45)
            }
        },
        NetworkType::Tor => Duration::from_secs(15),
        NetworkType::Clearnet => Duration::from_secs(15),
    }
}
```

### Database Schema Extension

**Migration:** Add `network` column to `page` table.

```sql
-- Migration: 2026-08-10-add-network-field
ALTER TABLE page ADD COLUMN network VARCHAR(16) DEFAULT 'clearnet' NOT NULL;
CREATE INDEX idx_page_network ON page(network);
CREATE INDEX idx_page_network_host ON page(network, host);
```

**Values:** `'clearnet'`, `'tor'`, `'i2p'`

Existing pages default to `'clearnet'`. New pages set `network` based on URL TLD at insertion time.

## Components

### 1. Network Type Detection
- **File:** `src/lib.rs`
- **Exports:** `NetworkType` enum, `detect_network_type()` function
- **Tests:** Unit tests for TLD detection edge cases

### 2. Proxy Configuration
- **File:** `src/bin/spyder.rs`
- **Changes:**
  - Modify `build_http_client()` to `build_http_client_for_network(network: NetworkType)`
  - Add `get_proxy_for_network()`
  - Update all call sites: `enqueue_seed_and_links`, `work_queue`, `rescan_known_pages`

### 3. Adaptive Timeout Manager
- **File:** `src/config.rs` (new) or inline in `src/bin/spyder.rs`
- **Responsibility:** Track I2P request outcomes, compute current timeout
- **Exports:** `get_timeout_for_network()`, `record_i2p_outcome(success: bool)`

### 4. Database Migration
- **File:** `migrations_postgres/YYYY-MM-DD-HHMMSS_add_network_field/up.sql`
- **Actions:** Add column, create indexes
- **Down migration:** Drop indexes, drop column

### 5. Modified Crawl Functions
- **File:** `src/bin/spyder.rs`
- **Changes:**
  - `enqueue_seed_and_links`: Detect network from seed URL, use appropriate client
  - `work_queue`: Detect network from work unit URL before processing
  - `save_page_full`: Accept `network` parameter, store in database
  - `rescan_known_pages`: Read `network` from page record, use appropriate client

## Data Flow

### Seed URL Addition

```
User: cargo run --bin spyder -- add http://example.i2p
  ↓
detect_network_type("http://example.i2p") → NetworkType::I2p
  ↓
build_http_client_for_network(I2p) → Client with I2P_PROXY, 45s timeout
  ↓
Fetch page via i2pd HTTP proxy (127.0.0.1:4444)
  ↓
Extract: title, links, emails, crypto, language, topics (unchanged)
  ↓
save_page_full(..., network: "i2p") → Store with network tag
  ↓
Queue discovered links:
  - http://another.i2p → work_unit (will use I2P client)
  - http://somesite.onion → work_unit (will use Tor client)
  - https://clearnet.com → work_unit (will use no proxy)
  ↓
record_i2p_outcome(success) → Update adaptive timeout stats
```

### Work Queue Processing

```
work_queue() fetches pending work_unit
  ↓
For each URL in queue:
  ↓
  detect_network_type(url) → NetworkType
  ↓
  build_http_client_for_network(network_type)
  ↓
  Fetch with network-specific timeout
  ↓
  Extract data (same pipeline regardless of network)
  ↓
  save_page_full(..., network: network_type_string)
  ↓
  Queue discovered outbound links (each routed per its own TLD)
  ↓
  record_i2p_outcome(success/failure)
```

### Cross-Network Intelligence Queries

Existing normalized tables support cross-network correlation with no code changes:

```sql
-- Find emails that appear on both Tor and I2P
SELECT eo.email_address, 
       COUNT(DISTINCT CASE WHEN p.network = 'tor' THEN p.host END) as tor_hosts,
       COUNT(DISTINCT CASE WHEN p.network = 'i2p' THEN p.host END) as i2p_hosts
FROM email_observation eo
JOIN page p ON eo.page_id = p.id
GROUP BY eo.email_address
HAVING COUNT(DISTINCT p.network) > 1;

-- Find same crypto wallet across networks
SELECT co.crypto_ref,
       array_agg(DISTINCT p.network) as networks,
       array_agg(DISTINCT p.url) as locations
FROM crypto_observation co
JOIN page p ON co.page_id = p.id
GROUP BY co.crypto_ref
HAVING COUNT(DISTINCT p.network) > 1;

-- Cross-network SSH fingerprint matching
SELECT ssh.fingerprint,
       array_agg(DISTINCT p.network) as networks,
       array_agg(DISTINCT ssh.host) as hosts
FROM host_ssh_observation ssh
JOIN page p ON p.host = ssh.host
GROUP BY ssh.fingerprint
HAVING COUNT(DISTINCT p.network) > 1;
```

Frontend analytics views (`/entities/emails`, `/entities/crypto`, `/entities/ssh`) automatically include I2P data.

## Error Handling

### I2P-Specific Failures

**Tunnel not ready (503/connection refused):**
- **Cause:** I2P tunnels establishing (1-3 minutes after i2pd start)
- **Handling:** Treat as transient failure, requeue with backoff
- **Log:** `"I2P tunnel not ready for {url}, requeueing"`

**Destination unreachable (502 from i2pd proxy):**
- **Cause:** Target .i2p eepsite offline or nonexistent
- **Handling:** Retry 2-3 times (eepsite might be restarting), then mark failed
- **Log:** `"I2P destination unreachable: {url}"`

**Timeout during tunnel building:**
- **Cause:** Request times out during initial connection
- **Handling:** Requeue, record timeout in adaptive stats (prevents premature timeout reduction)
- **Log:** `"I2P timeout after {duration}s for {url}"`

**I2P proxy not available (connection refused to 127.0.0.1:4444):**
- **Cause:** i2pd not running or HTTP proxy disabled
- **Handling:** Log clear error, mark work unit as failed (operator must fix i2pd)
- **Log:** `"I2P proxy unreachable at {proxy_url}, check i2pd status"`

### Mixed Network Link Handling

When a Tor site links to an I2P site (or vice versa):
- Extract and queue link normally
- Each work unit fetches using its own network's client
- Cross-network references visible in `page_link` table and `/relationships` view
- Example: marketplace.onion → forum.i2p shows in relationships graph

### Proxy Configuration Fallback

```
I2P_PROXY set? → Use it
  Not set? → Default: http://127.0.0.1:4444

TOR_PROXY set? → Use it for .onion
  Not set? → Check ALL_PROXY
    Not set? → Default: socks5h://127.0.0.1:9050
```

Clear error messages on proxy connection failure:
- `"Tor proxy unavailable at {url}, ensure Tor is running"`
- `"I2P proxy unavailable at {url}, ensure i2pd is running and HTTP proxy is enabled"`

### Blacklist Interaction

Domain blacklist works identically across networks:
- `blacklist add example.i2p` → blocks I2P eepsite
- `blacklist add example.onion` → blocks Tor hidden service
- Auto-blacklist by category/keyword → network-agnostic
- Subdomain matching → applies to .i2p hostnames

## Testing Strategy

### Unit Tests

**Network detection** (`src/lib.rs`):
```rust
#[test]
fn test_detect_network_type() {
    assert_eq!(detect_network_type("http://example.i2p"), NetworkType::I2p);
    assert_eq!(detect_network_type("http://example.i2p/path"), NetworkType::I2p);
    assert_eq!(detect_network_type("http://example.i2p:8080"), NetworkType::I2p);
    assert_eq!(detect_network_type("http://site.onion"), NetworkType::Tor);
    assert_eq!(detect_network_type("https://clearnet.com"), NetworkType::Clearnet);
}
```

**Proxy selection** (`src/bin/spyder.rs`):
```rust
#[test]
fn test_proxy_selection() {
    env::set_var("I2P_PROXY", "http://custom:4444");
    assert_eq!(get_proxy_for_network(NetworkType::I2p), 
               Some("http://custom:4444".to_string()));
    
    env::remove_var("I2P_PROXY");
    assert_eq!(get_proxy_for_network(NetworkType::I2p),
               Some("http://127.0.0.1:4444".to_string()));
}
```

**Adaptive timeout** (`src/config.rs`):
```rust
#[test]
fn test_adaptive_timeout() {
    let mut tracker = TimeoutTracker::new(100);
    
    // Initial state: 45s timeout
    assert_eq!(tracker.get_timeout(), Duration::from_secs(45));
    
    // 60 successes: should reduce to 30s
    for _ in 0..60 {
        tracker.record(true);
    }
    assert_eq!(tracker.get_timeout(), Duration::from_secs(30));
    
    // High failure rate: revert to 45s
    for _ in 0..50 {
        tracker.record(false);
    }
    assert_eq!(tracker.get_timeout(), Duration::from_secs(45));
}
```

### Integration Tests

**Mock I2P proxy server:**
- HTTP server on localhost:14444 responding to .i2p URLs
- Test scenarios:
  - Successful fetch → 200 OK
  - Tunnel not ready → 503 Service Unavailable
  - Destination unreachable → 502 Bad Gateway
- Verify timeout behavior and retry logic

**Mixed network crawl:**
- Seed with test .i2p URL containing links to .onion and clearnet
- Verify all three network types queued
- Verify each uses appropriate proxy when processed

**Database schema:**
- Run migration, verify `network` column exists with default
- Insert test pages with `network='i2p'`, `network='tor'`, `network='clearnet'`
- Query cross-network entities, verify joins work

### Manual Testing Checklist

**Prerequisites:** i2pd running with HTTP proxy enabled on 127.0.0.1:4444

**1. Basic I2P crawl:**
```bash
I2P_PROXY=http://127.0.0.1:4444 cargo run --bin spyder -- add http://stats.i2p
I2P_PROXY=http://127.0.0.1:4444 cargo run --bin spyder -- work
```
Verify:
- Page fetched successfully
- Database record has `network='i2p'`
- Links extracted and queued

**2. Mixed network seed:**
```bash
# Add a .onion site known to link to I2P and clearnet
ALL_PROXY=socks5h://127.0.0.1:9050 I2P_PROXY=http://127.0.0.1:4444 \
  cargo run --bin spyder -- add http://example.onion
ALL_PROXY=socks5h://127.0.0.1:9050 I2P_PROXY=http://127.0.0.1:4444 \
  cargo run --bin spyder -- work
```
Verify:
- Tor, I2P, and clearnet links all processed
- Each uses correct proxy

**3. Cross-network queries:**
```bash
cargo run --bin frontend
```
Visit:
- `/entities/emails` → verify emails from I2P sites appear
- `/entities/crypto` → verify crypto refs from I2P sites appear
- `/relationships` → verify cross-network links visible

Run SQL queries from "Cross-Network Intelligence Queries" section.

**4. Error scenarios:**
```bash
# Stop i2pd
sudo systemctl stop i2pd
I2P_PROXY=http://127.0.0.1:4444 cargo run --bin spyder -- work
# Verify clear error: "I2P proxy unreachable..."

# Start i2pd, add invalid .i2p URL
sudo systemctl start i2pd
I2P_PROXY=http://127.0.0.1:4444 cargo run --bin spyder -- add http://nonexistent12345.i2p
I2P_PROXY=http://127.0.0.1:4444 cargo run --bin spyder -- work
# Verify destination unreachable handling
```

**5. Adaptive timeout observation:**
```bash
# Crawl 50+ .i2p pages, watch logs for timeout adjustments
# Verify initial 45s timeout reduces to 30s after successful crawls
```

## Limitations & Future Work

### Current Limitations

**1. No addressbook resolution**
- Human-readable `.i2p` names and base32 addresses treated as separate hosts
- Example: `forum.i2p` and `ukeu3k...b32.i2p` stored separately even if same eepsite
- **Impact:** May cause duplicate entries
- **Future:** Phase 2 enhancement (see below)

**2. Single global timeout per network**
- All I2P sites share one adaptive timeout
- Some eepsites fast (well-peered), others slow (new routers)
- **Impact:** Fast sites wait unnecessarily, slow sites may time out
- **Future:** Phase 5 per-site adaptive timeout

**3. No I2P router health monitoring**
- Spyder doesn't check i2pd status, tunnel count, router integration
- **Impact:** Failures indistinguishable (i2pd unhealthy vs. destination down)
- **Mitigation:** Manual monitoring via i2pd web console (http://127.0.0.1:7070)

**4. HTTP proxy only**
- Uses i2pd HTTP proxy, not SOCKS
- **Impact:** None for web crawling
- **Note:** HTTP proxy is standard for I2P web access

**5. No jump service integration**
- Doesn't use I2P jump services (`http://stats.i2p/?i2paddresshelper=...`)
- **Impact:** Can't auto-resolve newly discovered base32 addresses
- **Mitigation:** Manual seed URLs work fine

**6. Clearnet leakage risk**
- If I2P proxy misconfigured, clearnet requests might leak
- **Mitigation:** Validate proxy connectivity before crawl, log all proxy selections

### Future Enhancement Phases

**Phase 2 — Addressbook Resolution**
- Parse i2pd's `addressbook/addresses.csv` or scrape addressbook.cgi
- Build `i2p_address` table linking human-readable ↔ base32
- Normalize queries to canonical base32 for deduplication
- **Trigger:** After initial deployment, if duplicate .i2p hosts become a problem

**Phase 3 — Network-Aware UI**
- Add network filter to `/pages`, `/search` (show only I2P sites)
- Cross-network relationship visualization (graph showing Tor ↔ I2P links)
- Network breakdown in `/analytics` (X sites on Tor, Y on I2P, Z on both)
- **Trigger:** After accumulating significant I2P crawl corpus

**Phase 4 — Advanced I2P Features**
- Parse eepsite metadata (router version, tunnel count from headers)
- I2P network database integration for destination reachability pre-checks
- Outproxy detection (sites proxying to clearnet)
- **Trigger:** User request or specific investigative need

**Phase 5 — Per-Site Adaptive Timeout**
- Track success/timeout rates per host, not just per network
- Fast eepsites get 30s timeout, slow/unreliable ones get 60s+
- **Trigger:** After observing wide variance in I2P site performance

### Operational Considerations

**i2pd configuration required:**
```ini
# i2pd.conf
[http]
enabled = true
address = 127.0.0.1
port = 4444
```

**Performance expectations:**
- I2P slower than Tor (especially cold starts)
- Expect 30-60s per page initially
- Performance improves to 15-30s as router integrates into network
- Recommend limiting concurrency for I2P crawls (fewer workers than Tor)

**Recommended deployment:**
- Dedicated i2pd instance for crawling (separate from browsing)
- Monitor i2pd logs during initial crawls
- Allow 10-15 minutes for i2pd tunnel establishment after startup
- Use `I2P_PROXY` env var to point to dedicated instance

## Implementation Checklist

- [ ] Add `NetworkType` enum and `detect_network_type()` to `src/lib.rs`
- [ ] Add unit tests for network detection
- [ ] Create `TimeoutTracker` for adaptive timeouts (in `src/config.rs` or inline)
- [ ] Modify `build_http_client()` → `build_http_client_for_network()`
- [ ] Add `get_proxy_for_network()` and `get_timeout_for_network()`
- [ ] Update `enqueue_seed_and_links()` to use network-aware client
- [ ] Update `work_queue()` to use network-aware client
- [ ] Modify `save_page_full()` to accept and store `network` parameter
- [ ] Update `rescan_known_pages()` to read and use network from page record
- [ ] Create database migration for `network` column and indexes
- [ ] Add unit tests for proxy selection and adaptive timeout
- [ ] Write integration tests with mock I2P proxy
- [ ] Update README with I2P usage examples
- [ ] Manual testing with live i2pd instance
- [ ] Document i2pd configuration requirements

## Success Criteria

- [ ] `.i2p` URLs crawled successfully via i2pd HTTP proxy
- [ ] Pages tagged with `network='i2p'` in database
- [ ] Cross-network SQL queries return expected results
- [ ] Mixed network crawls route correctly (Tor/I2P/clearnet)
- [ ] Adaptive timeout reduces from 45s to 30s after successful I2P crawls
- [ ] Clear error messages when i2pd unavailable
- [ ] Frontend views (`/entities/emails`, `/entities/crypto`) show I2P data
- [ ] No regressions in existing Tor or clearnet crawling

## References

- I2P Network Documentation: https://i2p.net/en/docs/
- Garlic Routing: https://i2p.net/en/docs/overview/garlic-routing/
- i2pd (I2P Daemon): https://i2pd.readthedocs.io/
- i2pd HTTP Proxy: https://i2pd.readthedocs.io/en/latest/user-guide/tunnels/#http-proxy
