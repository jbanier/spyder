# Spyder

Spyder is a Rust crawler plus Rocket dashboard for collecting pages, links, and page-level observations from the clearnet and Tor-hosted sites.

It ships with two binaries:

- `spyder`: CLI crawler and queue worker
- `frontend`: web interface over the same PostgreSQL database

## What It Stores

For each scanned page, Spyder now stores:

- title
- URL
- primary detected language when possible
- language detection confidence, source, and evidence
- static topic tags with scores and evidence
- heuristic host/site category derived from page content and structure
- last scan timestamp
- outbound links
- extracted email addresses
- extracted crypto references

Normalized tables are also maintained for:

- page-to-page references
- email observations
- crypto observations
- page language detections
- page topic tags

That makes it possible to answer questions like:

- which sites reference other sites
- which pages mention the same email address
- which pages reuse the same wallet or payment reference
- whether customer watchlist indicators have appeared in crawled pages or host observations

## Requirements

- Rust toolchain
- PostgreSQL
- `libpq` development headers
- `diesel_cli` with PostgreSQL support

On Ubuntu, install PostgreSQL and the client development package:

```bash
sudo apt update
sudo apt install postgresql libpq-dev
```

Install Diesel CLI if needed:

```bash
cargo install diesel_cli --no-default-features --features postgres
```

## First Start

### 1. Configure the database

Create a `.env` file in the project root:

```bash
printf 'DATABASE_URL=postgres://localhost/spyder\n' > .env
```

### 2. Create the PostgreSQL database

If your local Unix user does not already have a matching PostgreSQL role, create one first:

```bash
sudo -u postgres createuser --superuser "$USER"
```

Create the database:

```bash
createdb spyder
diesel setup
diesel migration run
```

The shipped migration tree in [migrations_postgres](/Users/jbanier/Documents/work/code/spyder/migrations_postgres) creates the full PostgreSQL schema used by the crawler and frontend.

If you already have an older SQLite database, back it up first, create a fresh PostgreSQL database with the migrations above, then import it with:

```bash
cargo run --bin spyder -- import-sqlite /path/to/spyder.sqlite
```

The importer expects the target PostgreSQL database to be empty. It copies the current SQLite tables into the new PostgreSQL schema and preserves the existing integer ids so relationships remain intact.

### 3. Build the project

```bash
cargo build
```

## Basic Crawl Workflow

### Add a seed URL

```bash
cargo run --bin spyder -- add https://example.com
```

This:

- inserts the seed URL into `work_unit`
- fetches that page immediately
- extracts links from the fetched page
- queues discovered `http` and `https` links unless their host matches the blacklist

### Process the queue

```bash
cargo run --bin spyder -- work
```

Use `cargo run --bin spyder -- work --onion-only` to process only pending URLs whose host ends in `.onion`.

For each pending work unit, Spyder:

- fetches the page
- extracts title, links, emails, crypto references, language, and page topics
- updates the `page` record
- recomputes the host’s heuristic site category and evidence
- queues newly discovered links for recursive crawling unless their host matches the blacklist
- refreshes normalized link/email/crypto observations
- marks the work unit as `done`, reschedules transient failures, or marks terminal failures as `failed`

### Rescan already-known pages

If the crawl queue is empty or has been truncated, rebuild it from pages already stored in `page` and scan them for updates:

```bash
cargo run --bin spyder -- rescan-known
```

Use `--queue-only` to repopulate `work_unit` without immediately processing it, `--limit N` to bound the number of known pages queued, and `--onion-only` to only rescan known `.onion` sites.

## Domain Blacklist

Manage the blacklist from the CLI:

```bash
# list current rules
cargo run --bin spyder -- blacklist list

# block a domain and all of its subdomains
cargo run --bin spyder -- blacklist add example.com

# remove a rule
cargo run --bin spyder -- blacklist remove example.com

# auto-blacklist scanned sites classified as a category
cargo run --bin spyder -- blacklist auto add-category market

# auto-blacklist SEO spam detections
cargo run --bin spyder -- blacklist auto add-category seo-spam

# auto-blacklist scanned sites whose scan corpus includes a phrase
cargo run --bin spyder -- blacklist auto add-keyword "escrow required"

# review existing data without writing blacklist entries
cargo run --bin spyder -- blacklist auto apply-existing --dry-run
```

Behavior:

- blacklisted domains match the exact host and any subdomain
- blacklisted discovered links are still stored in page and history views
- blacklisted discovered links are not queued into `work_unit`
- manually seeded URLs are not removed retroactively
- auto-blacklist rules add the scanned host to the same domain blacklist when a category or keyword phrase matches
- auto-blacklist backfill is explicit and dry-run by default

Retry behavior:

- transient network and Tor-related failures are requeued automatically
- retries are bounded and use increasing backoff
- permanently bad inputs such as invalid URLs are not retried forever

## Tor / Onion Usage

Spyder uses `reqwest` with SOCKS support. To crawl `.onion` targets, run it through a Tor SOCKS proxy.

Typical local setup:

- start Tor locally
- make sure the SOCKS proxy is reachable on `localhost:9050`

Seed an onion URL:

```bash
all_proxy=socks5h://localhost:9050 cargo run --bin spyder -- add http://somesite.onion
```

Process the queued onion URLs:

```bash
all_proxy=socks5h://localhost:9050 cargo run --bin spyder -- work --onion-only
```

The `socks5h` form is important because hostname resolution must happen through Tor for `.onion` hosts.

## Start The Web Interface

Run the frontend against the same `DATABASE_URL`:

```bash
cargo run --bin frontend
```

Rocket listens on `127.0.0.1:8000` by default.

The dashboard keeps its default request path light for large databases. It shows scan stats, recent pages, and links to dedicated rollup views. To restore the older dashboard behavior that also renders entity, service, and relationship rollups on `/`, set:

```bash
SPYDER_DASHBOARD_DEEP=1 cargo run --bin frontend
```

On large PostgreSQL databases, run new index migrations during a maintenance window. If writes must continue while indexes build, create the same indexes manually with PostgreSQL `CREATE INDEX CONCURRENTLY` before marking the migration applied.

The `/` and `/analytics` pages use the frontend context cache. On a cold cache, the first request waits up to 750 ms for the background refresh, then serves a lightweight warming view instead of blocking on expensive rollups. Tune that first-request wait with:

```bash
SPYDER_FRONTEND_CACHE_COLD_WAIT_MS=250 cargo run --bin frontend
```

The default `/pages` view avoids an exact filtered total count because excluding blacklist suffix matches requires a large anti-match over `site_profile` on large crawls. It fetches one extra row to keep next/previous pagination accurate and displays a lower-bound count such as `50+ records`. The explicit `include_blacklisted=true` view still uses an exact total because it does not need the blacklist anti-match.

By default the background warmer now prebuilds only `/` and `/analytics`. To disable it entirely or opt back into more routes:

```bash
SPYDER_FRONTEND_CACHE_WARM_ROUTES=none cargo run --bin frontend
SPYDER_FRONTEND_CACHE_WARM_ROUTES=all cargo run --bin frontend
SPYDER_FRONTEND_CACHE_WARM_ROUTES=/,/analytics,/pages cargo run --bin frontend
```

Frontend cache refreshes slower than 5 seconds are written to stderr. Tune or disable that route-level timing log with:

```bash
SPYDER_FRONTEND_CACHE_SLOW_ROUTE_MS=1000 cargo run --bin frontend
SPYDER_FRONTEND_CACHE_SLOW_ROUTE_MS=0 cargo run --bin frontend
```

For statement-level slow query logging, enable PostgreSQL's `log_min_duration_statement` on the `spyder` database or role. While a long query is still running, inspect it live with:

```sql
SELECT pid, now() - query_start AS runtime, wait_event_type, wait_event, query
FROM pg_stat_activity
WHERE application_name = 'spyder-frontend'
  AND state <> 'idle'
ORDER BY query_start;
```

Main pages:

- `http://127.0.0.1:8000/`: dashboard
- `http://127.0.0.1:8000/pages`: scanned pages list
- `http://127.0.0.1:8000/analytics`: site, service, language, and topic analytics
- `http://127.0.0.1:8000/sites`: host-level site classification view
- `http://127.0.0.1:8000/watchlists`: customer watchlist indicators
- `http://127.0.0.1:8000/leads`: deterministic intel lead queue
- `http://127.0.0.1:8000/blacklist`: current blacklist rules, auto rules, and match counts
- `http://127.0.0.1:8000/work`: crawl queue
- `http://127.0.0.1:8000/relationships`: host-to-host reference summary
- `http://127.0.0.1:8000/entities/emails`: shared email view
- `http://127.0.0.1:8000/entities/crypto`: shared crypto reference view
- `http://127.0.0.1:8000/entities/http`: HTTP endpoint fingerprints
- `http://127.0.0.1:8000/entities/services`: non-HTTP service endpoints
- `http://127.0.0.1:8000/entities/ssh`: SSH host keys
- `http://127.0.0.1:8000/search`: search titles, URLs, language, topics, emails, and wallets

## Web UI Views

### Pages

The pages list shows:

- title
- URL and host
- host category badge and confidence when classification exists
- detected language
- language detection confidence and source
- static page topics when matched
- last scan timestamp
- counts for links, emails, and crypto references
- previous/next navigation through the dataset

Each page links to a detail view that shows:

- outbound links
- inbound references from other scanned pages
- extracted emails
- extracted crypto references
- language detection evidence
- page topic tags, scores, confidence tiers, and matched evidence
- site classification badge, evidence, and evidence source page when available
- scan timestamps

Each page also links to history views that show:

- stored scan snapshots for that page
- diff summaries against the previous successful scan
- added and removed links, emails, and crypto references between scans
- explicit blacklist badges when an outbound link target matches a blocked domain

### Shared Emails

`/entities/emails` groups identical email addresses across pages.

Selecting one email shows every page that referenced it.

The list view is paginated.

### Shared Crypto References

`/entities/crypto` groups identical wallet or payment references across pages.

Selecting one reference shows every page that referenced it.

The list view is paginated.

### Site Relationships

`/relationships` summarizes host-to-host links observed during scanning, so you can see which sites reference which other sites.

The list view is paginated.

The relationship graph opens with an overview of the strongest observed host links. Entering a focus host switches to a three-hop inbound path view that shows how scanned sites led to that host, with zoom, pan, fit, and reset controls.

Relationship queries use the denormalized `page_link.source_host` column and partial indexes on `(source_host, lower(target_host))` and `(lower(target_host), source_host)`. Run the relationship graph index migration before using the graph on large crawls.

Blacklisted target hosts are clearly marked in both the dashboard and relationship table views.

Source and target hosts also show category badges when the host has been classified.

### Site Profiles

`/sites` lists the current host-level site profiles with:

- primary category
- confidence tier
- first-found and last-scanned timestamps
- number of scanned pages
- short evidence markers explaining the classification
- the page that contributed the strongest evidence

### Analytics

`/analytics` is the main rollup view. It summarizes:

- classified hosts and active site categories
- discovered service endpoints across HTTP, non-HTTP services, and SSH
- page language detections
- static topic-tagged pages
- category, keyword, and topic timelines

The dedicated `/top` leaderboards remain available from the dashboard and from `/analytics`, but they are no longer duplicated in the top navigation.

### Language And Topics

Language detection is fully local and deterministic. Spyder combines declared page language metadata such as `html[lang]` and language-related meta tags with the existing `whatlang` text detector, then stores the chosen language, confidence, source, and compact evidence.

Page topics are also static. They are scored from weighted matches across title, headings, meta tags, body text, URL paths, and outbound link paths. The current rules cover broad operational categories such as marketplace, forum, directory, search, documentation, crypto, credentials, data leak, malware, phishing, exploit, and infrastructure. No LLM call is used.

SEO spam detection is also deterministic. It contributes a `seo-spam` site category when a page combines signals such as oversized multilingual meta keywords, explicit `index, follow` robots metadata, hidden links, visible links concentrated on one external host, and repeated title churn across recent scans.

### Customer Watchlists

`/watchlists` stores customer-specific indicators that should become leads when observed.

Supported indicator types:

- domain
- URL
- email
- crypto reference
- keyword
- SSH fingerprint
- HTTP fingerprint
- favicon hash

Watchlist matches are generated by the `watchlist-match` lead rule:

```bash
cargo run --bin spyder -- leads recompute --rule watchlist-match
```

The same indicators can also be managed from the CLI:

```bash
cargo run --bin spyder -- watchlist list
cargo run --bin spyder -- watchlist add domain example.com "Acme corporate domain"
cargo run --bin spyder -- watchlist remove 1
```

### Domain Blacklist

`/blacklist` lists configured blacklist rules together with counts of matching current page links and historical scan links. It also manages auto-blacklist category and keyword rules, and shows recent automatic matches.

## JSON Endpoints

- `GET /api/stats`
- `GET /api/search?query=example`
- `GET /api/search?query=example&limit=25`
- `GET /api/blacklist`
- `GET /api/blacklist/auto`
- `GET /api/sites`
- `GET /api/pages/<id>/history`
- `GET /api/pages/<id>/history/<scan_id>`

## Typical Local Session

1. Create `.env` with `DATABASE_URL=postgres://localhost/spyder`.
2. Run `diesel setup`.
3. Run `diesel migration run`.
4. If you are migrating an existing SQLite dataset, run `cargo run --bin spyder -- import-sqlite /path/to/spyder.sqlite`.
5. Seed one or more URLs with `cargo run --bin spyder -- add <url>`.
6. Process pending work with `cargo run --bin spyder -- work`.
7. Start the frontend with `cargo run --bin frontend`.
8. Manage any blocked domains with `cargo run --bin spyder -- blacklist add <domain>`.
9. Open `/pages`, `/blacklist`, `/relationships`, `/entities/emails`, and `/entities/crypto`.
10. Open `/sites` to review the current host classifications.
11. Add customer indicators in `/watchlists`, then run `cargo run --bin spyder -- leads recompute --rule watchlist-match`.
12. Open `/leads?rule_id=watchlist-match` to review customer-specific matches.
13. Open a page detail and use its history links to inspect stored scan diffs.

## Useful Commands

```bash
# build everything
cargo build

# type-check everything
cargo check

# re-run all migrations during development
diesel migration redo

# import an existing SQLite database into a fresh PostgreSQL database
cargo run --bin spyder -- import-sqlite /path/to/spyder.sqlite

# regenerate Diesel schema after schema changes
diesel print-schema > src/schema.rs
```

## Current Limitations

- Language detection is best effort and stores one primary language per page, with confidence and evidence for review.
- Topic detection is static keyword/path matching. It is explainable and safe for sensitive crawls, but it will miss phrasing not covered by the rule set.
- Crypto extraction is pattern-based. It is useful for discovery and cross-matching, not for full wallet validation.
- Site categorization is heuristic and deterministic. It is explainable, but it is still an inference layer rather than ground truth.
- Tor crawling depends on an external SOCKS proxy such as Tor running locally.
- Completed pages are not automatically recrawled on a schedule yet; rescanning still requires explicit operator action.

## Troubleshooting

### `DATABASE_URL must be set`

Set it in your shell or create the `.env` file shown above.

### `error connecting to postgres://localhost/spyder`

Check that PostgreSQL is running, the `spyder` database exists, and your local PostgreSQL role matches the user in `DATABASE_URL`.

### `no such table`

Your database schema is missing or outdated. Run:

```bash
diesel migration run
```

### `target PostgreSQL database is not empty`

The SQLite importer only runs against a fresh PostgreSQL database. Create a new database, run the PostgreSQL migrations, and retry the import there.

### `.onion` requests fail

Check that:

- Tor is running
- the SOCKS proxy is available on the host and port you configured
- you used `socks5h`, not plain `socks5`
