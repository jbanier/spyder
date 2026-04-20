# Spyder

Spyder is a Rust crawler plus Rocket dashboard for collecting pages, links, and page-level observations from the clearnet and Tor-hosted sites.

It ships with two binaries:

- `spyder`: CLI crawler and queue worker
- `frontend`: web interface over the same SQLite database

## What It Stores

For each scanned page, Spyder now stores:

- title
- URL
- primary detected language when possible
- heuristic host/site category derived from page content and structure
- last scan timestamp
- outbound links
- extracted email addresses
- extracted crypto references

Normalized tables are also maintained for:

- page-to-page references
- email observations
- crypto observations

That makes it possible to answer questions like:

- which sites reference other sites
- which pages mention the same email address
- which pages reuse the same wallet or payment reference

## Requirements

- Rust toolchain
- SQLite
- `diesel_cli` with SQLite support

Install Diesel CLI if needed:

```bash
cargo install diesel_cli --no-default-features --features sqlite
```

If SQLite linking is a problem on your machine, the bundled SQLite dependency is already enabled in [Cargo.toml](/Users/jbanier/Documents/work/code/spyder/Cargo.toml).

## First Start

### 1. Configure the database

Create a `.env` file in the project root:

```bash
printf 'DATABASE_URL=spyder.sqlite\n' > .env
```

### 2. Create the SQLite database

```bash
diesel setup
diesel migration run
```

This applies:

- the base schema
- the domain blacklist migration for blocking discovered-link queueing by host
- the enrichment migration for language, scan timestamps, links, emails, and crypto observations
- the retry/backfill migration that adds retry scheduling and preserves legacy intel in normalized tables
- the page scan history migration that stores point-in-time snapshots and per-scan diffs
- the site classification migration that stores host-level categories and supporting evidence

If you already have an older database, `diesel migration run` upgrades it in place. Back it up first if the data matters.

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

For each pending work unit, Spyder:

- fetches the page
- extracts title, links, emails, crypto references, and language
- updates the `page` record
- recomputes the host’s heuristic site category and evidence
- queues newly discovered links for recursive crawling unless their host matches the blacklist
- refreshes normalized link/email/crypto observations
- marks the work unit as `done`, reschedules transient failures, or marks terminal failures as `failed`

## Domain Blacklist

Manage the blacklist from the CLI:

```bash
# list current rules
cargo run --bin spyder -- blacklist list

# block a domain and all of its subdomains
cargo run --bin spyder -- blacklist add example.com

# remove a rule
cargo run --bin spyder -- blacklist remove example.com
```

Behavior:

- blacklisted domains match the exact host and any subdomain
- blacklisted discovered links are still stored in page and history views
- blacklisted discovered links are not queued into `work_unit`
- manually seeded URLs are not removed retroactively

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
all_proxy=socks5h://localhost:9050 cargo run --bin spyder -- work
```

The `socks5h` form is important because hostname resolution must happen through Tor for `.onion` hosts.

## Start The Web Interface

Run the frontend against the same `DATABASE_URL`:

```bash
cargo run --bin frontend
```

Rocket listens on `127.0.0.1:8000` by default.

Main pages:

- `http://127.0.0.1:8000/`: dashboard
- `http://127.0.0.1:8000/pages`: scanned pages list
- `http://127.0.0.1:8000/sites`: host-level site classification view
- `http://127.0.0.1:8000/blacklist`: current blacklist rules and match counts
- `http://127.0.0.1:8000/work`: crawl queue
- `http://127.0.0.1:8000/relationships`: host-to-host reference summary
- `http://127.0.0.1:8000/entities/emails`: shared email view
- `http://127.0.0.1:8000/entities/crypto`: shared crypto reference view
- `http://127.0.0.1:8000/search`: search titles, URLs, language, emails, and wallets

## Web UI Views

### Pages

The pages list shows:

- title
- URL and host
- host category badge and confidence when classification exists
- detected language
- last scan timestamp
- counts for links, emails, and crypto references
- previous/next navigation through the dataset

Each page links to a detail view that shows:

- outbound links
- inbound references from other scanned pages
- extracted emails
- extracted crypto references
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

Blacklisted target hosts are clearly marked in both the dashboard and relationship table views.

Source and target hosts also show category badges when the host has been classified.

### Site Profiles

`/sites` lists the current host-level site profiles with:

- primary category
- confidence tier
- number of supporting pages
- short evidence markers explaining the classification
- the page that contributed the strongest evidence

### Domain Blacklist

`/blacklist` lists configured blacklist rules together with counts of matching current page links and historical scan links.

## JSON Endpoints

- `GET /api/stats`
- `GET /api/search?query=example`
- `GET /api/search?query=example&limit=25`
- `GET /api/blacklist`
- `GET /api/sites`
- `GET /api/pages/<id>/history`
- `GET /api/pages/<id>/history/<scan_id>`

## Typical Local Session

1. Create `.env` with `DATABASE_URL=spyder.sqlite`.
2. Run `diesel setup`.
3. Run `diesel migration run`.
4. Seed one or more URLs with `cargo run --bin spyder -- add <url>`.
5. Process pending work with `cargo run --bin spyder -- work`.
6. Start the frontend with `cargo run --bin frontend`.
7. Manage any blocked domains with `cargo run --bin spyder -- blacklist add <domain>`.
8. Open `/pages`, `/blacklist`, `/relationships`, `/entities/emails`, and `/entities/crypto`.
9. Open `/sites` to review the current host classifications.
10. Open a page detail and use its history links to inspect stored scan diffs.

## Useful Commands

```bash
# build everything
cargo build

# run tests
cargo test

# re-run all migrations during development
diesel migration redo

# regenerate Diesel schema after schema changes
diesel print-schema > src/schema.rs
```

## Current Limitations

- Language detection is best effort and stores one primary language per page.
- Crypto extraction is pattern-based. It is useful for discovery and cross-matching, not for full wallet validation.
- Site categorization is heuristic and deterministic. It is explainable, but it is still an inference layer rather than ground truth.
- The host relationship view is table-based. There is no visual graph yet.
- Tor crawling depends on an external SOCKS proxy such as Tor running locally.
- Completed pages are not automatically recrawled on a schedule yet; rescanning still requires explicit operator action.

## Troubleshooting

### `DATABASE_URL must be set`

Set it in your shell or create the `.env` file shown above.

### `error connecting to spyder.sqlite`

Run `diesel setup` and `diesel migration run` from the project root.

### `no such table`

Your database schema is missing or outdated. Run:

```bash
diesel migration run
```

### `.onion` requests fail

Check that:

- Tor is running
- the SOCKS proxy is available on the host and port you configured
- you used `socks5h`, not plain `socks5`
