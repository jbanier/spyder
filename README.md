# Spyder

Spyder is a small Rust web indexing project with two apps:

- `spyder`: a CLI crawler that queues URLs, fetches pages, and stores extracted metadata.
- `frontend`: a Rocket web app that reads the same database and exposes a dashboard plus JSON endpoints.

## Requirements

- Rust toolchain
- SQLite
- `diesel_cli` with SQLite support

Install Diesel CLI if you do not already have it:

```bash
cargo install diesel_cli --no-default-features --features sqlite
```

If SQLite linking is a problem on your machine, there is an optional bundled SQLite dependency commented in [Cargo.toml](/Users/jbanier/Documents/work/code/spyder/Cargo.toml).

## Project Layout

- [src/bin/spyder.rs](/Users/jbanier/Documents/work/code/spyder/src/bin/spyder.rs): crawler CLI
- [src/bin/frontend.rs](/Users/jbanier/Documents/work/code/spyder/src/bin/frontend.rs): Rocket frontend
- [migrations/2025-05-15-155131_base/up.sql](/Users/jbanier/Documents/work/code/spyder/migrations/2025-05-15-155131_base/up.sql): initial SQLite schema

## Database Bootstrap

The app expects `DATABASE_URL` to be set. The simplest setup is a local SQLite file in the project root.

Create a `.env` file:

```bash
printf 'DATABASE_URL=spyder.sqlite\n' > .env
```

Bootstrap the database and apply migrations:

```bash
cargo install diesel_cli --no-default-features --features sqlite
diesel setup
diesel migration run
```

Notes:

- `diesel setup` creates the SQLite database pointed to by `DATABASE_URL`.
- `diesel migration run` creates the `work_unit` and `page` tables.
- If the database already exists, rerunning `diesel migration run` is safe for unapplied migrations only.

To reset from scratch during development:

```bash
diesel migration redo
```

## Start The CLI App

Build or run the crawler with Cargo:

```bash
cargo run --bin spyder -- add https://example.com
```

This command:

- inserts the seed URL into `work_unit`
- fetches the seed page
- extracts links from that page
- enqueues discovered `http` and `https` links

Process pending work units:

```bash
cargo run --bin spyder -- work
```

The crawler stores page metadata in the `page` table:

- page title
- discovered links
- email addresses
- crypto-like addresses matching the current regex

If a page fails to process, the work unit is marked as `failed` and the error is stored in `last_error`.

## Start The Frontend App

Run the web UI against the same database:

```bash
cargo run --bin frontend
```

Rocket listens on `127.0.0.1:8000` by default unless you override its config. Open:

- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/data`
- `http://127.0.0.1:8000/work`

Available JSON endpoints:

- `GET /api/stats`
- `GET /api/search?query=example`
- `GET /api/search?query=example&limit=25`

Search UI:

- `GET /search`

## Typical Local Workflow

1. Set `DATABASE_URL` in `.env`.
2. Run `diesel setup`.
3. Run `diesel migration run`.
4. Seed one or more URLs with `cargo run --bin spyder -- add <url>`.
5. Process the queue with `cargo run --bin spyder -- work`.
6. Start the UI with `cargo run --bin frontend`.

## Useful Commands

```bash
# compile everything
cargo build

# run tests
cargo test

# regenerate Diesel schema after schema changes
diesel print-schema > src/schema.rs
```

## Troubleshooting

`DATABASE_URL must be set`

Set it in your shell or create the `.env` file shown above.

`error connecting to spyder.sqlite`

Make sure you ran `diesel setup` and `diesel migration run` from the project root.

SQLite build or linking errors

Install SQLite development libraries on your machine, or enable the bundled SQLite dependency noted in [Cargo.toml](/Users/jbanier/Documents/work/code/spyder/Cargo.toml).
