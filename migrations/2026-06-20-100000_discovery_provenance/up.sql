-- Create import_source table first (no dependencies)
CREATE TABLE import_source (
  id SERIAL PRIMARY KEY,
  source_type TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMP NOT NULL DEFAULT NOW(),
  imported_by TEXT NOT NULL,
  total_urls INTEGER NOT NULL DEFAULT 0,
  metadata JSONB
);

-- Create url_discovery table
CREATE TABLE url_discovery (
  id SERIAL PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,
  discovered_from_page_id INTEGER REFERENCES page(id),
  discovery_chain INTEGER[] NOT NULL DEFAULT '{}',
  discovery_depth INTEGER NOT NULL DEFAULT 0,
  import_source_id INTEGER REFERENCES import_source(id),
  discovered_at TIMESTAMP NOT NULL DEFAULT NOW(),
  first_queued_at TIMESTAMP NOT NULL DEFAULT NOW(),

  CONSTRAINT url_discovery_url_unique UNIQUE (url)
);

-- Indexes for url_discovery
CREATE INDEX idx_url_discovery_depth ON url_discovery(discovery_depth);
CREATE INDEX idx_url_discovery_import_source ON url_discovery(import_source_id);
CREATE INDEX idx_url_discovery_discovered_from ON url_discovery(discovered_from_page_id);
