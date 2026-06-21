# URL Import Guide

This guide covers the new URL import system for tracking the provenance of discovered onion sites.

## Quick Start

### Import URLs from a file

```bash
# Plain text file (one URL per line)
cargo run --bin spyder -- import seeds.txt \
  --source-type custom \
  --source-name "My Seed List"

# With source URL
cargo run --bin spyder -- import ahmia_list.txt \
  --source-type ahmia \
  --source-name "Ahmia Directory" \
  --source-url "https://ahmia.fi"
```

### Check the import worked

```sql
-- View recent imports
SELECT * FROM import_source ORDER BY imported_at DESC LIMIT 5;

-- Check discovery records created
SELECT id, url, discovery_depth, import_source_id 
FROM url_discovery 
WHERE import_source_id IS NOT NULL 
ORDER BY id DESC LIMIT 10;

-- Verify work_units are linked
SELECT w.url, w.url_discovery_id IS NOT NULL as has_discovery
FROM work_unit w
ORDER BY w.created_at DESC
LIMIT 10;
```

## File Formats

### Plain Text (`.txt`)

One URL per line. Lines starting with `#` are treated as comments.

```text
# DuckDuckGo Search
https://duckduckgogg42xjoc72x3sjasowoarfbgcmvfimaftt6twagswzczad.onion

# ProPublica
https://p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion
```

### JSON (`.json`)

Array of strings:
```json
[
  "https://duckduckgogg42xjoc72x3sjasowoarfbgcmvfimaftt6twagswzczad.onion",
  "https://p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion"
]
```

Or array of objects with "url" field:
```json
[
  {
    "url": "https://duckduckgogg42xjoc72x3sjasowoarfbgcmvfimaftt6twagswzczad.onion",
    "name": "DuckDuckGo"
  },
  {
    "url": "https://p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion",
    "name": "ProPublica"
  }
]
```

### CSV (`.csv`)

First column is treated as the URL:

```csv
url,category,notes
https://duckduckgogg42xjoc72x3sjasowoarfbgcmvfimaftt6twagswzczad.onion,search,Privacy search
https://p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion,news,Investigative journalism
```

## Source Types

Use `--source-type` to categorize your imports:

- **`ahmia`** - Ahmia search engine directory
- **`oniontree`** - OnionTree GitHub mirror
- **`manual`** - Manually curated lists
- **`paste`** - URLs from pastebin/similar services
- **`custom`** - Any other source

## Finding Onion Sites to Import

### Ahmia Directory

```bash
# Visit https://ahmia.fi and export their list
curl "https://ahmia.fi/onions/" > ahmia_onions.txt

cargo run --bin spyder -- import ahmia_onions.txt \
  --source-type ahmia \
  --source-name "Ahmia Directory" \
  --source-url "https://ahmia.fi"
```

### OnionTree (GitHub)

```bash
git clone https://github.com/onionltd/oniontree-mirror.git

# Extract URLs from JSON files
grep -r "http.*\.onion" oniontree-mirror/ | \
  grep -o "http[s]*://[^\"]*\.onion[^\"]*" | \
  sort -u > oniontree_onions.txt

cargo run --bin spyder -- import oniontree_onions.txt \
  --source-type oniontree \
  --source-name "OnionTree Mirror"
```

### Test Sites

Known stable onion sites for testing:

```bash
cat > test_onions.txt << 'EOF'
# DuckDuckGo Search
https://duckduckgogg42xjoc72x3sjasowoarfbgcmvfimaftt6twagswzczad.onion

# ProPublica
https://p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion

# CIA
https://ciadotgov4sjwlzihbbgxnqg3xiyrg7so2r2o3lt5wz5ypk4sxyjstad.onion

# BBC News
https://www.bbcnewsd73hkzno2ini43t4gblxvycyac5aw4gnv7t2rccijh7745uqd.onion
EOF

cargo run --bin spyder -- import test_onions.txt \
  --source-type custom \
  --source-name "Test Sites"
```

## Provenance Tracking

The import system tracks the complete chain of how URLs were discovered:

### Import Flow

```
Import File
    ↓
import_source (with SHA256 hash, metadata)
    ↓
url_discovery (depth=0, import_source_id set)
    ↓
work_unit (url_discovery_id set automatically)
    ↓
Crawler fetches page
    ↓
Discovered links → url_discovery (depth=N, chain tracking)
    ↓
New work_units (url_discovery_id set automatically)
```

### Discovery Chain Example

```
Seed: http://seed.onion
  discovery_depth: 0
  discovery_chain: []
  import_source_id: 1

Page discovered from seed: http://page1.onion
  discovery_depth: 1
  discovery_chain: [seed_page_id]
  discovered_from_page_id: seed_page_id

Page discovered from page1: http://page2.onion
  discovery_depth: 2
  discovery_chain: [seed_page_id, page1_id]
  discovered_from_page_id: page1_id
```

## Monitoring Imports

### View Import Summary

```sql
-- All imports with URL counts
SELECT 
  s.source_name,
  s.source_type,
  s.total_urls,
  s.imported_at,
  COUNT(d.id) as discovery_records
FROM import_source s
LEFT JOIN url_discovery d ON d.import_source_id = s.id
GROUP BY s.id, s.source_name, s.source_type, s.total_urls, s.imported_at
ORDER BY s.imported_at DESC;
```

### Discovery Depth Distribution

```sql
-- See how deep the crawler has gone
SELECT 
  discovery_depth,
  COUNT(*) as url_count
FROM url_discovery
GROUP BY discovery_depth
ORDER BY discovery_depth;
```

### Trace Discovery Path

```sql
-- Find how a specific URL was discovered
SELECT 
  d.url,
  d.discovery_depth,
  d.discovery_chain,
  p.url as discovered_from_page,
  s.source_name as import_source
FROM url_discovery d
LEFT JOIN page p ON p.id = d.discovered_from_page_id
LEFT JOIN import_source s ON s.id = d.import_source_id
WHERE d.url = 'http://example.onion';
```

## Import Statistics

After importing and crawling, check the stats:

```sql
-- Import sources ranked by discovery productivity
SELECT 
  s.source_name,
  s.total_urls as imported,
  COUNT(DISTINCT d.id) FILTER (WHERE d.discovery_depth = 0) as seeds,
  COUNT(DISTINCT d.id) FILTER (WHERE d.discovery_depth > 0) as discovered,
  MAX(d.discovery_depth) as max_depth
FROM import_source s
LEFT JOIN url_discovery d ON d.import_source_id = s.id 
  OR d.id IN (
    -- URLs discovered from this import source's seeds
    SELECT DISTINCT dd.id 
    FROM url_discovery dd
    WHERE dd.discovery_chain && ARRAY(
      SELECT p.id FROM page p
      JOIN url_discovery seed ON seed.url = p.url
      WHERE seed.import_source_id = s.id
    )
  )
GROUP BY s.id, s.source_name, s.total_urls
ORDER BY discovered DESC;
```

## Blacklist Handling

URLs matching the blacklist are automatically skipped during import:

```bash
# Import will skip blacklisted domains
cargo run --bin spyder -- import seeds.txt \
  --source-type custom \
  --source-name "Seeds"

# Check import summary
# Output shows:
# - Total URLs in file: 100
# - New URLs queued: 85
# - Already known (skipped): 10
# - Blacklisted (skipped): 5
```

## Troubleshooting

### Import shows 0 queued URLs

**Check:** URLs might already exist or be blacklisted

```sql
-- Check if URLs already in work_unit
SELECT url FROM work_unit WHERE url IN ('http://example.onion');

-- Check if domain is blacklisted
SELECT * FROM domain_blacklist WHERE domain = 'example.onion';
```

### work_unit.url_discovery_id is NULL

**Reason:** Legacy work_units created before the provenance system

**Solution:** This is expected. New imports will have url_discovery_id set automatically.

```sql
-- Check what percentage have provenance tracking
SELECT 
  COUNT(*) FILTER (WHERE url_discovery_id IS NOT NULL) as with_provenance,
  COUNT(*) FILTER (WHERE url_discovery_id IS NULL) as legacy,
  COUNT(*) as total,
  ROUND(100.0 * COUNT(*) FILTER (WHERE url_discovery_id IS NOT NULL) / COUNT(*), 1) as percent_tracked
FROM work_unit;
```

### Discovery chains not building

**Check:** Make sure crawler is creating url_discovery records for discovered links

**TODO:** The crawler integration (calling `create_url_discovery_for_link`) needs to be added to the crawler code. Currently only imports create url_discovery records.

## Next Steps

After importing URLs:

1. **Run the crawler** to fetch pages and discover new links:
   ```bash
   cargo run --bin spyder -- work --concurrency 4
   ```

2. **Monitor discovery chains** forming as the crawler finds new URLs

3. **Analyze provenance** to see which import sources are most productive

4. **Use bulk operations** to manage failures by category

## Related Documentation

- **Bulk Failure Operations:** See HANDOFF.md for `bulk_retry_by_category()`, `bulk_abandon_by_category()`, etc.
- **Database Schema:** See migration files in `migrations_postgres/2026-06-20-*`
- **Implementation Plan:** See `docs/superpowers/plans/2026-06-20-discovery-provenance.md`
