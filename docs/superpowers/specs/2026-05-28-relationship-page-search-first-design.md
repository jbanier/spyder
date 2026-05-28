# Relationship Page: Search-First Design

**Date:** 2026-05-28  
**Status:** Approved  
**Author:** Claude Code

## Problem Statement

The relationship page currently attempts to load the complete relationship graph on initial page load, resulting in queries that take 12+ hours without returning results. This makes the page unusable and blocks users from exploring site relationships effectively.

The materialized view (`site_relationship_overview`) that powers the overview query takes 2+ hours to refresh and should be scheduled to run once daily during low-traffic periods.

## Solution Overview

Restructure the relationship page to use a search-first approach where:
1. Initial page load shows only a search form and the relationship data table (no graph)
2. Graph visualization loads only when a user explicitly searches for a specific host
3. Table data loads quickly from the materialized view and remains always visible
4. Users can click table rows to quickly visualize relationships for interesting hosts

## User Experience

### Initial Page Load

When users navigate to `/relationships`, they see:

1. **Hero section** with title and description (unchanged)
2. **Search form** prominently displayed:
   - Large "Focus Host" text input with placeholder: "Enter a host to visualize relationships (e.g., market.onion)"
   - Depth selector (1-4, default 3)
   - "Visualize" button
   - "Clear" button
3. **Graph area** showing placeholder message:
   - "Enter a host above to explore its relationship network"
   - "Or browse the table below to find hosts of interest"
4. **Relationship table** with pagination (same as current):
   - Source Site, Target Site, Observed Links, Leads columns
   - Loads from `site_relationship_overview` materialized view
   - Fast query (~100ms)

### Visualizing a Host

When user enters a host and clicks "Visualize":

1. URL updates to `/relationships?focus={host}&depth={depth}`
2. Graph area shows loading indicator
3. API call to `/api/relationships/graph?focus={host}&depth={depth}` executes
4. Graph renders with the focused relationship network
5. All existing graph controls become available (zoom, fit, reset, overview)
6. Table remains visible below the graph

### Browsing from Table

Users can click any row in the relationship table:

1. Click populates the search box with the row's source host
2. Automatically triggers graph visualization for that host
3. Same behavior as manual search

### Clearing Search

When user clicks "Clear":

1. Search box empties
2. Graph area returns to placeholder state
3. URL updates to `/relationships` (no query params)
4. Table remains visible with all relationships
5. No page reload required

## Component Layout

### 1. Hero Section
```
┌─────────────────────────────────────────────┐
│ Host Graph                                   │
│ Site Relationships                           │
│ Host-level references observed while         │
│ scanning pages.                              │
└─────────────────────────────────────────────┘
```

### 2. Search & Graph Section
```
┌─────────────────────────────────────────────┐
│ Reference Graph                              │
│                                              │
│ ┌──────────────────────────────────────────┐│
│ │ Focus Host: [________________]  Depth: 3 ││
│ │ [Visualize] [Clear]                      ││
│ └──────────────────────────────────────────┘│
│                                              │
│ ┌──────────────────────────────────────────┐│
│ │                                          ││
│ │   Enter a host above to explore its     ││
│ │   relationship network                   ││
│ │                                          ││
│ │   Or browse the table below to find     ││
│ │   hosts of interest                      ││
│ │                                          ││
│ └──────────────────────────────────────────┘│
│                                              │
│ [Graph visualization appears here after     │
│  search, with zoom/fit/reset controls]      │
└─────────────────────────────────────────────┘
```

### 3. Relationship Table Section
```
┌─────────────────────────────────────────────┐
│ Observed Site Links           1,234 rel.    │
│                                              │
│ ┌──────────────────────────────────────────┐│
│ │ Source    │ Target    │ Links │ Leads   ││
│ │─────────────────────────────────────────││
│ │ vendor.on │ market.on │  45   │ high    ││
│ │ shop.onion│ crypto.on │  23   │ None    ││
│ │ ...                                      ││
│ └──────────────────────────────────────────┘│
│                                              │
│ [Previous] [Next]                            │
└─────────────────────────────────────────────┘
```

## Technical Implementation

### Backend Changes

**No changes required** to Rust code in `src/bin/frontend.rs` or `src/lib.rs`.

The existing implementation already supports:
- Optional `focus` parameter in the `/relationships` route
- Optional `focus` parameter in the `/api/relationships/graph` endpoint
- Fast materialized view queries for the table
- Recursive CTE queries for focused graphs with 30s timeout

### Frontend Template Changes

**File:** `templates/relationships.html.hbs`

**Modifications:**

1. Restructure the layout to have three distinct sections:
   - Search form (always at top)
   - Graph visualization area (with conditional rendering)
   - Relationship table (always visible)

2. Add data attributes to table rows for click handling:
   ```handlebars
   <tr data-source-host="{{source_host}}">
   ```

3. Add placeholder state for graph area when no focus is set:
   ```handlebars
   {{#unless relationship_focus}}
   <div class="relationship-graph-placeholder">
     <p>Enter a host above to explore its relationship network</p>
     <p class="muted">Or browse the table below to find hosts of interest</p>
   </div>
   {{/unless}}
   ```

4. Change button text from "Focus" to "Visualize"

5. Add "Clear" button to form

### JavaScript Changes

**File:** `static/js/app.js`

**Modifications to `initRelationshipGraph` function:**

#### 1. Prevent Automatic Graph Loading

```javascript
// At initialization
const initialFocus = container.dataset.initialFocus || "";
const initialDepth = parseInt(container.dataset.initialDepth) || 3;

// Only load graph if focus is explicitly set
if (initialFocus.trim()) {
    loadGraph(initialFocus, initialDepth);
} else {
    renderPlaceholder();
}
```

#### 2. Add Placeholder Rendering

```javascript
const renderPlaceholder = () => {
    viewport.replaceChildren();
    const size = graphSize();
    svg.setAttribute("viewBox", `0 0 ${size.width} ${size.height}`);
    
    const line1 = createSvgElement("text", {
        x: size.width / 2,
        y: size.height / 2 - 15,
        class: "relationship-graph-empty",
        "text-anchor": "middle",
    });
    line1.textContent = "Enter a host above to explore its relationship network";
    
    const line2 = createSvgElement("text", {
        x: size.width / 2,
        y: size.height / 2 + 15,
        class: "relationship-graph-empty",
        "text-anchor": "middle",
        "font-size": "14",
    });
    line2.textContent = "Or browse the table below to find hosts of interest";
    
    viewport.appendChild(line1);
    viewport.appendChild(line2);
    
    setStatus("Ready to visualize");
    scale = 1;
    translate = { x: 0, y: 0 };
    setTransform();
};
```

#### 3. Table Row Click Handler

```javascript
// Add click handler for table rows
document.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-source-host]");
    if (!row) return;
    
    const host = row.dataset.sourceHost;
    if (host) {
        focusHost(host);
    }
});
```

#### 4. Clear Button Handler

```javascript
// In the form submit handler area, add clear button handler
container.addEventListener("click", (event) => {
    const clearButton = event.target.closest("[data-relationship-graph-clear]");
    if (clearButton) {
        event.preventDefault();
        focusInput.value = "";
        const depth = relationshipGraphDepth(depthInput);
        updateRelationshipPageUrl("", depth);
        renderPlaceholder();
        return;
    }
    
    // ... existing button handlers for zoom, fit, reset, overview
});
```

#### 5. Update Graph Loading Logic

```javascript
const loadGraph = async (focus, depth) => {
    const trimmedFocus = focus.trim();
    
    // If no focus, show placeholder instead of loading
    if (!trimmedFocus) {
        renderPlaceholder();
        return;
    }
    
    // ... rest of existing loadGraph implementation
};
```

## Data Flow

### Initial Page Load (No Graph)

```
User requests: GET /relationships

Backend:
1. relationships() handler called with query.focus = None
2. build_relationships_context() executes
3. list_site_relationships() queries materialized view:
   SELECT source_host, target_host, reference_count
   FROM site_relationship_overview
   ORDER BY reference_count DESC
   LIMIT 50 OFFSET 0
4. Returns context with:
   - relationships: [...table data...]
   - relationship_focus: "" (empty)
   - has_relationships: true

Frontend:
1. Template renders with empty relationship_focus
2. JavaScript initRelationshipGraph() runs
3. Sees initialFocus is empty, calls renderPlaceholder()
4. Table displays below with pagination
5. NO API call to /api/relationships/graph

Result: Page loads in ~100ms
```

### User Searches for Host

```
User enters "market.onion", clicks "Visualize"

Frontend:
1. Form submit handler prevents default
2. JavaScript calls focusHost("market.onion")
3. Updates URL: /relationships?focus=market.onion&depth=3
4. Calls loadGraph("market.onion", 3)
5. Sets status to "Loading graph..."
6. Fetches: GET /api/relationships/graph?focus=market.onion&depth=3

Backend:
1. api_relationship_graph() handler called
2. Sets 30s statement timeout
3. load_focused_site_relationship_graph_edges() executes recursive CTE:
   WITH RECURSIVE walk(source_host, target_host, ...) AS (
     -- Find all hosts linking to market.onion
     SELECT ... FROM page_link WHERE target_host = 'market.onion'
     UNION ALL
     -- Recursively walk up to depth 3
     SELECT ... FROM walk JOIN page_link ...
   )
4. build_site_relationship_graph() processes results
5. Returns JSON: { success: true, data: { nodes: [...], edges: [...] } }

Frontend:
1. Receives graph data
2. Calls renderGraph()
3. Graph visualizes with D3 force simulation
4. All controls become active (zoom, fit, reset)
5. Table remains visible below

Result: Graph loads in 1-30s depending on host connectivity
```

### User Clicks Table Row

```
User clicks row where source_host = "vendor.onion"

Frontend:
1. Click event bubbles to document listener
2. Finds closest tr[data-source-host]
3. Extracts data-source-host = "vendor.onion"
4. Calls focusHost("vendor.onion")
5. Same flow as manual search above
```

### User Clicks Clear

```
User clicks "Clear" button

Frontend:
1. Clear button click handler fires
2. Clears focusInput.value = ""
3. Updates URL: /relationships (no query params)
4. Calls renderPlaceholder()
5. Graph area shows placeholder message again
6. Table remains visible (no reload)

Result: Instant return to initial state
```

## Performance Analysis

### Before (Current Implementation)

**Initial page load:**
```
GET /relationships (no focus)
  → load_overview_site_relationship_graph_edges()
    → SELECT FROM site_relationship_overview ORDER BY reference_count DESC LIMIT 1000
    → Returns 1000 edges
  → build_site_relationship_graph() processes all edges
  → Attempts to build massive graph with hundreds/thousands of nodes
  → Query times out or takes 12+ hours
  → Page unusable
```

**Problem:** The overview query was trying to visualize the entire relationship network at once.

### After (This Design)

**Initial page load:**
```
GET /relationships (no focus)
  → list_site_relationships()
    → SELECT FROM site_relationship_overview ORDER BY reference_count DESC LIMIT 50
    → Returns 50 rows for table display only
    → ~100ms query time
  → Template renders with empty graph placeholder
  → JavaScript skips graph API call
  → Page loads successfully
```

**Focused search:**
```
GET /api/relationships/graph?focus=market.onion&depth=3
  → load_focused_site_relationship_graph_edges()
    → Recursive CTE walks backward from specific host
    → Limited by depth (max 4) and 30s timeout
    → Returns manageable subgraph (typically 10-200 nodes)
  → Graph renders successfully
  → 1-30s depending on host connectivity
```

**Result:** 
- Initial load: 12h+ → ~100ms (99.9%+ improvement)
- Focused search: Works within 30s timeout (already working)
- Table: Always fast (~100ms from materialized view)

## Materialized View Refresh Strategy

The `site_relationship_overview` materialized view should be refreshed once daily during low-traffic periods.

**Recommendation:** Schedule via `pg_cron` or system cron:

```sql
-- Refresh happens concurrently (doesn't block reads)
-- Takes ~2 hours on current dataset
REFRESH MATERIALIZED VIEW CONCURRENTLY site_relationship_overview;
```

**Suggested schedule:**
- Run daily at 3:00 AM server time
- Monitor duration and adjust schedule if it grows beyond 3 hours
- Log refresh start/end times for monitoring

**Implementation options:**

1. **pg_cron extension:**
   ```sql
   SELECT cron.schedule(
     'refresh-relationship-overview',
     '0 3 * * *',
     'REFRESH MATERIALIZED VIEW CONCURRENTLY site_relationship_overview'
   );
   ```

2. **System cron with psql:**
   ```bash
   0 3 * * * psql -U spyder -d spyder_db -c "REFRESH MATERIALIZED VIEW CONCURRENTLY site_relationship_overview"
   ```

3. **Rust background job** (if cron is not available):
   - Add scheduled task in application
   - Run refresh via diesel connection
   - Log to application logs

## Error Handling

### Graph Timeout (30s)

**Scenario:** User searches for a highly connected host that can't be processed within 30s.

**Display:**
- Graph area shows: "Graph load timed out after 30 seconds"
- Hint text: "Try focusing on a more specific host or reducing the depth"
- Table remains visible and functional

**User options:**
- Try a different host from the table
- Reduce depth to 1 or 2
- Use table to explore relationships textually

### Empty Results

**Scenario:** User searches for a host with no inbound references.

**Display:**
- Graph area shows: "No inbound references found for {host}"
- Table remains visible

**User options:**
- Try a different host
- Check table for hosts with known connections

### Network Errors

**Scenario:** API request fails due to network issues.

**Display:**
- Graph area shows: "Graph failed to load"
- Error logged to console with details
- Table remains visible

**User options:**
- Retry search
- Refresh page if persistent

### Materialized View Not Populated

**Scenario:** Materialized view hasn't been refreshed yet (empty).

**Display:**
- Table shows empty state: "No cross-site references yet"
- Graph search still works (uses live page_link queries)

**Resolution:**
- Run initial: `REFRESH MATERIALIZED VIEW CONCURRENTLY site_relationship_overview;`
- Set up daily refresh schedule

## Testing Considerations

### Manual Testing Checklist

- [ ] Initial page load shows search form, placeholder, and table
- [ ] Table pagination works without affecting graph area
- [ ] Entering a host and clicking Visualize loads the graph
- [ ] Graph controls (zoom, fit, reset) work after loading
- [ ] Clicking a table row populates search and loads graph
- [ ] Clear button resets to placeholder state without page reload
- [ ] URL updates correctly when focusing/clearing
- [ ] Direct navigation to `/relationships?focus=host.onion` loads graph automatically
- [ ] Timeout errors display helpful message
- [ ] Empty results show appropriate message
- [ ] Network errors are handled gracefully

### Performance Testing

- [ ] Initial page load completes in < 500ms
- [ ] Table query returns in < 200ms
- [ ] Focused graph for moderately connected host loads in < 5s
- [ ] Highly connected host either loads or times out within 30s
- [ ] Page remains responsive during graph loading
- [ ] Memory usage stays reasonable for large graphs

### Edge Cases

- [ ] Empty search string triggers placeholder (not API call)
- [ ] Whitespace-only search triggers placeholder
- [ ] Special characters in host names don't break queries
- [ ] Very long host names don't break UI layout
- [ ] Rapid clicking on table rows doesn't cause race conditions
- [ ] Browser back/forward buttons work correctly with URL state

## Future Enhancements (Out of Scope)

These improvements could be added later if needed:

1. **Autocomplete for host search:**
   - Suggest hosts as user types
   - Query top hosts from materialized view

2. **Filter table by focused host:**
   - When graph is focused, optionally filter table to show only related rows
   - Toggle between "All" and "Related to {host}"

3. **Bidirectional graph exploration:**
   - Current implementation only shows inbound references (who links to X)
   - Could add option to show outbound references (who X links to)

4. **Graph export:**
   - Download graph as PNG/SVG
   - Export node/edge data as JSON/CSV

5. **Saved searches:**
   - Bookmark frequently explored hosts
   - Quick access to interesting subgraphs

6. **Real-time refresh indicator:**
   - Show age of materialized view data
   - Notify when refresh is in progress

## Implementation Checklist

- [ ] Update `templates/relationships.html.hbs`:
  - [ ] Restructure layout (search → graph → table)
  - [ ] Add data attributes to table rows
  - [ ] Add placeholder template content
  - [ ] Change "Focus" to "Visualize" button
  - [ ] Add "Clear" button
- [ ] Update `static/js/app.js`:
  - [ ] Modify `initRelationshipGraph()` to skip initial load
  - [ ] Add `renderPlaceholder()` function
  - [ ] Add table row click handler
  - [ ] Add clear button handler
  - [ ] Update `loadGraph()` to handle empty focus
- [ ] Update CSS (if needed):
  - [ ] Style placeholder message
  - [ ] Style clear button
  - [ ] Ensure search form is prominent
- [ ] Set up materialized view refresh:
  - [ ] Choose refresh method (pg_cron, system cron, or app job)
  - [ ] Schedule daily refresh at 3 AM
  - [ ] Add monitoring/logging
- [ ] Test all scenarios from checklist above
- [ ] Update documentation if needed

## Success Metrics

**Primary goal:** Make the relationship page usable.

**Metrics:**
- Initial page load time: < 500ms (vs. 12h+ timeout)
- Graph visualization success rate: > 90% of searches complete within 30s
- User workflow: Clear path from landing → browsing table → visualizing host
- Zero automatic timeouts on page load

**User feedback indicators:**
- Users can successfully explore relationships
- Table provides useful discovery mechanism
- Graph provides detailed visualization when needed
- Page feels responsive and intentional
