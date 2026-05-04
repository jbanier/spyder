function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#39;");
}

function renderSiteCategoryBadge(siteCategory) {
    if (!siteCategory) {
        return "";
    }

    return `
        <div class="meta-badges">
            <span class="site-category-badge site-confidence-${escapeHtml(siteCategory.confidence)}">
                ${escapeHtml(siteCategory.label)} · ${escapeHtml(siteCategory.confidence)}
            </span>
        </div>
    `;
}

function buildSearchPageUrl(query, limit, offset) {
    const params = new URLSearchParams();
    params.set("query", query);
    params.set("limit", String(limit));
    params.set("offset", String(offset));
    return `/search?${params.toString()}`;
}

function renderSearchPagination(query, page) {
    const hasPreviousPage = page.offset > 0;
    const hasNextPage = page.offset + page.limit < page.total_count;
    if (!hasPreviousPage && !hasNextPage) {
        return "";
    }

    const previousOffset = Math.max(0, page.offset - page.limit);
    const nextOffset = page.offset + page.limit;

    return `
        <section class="pagination-bar">
            ${hasPreviousPage ? `<a class="btn btn-secondary" href="${escapeHtml(buildSearchPageUrl(query, page.limit, previousOffset))}">Previous</a>` : ""}
            ${hasNextPage ? `<a class="btn btn-secondary" href="${escapeHtml(buildSearchPageUrl(query, page.limit, nextOffset))}">Next</a>` : ""}
        </section>
    `;
}

function renderSearchResults(query, page) {
    const results = page.items ?? [];
    if (!results.length) {
        return `
            <section class="empty-state">
                <h2>No results</h2>
                <p>No indexed page or tagged site matched <strong>${escapeHtml(query)}</strong>.</p>
            </section>
            ${renderSearchPagination(query, page)}
        `;
    }

    const cards = results
        .map(
            (item) => `
                <article class="search-result-card">
                    <h3><a class="row-link" href="/pages/${item.page_id}">${escapeHtml(item.title)}</a></h3>
                    <p><a href="${escapeHtml(item.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(item.url)}</a></p>
                    <div class="meta-strip">
                        <span><strong>Host:</strong> ${escapeHtml(item.host)}</span>
                        <span><strong>Language:</strong> ${escapeHtml(item.language)}</span>
                        <span><strong>Last Scan:</strong> ${escapeHtml(item.scraped_at)}</span>
                    </div>
                    ${renderSiteCategoryBadge(item.site_category)}
                </article>
            `,
        )
        .join("");

    return `
        <section class="card">
            <div class="section-heading">
                <h2>Results</h2>
                <span class="muted">${escapeHtml(page.total_count)} matches</span>
            </div>
            <div class="search-results-grid">
                ${cards}
            </div>
        </section>
        ${renderSearchPagination(query, page)}
    `;
}

document.addEventListener("DOMContentLoaded", () => {
    const searchForms = document.querySelectorAll("[data-api-search]");

    for (const form of searchForms) {
        form.addEventListener("submit", async (event) => {
            event.preventDefault();

            const queryInput = form.querySelector("input[name=query]");
            const limitInput = form.querySelector("input[name=limit]");
            const resultsContainer = document.querySelector(form.dataset.resultsTarget);
            if (!queryInput || !resultsContainer) {
                return;
            }

            const query = queryInput.value.trim();
            const parsedLimit = Number.parseInt(limitInput?.value ?? "20", 10);
            const limit = Number.isNaN(parsedLimit) ? 20 : Math.min(50, Math.max(1, parsedLimit));
            if (!query) {
                resultsContainer.innerHTML = "<p class=\"empty-copy\">Enter a search term.</p>";
                return;
            }

            const searchPath = buildSearchPageUrl(query, limit, 0);
            window.history.replaceState({}, "", searchPath);
            resultsContainer.innerHTML = `
                <section class="empty-state">
                    <h2>Searching...</h2>
                    <p>Looking for indexed pages and tagged sites that match <strong>${escapeHtml(query)}</strong>.</p>
                </section>
            `;

            try {
                const response = await fetch(
                    `/api/search?query=${encodeURIComponent(query)}&limit=${limit}&offset=0`,
                );
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }

                const payload = await response.json();
                if (!payload.success) {
                    throw new Error("Search response was not successful");
                }

                resultsContainer.innerHTML = renderSearchResults(query, payload.data);
            } catch (error) {
                console.error("Search request failed", error);
                resultsContainer.innerHTML = `
                    <section class="empty-state">
                        <h2>Search failed</h2>
                        <p>Check the server logs and try the query again.</p>
                    </section>
                `;
            }
        });
    }
});
