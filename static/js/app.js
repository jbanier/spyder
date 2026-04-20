document.addEventListener("DOMContentLoaded", () => {
    const searchForms = document.querySelectorAll("[data-api-search]");

    for (const form of searchForms) {
        form.addEventListener("submit", async (event) => {
            event.preventDefault();

            const queryInput = form.querySelector("input[name=query]");
            const resultsContainer = document.querySelector(form.dataset.resultsTarget);
            if (!queryInput || !resultsContainer) {
                return;
            }

            const query = queryInput.value.trim();
            if (!query) {
                resultsContainer.innerHTML = "<p class=\"empty-copy\">Enter a search term.</p>";
                return;
            }

            resultsContainer.innerHTML = "<p class=\"empty-copy\">Searching...</p>";

            try {
                const response = await fetch(`/api/search?query=${encodeURIComponent(query)}&limit=5`);
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }

                const payload = await response.json();
                if (!payload.success || !payload.data.length) {
                    resultsContainer.innerHTML = "<p class=\"empty-copy\">No results.</p>";
                    return;
                }

                resultsContainer.innerHTML = payload.data
                    .map(
                        (item) => `
                            <a class="compact-row" href="/pages/${item.page_id}">
                                <span class="compact-title">${item.title}</span>
                                <span class="compact-meta">${item.language}</span>
                                <span class="compact-meta">${item.scraped_at}</span>
                            </a>
                        `,
                    )
                    .join("");
            } catch (error) {
                console.error("Search request failed", error);
                resultsContainer.innerHTML =
                    "<p class=\"empty-copy\">Search failed. Check the server logs.</p>";
            }
        });
    }
});
