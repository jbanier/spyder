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

const SVG_NS = "http://www.w3.org/2000/svg";

function clampNumber(value, min, max) {
    return Math.min(max, Math.max(min, value));
}

function createSvgElement(tagName, attributes = {}) {
    const element = document.createElementNS(SVG_NS, tagName);
    for (const [key, value] of Object.entries(attributes)) {
        element.setAttribute(key, String(value));
    }
    return element;
}

function shortGraphLabel(value) {
    const text = String(value ?? "");
    return text.length > 26 ? `${text.slice(0, 23)}...` : text;
}

function relationshipGraphDepth(input) {
    const parsed = Number.parseInt(input?.value ?? "3", 10);
    return Number.isNaN(parsed) ? 3 : clampNumber(parsed, 1, 4);
}

function relationshipGraphUrl(focus, depth) {
    const params = new URLSearchParams();
    params.set("limit", "80");
    params.set("depth", String(depth));
    if (focus) {
        params.set("focus", focus);
    }
    return `/api/relationships/graph?${params.toString()}`;
}

function updateRelationshipPageUrl(focus, depth) {
    const params = new URLSearchParams(window.location.search);
    params.delete("offset");
    if (focus) {
        params.set("focus", focus);
    } else {
        params.delete("focus");
    }
    params.set("depth", String(depth || 3));
    const query = params.toString();
    window.history.replaceState({}, "", `${window.location.pathname}${query ? `?${query}` : ""}`);
}

function graphNodeWeight(node) {
    return (node.incoming_count ?? 0) + (node.outgoing_count ?? 0);
}

function layoutOverviewGraph(nodes, edges, width, height) {
    const positions = new Map();
    if (!nodes.length) {
        return positions;
    }
    if (nodes.length === 1) {
        positions.set(nodes[0].host, { x: width / 2, y: height / 2 });
        return positions;
    }

    const centerX = width / 2;
    const centerY = height / 2;
    const sortedNodes = [...nodes].sort((left, right) => {
        return graphNodeWeight(right) - graphNodeWeight(left) || left.host.localeCompare(right.host);
    });
    const baseRadius = Math.max(120, Math.min(width, height) * 0.26);
    sortedNodes.forEach((node, index) => {
        if (index === 0) {
            positions.set(node.host, { x: centerX, y: centerY });
            return;
        }
        const ring = Math.floor((index - 1) / 18);
        const ringIndex = (index - 1) % 18;
        const ringSize = Math.min(18, sortedNodes.length - 1 - ring * 18);
        const angle = (Math.PI * 2 * ringIndex) / Math.max(1, ringSize) - Math.PI / 2;
        const radius = baseRadius + ring * 95;
        positions.set(node.host, {
            x: centerX + Math.cos(angle) * radius,
            y: centerY + Math.sin(angle) * radius,
        });
    });

    const edgePairs = edges
        .map((edge) => ({
            sourceHost: edge.source_host,
            targetHost: edge.target_host,
            source: positions.get(edge.source_host),
            target: positions.get(edge.target_host),
            weight: Math.max(1, edge.reference_count ?? 1),
        }))
        .filter((edge) => edge.source && edge.target);
    const movableNodes = sortedNodes.slice(1);
    const padding = 70;
    for (let iteration = 0; iteration < 70; iteration += 1) {
        const forces = new Map(movableNodes.map((node) => [node.host, { x: 0, y: 0 }]));
        for (let leftIndex = 0; leftIndex < sortedNodes.length; leftIndex += 1) {
            for (let rightIndex = leftIndex + 1; rightIndex < sortedNodes.length; rightIndex += 1) {
                const leftNode = sortedNodes[leftIndex];
                const rightNode = sortedNodes[rightIndex];
                const left = positions.get(leftNode.host);
                const right = positions.get(rightNode.host);
                const dx = right.x - left.x || 0.01;
                const dy = right.y - left.y || 0.01;
                const distanceSq = Math.max(90, dx * dx + dy * dy);
                const force = 4200 / distanceSq;
                const distance = Math.sqrt(distanceSq);
                const fx = (dx / distance) * force;
                const fy = (dy / distance) * force;
                if (forces.has(leftNode.host)) {
                    forces.get(leftNode.host).x -= fx;
                    forces.get(leftNode.host).y -= fy;
                }
                if (forces.has(rightNode.host)) {
                    forces.get(rightNode.host).x += fx;
                    forces.get(rightNode.host).y += fy;
                }
            }
        }
        for (const edge of edgePairs) {
            const dx = edge.target.x - edge.source.x;
            const dy = edge.target.y - edge.source.y;
            const distance = Math.max(1, Math.hypot(dx, dy));
            const pull = (distance - 180) * 0.0008 * Math.log2(edge.weight + 1);
            const fx = dx * pull;
            const fy = dy * pull;
            for (const [host, sign] of [
                [edge.sourceHost, 1],
                [edge.targetHost, -1],
            ]) {
                if (host && forces.has(host)) {
                    forces.get(host).x += fx * sign;
                    forces.get(host).y += fy * sign;
                }
            }
        }
        for (const node of movableNodes) {
            const position = positions.get(node.host);
            const force = forces.get(node.host);
            position.x = clampNumber(position.x + force.x, padding, width - padding);
            position.y = clampNumber(position.y + force.y, padding, height - padding);
        }
    }

    return positions;
}

function layoutFocusedGraph(nodes, width, height) {
    const positions = new Map();
    if (!nodes.length) {
        return positions;
    }
    const byDepth = new Map();
    for (const node of nodes) {
        const depth = Math.max(0, node.depth ?? 0);
        if (!byDepth.has(depth)) {
            byDepth.set(depth, []);
        }
        byDepth.get(depth).push(node);
    }
    const maxDepth = Math.max(...Array.from(byDepth.keys()), 1);
    const horizontalPadding = 95;
    const verticalPadding = 72;
    for (const [depth, depthNodes] of byDepth.entries()) {
        depthNodes.sort((left, right) => {
            return graphNodeWeight(right) - graphNodeWeight(left) || left.host.localeCompare(right.host);
        });
        const x = width - horizontalPadding - (depth / maxDepth) * (width - horizontalPadding * 2);
        const step = (height - verticalPadding * 2) / Math.max(1, depthNodes.length);
        depthNodes.forEach((node, index) => {
            positions.set(node.host, {
                x,
                y: verticalPadding + step * (index + 0.5),
            });
        });
    }
    return positions;
}

function relationshipGraphBounds(positions) {
    const values = Array.from(positions.values());
    if (!values.length) {
        return null;
    }
    return values.reduce(
        (bounds, position) => ({
            minX: Math.min(bounds.minX, position.x),
            minY: Math.min(bounds.minY, position.y),
            maxX: Math.max(bounds.maxX, position.x),
            maxY: Math.max(bounds.maxY, position.y),
        }),
        {
            minX: values[0].x,
            minY: values[0].y,
            maxX: values[0].x,
            maxY: values[0].y,
        },
    );
}

function initRelationshipGraph(container) {
    const form = container.querySelector("[data-relationship-graph-form]");
    const focusInput = form?.querySelector("input[name=focus]");
    const depthInput = form?.querySelector("input[name=depth]");
    const status = container.querySelector("[data-relationship-graph-status]");
    const svg = container.querySelector("[data-relationship-graph-svg]");
    const viewport = container.querySelector("[data-relationship-graph-viewport]");
    const tooltip = container.querySelector("[data-relationship-graph-tooltip]");
    if (!form || !focusInput || !depthInput || !svg || !viewport) {
        return;
    }

    let graph = null;
    let positions = new Map();
    let scale = 1;
    let translate = { x: 0, y: 0 };
    let dragStart = null;

    const setStatus = (text) => {
        if (status) {
            status.textContent = text;
        }
    };
    const setTransform = () => {
        viewport.setAttribute("transform", `translate(${translate.x} ${translate.y}) scale(${scale})`);
    };
    const graphSize = () => {
        const rect = svg.getBoundingClientRect();
        return {
            width: Math.max(640, rect.width || 960),
            height: Math.max(420, rect.height || 520),
        };
    };
    const showTooltip = (event, lines) => {
        if (!tooltip) {
            return;
        }
        tooltip.innerHTML = "";
        for (const line of lines) {
            const item = document.createElement("div");
            item.textContent = line;
            tooltip.appendChild(item);
        }
        const shellRect = container.querySelector(".relationship-graph-shell")?.getBoundingClientRect();
        tooltip.hidden = false;
        tooltip.style.left = `${event.clientX - (shellRect?.left ?? 0) + 14}px`;
        tooltip.style.top = `${event.clientY - (shellRect?.top ?? 0) + 14}px`;
    };
    const hideTooltip = () => {
        if (tooltip) {
            tooltip.hidden = true;
        }
    };
    const focusHost = (host) => {
        focusInput.value = host;
        const depth = relationshipGraphDepth(depthInput);
        updateRelationshipPageUrl(host, depth);
        loadGraph(host, depth);
    };
    const fitGraph = () => {
        const size = graphSize();
        const bounds = relationshipGraphBounds(positions);
        if (!bounds) {
            scale = 1;
            translate = { x: 0, y: 0 };
            setTransform();
            return;
        }
        const graphWidth = Math.max(1, bounds.maxX - bounds.minX + 150);
        const graphHeight = Math.max(1, bounds.maxY - bounds.minY + 130);
        scale = clampNumber(Math.min(size.width / graphWidth, size.height / graphHeight), 0.25, 2.2);
        translate = {
            x: size.width / 2 - ((bounds.minX + bounds.maxX) / 2) * scale,
            y: size.height / 2 - ((bounds.minY + bounds.maxY) / 2) * scale,
        };
        setTransform();
    };
    const renderGraph = () => {
        viewport.replaceChildren();
        const size = graphSize();
        svg.setAttribute("viewBox", `0 0 ${size.width} ${size.height}`);
        if (!graph || !graph.nodes.length) {
            const emptyText = createSvgElement("text", {
                x: size.width / 2,
                y: size.height / 2,
                class: "relationship-graph-empty",
                "text-anchor": "middle",
            });
            emptyText.textContent = graph?.focus_host
                ? `No inbound references found for ${graph.focus_host}`
                : "No cross-site references found";
            viewport.appendChild(emptyText);
            scale = 1;
            translate = { x: 0, y: 0 };
            setTransform();
            return;
        }

        positions = graph.mode === "focus"
            ? layoutFocusedGraph(graph.nodes, size.width, size.height)
            : layoutOverviewGraph(graph.nodes, graph.edges, size.width, size.height);
        const defs = createSvgElement("defs");
        const marker = createSvgElement("marker", {
            id: "relationship-graph-arrowhead",
            markerWidth: "10",
            markerHeight: "8",
            refX: "9",
            refY: "4",
            orient: "auto",
            markerUnits: "strokeWidth",
        });
        const markerPath = createSvgElement("path", {
            d: "M 0 0 L 10 4 L 0 8 z",
            class: "relationship-graph-arrowhead",
        });
        marker.appendChild(markerPath);
        defs.appendChild(marker);
        viewport.appendChild(defs);

        const edgeLayer = createSvgElement("g", { class: "relationship-graph-edge-layer" });
        for (const edge of graph.edges) {
            const source = positions.get(edge.source_host);
            const target = positions.get(edge.target_host);
            if (!source || !target) {
                continue;
            }
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const length = Math.max(1, Math.hypot(dx, dy));
            const start = {
                x: source.x + (dx / length) * 38,
                y: source.y + (dy / length) * 38,
            };
            const end = {
                x: target.x - (dx / length) * 42,
                y: target.y - (dy / length) * 42,
            };
            const path = createSvgElement("path", {
                d: `M ${start.x} ${start.y} L ${end.x} ${end.y}`,
                class: `relationship-graph-edge${edge.is_blacklisted ? " relationship-graph-edge-blacklisted" : ""}`,
                "stroke-width": clampNumber(1.4 + Math.log2((edge.reference_count ?? 1) + 1), 1.6, 6),
                "marker-end": "url(#relationship-graph-arrowhead)",
            });
            path.addEventListener("mouseenter", (event) => {
                showTooltip(event, [
                    `${edge.source_host} -> ${edge.target_host}`,
                    `${edge.reference_count} observed links`,
                ]);
            });
            path.addEventListener("mousemove", (event) => {
                showTooltip(event, [
                    `${edge.source_host} -> ${edge.target_host}`,
                    `${edge.reference_count} observed links`,
                ]);
            });
            path.addEventListener("mouseleave", hideTooltip);
            edgeLayer.appendChild(path);
        }
        viewport.appendChild(edgeLayer);

        const nodeLayer = createSvgElement("g", { class: "relationship-graph-node-layer" });
        for (const node of graph.nodes) {
            const position = positions.get(node.host);
            if (!position) {
                continue;
            }
            const group = createSvgElement("g", {
                class: `relationship-graph-node${node.is_focus ? " relationship-graph-node-focus" : ""}${node.is_blacklisted ? " relationship-graph-node-blacklisted" : ""}`,
                transform: `translate(${position.x} ${position.y})`,
                tabindex: "0",
                role: "button",
                "aria-label": `Focus ${node.host}`,
            });
            const radius = clampNumber(18 + Math.log2(graphNodeWeight(node) + 1) * 4, 20, 42);
            const circle = createSvgElement("circle", { r: radius });
            const label = createSvgElement("text", {
                y: radius + 18,
                "text-anchor": "middle",
                class: "relationship-graph-node-label",
            });
            label.textContent = shortGraphLabel(node.host);
            group.appendChild(circle);
            group.appendChild(label);
            if (node.site_category) {
                const category = createSvgElement("text", {
                    y: radius + 34,
                    "text-anchor": "middle",
                    class: "relationship-graph-node-category",
                });
                category.textContent = node.site_category.label;
                group.appendChild(category);
            }
            group.addEventListener("click", (event) => {
                event.stopPropagation();
                focusHost(node.host);
            });
            group.addEventListener("keydown", (event) => {
                if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    focusHost(node.host);
                }
            });
            group.addEventListener("mouseenter", (event) => {
                const category = node.site_category ? `${node.site_category.label} (${node.site_category.confidence})` : "Unclassified";
                showTooltip(event, [
                    node.host,
                    category,
                    `${node.incoming_count} incoming, ${node.outgoing_count} outgoing`,
                    node.is_blacklisted ? `Blacklisted: ${node.blacklist_match_domain}` : "Not blacklisted",
                ]);
            });
            group.addEventListener("mousemove", (event) => {
                const category = node.site_category ? `${node.site_category.label} (${node.site_category.confidence})` : "Unclassified";
                showTooltip(event, [
                    node.host,
                    category,
                    `${node.incoming_count} incoming, ${node.outgoing_count} outgoing`,
                    node.is_blacklisted ? `Blacklisted: ${node.blacklist_match_domain}` : "Not blacklisted",
                ]);
            });
            group.addEventListener("mouseleave", hideTooltip);
            nodeLayer.appendChild(group);
        }
        viewport.appendChild(nodeLayer);
        fitGraph();
    };
    const loadGraph = async (focus = focusInput.value.trim(), depth = relationshipGraphDepth(depthInput)) => {
        const normalizedFocus = focus.trim();
        const startTime = Date.now();
        const timeoutMs = 30000; // 30 seconds

        setStatus("Loading graph...");

        // Update elapsed time every second
        const progressInterval = setInterval(() => {
            const elapsed = Math.floor((Date.now() - startTime) / 1000);
            setStatus(`Loading graph... ${elapsed}s elapsed`);
        }, 1000);

        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

            const response = await fetch(relationshipGraphUrl(normalizedFocus, depth), {
                signal: controller.signal
            });
            clearTimeout(timeoutId);
            clearInterval(progressInterval);

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            const payload = await response.json();
            if (!payload.success) {
                throw new Error("Graph response was not successful");
            }
            graph = payload.data;
            const elapsed = Math.floor((Date.now() - startTime) / 1000);
            setStatus(`${graph.mode === "focus" ? "Focused" : "Overview"}: ${graph.nodes.length} sites, ${graph.edges.length} links (loaded in ${elapsed}s)`);
            renderGraph();
        } catch (error) {
            clearInterval(progressInterval);
            console.error("Relationship graph request failed", error);
            graph = null;
            positions = new Map();
            viewport.replaceChildren();

            if (error.name === 'AbortError') {
                setStatus("Graph load timed out after 30s. Try focusing on a specific host or reducing depth.");
                const emptyText = createSvgElement("text", {
                    x: graphSize().width / 2,
                    y: graphSize().height / 2 - 20,
                    class: "relationship-graph-empty",
                    "text-anchor": "middle",
                });
                emptyText.textContent = "Graph load timed out after 30 seconds";
                viewport.appendChild(emptyText);
                const hintText = createSvgElement("text", {
                    x: graphSize().width / 2,
                    y: graphSize().height / 2 + 10,
                    class: "relationship-graph-empty",
                    "text-anchor": "middle",
                    "font-size": "14",
                });
                hintText.textContent = "Try focusing on a specific host or reducing the depth";
                viewport.appendChild(hintText);
            } else {
                setStatus("Graph failed to load");
            }
        }
    };

    form.addEventListener("submit", (event) => {
        event.preventDefault();
        const focus = focusInput.value.trim();
        const depth = relationshipGraphDepth(depthInput);
        updateRelationshipPageUrl(focus, depth);
        loadGraph(focus, depth);
    });

    // Clear button handler
    container.addEventListener("click", (event) => {
        const clearButton = event.target.closest("[data-relationship-graph-clear]");
        if (clearButton) {
            event.preventDefault();
            focusInput.value = "";
            const depth = relationshipGraphDepth(depthInput);
            updateRelationshipPageUrl("", depth);

            // Reset graph to placeholder state
            graph = null;
            positions = new Map();
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
            return;
        }
    });

    container.addEventListener("click", (event) => {
        const button = event.target.closest("[data-relationship-graph-action]");
        if (!button) {
            return;
        }
        const action = button.dataset.relationshipGraphAction;
        if (action === "zoom-in") {
            scale = clampNumber(scale * 1.2, 0.15, 4);
            setTransform();
        } else if (action === "zoom-out") {
            scale = clampNumber(scale / 1.2, 0.15, 4);
            setTransform();
        } else if (action === "fit") {
            fitGraph();
        } else if (action === "reset") {
            scale = 1;
            translate = { x: 0, y: 0 };
            setTransform();
        }
    });
    svg.addEventListener("wheel", (event) => {
        event.preventDefault();
        const rect = svg.getBoundingClientRect();
        const point = { x: event.clientX - rect.left, y: event.clientY - rect.top };
        const nextScale = clampNumber(scale * (event.deltaY < 0 ? 1.12 : 0.88), 0.15, 4);
        translate = {
            x: point.x - ((point.x - translate.x) / scale) * nextScale,
            y: point.y - ((point.y - translate.y) / scale) * nextScale,
        };
        scale = nextScale;
        setTransform();
    }, { passive: false });
    svg.addEventListener("pointerdown", (event) => {
        if (event.button !== 0 || event.target.closest(".relationship-graph-node")) {
            return;
        }
        dragStart = {
            x: event.clientX,
            y: event.clientY,
            translateX: translate.x,
            translateY: translate.y,
        };
        svg.setPointerCapture(event.pointerId);
    });
    svg.addEventListener("pointermove", (event) => {
        if (!dragStart) {
            return;
        }
        translate = {
            x: dragStart.translateX + event.clientX - dragStart.x,
            y: dragStart.translateY + event.clientY - dragStart.y,
        };
        setTransform();
    });
    svg.addEventListener("pointerup", () => {
        dragStart = null;
    });
    svg.addEventListener("pointercancel", () => {
        dragStart = null;
    });
    window.addEventListener("resize", () => {
        if (graph) {
            renderGraph();
        }
    });

    focusInput.value = container.dataset.initialFocus ?? focusInput.value;
    depthInput.value = container.dataset.initialDepth ?? depthInput.value;

    // Only auto-load if there's a focus host, otherwise show a prompt
    const initialFocus = focusInput.value.trim();
    if (initialFocus) {
        loadGraph(initialFocus, relationshipGraphDepth(depthInput));
    } else {
        setStatus("Ready to visualize");
        const size = graphSize();

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
    }
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
                const apiParams = new URLSearchParams();
                apiParams.set("query", query);
                apiParams.set("limit", String(limit));
                apiParams.set("offset", "0");
                const response = await fetch(`/api/search?${apiParams.toString()}`);
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

    for (const graphContainer of document.querySelectorAll("[data-relationship-graph]")) {
        initRelationshipGraph(graphContainer);
    }
});
