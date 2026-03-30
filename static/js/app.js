// Chargement des statistiques
async function loadStats() {
    const container = document.getElementById('stats-container');

    try {
        const response = await fetch('/api/stats');
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        const result = await response.json();

        if (result.success) {
            container.innerHTML = `
                <div class="stats-grid">
                    <div class="stat-item">
                        <strong>${result.data.total_pages}</strong>
                        <span>Pages scrapées</span>
                    </div>
                    <div class="stat-item">
                        <strong>${result.data.total_domains}</strong>
                        <span>Domaines</span>
                    </div>
                    <div class="stat-item">
                        <strong>${result.data.pending_work_units}</strong>
                        <span>En attente</span>
                    </div>
                    <div class="stat-item">
                        <strong>${result.data.failed_work_units}</strong>
                        <span>En échec</span>
                    </div>
                    <div class="stat-item">
                        <strong>${result.data.last_scrape}</strong>
                        <span>Dernier scraping</span>
                    </div>
                </div>
            `;
        } else {
            container.innerHTML = '<p class="error">Erreur lors du chargement des stats</p>';
        }
    } catch (error) {
        console.error('Erreur:', error);
        document.getElementById('stats-container').innerHTML = 
            '<p class="error">Impossible de charger les statistiques</p>';
    }
}

// Recherche rapide
async function quickSearch(event) {
    event.preventDefault();

    const query = document.getElementById('search-input').value;
    const resultsContainer = document.getElementById('search-results');

    if (!query.trim()) {
        resultsContainer.innerHTML = '<p>Veuillez entrer un terme de recherche</p>';
        return;
    }

    try {
        resultsContainer.innerHTML = '<p>Recherche en cours...</p>';

        const response = await fetch(`/api/search?query=${encodeURIComponent(query)}&limit=5`);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        const result = await response.json();

        if (result.success && result.data.length > 0) {
            renderSearchResults(resultsContainer, result.data);
        } else {
            resultsContainer.innerHTML = '<p>Aucun résultat trouvé</p>';
        }
    } catch (error) {
        console.error('Erreur de recherche:', error);
        resultsContainer.innerHTML = '<p class="error">Erreur lors de la recherche</p>';
    }
}

function renderSearchResults(container, results) {
    container.innerHTML = '';

    const title = document.createElement('h4');
    title.textContent = `Résultats (${results.length})`;
    container.appendChild(title);

    const list = document.createElement('ul');
    list.className = 'search-list';

    for (const item of results) {
        const listItem = document.createElement('li');
        listItem.className = 'search-item';

        const heading = document.createElement('strong');
        heading.textContent = item.title;
        listItem.appendChild(heading);
        listItem.appendChild(document.createElement('br'));

        const small = document.createElement('small');
        const link = document.createElement('a');
        link.href = item.url;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.textContent = item.url;
        small.appendChild(link);
        listItem.appendChild(small);
        listItem.appendChild(document.createElement('br'));

        const scrapedAt = document.createElement('em');
        scrapedAt.textContent = item.scraped_at;
        listItem.appendChild(scrapedAt);

        list.appendChild(listItem);
    }

    container.appendChild(list);
}

// Styles CSS additionnels via JavaScript
document.addEventListener('DOMContentLoaded', function() {
    // Ajouter des styles pour les statistiques
    const style = document.createElement('style');
    style.textContent = `
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 1rem;
            margin-top: 1rem;
        }
        
        .stat-item {
            text-align: center;
            padding: 1rem;
            background: white;
            border-radius: 5px;
            border: 1px solid #ddd;
        }
        
        .stat-item strong {
            display: block;
            font-size: 1.5rem;
            color: #3498db;
            margin-bottom: 0.5rem;
        }
        
        .search-list {
            list-style: none;
            margin-top: 1rem;
        }
        
        .search-item {
            padding: 1rem;
            background: white;
            margin-bottom: 0.5rem;
            border-radius: 5px;
            border: 1px solid #ddd;
        }
        
        .error {
            color: #e74c3c;
            font-weight: bold;
        }
    `;
    document.head.appendChild(style);
});
