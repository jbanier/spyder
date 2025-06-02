// Chargement des statistiques
async function loadStats() {
    try {
        const response = await fetch('/api/stats');
        const result = await response.json();
        
        const container = document.getElementById('stats-container');
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
        
        const response = await fetch(`/search?query=${encodeURIComponent(query)}&limit=5`);
        const result = await response.json();
        
        if (result.success && result.data.length > 0) {
            resultsContainer.innerHTML = `
                <h4>Résultats (${result.data.length}):</h4>
                <ul class="search-list">
                    ${result.data.map(item => `
                        <li class="search-item">
                            <strong>${item.title}</strong><br>
                            <small><a href="${item.url}" target="_blank">${item.url}</a></small><br>
                            <em>${item.scraped_at}</em>
                        </li>
                    `).join('')}
                </ul>
            `;
        } else {
            resultsContainer.innerHTML = '<p>Aucun résultat trouvé</p>';
        }
    } catch (error) {
        console.error('Erreur de recherche:', error);
        resultsContainer.innerHTML = '<p class="error">Erreur lors de la recherche</p>';
    }
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
