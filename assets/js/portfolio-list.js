
(function () {
    function getAlgoBaseUrl() {
        const configuredBaseUrl = (window.APP_CONFIG && window.APP_CONFIG.algoApiBaseUrl) || window.APP_ALGO_API_BASE_URL || '';
        if (configuredBaseUrl) {
            return configuredBaseUrl;
        }
        const hostname = String((window.location && window.location.hostname) || '').toLowerCase();
        return (hostname === 'finedgealgo.com' || hostname === 'www.finedgealgo.com')
            ? 'https://finedgealgo.com/algo'
            : 'http://localhost:8000/algo';
    }

    function getPortfolioListApiUrl(routeName, suffix) {
        if (typeof window.buildNamedApiUrl === 'function') {
            return window.buildNamedApiUrl(routeName, suffix);
        }
        const baseUrl = getAlgoBaseUrl();
        const routeMap = window.APP_API_ROUTES || {};
        const routePath = routeMap[routeName] || routeName || '';
        const normalizedRoute = String(routePath).replace(/\/+$/, '');
        const normalizedSuffix = String(suffix || '').replace(/^\/+/, '');
        return baseUrl.replace(/\/+$/, '') + '/' + (normalizedSuffix ? normalizedRoute + '/' + normalizedSuffix : normalizedRoute);
    }

    function getPortfolioListPageUrl(routeName, queryString) {
        if (typeof window.buildNamedPageUrl === 'function') {
            return window.buildNamedPageUrl(routeName) + (queryString || '');
        }
        const routeMap = window.APP_PAGE_ROUTES || {};
        const routePath = routeMap[routeName] || routeName || '';
        const builder = typeof window.buildAppUrl === 'function' ? window.buildAppUrl : function (path) { return './' + String(path || '').replace(/^\/+/, ''); };
        return builder(routePath) + (queryString || '');
    }

    const openButton = document.querySelector('[data-testid="portfolio-new-btn"]');
    const modalOverlay = document.querySelector('[data-portfolio-modal]');
    const closeButtons = document.querySelectorAll('[data-portfolio-modal-close]');
    const strategyList = document.getElementById('portfolio-strategy-list');
    const strategySearch = document.getElementById('portfolio-strategy-search');
    const selectAllCheckbox = document.getElementById('portfolio-select-all');
    const selectAllLabel = document.getElementById('portfolio-select-all-label');
    const selectedPill = document.getElementById('portfolio-selected-pill');
    const statusBox = document.getElementById('portfolio-modal-status');
    const portfolioNameInput = document.getElementById('portfolio-name-input');
    const createButton = document.getElementById('portfolio-create-btn');
    const portfolioGrid = document.getElementById('portfolio-grid');
    const portfolioSearchInput = document.getElementById('portfolio-search-input');
    if (!openButton || !modalOverlay || !strategyList) {
        return;
    }

    const state = {
        strategies: [],
        portfolios: [],
        selected: new Set(),
        query: '',
        portfolioQuery: '',
        loading: false
    };

    function escapeHtml(value) {
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function setStatus(message, type) {
        statusBox.textContent = message || '';
        statusBox.className = 'portfolio-modal-status';
        if (type) {
            statusBox.classList.add(type === 'error' ? 'is-error' : 'is-success');
        }
    }

    function getFilteredPortfolios() {
        const query = state.portfolioQuery.trim().toLowerCase();
        if (!query) {
            return state.portfolios;
        }

        return state.portfolios.filter(function (portfolio) {
            const portfolioName = (portfolio.name || '').toLowerCase();
            const strategyNames = (portfolio.strategy_names || []).join(' ').toLowerCase();
            return portfolioName.includes(query) || strategyNames.includes(query);
        });
    }

    function renderPortfolioGrid() {
        if (!portfolioGrid) {
            return;
        }

        const portfolios = getFilteredPortfolios();
        if (!portfolios.length) {
            portfolioGrid.innerHTML = '<div class="portfolio-grid-status md:col-span-4">No portfolios found.</div>';
            return;
        }

        portfolioGrid.innerHTML = portfolios.map(function (portfolio) {
            const strategyNames = Array.isArray(portfolio.strategy_names) ? portfolio.strategy_names : [];
            const strategyCount = strategyNames.length;
            const portfolioViewUrl = getPortfolioListPageUrl('portfolio', '?strategy_id=' + encodeURIComponent(portfolio._id || ''));
            const strategyItems = strategyNames.map(function (strategyName) {
                return `
                            <li class="flex items-center gap-2 border-b py-3 text-xs text-primaryBlack-725">
                                <div class="group relative" data-testid="special-text-tooltip">${escapeHtml(strategyName)}</div>
                            </li>
                        `;
            }).join('');

            return `
                        <div class="flex h-full w-full flex-col rounded-md border border-primaryBlack-400 bg-primaryBlack-0 pb-4">
                            <div class="flex items-center justify-between p-4">
                                <p class="text-lg font-medium capitalize leading-6">${escapeHtml(portfolio.name || 'Untitled Portfolio')}</p>
                                <div class="relative flex items-center justify-center">
                                    <button class="flex items-center justify-center rounded text-primaryBlack-750" type="button" aria-label="Portfolio menu">
                                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="h-5 w-5">
                                            <path stroke-linecap="round" stroke-linejoin="round" d="M12 6.75a.75.75 0 110-1.5.75.75 0 010 1.5zM12 12.75a.75.75 0 110-1.5.75.75 0 010 1.5zM12 18.75a.75.75 0 110-1.5.75.75 0 010 1.5z"></path>
                                        </svg>
                                    </button>
                                </div>
                            </div>
                            <div class="bg-primaryBlack-200 px-3 py-2 text-xs text-primaryBlack-725">
                                <p>Strategies: <span class="font-semibold">${strategyCount}</span></p>
                            </div>
                            <ul class="modern-scrollbar mb-4 flex max-h-44 flex-col gap-1.5 overflow-auto px-4 py-3">
                                ${strategyItems || '<li class="py-3 text-xs text-primaryBlack-725">No strategies added</li>'}
                            </ul>
                            <div class="mt-auto px-4">
                                <a class="portfolio-view-link"
                                    href="${portfolioViewUrl}">View</a>
                            </div>
                        </div>
                    `;
        }).join('');
    }

    function loadPortfolios() {
        if (!portfolioGrid) {
            return Promise.resolve();
        }

        portfolioGrid.innerHTML = '<div class="portfolio-grid-status md:col-span-4">Loading portfolios...</div>';

        return fetch(getPortfolioListApiUrl('portfolioList'))
            .then(function (response) {
                if (!response.ok) {
                    throw new Error('Failed to fetch portfolios');
                }
                return response.json();
            })
            .then(function (data) {
                state.portfolios = (data.portfolios || []).map(function (portfolio) {
                    return {
                        _id: portfolio._id,
                        name: portfolio.name || '',
                        strategy_ids: Array.isArray(portfolio.strategy_ids) ? portfolio.strategy_ids : [],
                        strategy_names: Array.isArray(portfolio.strategy_names) ? portfolio.strategy_names : []
                    };
                });
                renderPortfolioGrid();
            })
            .catch(function () {
                state.portfolios = [];
                portfolioGrid.innerHTML = '<div class="portfolio-grid-status md:col-span-4">Failed to load portfolios. Check backend server connection.</div>';
            });
    }

    function getFilteredStrategies() {
        const query = state.query.trim().toLowerCase();
        if (!query) {
            return state.strategies;
        }

        return state.strategies.filter((strategy) => strategy.name.toLowerCase().includes(query));
    }

    function updateCreateButton() {
        createButton.disabled = state.loading || !portfolioNameInput.value.trim() || state.selected.size === 0;
    }

    function portfolioNameExists(name) {
        const normalizedName = (name || '').trim().toLowerCase();
        if (!normalizedName) {
            return false;
        }

        return state.portfolios.some(function (portfolio) {
            return (portfolio.name || '').trim().toLowerCase() === normalizedName;
        });
    }

    function renderSelectedPill() {
        const count = state.selected.size;
        if (!count) {
            selectedPill.innerHTML = '';
            return;
        }

        selectedPill.innerHTML = `
                    <span class="portfolio-selection-pill">
                        ${count} ${count === 1 ? 'Strategy' : 'Strategies'} Selected
                        <button type="button" aria-label="Clear selected strategies" data-clear-selected>&times;</button>
                    </span>
                `;

        const clearButton = selectedPill.querySelector('[data-clear-selected]');
        if (clearButton) {
            clearButton.addEventListener('click', function () {
                state.selected.clear();
                render();
            });
        }
    }

    function renderList() {
        const filteredStrategies = getFilteredStrategies();
        selectAllLabel.textContent = `Select all (${filteredStrategies.length})`;

        if (state.loading) {
            strategyList.innerHTML = '<div class="portfolio-empty-state">Loading strategies...</div>';
            selectAllCheckbox.checked = false;
            selectAllCheckbox.indeterminate = false;
            return;
        }

        if (!filteredStrategies.length) {
            strategyList.innerHTML = '<div class="portfolio-empty-state">No strategies found.</div>';
            selectAllCheckbox.checked = false;
            selectAllCheckbox.indeterminate = false;
            return;
        }

        strategyList.innerHTML = filteredStrategies.map((strategy) => {
            const isSelected = state.selected.has(strategy._id);
            return `
                        <label class="portfolio-strategy-row ${isSelected ? 'is-selected' : ''}">
                            <input class="portfolio-checkbox portfolio-strategy-checkbox" type="checkbox" value="${escapeHtml(strategy._id)}" ${isSelected ? 'checked' : ''}>
                            <span class="portfolio-strategy-name">${escapeHtml(strategy.name)}</span>
                        </label>
                    `;
        }).join('');

        const totalStrategiesCount = state.strategies.length;
        selectAllCheckbox.checked = totalStrategiesCount > 0 && state.selected.size === totalStrategiesCount;
        selectAllCheckbox.indeterminate = state.selected.size > 0 && state.selected.size < totalStrategiesCount;

        strategyList.querySelectorAll('.portfolio-strategy-checkbox').forEach((checkbox) => {
            checkbox.addEventListener('change', function (event) {
                const { value, checked } = event.target;
                if (checked) {
                    state.selected.add(value);
                } else {
                    state.selected.delete(value);
                }
                render();
            });
        });
    }

    function render() {
        renderSelectedPill();
        renderList();
        updateCreateButton();
    }

    function resetModalState() {
        state.selected.clear();
        state.query = '';
        state.loading = false;
        portfolioNameInput.value = '';
        strategySearch.value = '';
        createButton.textContent = 'Create Portfolio';
        setStatus('');
        render();
    }

    function loadStrategies() {
        state.loading = true;
        setStatus('');
        render();

        return fetch(getPortfolioListApiUrl('strategyList'))
            .then(function (response) {
                if (!response.ok) {
                    throw new Error('Failed to fetch strategies');
                }
                return response.json();
            })
            .then(function (data) {
                state.strategies = (data.strategies || [])
                    .filter(function (strategy) {
                        return strategy && strategy._id && strategy.name;
                    })
                    .sort(function (first, second) {
                        return first.name.localeCompare(second.name);
                    });
                state.selected.forEach(function (strategyId) {
                    var exists = state.strategies.some(function (strategy) {
                        return strategy._id === strategyId;
                    });
                    if (!exists) {
                        state.selected.delete(strategyId);
                    }
                });
                if (!state.strategies.length) {
                    setStatus('No saved strategies available.', 'error');
                }
            })
            .catch(function () {
                state.strategies = [];
                setStatus('Failed to load strategies. Check backend server connection.', 'error');
            })
            .finally(function () {
                state.loading = false;
                render();
            });
    }

    function openModal() {
        modalOverlay.classList.add('is-open');
        document.body.classList.add('portfolio-modal-open');
        loadStrategies();
        window.setTimeout(() => portfolioNameInput.focus(), 0);
    }

    function closeModal() {
        modalOverlay.classList.remove('is-open');
        document.body.classList.remove('portfolio-modal-open');
        resetModalState();
    }

    openButton.addEventListener('click', openModal);

    closeButtons.forEach((button) => {
        button.addEventListener('click', closeModal);
    });

    modalOverlay.addEventListener('click', function (event) {
        if (event.target === modalOverlay) {
            closeModal();
        }
    });

    document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape' && modalOverlay.classList.contains('is-open')) {
            closeModal();
        }
    });

    strategySearch.addEventListener('input', function (event) {
        state.query = event.target.value;
        render();
    });

    selectAllCheckbox.addEventListener('change', function (event) {
        if (event.target.checked) {
            state.strategies.forEach(function (strategy) {
                state.selected.add(strategy._id);
            });
        } else {
            state.selected.clear();
        }
        render();
    });

    portfolioNameInput.addEventListener('input', function () {
        if (portfolioNameExists(portfolioNameInput.value)) {
            setStatus('Portfolio name already exists. Please use a different name.', 'error');
        } else {
            setStatus('');
        }
        updateCreateButton();
    });

    createButton.addEventListener('click', function () {
        if (createButton.disabled) {
            return;
        }

        if (portfolioNameExists(portfolioNameInput.value)) {
            setStatus('Portfolio name already exists. Please use a different name.', 'error');
            return;
        }

        const payload = {
            name: portfolioNameInput.value.trim(),
            strategy_ids: Array.from(state.selected)
        };

        state.loading = true;
        createButton.textContent = 'Saving...';
        setStatus('');
        updateCreateButton();

        fetch(getPortfolioListApiUrl('portfolioSave'), {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        })
            .then(function (response) {
                return response.json().catch(function () {
                    return {};
                }).then(function (data) {
                    if (!response.ok) {
                        var detail = data.detail || 'Failed to save portfolio.';
                        throw new Error(detail);
                    }
                    return data;
                });
            })
            .then(function (data) {
                setStatus('Portfolio created successfully.', 'success');
                state.selected.clear();
                portfolioNameInput.value = '';
                render();
                loadPortfolios();
                window.setTimeout(function () {
                    closeModal();
                }, 800);
                return data;
            })
            .catch(function (error) {
                setStatus(error.message || 'Failed to save portfolio.', 'error');
            })
            .finally(function () {
                state.loading = false;
                createButton.textContent = 'Create Portfolio';
                render();
            });
    });

    document.querySelectorAll('.portfolio-tab').forEach((tab) => {
        tab.addEventListener('click', function () {
            document.querySelectorAll('.portfolio-tab').forEach((item) => item.classList.remove('is-active'));
            tab.classList.add('is-active');
        });
    });

    if (portfolioSearchInput) {
        portfolioSearchInput.addEventListener('input', function (event) {
            state.portfolioQuery = event.target.value || '';
            renderPortfolioGrid();
        });
    }

    loadPortfolios();
    render();
})();
