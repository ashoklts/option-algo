function getPortfolioApiUrl(routeName, suffix) {
    if (typeof window.buildNamedApiUrl === 'function') {
        return window.buildNamedApiUrl(routeName, suffix);
    }
    var baseUrl = (window.APP_CONFIG && window.APP_CONFIG.algoApiBaseUrl) || window.APP_ALGO_API_BASE_URL || '';
    var routeMap = window.APP_API_ROUTES || {};
    var routePath = routeMap[routeName] || routeName || '';
    var normalizedRoute = String(routePath).replace(/\/+$/, '');
    var normalizedSuffix = String(suffix || '').replace(/^\/+/, '');
    return baseUrl.replace(/\/+$/, '') + '/' + (normalizedSuffix ? normalizedRoute + '/' + normalizedSuffix : normalizedRoute);
}

function getPortfolioPageUrl(routeName, queryString) {
    if (typeof window.buildNamedPageUrl === 'function') {
        return window.buildNamedPageUrl(routeName) + (queryString || '');
    }
    var routeMap = window.APP_PAGE_ROUTES || {};
    var routePath = routeMap[routeName] || routeName || '';
    var builder = typeof window.buildAppUrl === 'function' ? window.buildAppUrl : function (path) { return './' + String(path || '').replace(/^\/+/, ''); };
    return builder(routePath) + (queryString || '');
}

(function () {

    var params = new URLSearchParams(window.location.search);
    var portfolioId = params.get('strategy_id');
    var titleEl = document.getElementById('portfolio-page-title');
    var selectedCountEl = document.getElementById('portfolio-selected-count');
    var selectedTotalEl = document.getElementById('portfolio-selected-total');
    var rowsContainer = document.getElementById('portfolio-strategy-rows');
    var rowTemplate = document.getElementById('portfolio-strategy-row-template');
    var overallSelect = document.getElementById('overallSelect');
    var rowTemplateSource = rowTemplate ? rowTemplate.cloneNode(true) : null;
    var dteTab = document.querySelector('[data-testid="DTE-button"]');
    var weekdaysTab = document.querySelector('[data-testid="Weekdays-button"]');
    var budgetDaysTab = document.querySelector('[data-testid="Budget Days-button"]');
    var topFilter = document.getElementById('portfolio-top-filter');
    var topFilterButton = document.querySelector('[data-portfolio-filter-button]');
    var topFilterMenu = document.getElementById('portfolio-top-filter-menu');
    var qtyFilter = document.getElementById('portfolio-qty-filter');
    var qtyButton = document.querySelector('[data-portfolio-qty-button]');
    var qtyPopover = document.getElementById('portfolio-qty-popover');
    var qtySelect = document.getElementById('portfolio-qty-select');
    var qtyApply = document.getElementById('portfolio-qty-apply');
    var slippageFilter = document.getElementById('portfolio-slippage-filter');
    var slippageButton = document.querySelector('[data-portfolio-slippage-button]');
    var slippagePopover = document.getElementById('portfolio-slippage-popover');
    var slippageInput = document.getElementById('portfolio-slippage-input');
    var slippageApply = document.getElementById('portfolio-slippage-apply');
    var updatePortfolioBtn = document.querySelector('[data-testid="portfolio-update-btn"]');
    var saveAsNewPortfolioBtn = document.querySelector('[data-testid="portfolio-save-as-new-btn"]');
    var editPortfolioBtn = document.querySelector('[data-testid="portfolio-edit-btn"]');
    var editPortfolioModal = document.getElementById('portfolio-edit-modal');
    var editPortfolioClose = document.getElementById('portfolio-edit-close');
    var editPortfolioCancel = document.getElementById('portfolio-edit-cancel');
    var editPortfolioSave = document.getElementById('portfolio-edit-save');
    var editPortfolioName = document.getElementById('portfolio-edit-name');
    var editPortfolioSearch = document.getElementById('portfolio-edit-search');
    var editPortfolioList = document.getElementById('portfolio-edit-list');
    var editPortfolioSelectAll = document.getElementById('portfolio-edit-select-all');
    var editPortfolioSelectAllLabel = document.getElementById('portfolio-edit-select-all-label');
    var editPortfolioSelectedPill = document.getElementById('portfolio-edit-selected-pill');
    var currentFilterMode = 'Weekdays';
    var filterOptions = {
        DTE: ['0 DTE', '1 DTE', '2 DTE', '3 DTE', '4 DTE', '5 DTE', '6 DTE'],
        Weekdays: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
        'Budget Days': ['Event Day', '1 Day Before', '2 Days Before']
    };
    var selectedTopFilters = {
        DTE: ['0 DTE'],
        Weekdays: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
        'Budget Days': ['Event Day']
    };
    var loadedPortfolioName = '';
    var toastTimer = null;
    var portfolioEditStrategies = [];
    var portfolioEditSelectedIds = {};
    var portfolioEditSearchTerm = '';

    function getPortfolioQtyMultiplier() {
        return qtySelect ? parseInt(qtySelect.value || '1', 10) || 1 : 1;
    }

    function setPortfolioBacktestQtySnapshot(map) {
        window._portfolioBacktestQtySnapshot = Object.assign({}, map || {});
    }
    window.setPortfolioBacktestQtySnapshot = setPortfolioBacktestQtySnapshot;

    function updateQtyButtonLabel() {
        if (!qtyButton) {
            return;
        }
        var label = qtyButton.querySelector('span');
        if (label) {
            label.textContent = 'Qty Multiplier: ' + String(getPortfolioQtyMultiplier());
        }
    }

    function escapePortfolioEditHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function ensureSelectHasOption(select, value) {
        if (!select) {
            return;
        }
        var normalized = String(value);
        var exists = Array.prototype.some.call(select.options || [], function (option) {
            return String(option.value) === normalized;
        });
        if (!exists) {
            var option = document.createElement('option');
            option.value = normalized;
            option.textContent = normalized;
            select.appendChild(option);
        }
    }

    function getToastElement() {
        var toast = document.getElementById('portfolio-toast');
        if (toast) {
            return toast;
        }
        toast = document.createElement('div');
        toast.id = 'portfolio-toast';
        toast.className = 'portfolio-toast rounded border border-green-200 bg-green-50 px-4 py-3 text-sm font-medium text-green-700 shadow-lg';
        document.body.appendChild(toast);
        return toast;
    }

    function showToast(message, type) {
        var toast = getToastElement();
        toast.textContent = message;
        toast.className = 'portfolio-toast rounded px-4 py-3 text-sm font-medium shadow-lg';
        if (type === 'error') {
            toast.classList.add('border', 'border-red-200', 'bg-red-50', 'text-red-700');
        } else {
            toast.classList.add('border', 'border-green-200', 'bg-green-50', 'text-green-700');
        }
        window.clearTimeout(toastTimer);
        requestAnimationFrame(function () {
            toast.classList.add('is-visible');
        });
        toastTimer = window.setTimeout(function () {
            toast.classList.remove('is-visible');
        }, 2200);
    }

    function getRowBaseQty(row) {
        var stored = row ? parseInt(row.getAttribute('data-strategy-qty-base') || '0', 10) : 0;
        if (stored > 0) {
            return stored;
        }
        var rowQtySelect = getRowQtySelect(row);
        return rowQtySelect ? parseInt(rowQtySelect.value || '1', 10) || 1 : 1;
    }
    window.getRowBaseQty = getRowBaseQty;
    window.getPortfolioQtyMultiplier = getPortfolioQtyMultiplier;

    function setRowBaseQty(row, qty) {
        if (!row) {
            return;
        }
        row.setAttribute('data-strategy-qty-base', String(Math.max(1, parseInt(qty || '1', 10) || 1)));
    }

    function updateRowEffectiveQty(row) {
        var rowQtySelect = getRowQtySelect(row);
        if (!rowQtySelect) {
            return;
        }
        var effectiveQty = getRowBaseQty(row) * getPortfolioQtyMultiplier();
        ensureSelectHasOption(rowQtySelect, effectiveQty);
        rowQtySelect.value = String(effectiveQty);
    }

    function updateAllRowEffectiveQty() {
        rowsContainer.querySelectorAll('[data-strategy-id]').forEach(function (row) {
            updateRowEffectiveQty(row);
        });
    }

    function removePortfolioStrategyRow(row) {
        if (!row || !rowsContainer || !rowsContainer.contains(row)) {
            return;
        }
        var strategyId = String(row.getAttribute('data-strategy-id') || '');
        row.remove();
        if (strategyId && window._portfolioStrategyMap) {
            delete window._portfolioStrategyMap[strategyId];
        }
        updateSelectionSummary();
        if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
            window._renderPortfolioBacktestReports(window._portfolioResult);
        }
    }

    function handleUpdatePortfolioClick(event) {
        if (event) {
            event.preventDefault();
            event.stopPropagation();
        }
        console.log('portfolio-update-btn clicked');
        if (!portfolioId) {
            showToast('Portfolio ID not found in URL.', 'error');
            return;
        }
        if (!rowsContainer) {
            showToast('Portfolio rows are not ready yet.', 'error');
            return;
        }
        savePortfolioState({ includePortfolioId: true })
            .then(function () {
                showToast('Portfolio updated successfully.');
            })
            .catch(function (error) {
                showToast(error.message || 'Failed to update portfolio', 'error');
            });
    }
    window._portfolioHandleUpdateClick = handleUpdatePortfolioClick;

    function bindPortfolioActionButtons() {
        if (updatePortfolioBtn && !updatePortfolioBtn.dataset.boundPortfolioUpdate) {
            updatePortfolioBtn.dataset.boundPortfolioUpdate = 'true';
            updatePortfolioBtn.addEventListener('click', handleUpdatePortfolioClick);
        }

        if (editPortfolioBtn && !editPortfolioBtn.dataset.boundPortfolioEdit) {
            editPortfolioBtn.dataset.boundPortfolioEdit = 'true';
            editPortfolioBtn.addEventListener('click', function (event) {
                event.preventDefault();
                event.stopPropagation();
                openPortfolioEditModal();
            });
        }

        if (saveAsNewPortfolioBtn && !saveAsNewPortfolioBtn.dataset.boundPortfolioSaveAsNew) {
            saveAsNewPortfolioBtn.dataset.boundPortfolioSaveAsNew = 'true';
            saveAsNewPortfolioBtn.addEventListener('click', function (event) {
                event.preventDefault();
                var newName = window.prompt('Enter new portfolio name', (loadedPortfolioName || 'Portfolio') + ' Copy');
                if (!newName) {
                    return;
                }
                savePortfolioState({ includePortfolioId: false, name: newName.trim() })
                    .then(function (data) {
                        loadedPortfolioName = data.name || newName.trim();
                        showToast('Portfolio saved as new successfully.');
                    })
                    .catch(function (error) {
                        showToast(error.message || 'Failed to save new portfolio', 'error');
                    });
            });
        }
    }

    bindPortfolioActionButtons();

    if (!document.documentElement.dataset.boundPortfolioUpdateDelegate) {
        document.documentElement.dataset.boundPortfolioUpdateDelegate = 'true';
        document.addEventListener('click', function (event) {
            var target = event.target && event.target.closest
                ? event.target.closest('[data-testid="portfolio-update-btn"]')
                : null;
            if (!target) {
                return;
            }
            handleUpdatePortfolioClick(event);
        }, true);
    }

    if (!document.documentElement.dataset.boundPortfolioEditDelegate) {
        document.documentElement.dataset.boundPortfolioEditDelegate = 'true';
        document.addEventListener('click', function (event) {
            var target = event.target && event.target.closest
                ? event.target.closest('[data-testid="portfolio-edit-btn"]')
                : null;
            if (!target) {
                return;
            }
            event.preventDefault();
            event.stopPropagation();
            openPortfolioEditModal();
        }, true);
    }

    if (!portfolioId || !titleEl || !rowsContainer || !rowTemplate || !rowTemplateSource) {
        return;
    }

    document.querySelectorAll('[draggable="true"]').forEach(function (row) {
        if (row !== rowTemplate) {
            row.remove();
        }
    });
    rowTemplate.remove();
    rowsContainer.innerHTML = '';

    function setTitle(name) {
        var backButton = titleEl.querySelector('button');
        titleEl.textContent = '';
        if (backButton) {
            titleEl.appendChild(backButton);
        }
        titleEl.appendChild(document.createTextNode(name || 'Portfolio'));
    }

    function setSummary(count) {
        selectedCountEl.textContent = String(count);
        selectedTotalEl.textContent = '/' + String(count) + ' selected';
        overallSelect.checked = count > 0;
    }

    window.setPortfolioPageTitle = setTitle;
    window.setPortfolioSummary = setSummary;

    function updateSelectionSummary() {
        var rowCheckboxes = rowsContainer.querySelectorAll('input[id^="portfolio-strategy-"]');
        var totalCount = rowCheckboxes.length;
        var selectedCount = 0;

        rowCheckboxes.forEach(function (checkbox) {
            if (checkbox.checked) {
                selectedCount += 1;
            }
        });

        selectedCountEl.textContent = String(selectedCount);
        selectedTotalEl.textContent = '/' + String(totalCount) + ' selected';
        overallSelect.checked = totalCount > 0 && selectedCount === totalCount;
        overallSelect.indeterminate = selectedCount > 0 && selectedCount < totalCount;
    }

    function getCurrentPortfolioStrategyIds() {
        var ids = [];
        rowsContainer.querySelectorAll('[data-strategy-id]').forEach(function (row) {
            var strategyId = String(row.getAttribute('data-strategy-id') || '');
            if (strategyId) {
                ids.push(strategyId);
            }
        });
        return ids;
    }

    function syncPortfolioEditSelectedSummary(visibleStrategies) {
        var selectedCount = Object.keys(portfolioEditSelectedIds).filter(function (id) {
            return !!portfolioEditSelectedIds[id];
        }).length;
        if (editPortfolioSelectedPill) {
            editPortfolioSelectedPill.textContent = selectedCount + ' Strategies Selected';
        }
        if (editPortfolioSelectAll && editPortfolioSelectAllLabel) {
            var visibleIds = (visibleStrategies || []).map(function (item) { return String(item._id || ''); }).filter(Boolean);
            var checkedVisibleCount = visibleIds.filter(function (id) { return !!portfolioEditSelectedIds[id]; }).length;
            editPortfolioSelectAll.checked = !!visibleIds.length && checkedVisibleCount === visibleIds.length;
            editPortfolioSelectAll.indeterminate = checkedVisibleCount > 0 && checkedVisibleCount < visibleIds.length;
            editPortfolioSelectAllLabel.textContent = 'Select all (' + (visibleStrategies || []).length + ')';
        }
    }

    function renderPortfolioEditStrategyList() {
        if (!editPortfolioList) {
            return;
        }
        var term = String(portfolioEditSearchTerm || '').trim().toLowerCase();
        var visibleStrategies = portfolioEditStrategies.filter(function (strategy) {
            return !term || String(strategy.name || '').toLowerCase().indexOf(term) !== -1;
        });

        syncPortfolioEditSelectedSummary(visibleStrategies);

        if (!visibleStrategies.length) {
            editPortfolioList.innerHTML = '<div class="portfolio-edit-empty">No strategies found.</div>';
            return;
        }

        editPortfolioList.innerHTML = visibleStrategies.map(function (strategy) {
            var strategyId = String(strategy._id || '');
            var checked = !!portfolioEditSelectedIds[strategyId];
            return '<label class="portfolio-edit-list-row">'
                + '<input type="checkbox" data-portfolio-edit-strategy="' + escapePortfolioEditHtml(strategyId) + '" ' + (checked ? 'checked' : '') + '>'
                + '<span>' + escapePortfolioEditHtml(strategy.name || 'Untitled Strategy') + '</span>'
                + '</label>';
        }).join('');

        editPortfolioList.querySelectorAll('input[data-portfolio-edit-strategy]').forEach(function (input) {
            input.addEventListener('change', function () {
                var strategyId = String(input.getAttribute('data-portfolio-edit-strategy') || '');
                if (!strategyId) {
                    return;
                }
                portfolioEditSelectedIds[strategyId] = !!input.checked;
                renderPortfolioEditStrategyList();
            });
        });
    }

    function loadPortfolioEditStrategies() {
        return fetch(getPortfolioApiUrl('strategyList'), {
            headers: { Accept: 'application/json' }
        }).then(function (response) {
            return response.json().catch(function () {
                return {};
            }).then(function (data) {
                if (!response.ok) {
                    throw new Error(data.detail || 'Failed to load strategies');
                }
                return data;
            });
        }).then(function (data) {
            portfolioEditStrategies = Array.isArray(data.strategies) ? data.strategies.slice() : [];
            portfolioEditStrategies.sort(function (a, b) {
                return String(a && a.name || '').localeCompare(String(b && b.name || ''));
            });
        });
    }

    function openPortfolioEditModal() {
        if (!editPortfolioModal || !editPortfolioName) {
            return;
        }
        editPortfolioName.value = loadedPortfolioName || '';
        portfolioEditSearchTerm = '';
        if (editPortfolioSearch) {
            editPortfolioSearch.value = '';
        }
        portfolioEditSelectedIds = {};
        getCurrentPortfolioStrategyIds().forEach(function (id) {
            portfolioEditSelectedIds[id] = true;
        });
        loadPortfolioEditStrategies()
            .then(function () {
                renderPortfolioEditStrategyList();
                editPortfolioModal.hidden = false;
            })
            .catch(function (error) {
                showToast(error.message || 'Failed to load strategies', 'error');
            });
    }

    function closePortfolioEditModal() {
        if (editPortfolioModal) {
            editPortfolioModal.hidden = true;
        }
    }

    function updateWeekdayState(row, weekdays) {
        var activeDays = Array.isArray(weekdays) && weekdays.length ? weekdays : ['M', 'T', 'W', 'Th', 'F'];
        var labels = row.querySelectorAll('label[id^="weekly_filters--label__"]');
        labels.forEach(function (label) {
            var dayText = label.textContent.trim();
            var isActive = activeDays.indexOf(dayText) !== -1;
            label.classList.toggle('!border-secondaryBlue-500', isActive);
            label.classList.toggle('!bg-secondaryBlue-50', isActive);
            label.classList.toggle('!text-secondaryBlue-500', isActive);
            var input = label.querySelector('input');
            if (input) {
                input.checked = isActive;
            }
        });
    }

    function attachWeekdayToggle(row) {
        var labels = row.querySelectorAll('label[id^="weekly_filters--label__"]');
        labels.forEach(function (label) {
            label.addEventListener('click', function (event) {
                event.preventDefault();
                var input = label.querySelector('input');
                var willBeChecked = !(input && input.checked);

                label.classList.toggle('!border-secondaryBlue-500', willBeChecked);
                label.classList.toggle('!bg-secondaryBlue-50', willBeChecked);
                label.classList.toggle('!text-secondaryBlue-500', willBeChecked);

                if (input) {
                    input.checked = willBeChecked;
                }
                syncReportFilterMode('Weekdays');
                if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                    window._renderPortfolioBacktestReports(window._portfolioResult);
                }
            });
        });
    }

    function getRowQtySelect(row) {
        return row.querySelectorAll('.col-span-4.flex.flex-col.items-start.justify-center.gap-2.md\\:items-center.md\\:col-span-1 select, .col-span-4.flex.flex-col.items-start.justify-center.gap-2.md\\:items-center.md\\:col-span-1 .relative select, .col-span-4.flex.flex-col.items-start.justify-center.gap-2.md\\:items-center.md\\:col-span-1  select')[0];
    }

    function getRowSlippageInput(row) {
        return row.querySelectorAll('input[type="number"]')[0];
    }

    function normalizeDteValues(values) {
        if (!Array.isArray(values)) {
            return [];
        }
        return values.map(function (value) {
            if (typeof value === 'number') {
                return value;
            }
            var parsed = parseInt(String(value).replace(/\s*DTE\s*$/i, ''), 10);
            return Number.isFinite(parsed) ? parsed : null;
        }).filter(function (value) {
            return value !== null;
        });
    }

    function formatDteValues(values) {
        return normalizeDteValues(values).map(function (value) {
            return String(value) + ' DTE';
        });
    }

    function setRowWeekdays(row, selectedDays) {
        var mappedDays = (selectedDays || []).map(function (day) {
            return day === 'Monday' ? 'M'
                : day === 'Tuesday' ? 'T'
                    : day === 'Wednesday' ? 'W'
                        : day === 'Thursday' ? 'Th'
                            : day === 'Friday' ? 'F'
                                : day === 'Saturday' ? 'Sa'
                                    : day === 'Sunday' ? 'Su'
                                        : day;
        });
        updateWeekdayState(row, mappedDays);
    }

    function ensureDteControl(row) {
        var weekdaysBlock = row.querySelectorAll('.col-span-5.flex.w-full.flex-col.items-start.justify-end.gap-2.md\\:mt-auto.md\\:items-center.md\\:justify-center.md\\:col-span-3')[0];
        if (!weekdaysBlock) {
            return null;
        }

        var existing = weekdaysBlock.querySelector('[data-row-dte-wrap]');
        if (existing) {
            return existing;
        }

        var wrapper = document.createElement('div');
        wrapper.setAttribute('data-row-dte-wrap', 'true');
        wrapper.className = 'hidden';

        var dteOptions = ['0 DTE', '1 DTE', '2 DTE', '3 DTE', '4 DTE', '5 DTE', '6 DTE'];
        var selectedDtes = [];

        // Build multi-select dropdown
        var msContainer = document.createElement('div');
        msContainer.className = 'relative';
        msContainer.style.cssText = 'min-width:100px;';

        // Trigger button
        var triggerBtn = document.createElement('button');
        triggerBtn.type = 'button';
        triggerBtn.setAttribute('data-dte-trigger', 'true');
        triggerBtn.className = 'flex items-center justify-between gap-1 w-full cursor-pointer rounded border border-primaryBlack-500 bg-primaryBlack-0 dark:bg-primaryBlack-350 px-2 py-1.5 text-xs text-primaryBlack-750';

        var triggerLabel = document.createElement('span');
        triggerLabel.setAttribute('data-dte-label', 'true');
        triggerLabel.textContent = 'DTE';
        triggerBtn.appendChild(triggerLabel);

        var chevron = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        chevron.setAttribute('fill', 'none');
        chevron.setAttribute('viewBox', '0 0 24 24');
        chevron.setAttribute('stroke', 'currentColor');
        chevron.setAttribute('class', 'h-3 w-3 flex-shrink-0 text-primaryBlack-500');
        chevron.innerHTML = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/>';
        triggerBtn.appendChild(chevron);

        // Dropdown panel
        var panel = document.createElement('div');
        panel.setAttribute('data-dte-panel', 'true');
        panel.className = 'hidden absolute z-50 mt-1 rounded border border-primaryBlack-400 bg-primaryBlack-0 dark:bg-primaryBlack-350 shadow-lg py-1';
        panel.style.cssText = 'min-width:110px; top:100%; left:0;';

        function updateTriggerLabel() {
            triggerLabel.textContent = selectedDtes.length === 0
                ? 'DTE'
                : selectedDtes.length === 1
                    ? selectedDtes[0]
                    : selectedDtes.length + ' Selected';
        }

        function refreshOptions() {
            var selectAllItem = panel.querySelector('[data-dte-select-all]');
            var allSelected = selectedDtes.length === dteOptions.length;
            if (selectAllItem) {
                var selectAllTick = selectAllItem.querySelector('[data-dte-checkbox]');
                if (allSelected) {
                    selectAllItem.classList.add('bg-blue-50', 'text-blue-600', 'dark:bg-blue-900/30');
                    selectAllItem.classList.remove('text-primaryBlack-750');
                    if (selectAllTick) selectAllTick.checked = true;
                } else {
                    selectAllItem.classList.remove('bg-blue-50', 'text-blue-600', 'dark:bg-blue-900/30');
                    selectAllItem.classList.add('text-primaryBlack-750');
                    if (selectAllTick) selectAllTick.checked = false;
                }
            }

            panel.querySelectorAll('[data-dte-opt]').forEach(function (item) {
                var val = item.getAttribute('data-dte-opt');
                var tick = item.querySelector('[data-dte-checkbox]');
                var isSelected = selectedDtes.indexOf(val) !== -1;
                if (isSelected) {
                    item.classList.add('bg-blue-50', 'text-blue-600', 'dark:bg-blue-900/30');
                    item.classList.remove('text-primaryBlack-750');
                    if (tick) tick.checked = true;
                } else {
                    item.classList.remove('bg-blue-50', 'text-blue-600', 'dark:bg-blue-900/30');
                    item.classList.add('text-primaryBlack-750');
                    if (tick) tick.checked = false;
                }
            });
        }

        var selectAllItem = document.createElement('div');
        selectAllItem.setAttribute('data-dte-select-all', 'true');
        selectAllItem.className = 'flex items-center gap-2 px-3 py-1.5 text-xs cursor-pointer hover:bg-primaryBlack-100 dark:hover:bg-primaryBlack-400 text-primaryBlack-750 border-b border-primaryBlack-300';

        var selectAllTick = document.createElement('input');
        selectAllTick.type = 'checkbox';
        selectAllTick.setAttribute('data-dte-checkbox', 'true');
        selectAllTick.className = 'h-4 w-4 cursor-pointer rounded border border-secondaryBlue-500 text-secondaryBlue-500 focus:ring-secondaryBlue-500 pointer-events-none';

        var selectAllLabel = document.createElement('span');
        selectAllLabel.textContent = 'Select All';

        selectAllItem.appendChild(selectAllTick);
        selectAllItem.appendChild(selectAllLabel);
        selectAllItem.addEventListener('mousedown', function (e) {
            e.preventDefault();
            if (selectedDtes.length === dteOptions.length) {
                selectedDtes = [];
            } else {
                selectedDtes = dteOptions.slice();
            }
            refreshOptions();
            updateTriggerLabel();
            syncReportFilterMode('DTE');
            if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                window._renderPortfolioBacktestReports(window._portfolioResult);
            }
        });
        panel.appendChild(selectAllItem);

        dteOptions.forEach(function (opt) {
            var item = document.createElement('div');
            item.setAttribute('data-dte-opt', opt);
            item.className = 'flex items-center gap-2 px-3 py-1.5 text-xs cursor-pointer hover:bg-primaryBlack-100 dark:hover:bg-primaryBlack-400 text-primaryBlack-750';

            var tick = document.createElement('input');
            tick.type = 'checkbox';
            tick.setAttribute('data-dte-checkbox', 'true');
            tick.className = 'h-4 w-4 cursor-pointer rounded border border-secondaryBlue-500 text-secondaryBlue-500 focus:ring-secondaryBlue-500 pointer-events-none';

            var label = document.createElement('span');
            label.textContent = opt;

            item.appendChild(tick);
            item.appendChild(label);

            if (selectedDtes.indexOf(opt) !== -1) {
                item.classList.add('bg-blue-50', 'text-blue-600', 'dark:bg-blue-900/30');
                item.classList.remove('text-primaryBlack-750');
            }

            item.addEventListener('mousedown', function (e) {
                e.preventDefault();
                var idx = selectedDtes.indexOf(opt);
                if (idx === -1) {
                    selectedDtes.push(opt);
                } else {
                    selectedDtes.splice(idx, 1);
                }
                refreshOptions();
                updateTriggerLabel();
                syncReportFilterMode('DTE');
                if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                    window._renderPortfolioBacktestReports(window._portfolioResult);
                }
            });

            panel.appendChild(item);
        });

        refreshOptions();

        triggerBtn.addEventListener('click', function (e) {
            e.stopPropagation();
            syncReportFilterMode('DTE');
            var isOpen = !panel.classList.contains('hidden');
            // Close all other open DTE panels first
            document.querySelectorAll('[data-dte-panel]').forEach(function (p) {
                p.classList.add('hidden');
            });
            if (!isOpen) {
                panel.classList.remove('hidden');
            }
        });

        panel.addEventListener('click', function (e) {
            e.stopPropagation();
        });

        document.addEventListener('click', function () {
            panel.classList.add('hidden');
        });

        msContainer.appendChild(triggerBtn);
        msContainer.appendChild(panel);

        // Expose selected values via dataset for external reads
        msContainer.getSelectedDtes = function () { return selectedDtes.slice(); };
        msContainer.setSelectedDtes = function (values) {
            selectedDtes = Array.isArray(values) ? values.slice() : [];
            refreshOptions();
            updateTriggerLabel();
        };

        wrapper.appendChild(msContainer);
        weekdaysBlock.appendChild(wrapper);
        return wrapper;
    }

    function applyExecutionModeToRow(row) {
        var weekdaysBlock = row.querySelectorAll('.col-span-5.flex.w-full.flex-col.items-start.justify-end.gap-2.md\\:mt-auto.md\\:items-center.md\\:justify-center.md\\:col-span-3')[0];
        if (!weekdaysBlock) {
            return;
        }

        var weekdaysLabel = weekdaysBlock.querySelector('p');
        var weekdaysPills = weekdaysBlock.querySelector('.flex.w-full.flex-row.md\\:w-max.gap-2');
        var dteWrap = ensureDteControl(row);

        if (currentFilterMode === 'DTE') {
            weekdaysBlock.classList.add('portfolio-row-mode-dte');
            if (weekdaysLabel) {
                weekdaysLabel.classList.add('hidden');
            }
            if (weekdaysPills) {
                weekdaysPills.classList.add('hidden');
            }
            if (dteWrap) {
                dteWrap.classList.remove('hidden');
            }
        } else {
            weekdaysBlock.classList.remove('portfolio-row-mode-dte');
            if (weekdaysLabel) {
                weekdaysLabel.classList.remove('hidden');
                weekdaysLabel.textContent = 'Execution Weekdays:';
            }
            if (weekdaysPills) {
                weekdaysPills.classList.remove('hidden');
            }
            if (dteWrap) {
                dteWrap.classList.add('hidden');
            }
        }
    }

    function applyExecutionModeToAllRows() {
        rowsContainer.querySelectorAll('[draggable="true"]').forEach(function (row) {
            applyExecutionModeToRow(row);
        });
    }

    function setActiveTopFilter(tabButton) {
        [dteTab, weekdaysTab, budgetDaysTab].forEach(function (tab) {
            if (!tab) {
                return;
            }
            tab.classList.remove('!bg-secondaryBlue-50', '!font-bold', '!text-secondaryBlue-500');
        });
        if (tabButton) {
            tabButton.classList.add('!bg-secondaryBlue-50', '!font-bold', '!text-secondaryBlue-500');
        }
    }

    function syncReportFilterMode(mode) {
        if (mode !== 'DTE' && mode !== 'Weekdays') {
            return;
        }
        currentFilterMode = mode;
        window._portfolioActiveFilterMode = mode;
        setActiveTopFilter(mode === 'DTE' ? dteTab : weekdaysTab);
        updateTopFilterButtonLabel();
    }

    function updateTopFilterButtonLabel() {
        if (!topFilterButton) {
            return;
        }
        var values = selectedTopFilters[currentFilterMode] || [];
        var label = values.length ? values.length + ' Selected' : currentFilterMode;
        topFilterButton.querySelector('span').textContent = label;
    }

    function renderTopFilterMenu(mode) {
        if (!topFilterMenu || !topFilterButton) {
            return;
        }

        currentFilterMode = mode;
        window._portfolioActiveFilterMode = mode;
        var options = filterOptions[mode] || [];
        var selectedValues = selectedTopFilters[mode] || [];
        updateTopFilterButtonLabel();
        var optionsMarkup = [
            `
                        <label class="portfolio-top-filter-option">
                            <input type="checkbox" data-top-filter-select-all ${selectedValues.length === options.length && options.length ? 'checked' : ''}>
                            <span>Select All</span>
                        </label>
                    `
        ].concat(options.map(function (option) {
            var checked = selectedValues.indexOf(option) !== -1;
            return `
                        <label class="portfolio-top-filter-option ${checked ? 'is-selected' : ''}">
                            <input type="checkbox" data-top-filter-option value="${option}" ${checked ? 'checked' : ''}>
                            <span>${option}</span>
                        </label>
                    `;
        })).join('');

        topFilterMenu.innerHTML = mode === 'DTE' || mode === 'Weekdays'
            ? `
                        <div class="portfolio-top-filter-list">
                            ${optionsMarkup}
                        </div>
                        <div class="portfolio-top-filter-actions">
                            <button type="button" class="portfolio-top-filter-apply" data-top-filter-apply>Apply</button>
                        </div>
                    `
            : optionsMarkup;

        var selectAll = topFilterMenu.querySelector('[data-top-filter-select-all]');
        var optionInputs = topFilterMenu.querySelectorAll('[data-top-filter-option]');
        var applyButton = topFilterMenu.querySelector('[data-top-filter-apply]');

        if (selectAll) {
            selectAll.addEventListener('change', function () {
                if (selectAll.checked) {
                    selectedTopFilters[currentFilterMode] = options.slice();
                } else {
                    selectedTopFilters[currentFilterMode] = [];
                }
                renderTopFilterMenu(currentFilterMode);
            });
        }

        optionInputs.forEach(function (input) {
            input.addEventListener('change', function () {
                var values = selectedTopFilters[currentFilterMode] || [];
                if (input.checked) {
                    if (values.indexOf(input.value) === -1) {
                        values.push(input.value);
                    }
                } else {
                    values = values.filter(function (item) {
                        return item !== input.value;
                    });
                }
                selectedTopFilters[currentFilterMode] = values;
                renderTopFilterMenu(currentFilterMode);
            });
        });

        if (applyButton) {
            applyButton.addEventListener('click', function () {
                if (currentFilterMode === 'DTE') {
                    var selectedDtes = (selectedTopFilters.DTE || []).slice();
                    rowsContainer.querySelectorAll('[data-row-dte-wrap]').forEach(function (wrap) {
                        var control = wrap.firstElementChild;
                        if (control && typeof control.setSelectedDtes === 'function') {
                            control.setSelectedDtes(selectedDtes);
                        }
                    });
                    if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                        window._renderPortfolioBacktestReports(window._portfolioResult);
                    }
                }

                if (currentFilterMode === 'Weekdays') {
                    var selectedWeekdays = (selectedTopFilters.Weekdays || []).slice();
                    rowsContainer.querySelectorAll('[draggable="true"]').forEach(function (row) {
                        setRowWeekdays(row, selectedWeekdays);
                    });
                    if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                        window._renderPortfolioBacktestReports(window._portfolioResult);
                    }
                }

                toggleTopFilterMenu(false);
            });
        }
        applyExecutionModeToAllRows();
    }

    function toggleTopFilterMenu(show) {
        if (!topFilterMenu || !topFilterButton) {
            return;
        }

        var shouldShow = typeof show === 'boolean' ? show : topFilterMenu.hidden;
        topFilterMenu.hidden = !shouldShow;
        topFilterButton.setAttribute('aria-expanded', shouldShow ? 'true' : 'false');
    }

    function createRow(strategy, index) {
        var row = rowTemplateSource.cloneNode(true);
        row.removeAttribute('id');
        row.setAttribute('data-strategy-id', strategy && strategy._id ? String(strategy._id) : '');

        var checkbox = row.querySelector('input[type="checkbox"]');
        var checkboxLabel = checkbox ? row.querySelector('label[for="' + checkbox.id + '"]') : null;
        var rowName = row.querySelector('[data-testid="special-text-tooltip"]');
        var detailSpans = row.querySelectorAll('.text-xs.font-normal.text-primaryBlack-700');
        var rowActionButtons = row.querySelectorAll('button');
        var editButton = Array.prototype.find.call(rowActionButtons, function (button) {
            return button.querySelector && button.querySelector('svg.size-5') && !button.querySelector('.lucide-trash2, .lucide-trash-2');
        }) || null;
        var deleteButtons = Array.prototype.filter.call(rowActionButtons, function (button) {
            if (button === editButton || !button.querySelector) {
                return false;
            }
            if (button.querySelector('.lucide-trash2, .lucide-trash-2')) {
                return true;
            }
            var labelSpan = button.querySelector('span');
            return !!(labelSpan && labelSpan.textContent && labelSpan.textContent.trim() === 'Delete');
        });
        var qtySelect = row.querySelector('select');
        var slippageInput = row.querySelector('input[type="number"]');
        var rowQtySelect = getRowQtySelect(row);

        if (checkbox) {
            var checkboxId = 'portfolio-strategy-' + index;
            checkbox.id = checkboxId;
            checkbox.checked = strategy.checked !== false;
            if (checkboxLabel) {
                checkboxLabel.setAttribute('for', checkboxId);
            }
            checkbox.addEventListener('change', function () {
                updateSelectionSummary();
                if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                    window._renderPortfolioBacktestReports(window._portfolioResult);
                }
            });
        }

        if (rowName) {
            rowName.textContent = strategy.name || 'Untitled Strategy';
        }

        if (detailSpans.length >= 2) {
            detailSpans[0].textContent = strategy.underlying || 'NIFTY';
            detailSpans[1].textContent = strategy.product || 'INTRADAY';
        }

        if (editButton) {
            editButton.type = 'button';
            editButton.addEventListener('click', function (event) {
                event.preventDefault();
                event.stopPropagation();
                var strategyId = strategy && strategy._id ? String(strategy._id) : '';
                if (!strategyId) {
                    return;
                }
                window.open(getPortfolioPageUrl('strategyBuilder', '?strategy_id=' + encodeURIComponent(strategyId)), '_blank');
            });
        }

        deleteButtons.forEach(function (deleteButton) {
            deleteButton.type = 'button';
            deleteButton.addEventListener('click', function (event) {
                event.preventDefault();
                event.stopPropagation();
                var strategyName = strategy && strategy.name ? String(strategy.name) : 'this strategy';
                if (!window.confirm('Remove "' + strategyName + '" from this portfolio permanently?')) {
                    return;
                }
                var strategyId = strategy && strategy._id ? String(strategy._id) : '';
                savePortfolioState({ includePortfolioId: true, excludeStrategyId: strategyId })
                    .then(function () {
                        removePortfolioStrategyRow(row);
                        showToast('Strategy removed from portfolio successfully.');
                    })
                    .catch(function (error) {
                        showToast(error.message || 'Failed to remove strategy from portfolio', 'error');
                    });
            });
        });

        if (rowQtySelect) {
            setRowBaseQty(row, strategy.qty_multiplier || 1);
            updateRowEffectiveQty(row);
            rowQtySelect.addEventListener('change', function () {
                var multiplier = getPortfolioQtyMultiplier();
                var selectedQty = parseInt(rowQtySelect.value || '1', 10) || 1;
                var baseQty = multiplier > 1 && selectedQty % multiplier === 0
                    ? selectedQty / multiplier
                    : selectedQty;
                setRowBaseQty(row, baseQty);
                updateRowEffectiveQty(row);
                if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                    window._renderPortfolioBacktestReports(window._portfolioResult);
                }
            });
        }

        if (slippageInput) {
            slippageInput.value = String(strategy.slippage || 0);
            slippageInput.addEventListener('input', function () {
                if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                    window._renderPortfolioBacktestReports(window._portfolioResult);
                }
            });
            slippageInput.addEventListener('change', function () {
                if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                    window._renderPortfolioBacktestReports(window._portfolioResult);
                }
            });
        }

        updateWeekdayState(row, strategy.weekdays);
        attachWeekdayToggle(row);
        applyExecutionModeToRow(row);
        var dteWrap = ensureDteControl(row);
        if (dteWrap && dteWrap.firstElementChild && typeof dteWrap.firstElementChild.setSelectedDtes === 'function') {
            dteWrap.firstElementChild.setSelectedDtes(formatDteValues(strategy.dte && strategy.dte.length ? strategy.dte : [0]));
        }
        return row;
    }

    function renderPortfolio(data) {
        var strategies = Array.isArray(data.strategies) ? data.strategies : [];
        loadedPortfolioName = data.name || 'Portfolio';
        if (qtySelect) {
            qtySelect.value = String(data.qty_multiplier || 1);
        }
        updateQtyButtonLabel();
        currentFilterMode = data.is_weekdays ? 'Weekdays' : 'DTE';
        window._portfolioActiveFilterMode = currentFilterMode;
        window._portfolioStrategyMap = {};
        strategies.forEach(function (strategy) {
            if (strategy && strategy._id) {
                window._portfolioStrategyMap[String(strategy._id)] = strategy.name || 'Untitled Strategy';
            }
        });
        setTitle(data.name || 'Portfolio');
        setSummary(strategies.length);
        rowsContainer.innerHTML = '';
        strategies.forEach(function (strategy, index) {
            rowsContainer.appendChild(createRow(strategy, index));
        });
        window._portfolioBacktestQtySnapshot = null;
        updateSelectionSummary();
        setActiveTopFilter(currentFilterMode === 'DTE' ? dteTab : weekdaysTab);
        renderTopFilterMenu(currentFilterMode);
        if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
            window._renderPortfolioBacktestReports(window._portfolioResult);
        }
    }

    function loadPortfolioDetails() {
        return fetch(getPortfolioApiUrl('portfolioById', encodeURIComponent(portfolioId)))
            .then(function (response) {
                return response.json().catch(function () {
                    return {};
                }).then(function (data) {
                    if (!response.ok) {
                        throw new Error(data.detail || 'Failed to load portfolio');
                    }
                    return data;
                });
            })
            .then(function (data) {
                renderPortfolio(data);
                return data;
            });
    }

    function collectPortfolioStrategiesState(excludedStrategyId) {
        var strategies = [];
        rowsContainer.querySelectorAll('[data-strategy-id]').forEach(function (row) {
            var strategyId = String(row.getAttribute('data-strategy-id') || '');
            if (!strategyId || (excludedStrategyId && strategyId === String(excludedStrategyId))) {
                return;
            }

            var checkbox = row.querySelector('input[id^="portfolio-strategy-"]');
            var qtySelect = getRowQtySelect(row);
            var slippageInput = getRowSlippageInput(row);
            var dteWrap = row.querySelector('[data-row-dte-wrap]');
            var dteControl = dteWrap ? dteWrap.firstElementChild : null;
            var weekdayInputs = row.querySelectorAll('label[id^="weekly_filters--label__"] input');
            var weekdays = [];

            weekdayInputs.forEach(function (input) {
                if (!input.checked) {
                    return;
                }
                var label = input.closest('label');
                var dayText = label ? label.textContent.trim() : '';
                if (dayText) {
                    weekdays.push(dayText);
                }
            });

            strategies.push({
                id: strategyId,
                checked: !!(checkbox && checkbox.checked),
                dte: dteControl && typeof dteControl.getSelectedDtes === 'function'
                    ? normalizeDteValues(dteControl.getSelectedDtes())
                    : normalizeDteValues(selectedTopFilters.DTE || [0]),
                qty_multiplier: getRowBaseQty(row),
                slippage: slippageInput ? Number(slippageInput.value || 0) || 0 : 0,
                weekdays: weekdays.length ? weekdays : ['M', 'T', 'W', 'Th', 'F']
            });
        });
        return strategies;
    }

    function savePortfolioState(options) {
        var strategyStates = collectPortfolioStrategiesState(options && options.excludeStrategyId);
        var payload = {
            name: (options && options.name) || loadedPortfolioName || 'Portfolio',
            strategy_ids: strategyStates.map(function (item) { return item.id; }),
            strategies: strategyStates,
            qty_multiplier: qtySelect ? parseInt(qtySelect.value || '1', 10) || 1 : 1,
            is_weekdays: currentFilterMode === 'Weekdays'
        };

        if (options && options.includePortfolioId !== false) {
            payload.portfolio_id = portfolioId;
        }

        return fetch(getPortfolioApiUrl('portfolioSave'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        }).then(function (response) {
            return response.json().catch(function () {
                return {};
            }).then(function (data) {
                if (!response.ok) {
                    throw new Error(data.detail || 'Failed to save portfolio');
                }
                return data;
            });
        });
    }

    function buildPortfolioStrategyStateMap() {
        var map = {};
        collectPortfolioStrategiesState().forEach(function (item) {
            map[String(item.id)] = item;
        });
        return map;
    }

    function savePortfolioEditSelection() {
        var portfolioName = editPortfolioName ? String(editPortfolioName.value || '').trim() : '';
        var selectedIds = Object.keys(portfolioEditSelectedIds).filter(function (id) {
            return !!portfolioEditSelectedIds[id];
        });
        if (!portfolioName) {
            showToast('Portfolio name is required.', 'error');
            return;
        }
        if (!selectedIds.length) {
            showToast('Select at least one strategy.', 'error');
            return;
        }

        var currentStrategyMap = buildPortfolioStrategyStateMap();
        var strategyStates = selectedIds.map(function (id) {
            return currentStrategyMap[id] || {
                id: id,
                checked: true,
                dte: normalizeDteValues(selectedTopFilters.DTE || [0]),
                qty_multiplier: 1,
                slippage: 0,
                weekdays: ['M', 'T', 'W', 'Th', 'F']
            };
        });

        var payload = {
            portfolio_id: portfolioId,
            name: portfolioName,
            strategy_ids: selectedIds,
            strategies: strategyStates,
            qty_multiplier: qtySelect ? parseInt(qtySelect.value || '1', 10) || 1 : 1,
            is_weekdays: currentFilterMode === 'Weekdays'
        };

        fetch(getPortfolioApiUrl('portfolioSave'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        }).then(function (response) {
            return response.json().catch(function () {
                return {};
            }).then(function (data) {
                if (!response.ok) {
                    throw new Error(data.detail || 'Failed to update portfolio');
                }
                return data;
            });
        }).then(function () {
            return loadPortfolioDetails();
        }).then(function () {
            closePortfolioEditModal();
            showToast('Portfolio updated successfully.');
        }).catch(function (error) {
            showToast(error.message || 'Failed to update portfolio', 'error');
        });
    }

    function renderError(message) {
        setTitle('Portfolio');
        setSummary(0);
        rowsContainer.innerHTML = '<div class="rounded border border-red-200 bg-red-50 p-4 text-sm text-red-600">' + message + '</div>';
    }

    loadPortfolioDetails()
        .catch(function (error) {
            renderError(error.message || 'Failed to load portfolio');
        });

    overallSelect.addEventListener('change', function () {
        var shouldSelectAll = overallSelect.checked;
        rowsContainer.querySelectorAll('input[id^="portfolio-strategy-"]').forEach(function (checkbox) {
            checkbox.checked = shouldSelectAll;
        });
        updateSelectionSummary();
        if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
            window._renderPortfolioBacktestReports(window._portfolioResult);
        }
    });

    if (dteTab) {
        dteTab.addEventListener('click', function () {
            setActiveTopFilter(dteTab);
            renderTopFilterMenu('DTE');
            toggleTopFilterMenu(true);
        });
    }

    if (weekdaysTab) {
        weekdaysTab.addEventListener('click', function () {
            setActiveTopFilter(weekdaysTab);
            renderTopFilterMenu('Weekdays');
            toggleTopFilterMenu(true);
        });
    }

    if (budgetDaysTab) {
        budgetDaysTab.addEventListener('click', function () {
            setActiveTopFilter(budgetDaysTab);
            renderTopFilterMenu('Budget Days');
            toggleTopFilterMenu(true);
        });
    }

    if (topFilterButton) {
        topFilterButton.addEventListener('click', function (event) {
            event.preventDefault();
            toggleTopFilterMenu();
        });
    }

    document.addEventListener('click', function (event) {
        if (topFilter && !topFilter.contains(event.target)) {
            toggleTopFilterMenu(false);
        }
        if (qtyFilter && !qtyFilter.contains(event.target) && qtyPopover) {
            qtyPopover.hidden = true;
            if (qtyButton) {
                qtyButton.setAttribute('aria-expanded', 'false');
            }
        }
        if (slippageFilter && !slippageFilter.contains(event.target) && slippagePopover) {
            slippagePopover.hidden = true;
            if (slippageButton) {
                slippageButton.setAttribute('aria-expanded', 'false');
            }
        }
    });

    if (qtyButton && qtyPopover) {
        qtyButton.addEventListener('click', function (event) {
            event.preventDefault();
            event.stopPropagation();
            var shouldShow = qtyPopover.hidden;
            qtyPopover.hidden = !shouldShow;
            qtyButton.setAttribute('aria-expanded', shouldShow ? 'true' : 'false');
        });
    }

    if (qtyPopover) {
        qtyPopover.addEventListener('click', function (event) {
            event.stopPropagation();
        });
    }

    if (qtySelect) {
        qtySelect.addEventListener('change', updateQtyButtonLabel);
    }

    if (qtyApply && qtySelect) {
        qtyApply.addEventListener('click', function () {
            updateAllRowEffectiveQty();
            updateQtyButtonLabel();
            if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                window._renderPortfolioBacktestReports(window._portfolioResult);
            }
            qtyPopover.hidden = true;
            if (qtyButton) {
                qtyButton.setAttribute('aria-expanded', 'false');
            }
        });
    }

    if (editPortfolioClose) {
        editPortfolioClose.addEventListener('click', closePortfolioEditModal);
    }

    if (editPortfolioCancel) {
        editPortfolioCancel.addEventListener('click', closePortfolioEditModal);
    }

    if (editPortfolioModal) {
        editPortfolioModal.addEventListener('click', function (event) {
            if (event.target === editPortfolioModal) {
                closePortfolioEditModal();
            }
        });
    }

    if (editPortfolioSearch) {
        editPortfolioSearch.addEventListener('input', function () {
            portfolioEditSearchTerm = editPortfolioSearch.value || '';
            renderPortfolioEditStrategyList();
        });
    }

    if (editPortfolioSelectAll) {
        editPortfolioSelectAll.addEventListener('change', function () {
            var term = String(portfolioEditSearchTerm || '').trim().toLowerCase();
            portfolioEditStrategies.forEach(function (strategy) {
                var strategyId = String(strategy && strategy._id || '');
                var isVisible = !term || String(strategy && strategy.name || '').toLowerCase().indexOf(term) !== -1;
                if (strategyId && isVisible) {
                    portfolioEditSelectedIds[strategyId] = !!editPortfolioSelectAll.checked;
                }
            });
            renderPortfolioEditStrategyList();
        });
    }

    if (editPortfolioSave) {
        editPortfolioSave.addEventListener('click', savePortfolioEditSelection);
    }

    if (slippageButton && slippagePopover) {
        slippageButton.addEventListener('click', function (event) {
            event.preventDefault();
            event.stopPropagation();
            var shouldShow = slippagePopover.hidden;
            slippagePopover.hidden = !shouldShow;
            slippageButton.setAttribute('aria-expanded', shouldShow ? 'true' : 'false');
        });
    }

    if (slippagePopover) {
        slippagePopover.addEventListener('click', function (event) {
            event.stopPropagation();
        });
    }

    if (slippageApply && slippageInput) {
        slippageApply.addEventListener('click', function () {
            var selectedSlippage = String(slippageInput.value || '0');
            rowsContainer.querySelectorAll('[draggable="true"]').forEach(function (row) {
                var rowSlippageInput = getRowSlippageInput(row);
                if (rowSlippageInput) {
                    rowSlippageInput.value = selectedSlippage;
                }
            });
            slippagePopover.hidden = true;
            if (slippageButton) {
                slippageButton.setAttribute('aria-expanded', 'false');
            }
            if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                window._renderPortfolioBacktestReports(window._portfolioResult);
            }
        });
    }

    window._portfolioActiveFilterMode = currentFilterMode;
    setActiveTopFilter(weekdaysTab);
    renderTopFilterMenu(currentFilterMode);

    /* ── Portfolio Backtest Button ─────────────────────────────── */
    var backtestBtn = document.querySelector('[data-testid="portfolio-run-backtests-btn"]');
    var resultSection = document.getElementById('portfolio-backtest-result-section');
    var progressCard = document.getElementById('portfolio-backtest-progress');
    var progressTitleEl = document.getElementById('portfolio-backtest-progress-title');
    var progressPercentEl = document.getElementById('portfolio-backtest-progress-percent');
    var progressBarEl = document.getElementById('portfolio-backtest-progress-bar');
    var progressMetaEl = document.getElementById('portfolio-backtest-progress-meta');
    var activePortfolioPollInterval = null;

    function setPortfolioResultSectionVisible(visible) {
        if (!resultSection) {
            return;
        }
        resultSection.hidden = !visible;
    }

    function setPortfolioProgressVisible(visible) {
        if (!progressCard) {
            return;
        }
        progressCard.hidden = !visible;
    }

    function updatePortfolioBacktestProgress(state) {
        var total = Number(state && state.total || 0);
        var completed = Number(state && state.completed || 0);
        var percent = Number(state && state.percent);
        var clampedPercent;
        var strategyName = state && state.strategyName ? String(state.strategyName) : '';
        var currentDay = state && state.currentDay ? String(state.currentDay) : '';
        var label = state && state.label ? String(state.label) : 'Running Backtest...';
        var metaText = state && state.meta ? String(state.meta) : '';

        if (!Number.isFinite(percent)) {
            percent = total > 0 ? (completed / total) * 100 : 0;
        }
        clampedPercent = Math.max(0, Math.min(100, percent));

        if (progressTitleEl) {
            progressTitleEl.textContent = label;
        }
        if (progressPercentEl) {
            progressPercentEl.textContent = clampedPercent.toFixed(1) + '%';
        }
        if (progressBarEl) {
            progressBarEl.style.width = clampedPercent + '%';
        }
        if (progressMetaEl) {
            if (!metaText) {
                if (total > 0) {
                    metaText = 'Processing ' + completed + '/' + total;
                } else {
                    metaText = 'Preparing backtest...';
                }
                if (currentDay) {
                    metaText += ': ' + currentDay;
                } else if (strategyName) {
                    metaText += ': ' + strategyName;
                }
            }
            progressMetaEl.textContent = metaText;
        }
    }

    function resetPortfolioBacktestView() {
        setPortfolioResultSectionVisible(false);
        setPortfolioProgressVisible(false);
        updatePortfolioBacktestProgress({
            percent: 0,
            completed: 0,
            total: 0,
            label: 'Running Backtest...',
            meta: 'Preparing backtest...'
        });
    }

    window._setPortfolioResultSectionVisible = setPortfolioResultSectionVisible;
    window._setPortfolioProgressVisible = setPortfolioProgressVisible;
    window._updatePortfolioBacktestProgress = updatePortfolioBacktestProgress;
    window._resetPortfolioBacktestView = resetPortfolioBacktestView;

    resetPortfolioBacktestView();

    function toInputDate(date) {
        var year = date.getFullYear();
        var month = String(date.getMonth() + 1).padStart(2, '0');
        var day = String(date.getDate()).padStart(2, '0');
        return year + '-' + month + '-' + day;
    }

    function setDefaultBacktestDates() {
        var dateInputs = document.querySelectorAll('#strategy-date-input input[type="date"]');
        if (!dateInputs.length) return;
        var endDate = new Date();
        var startDate = new Date(endDate);
        startDate.setFullYear(startDate.getFullYear() - 1);
        if (dateInputs[0] && !dateInputs[0].value) {
            dateInputs[0].value = toInputDate(startDate);
        }
        if (dateInputs[1] && !dateInputs[1].value) {
            dateInputs[1].value = toInputDate(endDate);
        }
    }

    setDefaultBacktestDates();

    function getDateInputs() {
        var dateInputs = document.querySelectorAll('#strategy-date-input input[type="date"]');
        return {
            start: dateInputs[0] ? dateInputs[0].value : '',
            end: dateInputs[1] ? dateInputs[1].value : ''
        };
    }

    function getWeeklyOldRegime() {
        var toggle = document.querySelector('[data-test-id="toggle"]');
        return toggle ? toggle.getAttribute('aria-checked') === 'true' : false;
    }

    function setBtnState(text, disabled) {
        if (!backtestBtn) return;
        backtestBtn.disabled = !!disabled;
        var spanEl = backtestBtn.querySelector('span span');
        if (spanEl) {
            spanEl.textContent = text;
        } else {
            var mainSpan = backtestBtn.querySelector('span');
            if (mainSpan) mainSpan.textContent = text;
            else backtestBtn.textContent = text;
        }
    }

    if (backtestBtn) {
        backtestBtn.addEventListener('click', function (event) {
            event.preventDefault();

            var dates = getDateInputs();
            if (!dates.start || !dates.end) {
                alert('Please select Start Date and End Date before running backtest.');
                return;
            }
            if (!portfolioId) {
                alert('Portfolio ID not found in URL.');
                return;
            }

            var body = {
                portfolio: portfolioId,
                start_date: dates.start,
                end_date: dates.end,
                weekly_old_regime: getWeeklyOldRegime(),
                source: 'WEB'
            };

            if (activePortfolioPollInterval) {
                clearInterval(activePortfolioPollInterval);
                activePortfolioPollInterval = null;
            }

            setPortfolioResultSectionVisible(false);
            setPortfolioProgressVisible(true);
            updatePortfolioBacktestProgress({
                percent: 0,
                completed: 0,
                total: 0,
                label: 'Running Backtest...',
                meta: 'Preparing backtest...'
            });
            setBtnState('Starting...', true);

            fetch(getPortfolioApiUrl('portfolioBacktestStart'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            })
                .then(function (res) {
                    if (!res.ok) return res.json().then(function (e) {
                        throw new Error((e.detail && e.detail.message) || e.detail || 'Failed to start backtest');
                    });
                    return res.json();
                })
                .then(function (data) {
                    var jobId = data.job_id;
                    var strategyCount = data.strategy_count || 1;

                    updatePortfolioBacktestProgress({
                        percent: 0,
                        completed: 0,
                        total: strategyCount,
                        label: 'Running Backtest...',
                        meta: 'Processing 0/' + strategyCount
                    });

                    activePortfolioPollInterval = setInterval(function () {
                        fetch(getPortfolioApiUrl('backtestStatus', jobId))
                            .then(function (r) { return r.json(); })
                            .then(function (s) {
                                if (s.status === 'running') {
                                    var pct = s.percent || 0;
                                    var done = s.completed !== undefined ? s.completed : '?';
                                    var totalCount = s.total !== undefined ? s.total : strategyCount;
                                    var currentDay = s.current_day || '';
                                    var sname = s.strategy_name ? ' — ' + s.strategy_name : '';
                                    var doneCount = Number(s.completed !== undefined ? s.completed : 0);
                                    setBtnState(
                                        pct.toFixed(1) + '% (' + done + '/' + totalCount + sname + ')',
                                        true
                                    );
                                    updatePortfolioBacktestProgress({
                                        percent: pct,
                                        completed: doneCount,
                                        total: Number(totalCount || 0),
                                        label: 'Running Backtest...',
                                        strategyName: s.strategy_name || '',
                                        currentDay: currentDay,
                                        meta: 'Processing ' + done + '/' + totalCount + (currentDay ? ': ' + currentDay : (s.strategy_name ? ': ' + s.strategy_name : ''))
                                    });
                                } else if (s.status === 'done') {
                                    clearInterval(activePortfolioPollInterval);
                                    activePortfolioPollInterval = null;
                                    setBtnState('Fetching result...', true);
                                    updatePortfolioBacktestProgress({
                                        percent: 100,
                                        completed: strategyCount,
                                        total: strategyCount,
                                        label: 'Finalizing Backtest...',
                                        meta: 'Backtest completed. Fetching result...'
                                    });
                                    fetch(getPortfolioApiUrl('backtestResult', jobId))
                                        .then(function (r) { return r.json(); })
                                        .then(function (result) {
                                            setBtnState('Backtest', false);
                                            window._portfolioResult = result;
                                            window._portfolioBacktestQtySnapshot = null;
                                            if (typeof window._onPortfolioBacktestResult === 'function') {
                                                window._onPortfolioBacktestResult(result);
                                            }
                                            var resultChart = document.querySelector('[data-testid="result-chart"]');
                                            if (resultChart) {
                                                resultChart.scrollIntoView({ behavior: 'smooth' });
                                            }
                                        })
                                        .catch(function (err) {
                                            setBtnState('Backtest', false);
                                            setPortfolioProgressVisible(false);
                                            alert('Error fetching result: ' + err.message);
                                        });
                                } else if (s.status === 'error') {
                                    clearInterval(activePortfolioPollInterval);
                                    activePortfolioPollInterval = null;
                                    setBtnState('Backtest', false);
                                    setPortfolioProgressVisible(false);
                                    alert('Backtest error: ' + (s.error || 'Unknown error'));
                                }
                            })
                            .catch(function (err) {
                                clearInterval(activePortfolioPollInterval);
                                activePortfolioPollInterval = null;
                                setBtnState('Backtest', false);
                                setPortfolioProgressVisible(false);
                                alert('Poll error: ' + err.message);
                            });
                    }, 1000);
                })
                .catch(function (err) {
                    setBtnState('Backtest', false);
                    setPortfolioProgressVisible(false);
                    alert('Error: ' + err.message);
                });
        });
    }
})();


(function () {
    var SAMPLE_RESULT_URL = getPortfolioApiUrl('backtestSampleResult');
    var setTitle = typeof window.setPortfolioPageTitle === 'function'
        ? window.setPortfolioPageTitle
        : function (name) {
            var titleEl = document.getElementById('portfolio-page-title');
            if (!titleEl) return;
            var backButton = titleEl.querySelector('button');
            titleEl.textContent = '';
            if (backButton) {
                titleEl.appendChild(backButton);
            }
            titleEl.appendChild(document.createTextNode(name || 'Portfolio'));
        };
    var setSummary = typeof window.setPortfolioSummary === 'function'
        ? window.setPortfolioSummary
        : function (count) {
            var selectedCountEl = document.getElementById('portfolio-selected-count');
            var selectedTotalEl = document.getElementById('portfolio-selected-total');
            var overallSelect = document.getElementById('overallSelect');
            if (selectedCountEl) {
                selectedCountEl.textContent = String(count);
            }
            if (selectedTotalEl) {
                selectedTotalEl.textContent = '/' + String(count) + ' selected';
            }
            if (overallSelect) {
                overallSelect.checked = count > 0;
            }
        };
    var YEAR_TD = 'border-r border-r-primaryBlack-350 bg-primaryBlack-50 px-2 py-1 text-left text-xs font-light md:border-none md:px-4 md:py-3 !p-0 border-r border-r-primaryBlack-350';
    var MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    var METRIC_ROWS = [
        { key: 'overallProfit', label: 'Overall Profit', kind: 'currency' },
        { key: 'tradeCount', label: 'No. of Trades(Periods)', kind: 'plain' },
        { key: 'avgProfit', label: 'Average Profit per Period', kind: 'currency' },
        { key: 'winPct', label: 'Win %(Periods)', kind: 'number' },
        { key: 'lossPct', label: 'Loss %(Periods)', kind: 'number' },
        { key: 'avgWin', label: 'Average Profit on Winning Periods', kind: 'currency' },
        { key: 'avgLoss', label: 'Average Loss on Losing Periods', kind: 'currency' },
        { key: 'maxProfit', label: 'Max Profit in Single Period', kind: 'currency' },
        { key: 'maxLoss', label: 'Max Loss in Single Period', kind: 'currency' },
        { key: 'maxDrawdown', label: 'Max Drawdown', kind: 'currency' },
        { key: 'drawdownDuration', label: 'Duration of Max Drawdown', kind: 'plain' },
        { key: 'returnToMaxDD', label: 'Return/MaxDD', kind: 'number' },
        { key: 'rewardToRisk', label: 'Reward to Risk Ratio', kind: 'number' },
        { key: 'expectancyRatio', label: 'Expectancy Ratio', kind: 'number' },
        { key: 'maxWinStreak', label: 'Max Win Streak (Periods)', kind: 'plain' },
        { key: 'maxLossStreak', label: 'Max Losing Streak (Periods)', kind: 'plain' },
        { key: 'maxTradesInDrawdown', label: 'Max trades in any drawdown', kind: 'plain' }
    ];

    function getStrategyNameMap() {
        return window._portfolioStrategyMap || {};
    }

    function getResultPayload(payload) {
        if (!payload) return null;
        if (Array.isArray(payload.results)) return payload;
        if (payload.status && Array.isArray(payload.results)) return payload;
        return null;
    }

    function getTradeExpiryKey(trade) {
        var legs = trade && Array.isArray(trade.legs) ? trade.legs : [];
        for (var i = 0; i < legs.length; i += 1) {
            if (legs[i] && legs[i].expiry) {
                return String(legs[i].expiry).substring(0, 10);
            }
        }
        return null;
    }

    function buildPortfolioDteLookup(payload) {
        var lookup = {};
        var root = getResultPayload(payload);
        if (!root || !Array.isArray(root.results)) return lookup;

        var expiryToDates = {};
        root.results.forEach(function (item) {
            var trades = item && item.results && Array.isArray(item.results.trades) ? item.results.trades : [];
            trades.forEach(function (trade) {
                var expiryKey = getTradeExpiryKey(trade);
                if (!expiryKey || !trade || !trade.date) return;
                if (!expiryToDates[expiryKey]) expiryToDates[expiryKey] = {};
                expiryToDates[expiryKey][trade.date] = true;
            });
        });

        Object.keys(expiryToDates).forEach(function (expiryKey) {
            var dates = Object.keys(expiryToDates[expiryKey]).sort();
            lookup[expiryKey] = {};
            dates.forEach(function (date, index) {
                lookup[expiryKey][date] = dates.length - index - 1;
            });
        });

        return lookup;
    }

    function getTradeDTE(trade, dteLookup) {
        var expiryKey = getTradeExpiryKey(trade);
        if (expiryKey && dteLookup && dteLookup[expiryKey] && dteLookup[expiryKey][trade.date] !== undefined) {
            return dteLookup[expiryKey][trade.date];
        }

        if (expiryKey && trade && trade.date) {
            var p = String(trade.date).split('-');
            var e = expiryKey.split('-');
            if (p.length === 3 && e.length === 3) {
                var tradeDate = new Date(parseInt(p[0], 10), parseInt(p[1], 10) - 1, parseInt(p[2], 10));
                var expiryDate = new Date(parseInt(e[0], 10), parseInt(e[1], 10) - 1, parseInt(e[2], 10));
                return Math.max(0, Math.round((expiryDate - tradeDate) / 86400000));
            }
        }
        return null;
    }

    function getPortfolioSelectedDteMap() {
        var selectedMap = {};
        document.querySelectorAll('#portfolio-strategy-rows [data-strategy-id]').forEach(function (row) {
            var strategyId = row.getAttribute('data-strategy-id');
            if (!strategyId) return;
            var wrap = row.querySelector('[data-row-dte-wrap]');
            var control = wrap ? wrap.firstElementChild : null;
            if (!control || typeof control.getSelectedDtes !== 'function') return;
            var values = control.getSelectedDtes().map(function (value) {
                return parseInt(String(value).replace(/\s*DTE\s*$/i, ''), 10);
            }).filter(function (value) {
                return Number.isFinite(value);
            });
            if (values.length) {
                selectedMap[strategyId] = values;
            }
        });
        return selectedMap;
    }

    function getTradeWeekdayCode(trade) {
        if (!trade || !trade.date) return null;
        var parts = String(trade.date).split('-');
        if (parts.length !== 3) return null;
        var day = new Date(parseInt(parts[0], 10), parseInt(parts[1], 10) - 1, parseInt(parts[2], 10)).getDay();
        return day === 1 ? 'M'
            : day === 2 ? 'T'
                : day === 3 ? 'W'
                    : day === 4 ? 'Th'
                        : day === 5 ? 'F'
                            : day === 6 ? 'Sa'
                                : 'Su';
    }

    function getPortfolioSelectedWeekdayMap() {
        var selectedMap = {};
        document.querySelectorAll('#portfolio-strategy-rows [data-strategy-id]').forEach(function (row) {
            var strategyId = row.getAttribute('data-strategy-id');
            if (!strategyId) return;
            var values = [];
            row.querySelectorAll('label[id^="weekly_filters--label__"]').forEach(function (label) {
                var input = label.querySelector('input');
                if (input && input.checked) {
                    values.push(label.textContent.trim());
                }
            });
            if (values.length) {
                selectedMap[strategyId] = values;
            }
        });
        return selectedMap;
    }

    function getPortfolioSlippageMap() {
        var slippageMap = {};
        document.querySelectorAll('#portfolio-strategy-rows [data-strategy-id]').forEach(function (row) {
            var strategyId = row.getAttribute('data-strategy-id');
            if (!strategyId) return;
            var input = row.querySelector('input[type="number"]');
            var slippage = input ? Number(input.value || 0) : 0;
            slippageMap[strategyId] = Number.isFinite(slippage) ? slippage : 0;
        });
        return slippageMap;
    }

    function getPortfolioVisibleTrades() {
        return Array.isArray(window._portfolioVisibleTrades) ? window._portfolioVisibleTrades : [];
    }

    function getPortfolioSwitchEnabled(button) {
        return !!button && button.getAttribute('aria-checked') === 'true';
    }

    function setPortfolioSwitchState(button, enabled) {
        if (!button) return;
        button.setAttribute('aria-checked', enabled ? 'true' : 'false');
        button.classList.toggle('bg-primaryBlue-500', enabled);
        button.classList.toggle('bg-primaryBlack-600', !enabled);
        var knob = button.querySelector('span');
        if (knob) {
            knob.style.transform = enabled ? 'translateX(12px)' : 'translateX(4px)';
        }
    }

    function getPortfolioLegExecutionCount(leg) {
        if (!leg) return 0;
        if (Array.isArray(leg.sub_trades) && leg.sub_trades.length) return leg.sub_trades.length;
        return (leg.entry_time || leg.exit_time || leg.entry_price !== undefined || leg.exit_price !== undefined) ? 1 : 0;
    }

    function getPortfolioLegLotsTraded(leg) {
        if (!leg) return 0;
        var baseLots = Number(leg.lots || 0) || 0;
        if (Array.isArray(leg.sub_trades) && leg.sub_trades.length) {
            return leg.sub_trades.reduce(function (sum, subTrade) {
                var subLots = Number(subTrade && subTrade.lots || 0) || baseLots || 1;
                return sum + subLots;
            }, 0);
        }
        return getPortfolioLegExecutionCount(leg) > 0 ? (baseLots || 1) : 0;
    }

    function getPortfolioTradeLotCount(trade) {
        var totalLots = (trade && Array.isArray(trade.legs) ? trade.legs : []).reduce(function (sum, leg) {
            return sum + getPortfolioLegLotsTraded(leg);
        }, 0);
        return totalLots || 1;
    }

    function getPortfolioTradeOrderCount(trade) {
        var totalOrders = (trade && Array.isArray(trade.legs) ? trade.legs : []).reduce(function (sum, leg) {
            return sum + getPortfolioLegExecutionCount(leg);
        }, 0);
        return totalOrders || 1;
    }

    function getPortfolioDetailBrokerage(leg, subTrade) {
        var toggle = document.getElementById('toggle--result-brokerage_include_backtest');
        var rateInput = document.getElementById('portfolio-brokerage-rate-input');
        var rateType = document.getElementById('portfolio-brokerage-rate-type');
        if (!getPortfolioSwitchEnabled(toggle)) return 0;
        var rate = Number(rateInput && rateInput.value) || 0;
        if (rate <= 0) return 0;
        if (rateType && rateType.value === 'per_lot') {
            var detailLots = Number(subTrade && subTrade.lots || 0) || Number(leg && leg.lots || 0) || 1;
            return rate * detailLots;
        }
        return rate;
    }

    function getPortfolioBrokerage(trade) {
        var toggle = document.getElementById('toggle--result-brokerage_include_backtest');
        var rateInput = document.getElementById('portfolio-brokerage-rate-input');
        var rateType = document.getElementById('portfolio-brokerage-rate-type');
        if (!getPortfolioSwitchEnabled(toggle)) return 0;
        var rate = Number(rateInput && rateInput.value) || 0;
        if (rate <= 0) return 0;
        if (rateType && rateType.value === 'per_lot') {
            return rate * getPortfolioTradeLotCount(trade);
        }
        return rate * getPortfolioTradeOrderCount(trade);
    }

    function computePortfolioTotalBrokerage(trades) {
        return (trades || []).reduce(function (sum, trade) {
            return sum + getPortfolioBrokerage(trade);
        }, 0);
    }

    var PORTFOLIO_NSE_STT = 0.001;
    var PORTFOLIO_NSE_EXCHANGE = 0.00053;
    var PORTFOLIO_NSE_STAMP = 0.00003;
    var PORTFOLIO_NSE_SEBI = 0.0000010;
    var PORTFOLIO_NSE_GST = 0.18;

    function calcPortfolioTradeTaxes(trade) {
        var stt = 0, exchange = 0, stamp = 0, sebi = 0, gst = 0;
        (trade && Array.isArray(trade.legs) ? trade.legs : []).forEach(function (leg) {
            var lots = Number(leg.lots || 1);
            var lotSize = Number(leg.lot_size || 1);
            var subList = leg.sub_trades && leg.sub_trades.length ? leg.sub_trades : [null];
            var isSell = (leg.position || 'Sell').toLowerCase() !== 'buy';

            subList.forEach(function (st) {
                var stLots = st && st.lots ? Number(st.lots) : lots;
                var qty = stLots * lotSize;
                var entryPx = Number(st && st.entry_price !== undefined ? st.entry_price : (leg.entry_price || 0));
                var exitPx = Number(st && st.exit_price !== undefined ? st.exit_price : (leg.exit_price || 0));
                var entryTov = entryPx * qty;
                if (isSell) stt += entryTov * PORTFOLIO_NSE_STT;
                else stamp += entryTov * PORTFOLIO_NSE_STAMP;
                exchange += entryTov * PORTFOLIO_NSE_EXCHANGE;
                sebi += entryTov * PORTFOLIO_NSE_SEBI;

                var exitTov = exitPx * qty;
                if (!isSell) stt += exitTov * PORTFOLIO_NSE_STT;
                else stamp += exitTov * PORTFOLIO_NSE_STAMP;
                exchange += exitTov * PORTFOLIO_NSE_EXCHANGE;
                sebi += exitTov * PORTFOLIO_NSE_SEBI;
            });
        });
        gst = (exchange + sebi) * PORTFOLIO_NSE_GST;
        return { stt: stt, exchange: exchange, stamp: stamp, sebi: sebi, gst: gst, total: stt + exchange + stamp + sebi + gst };
    }

    function getPortfolioTradeTaxes(trade) {
        var toggle = document.getElementById('toggle--result_include_backtest');
        if (!getPortfolioSwitchEnabled(toggle)) return 0;
        return calcPortfolioTradeTaxes(trade).total;
    }

    function formatPortfolioCost(value, withSymbol) {
        var formatted = Number(value || 0).toLocaleString('en-IN', { minimumFractionDigits: 0, maximumFractionDigits: 2 });
        return withSymbol ? ('₹' + formatted) : formatted;
    }

    function syncPortfolioBrokerageTriggerState() {
        var toggle = document.getElementById('toggle--result-brokerage_include_backtest');
        var trigger = document.getElementById('portfolio-brokerage-cost-trigger');
        var editTrigger = document.getElementById('portfolio-brokerage-edit-trigger');
        var enabled = getPortfolioSwitchEnabled(toggle);
        if (trigger) {
            trigger.classList.toggle('opacity-50', !enabled);
            trigger.classList.toggle('opacity-100', enabled);
        }
        if (editTrigger) {
            editTrigger.classList.toggle('pointer-events-none', !enabled);
            editTrigger.setAttribute('aria-disabled', String(!enabled));
        }
    }

    function syncPortfolioTaxesTriggerState() {
        var toggle = document.getElementById('toggle--result_include_backtest');
        var trigger = document.getElementById('portfolio-taxes-cost-trigger');
        var editTrigger = document.getElementById('portfolio-taxes-edit-trigger');
        var enabled = getPortfolioSwitchEnabled(toggle);
        if (trigger) {
            trigger.classList.toggle('opacity-50', !enabled);
            trigger.classList.toggle('opacity-100', enabled);
        }
        if (editTrigger) {
            editTrigger.classList.toggle('pointer-events-none', !enabled);
            editTrigger.setAttribute('aria-disabled', String(!enabled));
        }
    }

    function syncPortfolioBrokerageCost() {
        var valueEl = document.getElementById('portfolio-brokerage-cost-value');
        if (!valueEl) return;
        valueEl.textContent = formatPortfolioCost(computePortfolioTotalBrokerage(getPortfolioVisibleTrades()), false);
    }

    function syncPortfolioTaxesCost() {
        var valueEl = document.getElementById('portfolio-taxes-cost-value');
        var trades = getPortfolioVisibleTrades();
        var agg = { stt: 0, exchange: 0, stamp: 0, sebi: 0, gst: 0, total: 0 };
        trades.forEach(function (trade) {
            var taxes = calcPortfolioTradeTaxes(trade);
            agg.stt += taxes.stt;
            agg.exchange += taxes.exchange;
            agg.stamp += taxes.stamp;
            agg.sebi += taxes.sebi;
            agg.gst += taxes.gst;
            agg.total += taxes.total;
        });
        var toggle = document.getElementById('toggle--result_include_backtest');
        var enabled = getPortfolioSwitchEnabled(toggle);
        if (valueEl) valueEl.textContent = enabled ? formatPortfolioCost(agg.total, false) : '0';
        var parts = {
            'portfolio-taxes-stt': agg.stt,
            'portfolio-taxes-exchange': agg.exchange,
            'portfolio-taxes-stamp': agg.stamp,
            'portfolio-taxes-sebi': agg.sebi,
            'portfolio-taxes-gst': agg.gst,
            'portfolio-taxes-total': agg.total
        };
        Object.keys(parts).forEach(function (id) {
            var el = document.getElementById(id);
            if (el) el.textContent = enabled ? formatPortfolioCost(parts[id], true) : '₹0';
        });
    }

    function syncPortfolioCostAnalysis() {
        syncPortfolioBrokerageTriggerState();
        syncPortfolioTaxesTriggerState();
        syncPortfolioBrokerageCost();
        syncPortfolioTaxesCost();
    }

    function bindPortfolioCostAnalysis() {
        var brokerageToggle = document.getElementById('toggle--result-brokerage_include_backtest');
        var taxesToggle = document.getElementById('toggle--result_include_backtest');
        var brokeragePopup = document.getElementById('portfolio-brokerage-popup');
        var brokerageEditTrigger = document.getElementById('portfolio-brokerage-edit-trigger');
        var brokerageDone = document.getElementById('portfolio-brokerage-popup-done');
        var brokerageRateInput = document.getElementById('portfolio-brokerage-rate-input');
        var brokerageRateType = document.getElementById('portfolio-brokerage-rate-type');
        var taxesPopup = document.getElementById('portfolio-taxes-popup');
        var taxesEditTrigger = document.getElementById('portfolio-taxes-edit-trigger');
        if (window._portfolioCostAnalysisBound) return;
        window._portfolioCostAnalysisBound = true;

        function rerenderPortfolioReports() {
            if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                window._renderPortfolioBacktestReports(window._portfolioResult);
            } else {
                syncPortfolioCostAnalysis();
            }
        }

        function togglePopup(popup, show) {
            if (!popup) return;
            popup.classList.toggle('hidden', !show);
            popup.style.display = show ? 'block' : 'none';
        }

        if (brokerageToggle) {
            setPortfolioSwitchState(brokerageToggle, false);
            brokerageToggle.addEventListener('click', function () {
                var next = !getPortfolioSwitchEnabled(brokerageToggle);
                setPortfolioSwitchState(brokerageToggle, next);
                if (!next) togglePopup(brokeragePopup, false);
                rerenderPortfolioReports();
            });
        }

        if (taxesToggle) {
            setPortfolioSwitchState(taxesToggle, false);
            taxesToggle.addEventListener('click', function () {
                var next = !getPortfolioSwitchEnabled(taxesToggle);
                setPortfolioSwitchState(taxesToggle, next);
                if (!next) togglePopup(taxesPopup, false);
                rerenderPortfolioReports();
            });
        }

        if (brokerageEditTrigger) {
            brokerageEditTrigger.addEventListener('click', function (event) {
                event.preventDefault();
                event.stopPropagation();
                if (!getPortfolioSwitchEnabled(brokerageToggle)) return;
                togglePopup(brokeragePopup, !(brokeragePopup && brokeragePopup.style.display === 'block'));
            });
        }

        if (taxesEditTrigger) {
            taxesEditTrigger.addEventListener('click', function (event) {
                event.preventDefault();
                event.stopPropagation();
                if (!getPortfolioSwitchEnabled(taxesToggle)) return;
                togglePopup(taxesPopup, !(taxesPopup && taxesPopup.style.display === 'block'));
                syncPortfolioTaxesCost();
            });
        }

        if (brokerageDone) {
            brokerageDone.addEventListener('click', function (event) {
                event.preventDefault();
                togglePopup(brokeragePopup, false);
                rerenderPortfolioReports();
            });
        }

        if (brokerageRateInput) {
            brokerageRateInput.addEventListener('input', function () {
                syncPortfolioBrokerageCost();
            });
        }

        if (brokerageRateType) {
            brokerageRateType.addEventListener('change', function () {
                syncPortfolioBrokerageCost();
            });
        }

        document.addEventListener('click', function (event) {
            if (brokeragePopup && !brokeragePopup.classList.contains('hidden') && !brokeragePopup.contains(event.target) && !(brokerageEditTrigger && brokerageEditTrigger.contains(event.target))) {
                togglePopup(brokeragePopup, false);
            }
            if (taxesPopup && !taxesPopup.classList.contains('hidden') && !taxesPopup.contains(event.target) && !(taxesEditTrigger && taxesEditTrigger.contains(event.target))) {
                togglePopup(taxesPopup, false);
            }
        });

        syncPortfolioCostAnalysis();
    }

    function applySlippageToTrade(trade, slippagePercent) {
        var clonedTrade = Object.assign({}, trade || {});
        var legs = Array.isArray(trade && trade.legs) ? trade.legs : [];
        var slipRate = Number(slippagePercent || 0) / 100;
        var totalPnl = 0;

        clonedTrade.legs = legs.map(function (leg) {
            var clonedLeg = Object.assign({}, leg || {});
            var qty = Number(clonedLeg && clonedLeg.lots || 0) * Number(clonedLeg && clonedLeg.lot_size || 0);
            var slippageCost = 0;
            var subTrades = Array.isArray(clonedLeg.sub_trades) ? clonedLeg.sub_trades : [];
            if (subTrades.length > 0) {
                subTrades.forEach(function (st) {
                    var entryPx = Number(st.entry_price || clonedLeg.entry_price || 0);
                    var exitPx = Number(st.exit_price || clonedLeg.exit_price || 0);
                    slippageCost += (entryPx + exitPx) * slipRate * qty;
                });
            } else {
                var entryPx = Number(clonedLeg.entry_price || 0);
                var exitPx = Number(clonedLeg.exit_price || 0);
                slippageCost = (entryPx + exitPx) * slipRate * qty;
            }
            clonedLeg.pnl = Number((clonedLeg && clonedLeg.pnl) || 0) - slippageCost;
            totalPnl += clonedLeg.pnl;
            return clonedLeg;
        });

        clonedTrade.total_pnl = totalPnl;
        return clonedTrade;
    }

    function applySlippageToTrades(trades, slippagePerOrder) {
        if (!Array.isArray(trades)) return [];
        var slippage = Number(slippagePerOrder || 0);
        if (!slippage) {
            return trades.map(function (trade) {
                return Object.assign({}, trade || {}, {
                    legs: Array.isArray(trade && trade.legs)
                        ? trade.legs.map(function (leg) { return Object.assign({}, leg || {}); })
                        : []
                });
            });
        }
        return trades.map(function (trade) {
            return applySlippageToTrade(trade, slippage);
        });
    }

    function filterTradesBySelectedDte(trades, selectedDtes, dteLookup) {
        if (!Array.isArray(selectedDtes) || !selectedDtes.length) return trades.slice();
        var allowed = {};
        selectedDtes.forEach(function (value) { allowed[value] = true; });
        return trades.filter(function (trade) {
            var dte = getTradeDTE(trade, dteLookup);
            return dte !== null && allowed[dte];
        });
    }

    function filterTradesBySelectedWeekdays(trades, selectedWeekdays) {
        if (!Array.isArray(selectedWeekdays) || !selectedWeekdays.length) return trades.slice();
        var allowed = {};
        selectedWeekdays.forEach(function (value) { allowed[value] = true; });
        return trades.filter(function (trade) {
            var weekday = getTradeWeekdayCode(trade);
            return weekday && allowed[weekday];
        });
    }

    function getActivePortfolioFilterMode() {
        if (typeof window !== 'undefined' && (window._portfolioActiveFilterMode === 'DTE' || window._portfolioActiveFilterMode === 'Weekdays')) {
            return window._portfolioActiveFilterMode;
        }
        return 'Weekdays';
    }

    function getPortfolioEffectiveQtyMap() {
        var qtyMap = {};
        document.querySelectorAll('#portfolio-strategy-rows [data-strategy-id]').forEach(function (row) {
            var strategyId = String(row.getAttribute('data-strategy-id') || '');
            if (!strategyId) {
                return;
            }
            qtyMap[strategyId] = (window.getRowBaseQty ? window.getRowBaseQty(row) : 1)
                * (window.getPortfolioQtyMultiplier ? window.getPortfolioQtyMultiplier() : 1);
        });
        return qtyMap;
    }

    function capturePortfolioBacktestQtySnapshot() {
        if (typeof window.setPortfolioBacktestQtySnapshot === 'function') {
            window.setPortfolioBacktestQtySnapshot(getPortfolioEffectiveQtyMap());
        } else {
            window._portfolioBacktestQtySnapshot = getPortfolioEffectiveQtyMap();
        }
    }
    window.capturePortfolioBacktestQtySnapshot = capturePortfolioBacktestQtySnapshot;

    function getPortfolioResultBaseQty(item) {
        var results = item && item.results ? item.results : {};
        var candidates = [
            results.base_qty,
            results.baseQty,
            results.qty_multiplier,
            results.qtyMultiplier,
            item && item.base_qty,
            item && item.baseQty
        ];
        for (var i = 0; i < candidates.length; i += 1) {
            var qty = Number(candidates[i] || 0);
            if (qty > 0) {
                return qty;
            }
        }
        return 1;
    }

    function buildPortfolioBacktestQtySnapshot(payload) {
        var snapshot = {};
        var root = getResultPayload(payload);
        if (!root || !Array.isArray(root.results)) {
            return snapshot;
        }
        root.results.forEach(function (item) {
            if (!item || !item.item_id) {
                return;
            }
            snapshot[String(item.item_id)] = getPortfolioResultBaseQty(item);
        });
        return snapshot;
    }

    function scalePortfolioTrade(trade, factor) {
        if (!trade || !Number.isFinite(factor) || factor === 1) {
            return Object.assign({}, trade || {});
        }
        var clonedTrade = Object.assign({}, trade || {});
        clonedTrade.total_pnl = Number(clonedTrade.total_pnl || 0) * factor;
        clonedTrade.legs = Array.isArray(trade && trade.legs) ? trade.legs.map(function (leg) {
            var clonedLeg = Object.assign({}, leg || {});
            clonedLeg.lots = Number(clonedLeg.lots || 0) * factor;
            clonedLeg.pnl = Number(clonedLeg.pnl || 0) * factor;
            if (Array.isArray(leg && leg.sub_trades)) {
                clonedLeg.sub_trades = leg.sub_trades.map(function (subTrade) {
                    var clonedSubTrade = Object.assign({}, subTrade || {});
                    clonedSubTrade.lots = Number(clonedSubTrade.lots || clonedLeg.lots || 0) * factor;
                    clonedSubTrade.pnl = Number(clonedSubTrade.pnl || 0) * factor;
                    return clonedSubTrade;
                });
            }
            return clonedLeg;
        }) : [];
        return clonedTrade;
    }

    function scalePortfolioTrades(trades, factor) {
        if (!Array.isArray(trades) || !Number.isFinite(factor) || factor === 1) {
            return Array.isArray(trades) ? trades.slice() : [];
        }
        return trades.map(function (trade) {
            return scalePortfolioTrade(trade, factor);
        });
    }

    function getPortfolioStrategyResults(payload, includeUnchecked) {
        var root = getResultPayload(payload);
        var selectedDteMap = getPortfolioSelectedDteMap();
        var selectedWeekdayMap = getPortfolioSelectedWeekdayMap();
        var slippageMap = getPortfolioSlippageMap();
        var snapshotQtyMap = window._portfolioBacktestQtySnapshot || {};
        var currentQtyMap = getPortfolioEffectiveQtyMap();
        var activeFilterMode = getActivePortfolioFilterMode();
        var dteLookup = buildPortfolioDteLookup(payload);
        var checkedStrategyIds = getCheckedPortfolioStrategyIds();
        var visibleStrategyIds = getVisiblePortfolioStrategyIds();
        if (!root || !Array.isArray(root.results)) return [];
        return root.results.filter(function (item) {
            return item
                && visibleStrategyIds.indexOf(String(item.item_id)) !== -1
                && (item.status === 'completed' || item.status === 'error')
                && (includeUnchecked || checkedStrategyIds.indexOf(String(item.item_id)) !== -1);
        }).map(function (item) {
            var cloned = {
                _id: item._id,
                item_id: item.item_id,
                status: item.status,
                error: item.error,
                results: item.results ? Object.assign({}, item.results) : { trades: [] }
            };
            if (item.status === 'error' || !item.results) {
                cloned.results = { trades: [] };
            }
            var trades = cloned.results && Array.isArray(cloned.results.trades) ? cloned.results.trades : [];
            var strategyId = String(item.item_id || '');
            var snapshotQty = Number(snapshotQtyMap[strategyId] || 1) || 1;
            var currentQty = Number(currentQtyMap[strategyId] || snapshotQty) || snapshotQty;
            trades = scalePortfolioTrades(trades, currentQty / snapshotQty);
            if (activeFilterMode === 'DTE') {
                trades = filterTradesBySelectedDte(trades, selectedDteMap[strategyId] || [], dteLookup);
            } else if (activeFilterMode === 'Weekdays') {
                trades = filterTradesBySelectedWeekdays(trades, selectedWeekdayMap[strategyId] || []);
            }
            trades = applySlippageToTrades(trades, slippageMap[strategyId] || 0);
            cloned.results.trades = trades;
            return cloned;
        });
    }

    function getStrategyResults(payload) {
        return getPortfolioStrategyResults(payload, false);
    }

    function getAllStrategyResults(payload) {
        return getPortfolioStrategyResults(payload, true);
    }

    function toTradePnl(trade) {
        // Combined periods (from buildCombinedPeriods) already have brokerage/taxes
        // embedded in total_pnl — don't deduct again.
        if (trade && trade._netPnlPrecomputed) {
            return Number(trade.total_pnl || 0);
        }
        var grossPnl = 0;
        if (trade && Number.isFinite(Number(trade.total_pnl))) {
            grossPnl = Number(trade.total_pnl);
        } else {
            grossPnl = (trade && Array.isArray(trade.legs) ? trade.legs : []).reduce(function (sum, leg) {
                return sum + Number((leg && leg.pnl) || 0);
            }, 0);
        }
        return grossPnl - getPortfolioBrokerage(trade) - getPortfolioTradeTaxes(trade);
    }

    function formatInteger(value) {
        var num = Math.round(Number(value) || 0);
        return Math.abs(num).toLocaleString('en-IN');
    }

    function formatSignedInteger(value) {
        var num = Math.round(Number(value) || 0);
        if (!num) return '0';
        return (num < 0 ? '-' : '') + Math.abs(num).toLocaleString('en-IN');
    }

    function formatCurrency(value) {
        var num = Number(value) || 0;
        var abs = Math.abs(num).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        return (num < 0 ? '₹ -' : '₹ ') + abs;
    }

    function formatNumber(value) {
        return Number(value || 0).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function dateLabel(dateString) {
        if (!dateString) return '—';
        var parts = String(dateString).split('-');
        if (parts.length !== 3) return dateString;
        return parts[2] + '/' + parts[1] + '/' + parts[0];
    }

    function buildCombinedPeriods(strategyResults) {
        var byDate = {};
        strategyResults.forEach(function (item) {
            var itemTrades = item && item.results && Array.isArray(item.results.trades) ? item.results.trades : [];
            itemTrades.forEach(function (trade) {
                if (!trade || !trade.date) return;
                if (!byDate[trade.date]) {
                    byDate[trade.date] = {
                        date: trade.date,
                        entry_time: trade.entry_time || '',
                        exit_time: trade.exit_time || '',
                        total_pnl: 0,
                        _netPnlPrecomputed: true,
                        legs: []
                    };
                }
                byDate[trade.date].total_pnl += toTradePnl(trade);
                if (!byDate[trade.date].entry_time && trade.entry_time) byDate[trade.date].entry_time = trade.entry_time;
                if (!byDate[trade.date].exit_time && trade.exit_time) byDate[trade.date].exit_time = trade.exit_time;
            });
        });
        return Object.keys(byDate).sort().map(function (date) {
            return byDate[date];
        });
    }

    function computeMetricsFromTrades(trades, totalTradeExecutions) {
        var pnls = trades.map(function (trade) { return toTradePnl(trade); });
        var totalProfit = pnls.reduce(function (sum, pnl) { return sum + pnl; }, 0);
        var winning = pnls.filter(function (pnl) { return pnl > 0; });
        var losing = pnls.filter(function (pnl) { return pnl <= 0; });
        var periods = pnls.length;
        var avgProfit = periods ? totalProfit / periods : 0;
        var avgWin = winning.length ? winning.reduce(function (sum, pnl) { return sum + pnl; }, 0) / winning.length : 0;
        var avgLoss = losing.length ? losing.reduce(function (sum, pnl) { return sum + pnl; }, 0) / losing.length : 0;
        var maxProfit = periods ? Math.max.apply(null, pnls) : 0;
        var maxLoss = periods ? Math.min.apply(null, pnls) : 0;
        var winPct = periods ? (winning.length / periods) * 100 : 0;
        var lossPct = periods ? (losing.length / periods) * 100 : 0;

        var cumulative = 0;
        var peak = 0;
        var peakIdx = 0;
        var maxDrawdown = 0;
        var maxDrawdownStartIdx = 0;
        var maxDrawdownEndIdx = 0;
        var maxDrawdownStart = trades[0] ? trades[0].date : null;
        var maxDrawdownEnd = trades[0] ? trades[0].date : null;
        var winStreak = 0;
        var maxWinStreak = 0;
        var lossStreak = 0;
        var maxLossStreak = 0;

        trades.forEach(function (trade, index) {
            var pnl = toTradePnl(trade);
            if (pnl > 0) {
                winStreak += 1;
                lossStreak = 0;
            } else {
                lossStreak += 1;
                winStreak = 0;
            }
            if (winStreak > maxWinStreak) maxWinStreak = winStreak;
            if (lossStreak > maxLossStreak) maxLossStreak = lossStreak;

            cumulative += pnl;
            if (cumulative >= peak) {
                peak = cumulative;
                peakIdx = index;
            }
            var drawdown = cumulative - peak;
            if (drawdown < maxDrawdown) {
                maxDrawdown = drawdown;
                maxDrawdownStartIdx = peakIdx;
                maxDrawdownEndIdx = index;
                maxDrawdownStart = trades[peakIdx] ? trades[peakIdx].date : null;
                maxDrawdownEnd = trade.date;
            }
        });

        var maxTradesInDrawdown = Math.max(0, maxDrawdownEndIdx - maxDrawdownStartIdx + 1);
        var expectancy = periods ? ((winPct / 100) * avgWin) + ((lossPct / 100) * avgLoss) : 0;
        var rewardToRisk = avgLoss !== 0 ? Math.abs(avgWin / avgLoss) : 0;
        var expectancyRatio = avgLoss !== 0 ? Math.abs(expectancy / avgLoss) : 0;
        var returnToMaxDD = maxDrawdown !== 0 ? Math.abs(totalProfit / maxDrawdown) : 0;
        var durationDays = maxDrawdownStart && maxDrawdownEnd && maxDrawdown < 0
            ? Math.round((new Date(maxDrawdownEnd) - new Date(maxDrawdownStart)) / 86400000)
            : 0;

        return {
            overallProfit: totalProfit,
            tradeCount: totalTradeExecutions !== undefined && totalTradeExecutions !== null
                ? String(totalTradeExecutions) + '(' + periods + ')'
                : String(periods),
            avgProfit: avgProfit,
            winPct: winPct,
            lossPct: lossPct,
            avgWin: avgWin,
            avgLoss: avgLoss,
            maxProfit: maxProfit,
            maxLoss: maxLoss,
            maxDrawdown: maxDrawdown,
            drawdownDuration: durationDays + ' [' + dateLabel(maxDrawdownStart) + ' to ' + dateLabel(maxDrawdownEnd) + ']',
            returnToMaxDD: returnToMaxDD,
            rewardToRisk: rewardToRisk,
            expectancyRatio: expectancyRatio,
            maxWinStreak: maxWinStreak,
            maxLossStreak: maxLossStreak,
            maxTradesInDrawdown: maxTradesInDrawdown
        };
    }

    function computeYearWiseFromPeriods(periods) {
        var byYear = {};
        var pnlByDay = {};
        periods.forEach(function (trade) {
            if (!trade || !trade.date) return;
            var parts = trade.date.split('-');
            var year = parseInt(parts[0], 10);
            var month = parseInt(parts[1], 10);
            if (!byYear[year]) {
                byYear[year] = { months: {} };
                for (var i = 1; i <= 12; i += 1) byYear[year].months[i] = 0;
            }
            var pnl = toTradePnl(trade);
            byYear[year].months[month] += pnl;
            pnlByDay[trade.date] = (pnlByDay[trade.date] || 0) + pnl;
        });

        var results = {};
        var allDays = Object.keys(pnlByDay).sort();
        Object.keys(byYear).sort().forEach(function (year) {
            var yearDays = allDays.filter(function (day) { return day.indexOf(year + '-') === 0; });
            if (!yearDays.length) return;
            var cumulative = 0;
            var peak = 0;
            var drawdownStartCandidate = yearDays[0];
            var maxDrawdown = 0;
            var mddStart = yearDays[0];
            var mddEnd = yearDays[0];

            yearDays.forEach(function (day) {
                cumulative += pnlByDay[day];
                if (cumulative >= peak) {
                    peak = cumulative;
                    drawdownStartCandidate = day;
                }
                var drawdown = cumulative - peak;
                if (drawdown < maxDrawdown) {
                    maxDrawdown = drawdown;
                    mddStart = drawdownStartCandidate;
                    mddEnd = day;
                }
            });

            var total = 0;
            for (var m = 1; m <= 12; m += 1) total += byYear[year].months[m] || 0;
            var mddDays = mddStart && mddEnd
                ? Math.round((new Date(mddEnd) - new Date(mddStart)) / 86400000) + 1
                : 0;

            results[year] = {
                months: byYear[year].months,
                total: total,
                maxDD: maxDrawdown,
                mddDays: mddDays || 1,
                mddRange: '[' + dateLabel(mddStart) + ' to ' + dateLabel(mddEnd) + ']',
                roMDD: maxDrawdown !== 0 ? Math.abs(total / maxDrawdown).toFixed(2) : '—'
            };
        });
        return results;
    }

    function renderYearWiseTable(yearData) {
        var tbody = document.getElementById('yearwise-tbody');
        if (!tbody) return;
        var years = Object.keys(yearData).sort();
        if (!years.length) {
            tbody.innerHTML = '<tr><td colspan="17" class="bg-primaryBlack-0 px-4 py-6 text-center text-sm text-primaryBlack-600">No year-wise data available.</td></tr>';
            return;
        }
        tbody.innerHTML = years.map(function (year) {
            var row = yearData[year];
            var html = '<td class="' + YEAR_TD + ' sticky left-0 z-[5] bg-primaryBlack-50"><p class="px-1 font-bold">' + year + '</p></td>';
            for (var month = 1; month <= 12; month += 1) {
                var val = row.months[month] || 0;
                var cls = val < 0 ? 'text-red-500' : 'text-green-500';
                html += '<td class="' + YEAR_TD + ' bg-primaryBlack-0"><div class="flex flex-col"><p class="whitespace-nowrap px-2.5 font-medium ' + cls + '">' + formatSignedInteger(val) + '</p></div></td>';
            }
            html += '<td class="' + YEAR_TD + ' bg-primaryBlack-0"><div class="flex flex-col"><p class="whitespace-nowrap px-2.5 font-medium ' + (row.total < 0 ? 'text-red-500' : 'text-green-500') + '">' + formatSignedInteger(row.total) + '</p></div></td>';
            html += '<td class="' + YEAR_TD + ' bg-primaryBlack-0"><div class="flex flex-col"><p class="whitespace-nowrap px-2.5 font-medium text-red-500">' + formatSignedInteger(row.maxDD) + '</p></div></td>';
            html += '<td class="' + YEAR_TD + ' z-[5] md:sticky md:right-[120px] bg-primaryBlack-50"><p class="px-4">' + row.mddDays + '</p><p class="px-4 text-xs text-primaryBlack-600">' + row.mddRange + '</p></td>';
            html += '<td class="' + YEAR_TD + ' right-0 z-[5] md:sticky bg-primaryBlack-50"><p class="px-2.5 font-medium text-primaryBlack-750">' + row.roMDD + '</p></td>';
            return '<tr class="rounded text-primaryBlack-750 !rounded-lg" style="box-shadow: rgba(0, 0, 0, 0.04) 0px 4px 20px 0px;">' + html + '</tr>';
        }).join('');
    }

    function metricToneClass(kind, value) {
        if (kind === 'currency' || kind === 'number') {
            if (Number(value) < 0) return 'text-red-500';
            if (Number(value) > 0 && kind === 'currency') return 'text-green-600';
        }
        return 'text-zinc-400';
    }

    function formatMetricValue(metric, value) {
        if (metric.kind === 'currency') return formatCurrency(value);
        if (metric.kind === 'number') return formatNumber(value);
        return String(value);
    }

    function buildSummaryTable(strategyResults, allStrategyResults) {
        var wrapper = document.querySelector('[data-testid="summary-table"]');
        if (!wrapper) return;
        if (!allStrategyResults.length) {
            wrapper.innerHTML = '<div class="rounded border border-primaryBlack-350 bg-primaryBlack-0 px-4 py-6 text-center text-sm text-primaryBlack-600">No strategy-wise report available.</div>';
            return;
        }

        var checkedStrategyIds = getCheckedPortfolioStrategyIds();
        var nameMap = getStrategyNameMap();
        var columns = allStrategyResults.map(function (item, index) {
            var isSelected = checkedStrategyIds.indexOf(String(item.item_id)) !== -1;
            var trades = item && item.results && Array.isArray(item.results.trades) ? item.results.trades : [];
            var metrics = computeMetricsFromTrades(trades, trades.length);
            var isError = item.status === 'error';
            var stratName = nameMap[String(item.item_id)] || 'Strategy ' + (index + 1);
            return {
                id: String(item.item_id),
                name: isError ? '⚠️ ' + stratName + ' (Errored)' : stratName,
                metrics: metrics,
                selected: isSelected
            };
        });

        var combinedPeriods = buildCombinedPeriods(strategyResults);
        var totalExecutions = strategyResults.reduce(function (sum, item) {
            var trades = item && item.results && Array.isArray(item.results.trades) ? item.results.trades : [];
            return sum + trades.length;
        }, 0);
        var aggregateMetrics = computeMetricsFromTrades(combinedPeriods, totalExecutions);

        var headerCells = [
            '<th class="border border-white p-5 text-sm font-bold leading-4">Statistic</th>',
            '<th class="border border-white p-5 text-sm font-bold leading-4">Aggregate</th>'
        ].concat(columns.map(function (column) {
            var headerClass = column.selected ? '' : ' opacity-40';
            return '<th class="border border-white p-5 text-sm font-bold leading-4' + headerClass + '" data-summary-column="' + escapeHtml(column.id) + '"><div class="flex flex-col items-center gap-2"><label class="flex cursor-pointer items-center gap-2 text-xs font-normal leading-6 scale-90"><input class="h-4 w-4 cursor-pointer rounded border border-secondaryBlue-500 text-secondaryBlue-500 focus:ring-secondaryBlue-500" type="checkbox" data-summary-strategy-id="' + escapeHtml(column.id) + '" ' + (column.selected ? 'checked ' : '') + '></label><span class="text-center"><div class="group relative" data-testid="special-text-tooltip">' + column.name + '</div></span></div></th>';
        }));

        var bodyRows = METRIC_ROWS.map(function (metric) {
            var rowHtml = '<td class="border border-white p-2 text-sm font-semibold leading-4">' + metric.label + ' <div class="m-2 text-xs font-medium tracking-wide text-primaryBlack-500 dark:text-primaryBlack-600"><b>AlgoTest.in</b></div></td>';
            rowHtml += '<td class="border border-white p-2 text-center text-xs font-semibold leading-4 whitespace-nowrap ' + metricToneClass(metric.kind, aggregateMetrics[metric.key]) + '">' + formatMetricValue(metric, aggregateMetrics[metric.key]) + '</td>';
            columns.forEach(function (column) {
                var toneClass = metricToneClass(metric.kind, column.metrics[metric.key]);
                var fadedClass = column.selected ? '' : ' opacity-40';
                rowHtml += '<td class="border border-white p-2 text-center text-xs font-semibold leading-4 whitespace-nowrap ' + toneClass + fadedClass + '" data-summary-cell="' + escapeHtml(column.id) + '">' + formatMetricValue(metric, column.metrics[metric.key]) + '</td>';
            });
            return '<tr>' + rowHtml + '</tr>';
        }).join('');

        wrapper.innerHTML = '<table class="box-border w-full border-collapse overflow-hidden rounded bg-primaryBlack-100"><thead><tr>' + headerCells.join('') + '</tr></thead><tbody>' + bodyRows + '</tbody></table>';

        wrapper.querySelectorAll('input[data-summary-strategy-id]').forEach(function (input) {
            input.addEventListener('change', function () {
                var strategyId = input.getAttribute('data-summary-strategy-id');
                if (!strategyId) return;
                var row = document.querySelector('#portfolio-strategy-rows [data-strategy-id="' + CSS.escape(strategyId) + '"]');
                var rowCheckbox = row ? row.querySelector('input[id^="portfolio-strategy-"]') : null;
                if (!rowCheckbox) return;
                rowCheckbox.checked = input.checked;
                rowCheckbox.dispatchEvent(new Event('change', { bubbles: true }));
            });
        });
    }

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function sortTradesByEntry(trades) {
        return trades.slice().sort(function (a, b) {
            var aKey = String((a && a.date) || '') + ' ' + String((a && a.entry_time) || '');
            var bKey = String((b && b.date) || '') + ' ' + String((b && b.entry_time) || '');
            return aKey.localeCompare(bKey);
        });
    }

    var PORTFOLIO_FR_PAGE_SIZE = 10;
    var portfolioFrAllTrades = [];
    var portfolioFrSortBy = 'EntryDate';
    var portfolioFrSortDir = 'Asc';
    var portfolioFrCurrentPage = 1;
    var portfolioTradeExplainModal = document.getElementById('portfolio-trade-explain-modal');
    var portfolioTradeExplainBody = document.getElementById('portfolio-trade-explain-body');
    var portfolioTradeExplainSubtitle = document.getElementById('portfolio-trade-explain-subtitle');
    var portfolioTradeExplainClose = document.getElementById('portfolio-trade-explain-close');
    var portfolioTradeExplainState = null;

    function isPortfolioExplanationNumeric(value) {
        if (typeof value === 'number') return Number.isFinite(value);
        if (typeof value !== 'string') return false;
        return /^-?\d+(\.\d+)?$/.test(value.trim());
    }

    function formatPortfolioExplanationValue(value, options) {
        var opts = options || {};
        if (value === null || value === undefined || value === '') return '—';
        if (opts.currency && isPortfolioExplanationNumeric(value)) {
            var amount = Number(value);
            var prefix = amount < 0 ? '-₹' : '₹';
            return prefix + Math.abs(amount).toLocaleString('en-IN', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            });
        }
        return String(value);
    }

    function makePortfolioExplanationMetaItem(label, value, extraClass) {
        return '<div class="trade-explain-step__box' + (extraClass ? ' ' + extraClass : '') + '">'
            + '<div class="trade-explain-step__box-title">' + escapeHtml(label) + '</div>'
            + '<div class="trade-explain-step__box-value">' + escapeHtml(value) + '</div>'
            + '</div>';
    }

    function renderPortfolioExplanationEntries(entries, type) {
        if (!Array.isArray(entries) || !entries.length) {
            return '<div class="trade-explain-step__empty">No ' + type + ' legs at this step.</div>';
        }
        return entries.map(function (entry) {
            var title = formatPortfolioExplanationValue(entry && entry.leg);
            if (type === 'realized') {
                return '<div class="trade-explain-step__entry">'
                    + '<div class="trade-explain-step__entry-title">' + escapeHtml(title) + '</div>'
                    + '<div class="trade-explain-step__entry-text">Exit '
                    + escapeHtml(formatPortfolioExplanationValue(entry && entry.exit_time))
                    + ' | Reason ' + escapeHtml(formatPortfolioExplanationValue(entry && entry.exit_reason))
                    + ' | P/L ' + escapeHtml(formatPortfolioExplanationValue(entry && entry.pnl, { currency: true }))
                    + '</div></div>';
            }
            return '<div class="trade-explain-step__entry">'
                + '<div class="trade-explain-step__entry-title">' + escapeHtml(title) + '</div>'
                + '<div class="trade-explain-step__entry-text">Entry '
                + escapeHtml(formatPortfolioExplanationValue(entry && entry.entry_time))
                + ' @ ' + escapeHtml(formatPortfolioExplanationValue(entry && entry.entry_price, { currency: true }))
                + ' | Candle ' + escapeHtml(formatPortfolioExplanationValue(entry && entry.candle_time))
                + ' @ ' + escapeHtml(formatPortfolioExplanationValue(entry && entry.cur_price, { currency: true }))
                + '</div>'
                + '<div class="trade-explain-step__entry-text">Qty '
                + escapeHtml(formatPortfolioExplanationValue(entry && entry.qty))
                + ' | Unrealized P/L '
                + escapeHtml(formatPortfolioExplanationValue(entry && entry.unrealized_pnl, { currency: true }))
                + '</div></div>';
        }).join('');
    }

    function renderPortfolioTradeExplanationSteps(steps) {
        if (!Array.isArray(steps) || !steps.length) {
            return '<div class="trade-explain-modal__empty">Trade explanation is not available for this trade.</div>';
        }
        return steps.map(function (step, stepIndex) {
            var breakdown = step && step.combined_mtm_breakdown ? step.combined_mtm_breakdown : null;
            var html = '<div class="trade-explain-step">'
                + '<div class="trade-explain-step__header">'
                + '<div class="trade-explain-step__badge">Step ' + escapeHtml(formatPortfolioExplanationValue(step && (step.step != null ? step.step : (stepIndex + 1)))) + '</div>'
                + '<div class="trade-explain-step__time">' + escapeHtml(formatPortfolioExplanationValue(step && step.time)) + '</div>'
                + '</div>'
                + '<div class="trade-explain-step__grid">'
                + makePortfolioExplanationMetaItem('Event Type', formatPortfolioExplanationValue(step && step.event_type))
                + makePortfolioExplanationMetaItem('Parent Leg', formatPortfolioExplanationValue(step && step.parent_leg))
                + makePortfolioExplanationMetaItem('Leg', formatPortfolioExplanationValue(step && step.leg))
                + makePortfolioExplanationMetaItem('Kind', formatPortfolioExplanationValue(step && step.kind))
                + makePortfolioExplanationMetaItem('Combined MTM', formatPortfolioExplanationValue(step && step.combined_mtm, { currency: true }), 'is-wide')
                + makePortfolioExplanationMetaItem('Overall SL Limit', formatPortfolioExplanationValue(step && step.overall_sl_limit), 'is-wide')
                + '</div>'
                + '<div class="trade-explain-step__section">'
                + '<div class="trade-explain-step__section-title">What happened</div>'
                + '<div class="trade-explain-step__section-text">' + escapeHtml(formatPortfolioExplanationValue(step && step.description)) + '</div>'
                + '</div>';
            if (breakdown) {
                html += '<div class="trade-explain-step__section">'
                    + '<div class="trade-explain-step__section-title">Combined MTM Breakdown</div>'
                    + '<div class="trade-explain-step__summary" style="margin-top:12px;">'
                    + makePortfolioExplanationMetaItem('Candle Time', formatPortfolioExplanationValue(breakdown.candle_time))
                    + makePortfolioExplanationMetaItem('Realized Total', formatPortfolioExplanationValue(breakdown.realized_total, { currency: true }))
                    + makePortfolioExplanationMetaItem('Unrealized Total', formatPortfolioExplanationValue(breakdown.unrealized_total, { currency: true }))
                    + makePortfolioExplanationMetaItem('Combined MTM', formatPortfolioExplanationValue(breakdown.combined_mtm, { currency: true }))
                    + '</div>'
                    + '<div class="trade-explain-step__subheading">Realized</div>'
                    + renderPortfolioExplanationEntries(breakdown.realized, 'realized')
                    + '<div class="trade-explain-step__subheading">Unrealized</div>'
                    + renderPortfolioExplanationEntries(breakdown.unrealized, 'unrealized')
                    + '</div>';
            }
            html += '</div>';
            return html;
        }).join('');
    }

    function closePortfolioTradeExplanationModal() {
        if (!portfolioTradeExplainModal) return;
        portfolioTradeExplainModal.hidden = true;
        document.body.style.overflow = '';
        portfolioTradeExplainState = null;
    }

    function renderPortfolioTradeExplanationModalState() {
        if (!portfolioTradeExplainModal || !portfolioTradeExplainBody || !portfolioTradeExplainSubtitle || !portfolioTradeExplainState) return;
        if (portfolioTradeExplainState.mode === 'date-group') {
            var groups = portfolioTradeExplainState.strategyGroups || [];
            var activeIndex = Number(portfolioTradeExplainState.activeStrategyIndex || 0);
            var activeGroup = null;
            if (activeIndex >= 0 && activeIndex < groups.length) {
                activeGroup = groups[activeIndex];
            }
            if (!activeGroup && groups.length) {
                activeGroup = groups[0];
                portfolioTradeExplainState.activeStrategyIndex = 0;
            }
            portfolioTradeExplainSubtitle.textContent = 'Overall Date Section | '
                + String(portfolioTradeExplainState.date || '—')
                + (activeGroup ? (' | ' + activeGroup.name) : '');
            var tabsHtml = groups.length > 1
                ? '<div class="trade-explain-tabs">' + groups.map(function (group, groupIndex) {
                    return '<button type="button" class="trade-explain-tab' + (groupIndex === portfolioTradeExplainState.activeStrategyIndex ? ' is-active' : '') + '" data-portfolio-explain-tab-index="' + groupIndex + '">' + escapeHtml(group.name) + '</button>';
                }).join('') + '</div>'
                : '';
            portfolioTradeExplainBody.innerHTML = tabsHtml + '<div id="portfolio-trade-explain-steps-panel">' + renderPortfolioTradeExplanationSteps(activeGroup ? activeGroup.steps : []) + '</div>';
            portfolioTradeExplainBody.querySelectorAll('[data-portfolio-explain-tab-index]').forEach(function (button) {
                button.addEventListener('click', function () {
                    var nextIndex = parseInt(button.getAttribute('data-portfolio-explain-tab-index') || '', 10);
                    var stepsPanel = document.getElementById('portfolio-trade-explain-steps-panel');
                    if (!Number.isFinite(nextIndex) || !portfolioTradeExplainState || portfolioTradeExplainState.activeStrategyIndex === nextIndex) return;
                    portfolioTradeExplainState.activeStrategyIndex = nextIndex;
                    portfolioTradeExplainBody.querySelectorAll('[data-portfolio-explain-tab-index]').forEach(function (tabButton, tabIndex) {
                        tabButton.classList.toggle('is-active', tabIndex === nextIndex);
                    });
                    if (stepsPanel) {
                        var nextGroup = groups[nextIndex];
                        stepsPanel.innerHTML = renderPortfolioTradeExplanationSteps(nextGroup ? nextGroup.steps : []);
                    }
                    var nextGroupForTitle = groups[nextIndex];
                    portfolioTradeExplainSubtitle.textContent = 'Overall Date Section | '
                        + String(portfolioTradeExplainState.date || '—')
                        + (nextGroupForTitle ? (' | ' + nextGroupForTitle.name) : '');
                    portfolioTradeExplainBody.scrollTop = 0;
                });
            });
            portfolioTradeExplainBody.scrollTop = 0;
            return;
        }
        var trade = portfolioTradeExplainState.trade;
        var tradePnl = getPortfolioTradePnlForSort(trade);
        var strategyName = trade && trade.strategy_name ? trade.strategy_name : 'Strategy';
        var steps = Array.isArray(trade && trade.trade_explanation_content && trade.trade_explanation_content.steps)
            ? trade.trade_explanation_content.steps
            : [];
        portfolioTradeExplainSubtitle.textContent = strategyName
            + ' | '
            + String(portfolioTradeExplainState.indexLabel || '—')
            + ' | '
            + String(trade && trade.date ? trade.date : '—')
            + ' | P/L '
            + formatPortfolioExplanationValue(tradePnl, { currency: true });
        portfolioTradeExplainBody.innerHTML = renderPortfolioTradeExplanationSteps(steps);
        portfolioTradeExplainBody.scrollTop = 0;
    }

    function openPortfolioTradeExplanationModal(trade, indexLabel) {
        if (!portfolioTradeExplainModal || !portfolioTradeExplainBody || !portfolioTradeExplainSubtitle) return;
        portfolioTradeExplainState = {
            mode: 'single-trade',
            trade: trade,
            indexLabel: indexLabel
        };
        renderPortfolioTradeExplanationModalState();
        portfolioTradeExplainModal.hidden = false;
        document.body.style.overflow = 'hidden';
    }

    function openPortfolioDateExplanationModal(group) {
        if (!portfolioTradeExplainModal || !portfolioTradeExplainBody || !portfolioTradeExplainSubtitle || !group) return;
        var groupedByStrategy = {};
        (group.trades || []).forEach(function (trade) {
            var key = String(trade && trade.strategy_name ? trade.strategy_name : 'Strategy');
            if (!groupedByStrategy[key]) {
                groupedByStrategy[key] = {
                    name: key,
                    steps: []
                };
            }
            var steps = Array.isArray(trade && trade.trade_explanation_content && trade.trade_explanation_content.steps)
                ? trade.trade_explanation_content.steps
                : [];
            steps.forEach(function (step) {
                groupedByStrategy[key].steps.push(step);
            });
        });
        var strategyGroups = Object.keys(groupedByStrategy).sort().map(function (key) {
            return groupedByStrategy[key];
        });
        portfolioTradeExplainState = {
            mode: 'date-group',
            date: group.date || '',
            strategyGroups: strategyGroups,
            activeStrategyIndex: 0
        };
        renderPortfolioTradeExplanationModalState();
        portfolioTradeExplainModal.hidden = false;
        document.body.style.overflow = 'hidden';
    }

    if (portfolioTradeExplainClose && !portfolioTradeExplainClose.dataset.boundPortfolioTradeExplainClose) {
        portfolioTradeExplainClose.dataset.boundPortfolioTradeExplainClose = 'true';
        portfolioTradeExplainClose.addEventListener('click', closePortfolioTradeExplanationModal);
    }

    if (portfolioTradeExplainModal && !portfolioTradeExplainModal.dataset.boundPortfolioTradeExplainOverlay) {
        portfolioTradeExplainModal.dataset.boundPortfolioTradeExplainOverlay = 'true';
        portfolioTradeExplainModal.addEventListener('click', function (event) {
            if (event.target && event.target.hasAttribute('data-portfolio-trade-explain-close')) {
                closePortfolioTradeExplanationModal();
            }
        });
        document.addEventListener('keydown', function (event) {
            if (event.key === 'Escape' && portfolioTradeExplainModal && !portfolioTradeExplainModal.hidden) {
                closePortfolioTradeExplanationModal();
            }
        });
    }

    function ensurePortfolioFullReportLayout() {
        var section = document.querySelector('[data-testid="trade-table"]');
        if (!section || section.dataset.dynamicFullReport === 'true') return;
        section.dataset.dynamicFullReport = 'true';
        section.innerHTML = '<div class="mb-5 flex flex-wrap items-center justify-between gap-5">'
            + '<div class="flex w-fit flex-col gap-1"><h4 class="text-2xl font-medium">Full Report</h4></div>'
            + '<div class="flex flex-wrap items-center gap-2">'
            + '<div id="select_SortBy:" class="relative flex items-center gap-2 justify-between">'
            + '<label class="flex items-center gap-2 text-xs font-normal leading-6 text-primaryBlack-750">Sort By:</label>'
            + '<div class="relative w-max"><select id="fr-sort-select" title="Sort By:" class="w-max cursor-pointer appearance-none rounded py-2 text-xs dark:bg-primaryBlack-350 md:text-xs/4 border border-primaryBlack-500 text-primaryBlack-750"><option value="EntryDate" class="w-max">Entry date</option><option value="ProfitLoss" class="w-max">P/L</option></select><svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" class="z-1 pointer-events-none absolute right-0 top-2.5 mr-3 h-4 w-4 dark:bg-primaryBlack-400 md:top-2 bg-primaryBlack-0"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg></div></div>'
            + '<div id="radio_" class="relative flex flex-wrap items-center justify-between gap-2"><div class="flex"><label id="fr-asc-label" class="cursor-pointer border border-tertiaryBlue-500 px-2 py-2 text-xs md:text-xs/4 bg-tertiaryBlue-500 text-white rounded-l">Asc</label><label id="fr-desc-label" class="cursor-pointer border border-tertiaryBlue-500 px-2 py-2 text-xs md:text-xs/4 rounded-r">Desc</label></div></div>'
            + '<span>Showing <b id="full-report-range">0 - 0</b> trades out of <b data-testid="total-trades" id="full-report-total">0</b></span>'
            + '</div></div>'
            + '<div class="w-full overflow-auto"><table class="w-full overflow-auto text-center"><thead class="rounded border-2 border-zinc-300 bg-primaryBlack-350 dark:border-primaryBlack-500"><tr>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Index</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Entry Date</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Entry Time</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Exit Date</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Exit Time</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Type</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Strike</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">B/S</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Qty</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Entry Type</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Entry Price</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Exit Type</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Exit Price</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Current Leg</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Reentry</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Reentry Reason</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Vix</th>'
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">P/L</th>'
            + '</tr></thead><tbody id="full-report-tbody" class="bg-primaryBlack-200"><tr class="border-b border-b-white"><td colspan="18" class="border-2 border-zinc-300 p-3 text-sm font-medium text-primaryBlack-600 dark:border-primaryBlack-500">Loading full report...</td></tr></tbody></table></div>'
            + '<div class="mt-5 flex flex-wrap items-center gap-5 justify-between"><button class="border border-primaryBlue-500 text-primaryBlue-500 hover:bg-primaryBlue-500 hover:text-white font-semibold leading-2 relative flex items-center justify-center gap-2 rounded px-3 py-3 text-center text-xs duration-75 md:px-4 md:py-3 md:leading-4">Download Report</button><div id="fr-pagination" class="flex items-center gap-1 rounded text-primaryBlue-500"></div></div>';
    }

    function formatReportPrice(value) {
        if (value === null || value === undefined || value === '') return '';
        return Number(value).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function formatReportPnl(value) {
        if (value === null || value === undefined || value === '') return '';
        return Number(value).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function getPortfolioTradePnlForSort(trade) {
        return toTradePnl(trade);
    }

    function sortPortfolioFullReportTrades() {
        var copy = portfolioFrAllTrades.slice();
        copy.sort(function (a, b) {
            var dateA = String((a && a.date) || '');
            var dateB = String((b && b.date) || '');
            var dateCmp = dateA < dateB ? -1 : dateA > dateB ? 1 : 0;
            var cmp = 0;
            if (portfolioFrSortBy === 'ProfitLoss') {
                cmp = getPortfolioTradePnlForSort(b) - getPortfolioTradePnlForSort(a);
                if (!cmp) {
                    var profitDateKeyA = dateA + ' ' + String((a && a.entry_time) || '');
                    var profitDateKeyB = dateB + ' ' + String((b && b.entry_time) || '');
                    cmp = profitDateKeyA < profitDateKeyB ? -1 : profitDateKeyA > profitDateKeyB ? 1 : 0;
                }
            } else {
                var ka = dateA + ' ' + String((a && a.entry_time) || '');
                var kb = dateB + ' ' + String((b && b.entry_time) || '');
                cmp = ka < kb ? -1 : ka > kb ? 1 : 0;
            }
            return portfolioFrSortDir === 'Desc' ? -cmp : cmp;
        });
        return copy;
    }

    function buildPortfolioFullReportDateGroups(trades) {
        var groups = [];
        var currentGroup = null;
        var tradeOffset = 0;

        (trades || []).forEach(function (trade) {
            var dateKey = String((trade && trade.date) || '');
            if (!currentGroup || currentGroup.date !== dateKey) {
                currentGroup = {
                    date: dateKey,
                    dayIndex: groups.length + 1,
                    startTradeIndex: tradeOffset,
                    trades: [],
                    totalPnl: 0
                };
                groups.push(currentGroup);
            }
            currentGroup.trades.push(trade);
            currentGroup.totalPnl += getPortfolioTradePnlForSort(trade);
            tradeOffset += 1;
        });

        return groups;
    }

    function renderPortfolioFullReport() {
        var tbody = document.getElementById('full-report-tbody');
        var totalEl = document.getElementById('full-report-total');
        var rangeEl = document.getElementById('full-report-range');
        var pagingEl = document.getElementById('fr-pagination');
        if (!tbody) return;

        var trades = sortPortfolioFullReportTrades();
        var totalTrades = trades.length;
        var dateGroups = buildPortfolioFullReportDateGroups(trades);
        var totalPages = Math.ceil(dateGroups.length / PORTFOLIO_FR_PAGE_SIZE);
        if (portfolioFrCurrentPage > totalPages) portfolioFrCurrentPage = 1;

        var startIdx = (portfolioFrCurrentPage - 1) * PORTFOLIO_FR_PAGE_SIZE;
        var pageGroups = dateGroups.slice(startIdx, startIdx + PORTFOLIO_FR_PAGE_SIZE);
        var cellCls = 'border-2 border-zinc-300 p-1 text-xs font-medium dark:border-primaryBlack-500';
        var html = '';

        pageGroups.forEach(function (group) {
            var dayRowBg = group.totalPnl < 0 ? '#dc2626' : '#16a34a';
            html += '<tr class="border-b-2 border-b-white" style="background:' + dayRowBg + '; color:#ffffff;">'
                + '<td class="' + cellCls + ' font-bold" style="background:' + dayRowBg + '; color:#ffffff;">' + group.dayIndex + '</td>'
                + '<td class="' + cellCls + ' font-semibold" style="background:' + dayRowBg + '; color:#ffffff;">' + escapeHtml(group.date || '') + '</td>'
                + '<td colspan="15" class="' + cellCls + ' text-left font-semibold" style="background:' + dayRowBg + '; color:#ffffff;"><div class="flex items-center justify-between gap-3"><span>Overall Date Section</span><button type="button" data-portfolio-date-view="' + group.dayIndex + '" class="inline-flex min-w-[72px] items-center justify-center rounded-full border px-4 text-xs font-semibold" style="background-color: rgb(238, 246, 255); height: 28px; border-color: rgb(15, 98, 165); color: rgb(15, 98, 165);">View</button></div></td>'
                + '<td class="' + cellCls + ' font-bold" style="background:' + dayRowBg + '; color:#ffffff;">' + formatReportPnl(group.totalPnl) + '</td>'
                + '</tr>';

            group.trades.forEach(function (trade, tradeIndex) {
                var globalIdx = group.startTradeIndex + tradeIndex + 1;
                var tradePnl = getPortfolioTradePnlForSort(trade);
                var pnlCls = tradePnl < 0 ? 'text-red-500' : 'text-green-600';

                html += '<tr class="border-b border-b-white" style="background:#f1f1f1;">'
                    + '<td class="' + cellCls + ' font-bold" style="background:#f1f1f1;">' + globalIdx + '</td>'
                    + '<td class="' + cellCls + '" style="background:#f1f1f1;">' + escapeHtml(trade.date || '') + '</td>'
                    + '<td class="' + cellCls + '" style="background:#f1f1f1;">' + escapeHtml(trade.entry_time || '') + '</td>'
                    + '<td class="' + cellCls + '" style="background:#f1f1f1;">' + escapeHtml(trade.date || '') + '</td>'
                    + '<td class="' + cellCls + '" style="background:#f1f1f1;">' + escapeHtml(trade.exit_time || '') + '</td>'
                    + '<td colspan="12" class="' + cellCls + ' text-center font-semibold text-primaryBlack-750" style="background:#f1f1f1;"><div class="flex items-center justify-center gap-3"><span>' + escapeHtml(trade.strategy_name || '') + '</span><button type="button" data-portfolio-trade-view="' + globalIdx + '" class="inline-flex min-w-[72px] items-center justify-center rounded-full border px-4 text-xs font-semibold" style="background-color: rgb(238, 246, 255); height: 28px; border-color: rgb(15, 98, 165); color: rgb(15, 98, 165);">View</button></div></td>'
                    + '<td class="' + cellCls + ' ' + pnlCls + ' font-bold" style="background:#f1f1f1;">' + formatReportPnl(tradePnl) + '</td>'
                    + '</tr>';

                (trade.legs || []).forEach(function (leg, legIndex) {
                    var parentIdx = globalIdx + '.' + (legIndex + 1);
                    var qty = Number(leg.lots || 0) * Number(leg.lot_size || 0);
                    var legPnl = Number(leg.pnl || 0) - Number(getPortfolioDetailBrokerage(leg, null) || 0);
                    var legPnlCls = Number(legPnl || 0) < 0 ? 'text-red-500' : 'text-green-600';
                    var legLabel = 'Leg ' + (legIndex + 1);

                    html += '<tr class="border-b border-b-white" style="background:rgb(234, 242, 255);">'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);">' + parentIdx + '</td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);">' + escapeHtml(trade.date || '') + '</td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);">' + escapeHtml(leg.entry_time || '') + '</td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);">' + escapeHtml(trade.date || '') + '</td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);">' + escapeHtml(leg.exit_time || '') + '</td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);">' + escapeHtml(leg.type || '') + '</td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);">' + escapeHtml(leg.strike || '') + '</td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);">' + escapeHtml(leg.position || '') + '</td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);">' + escapeHtml(qty || '') + '</td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);"></td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);">' + formatReportPrice(leg.entry_price) + '</td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);"></td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);">' + formatReportPrice(leg.exit_price) + '</td>'
                        + '<td class="' + cellCls + ' font-semibold" style="background:rgb(234, 242, 255);">' + legLabel + '</td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);"></td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);">' + escapeHtml(leg.exit_reason || '') + '</td>'
                        + '<td class="' + cellCls + '" style="background:rgb(234, 242, 255);"></td>'
                        + '<td class="' + cellCls + ' ' + legPnlCls + '" style="background:rgb(234, 242, 255);">' + formatReportPnl(legPnl) + '</td>'
                        + '</tr>';

                    var subTrades = leg.sub_trades && leg.sub_trades.length ? leg.sub_trades : [{}];
                    subTrades.forEach(function (st, subIndex) {
                        var childIdx = parentIdx + '.' + (subIndex + 1);
                        var rawStPnl = st && st.pnl !== undefined ? st.pnl : leg.pnl;
                        var detailBrokerage = getPortfolioDetailBrokerage(leg, st);
                        var stPnl = Number(rawStPnl || 0) - Number(detailBrokerage || 0);
                        var stPnlCls = Number(stPnl || 0) < 0 ? 'text-red-500' : 'text-green-600';
                        var optionLabel = st && st.option_type ? ' (' + st.option_type + ')' : (leg.type ? ' (' + leg.type + ')' : '');
                        var subLegLabel = legLabel + optionLabel;
                        html += '<tr class="border-b border-b-white" style="background:#ffffff;">'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + childIdx + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + escapeHtml(st && st.entry_date ? st.entry_date : (trade.date || '')) + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + escapeHtml(st && st.entry_time ? st.entry_time : (leg.entry_time || '')) + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + escapeHtml(st && st.exit_date ? st.exit_date : (trade.date || '')) + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + escapeHtml(st && st.exit_time ? st.exit_time : (leg.exit_time || '')) + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + escapeHtml(st && st.option_type ? st.option_type : (leg.type || '')) + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + escapeHtml(st && st.strike ? st.strike : (leg.strike || '')) + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + escapeHtml(leg.position || '') + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + escapeHtml(qty || '') + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + escapeHtml(st && st.entry_action ? st.entry_action : '') + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + formatReportPrice(st && st.entry_price !== undefined ? st.entry_price : leg.entry_price) + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + escapeHtml(st && st.exit_action ? st.exit_action : '') + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + formatReportPrice(st && st.exit_price !== undefined ? st.exit_price : leg.exit_price) + '</td>'
                            + '<td class="' + cellCls + ' font-semibold" style="background:#ffffff;">' + escapeHtml(subLegLabel) + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + escapeHtml(st && st.reentry_number !== undefined && st.reentry_number !== null ? st.reentry_number : '') + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;">' + escapeHtml(st && st.exit_reason ? st.exit_reason : (leg.exit_reason || '')) + '</td>'
                            + '<td class="' + cellCls + '" style="background:#ffffff;"></td>'
                            + '<td class="' + cellCls + ' ' + stPnlCls + '" style="background:#ffffff;">' + formatReportPnl(stPnl) + '</td>'
                            + '</tr>';
                    });
                });
            });
        });

        tbody.innerHTML = html || '<tr><td colspan="19" class="' + cellCls + ' p-3 text-center text-primaryBlack-600">No data.</td></tr>';
        var from = pageGroups.length ? pageGroups[0].startTradeIndex + 1 : 0;
        var to = pageGroups.length
            ? pageGroups[pageGroups.length - 1].startTradeIndex + pageGroups[pageGroups.length - 1].trades.length
            : 0;
        if (totalEl) totalEl.textContent = String(totalTrades);
        if (rangeEl) rangeEl.textContent = from + ' - ' + to;

        tbody.querySelectorAll('[data-portfolio-trade-view]').forEach(function (button) {
            button.addEventListener('click', function () {
                var rowNumber = parseInt(button.getAttribute('data-portfolio-trade-view') || '', 10);
                if (!Number.isFinite(rowNumber) || rowNumber < 1) return;
                var trade = trades[rowNumber - 1];
                if (!trade) return;
                openPortfolioTradeExplanationModal(trade, 'Index ' + rowNumber);
            });
        });
        tbody.querySelectorAll('[data-portfolio-date-view]').forEach(function (button) {
            button.addEventListener('click', function () {
                var dayIndex = parseInt(button.getAttribute('data-portfolio-date-view') || '', 10);
                if (!Number.isFinite(dayIndex) || dayIndex < 1) return;
                var selectedGroup = null;
                pageGroups.forEach(function (group) {
                    if (group.dayIndex === dayIndex) selectedGroup = group;
                });
                if (!selectedGroup) return;
                openPortfolioDateExplanationModal(selectedGroup);
            });
        });

        if (!pagingEl) return;
        var maxVisible = 5;
        var btnBase = 'border border-primaryBlue-500 min-w-[28px] px-2 py-0.5 text-sm rounded cursor-pointer';
        var btnActive = btnBase + ' bg-primaryBlue-500 text-white font-semibold';
        var btnNav = 'text-primaryBlue-500 px-1 py-0.5 cursor-pointer disabled:opacity-30';
        var half = Math.floor(maxVisible / 2);
        var start = Math.max(1, portfolioFrCurrentPage - half);
        var end = Math.min(totalPages, start + maxVisible - 1);
        if (end - start < maxVisible - 1) start = Math.max(1, end - maxVisible + 1);

        if (!totalPages) {
            pagingEl.innerHTML = '';
            return;
        }

        var pHtml = '';
        pHtml += '<button class="' + btnNav + '" data-pg="1" title="First">«</button>';
        pHtml += '<button class="' + btnNav + '" data-pg="' + Math.max(1, portfolioFrCurrentPage - 1) + '" title="Prev">‹</button>';
        for (var p = start; p <= end; p += 1) {
            pHtml += '<button class="' + (p === portfolioFrCurrentPage ? btnActive : btnBase) + '" data-pg="' + p + '">' + p + '</button>';
        }
        pHtml += '<button class="' + btnNav + '" data-pg="' + Math.min(totalPages, portfolioFrCurrentPage + 1) + '" title="Next">›</button>';
        pHtml += '<button class="' + btnNav + '" data-pg="' + totalPages + '" title="Last">»</button>';

        pagingEl.innerHTML = pHtml;
        pagingEl.querySelectorAll('button[data-pg]').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var pg = parseInt(this.getAttribute('data-pg'), 10);
                if (pg < 1 || pg > totalPages || pg === portfolioFrCurrentPage) return;
                portfolioFrCurrentPage = pg;
                renderPortfolioFullReport();
            });
        });
    }

    function initPortfolioFullReportControls() {
        ensurePortfolioFullReportLayout();
        var sel = document.getElementById('fr-sort-select');
        var ascLbl = document.getElementById('fr-asc-label');
        var descLbl = document.getElementById('fr-desc-label');

        function applyDir(dir) {
            portfolioFrSortDir = dir;
            portfolioFrCurrentPage = 1;
            if (ascLbl) ascLbl.className = 'cursor-pointer border border-tertiaryBlue-500 px-2 py-2 text-xs md:text-xs/4 rounded-l' + (dir === 'Asc' ? ' bg-tertiaryBlue-500 text-white' : ' text-primaryBlack-750');
            if (descLbl) descLbl.className = 'cursor-pointer border border-tertiaryBlue-500 px-2 py-2 text-xs md:text-xs/4 rounded-r' + (dir === 'Desc' ? ' bg-tertiaryBlue-500 text-white' : ' text-primaryBlack-750');
            renderPortfolioFullReport();
        }

        if (sel && !sel.dataset.boundFullReport) {
            sel.dataset.boundFullReport = 'true';
            sel.value = portfolioFrSortBy;
            sel.addEventListener('change', function () {
                portfolioFrSortBy = this.value;
                portfolioFrCurrentPage = 1;
                renderPortfolioFullReport();
            });
        }
        if (ascLbl && !ascLbl.dataset.boundFullReport) {
            ascLbl.dataset.boundFullReport = 'true';
            ascLbl.addEventListener('click', function () { applyDir('Asc'); });
        }
        if (descLbl && !descLbl.dataset.boundFullReport) {
            descLbl.dataset.boundFullReport = 'true';
            descLbl.addEventListener('click', function () { applyDir('Desc'); });
        }
        applyDir(portfolioFrSortDir);
    }

    function renderFullReportTable(strategyResults) {
        ensurePortfolioFullReportLayout();
        portfolioFrAllTrades = [];
        var nameMap = getStrategyNameMap();
        strategyResults.forEach(function (item) {
            var trades = item && item.results && Array.isArray(item.results.trades) ? item.results.trades : [];
            trades.forEach(function (trade) {
                portfolioFrAllTrades.push(Object.assign({}, trade, {
                    strategy_name: nameMap[String(item.item_id)] || ''
                }));
            });
        });
        if (portfolioFrCurrentPage < 1) portfolioFrCurrentPage = 1;
        renderPortfolioFullReport();
    }

    var pnlChart = null;
    var ddChart = null;

    function formatChartDate(dateStr) {
        if (!dateStr) return '';
        var p = String(dateStr).split('-');
        var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        if (p.length !== 3) return dateStr;
        return p[2] + ' ' + months[parseInt(p[1], 10) - 1] + ' ' + p[0];
    }

    function buildChartSeries(periods) {
        var pnlByDay = {};
        periods.forEach(function (trade) {
            if (!trade || !trade.date) return;
            pnlByDay[trade.date] = (pnlByDay[trade.date] || 0) + toTradePnl(trade);
        });

        var days = Object.keys(pnlByDay).sort();
        var cumulative = 0;
        var peak = 0;
        var pnlValues = [];
        var drawdownValues = [];

        days.forEach(function (day) {
            cumulative += pnlByDay[day];
            if (cumulative > peak) peak = cumulative;
            pnlValues.push(parseFloat(cumulative.toFixed(2)));
            drawdownValues.push(parseFloat((cumulative - peak).toFixed(2)));
        });

        return { labels: days, pnlValues: pnlValues, drawdownValues: drawdownValues };
    }

    function destroyPortfolioCharts() {
        if (pnlChart) { pnlChart.destroy(); pnlChart = null; }
        if (ddChart) { ddChart.destroy(); ddChart = null; }
    }

    function renderPortfolioPnlChart(labels, values) {
        var canvas = document.getElementById('pnl-chart-canvas');
        if (!canvas || typeof Chart === 'undefined') return;
        var ctx = canvas.getContext('2d');
        if (pnlChart) pnlChart.destroy();

        pnlChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Cumulative PnL',
                    data: values,
                    borderColor: '#22c55e',
                    backgroundColor: 'rgba(34,197,94,0.08)',
                    borderWidth: 3,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    fill: false,
                    tension: 0.3
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        display: true,
                        position: 'top',
                        align: 'center',
                        labels: {
                            usePointStyle: false,
                            boxWidth: 16,
                            boxHeight: 16,
                            font: { size: 12 },
                            color: '#374151'
                        }
                    },
                    tooltip: {
                        callbacks: {
                            title: function (items) { return formatChartDate(items[0].label); },
                            label: function (item) {
                                var v = item.parsed.y;
                                return ' Cumulative PnL: ' + (v < 0 ? '-' : '') + Math.abs(Math.round(v)).toLocaleString('en-IN');
                            }
                        }
                    },
                    zoom: {
                        zoom: {
                            wheel: { enabled: true },
                            pinch: { enabled: true },
                            drag: { enabled: true, backgroundColor: 'rgba(34,197,94,0.1)' },
                            mode: 'x'
                        },
                        pan: { enabled: true, mode: 'x' }
                    }
                },
                scales: {
                    x: {
                        grid: { color: 'rgba(0,0,0,0.05)' },
                        ticks: {
                            maxTicksLimit: 12,
                            color: '#6b7280',
                            font: { size: 11 },
                            callback: function (val) { return formatChartDate(this.getLabelForValue(val)); }
                        }
                    },
                    y: {
                        grid: { color: 'rgba(0,0,0,0.05)' },
                        ticks: {
                            color: '#6b7280',
                            font: { size: 11 },
                            callback: function (v) {
                                if (Math.abs(v) >= 1000) return (v / 1000).toFixed(0) + 'k';
                                return v;
                            }
                        },
                        title: {
                            display: true,
                            text: 'Cumulative PnL',
                            color: '#6b7280',
                            font: { size: 11 }
                        }
                    }
                }
            }
        });

        var resetBtn = document.getElementById('pnl-reset-zoom');
        if (resetBtn) resetBtn.onclick = function () { pnlChart.resetZoom(); };
    }

    function renderPortfolioDrawdownChart(labels, values) {
        var canvas = document.getElementById('dd-chart-canvas');
        if (!canvas || typeof Chart === 'undefined') return;
        var ctx = canvas.getContext('2d');
        if (ddChart) ddChart.destroy();

        ddChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Drawdown',
                    data: values,
                    borderColor: '#ef4444',
                    backgroundColor: 'rgba(239,68,68,0.08)',
                    borderWidth: 3,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    fill: false,
                    tension: 0.3
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            title: function (items) { return formatChartDate(items[0].label); },
                            label: function (item) {
                                var v = item.parsed.y;
                                return ' Drawdown: ' + (v < 0 ? '-' : '') + Math.abs(Math.round(v)).toLocaleString('en-IN');
                            }
                        }
                    },
                    zoom: {
                        zoom: {
                            wheel: { enabled: true },
                            pinch: { enabled: true },
                            drag: { enabled: true, backgroundColor: 'rgba(239,68,68,0.1)' },
                            mode: 'x'
                        },
                        pan: { enabled: true, mode: 'x' }
                    }
                },
                scales: {
                    x: {
                        grid: { color: 'rgba(0,0,0,0.05)' },
                        ticks: {
                            maxTicksLimit: 12,
                            color: '#6b7280',
                            font: { size: 11 },
                            callback: function (val) { return formatChartDate(this.getLabelForValue(val)); }
                        }
                    },
                    y: {
                        grid: { color: 'rgba(0,0,0,0.05)' },
                        ticks: {
                            color: '#6b7280',
                            font: { size: 11 },
                            callback: function (v) {
                                if (Math.abs(v) >= 1000) return (v / 1000).toFixed(0) + 'k';
                                return v;
                            }
                        },
                        title: {
                            display: true,
                            text: 'Drawdown',
                            color: '#6b7280',
                            font: { size: 11 }
                        }
                    }
                }
            }
        });

        var resetBtn = document.getElementById('dd-reset-zoom');
        if (resetBtn) resetBtn.onclick = function () { ddChart.resetZoom(); };
    }

    function renderPortfolioCharts(periods) {
        if (!periods || !periods.length) {
            destroyPortfolioCharts();
            return;
        }
        var series = buildChartSeries(periods);
        renderPortfolioPnlChart(series.labels, series.pnlValues);
        renderPortfolioDrawdownChart(series.labels, series.drawdownValues);
    }

    function getCheckedPortfolioStrategyIds() {
        var ids = [];
        document.querySelectorAll('#portfolio-strategy-rows [data-strategy-id]').forEach(function (row) {
            var checkbox = row.querySelector('input[id^="portfolio-strategy-"]');
            if (checkbox && checkbox.checked) {
                ids.push(String(row.getAttribute('data-strategy-id') || ''));
            }
        });
        return ids;
    }

    function getVisiblePortfolioStrategyIds() {
        var ids = [];
        document.querySelectorAll('#portfolio-strategy-rows [data-strategy-id]').forEach(function (row) {
            var strategyId = String(row.getAttribute('data-strategy-id') || '');
            if (strategyId) {
                ids.push(strategyId);
            }
        });
        return ids;
    }

    function buildDailyPnlMap(trades) {
        var pnlByDay = {};
        (trades || []).forEach(function (trade) {
            if (!trade || !trade.date) return;
            pnlByDay[trade.date] = (pnlByDay[trade.date] || 0) + toTradePnl(trade);
        });
        return pnlByDay;
    }

    function computeCorrelationFromMaps(mapA, mapB) {
        var keysA = Object.keys(mapA || {});
        var keysB = Object.keys(mapB || {});
        if (!keysA.length || !keysB.length) return -999;
        var allDaysObj = {};
        keysA.forEach(function (day) { allDaysObj[day] = true; });
        keysB.forEach(function (day) { allDaysObj[day] = true; });
        var days = Object.keys(allDaysObj).sort();
        if (days.length < 2) return 1;

        var valuesA = days.map(function (day) { return Number(mapA[day] || 0); });
        var valuesB = days.map(function (day) { return Number(mapB[day] || 0); });
        var meanA = valuesA.reduce(function (sum, value) { return sum + value; }, 0) / valuesA.length;
        var meanB = valuesB.reduce(function (sum, value) { return sum + value; }, 0) / valuesB.length;
        var numerator = 0;
        var denomA = 0;
        var denomB = 0;

        for (var i = 0; i < valuesA.length; i += 1) {
            var aDiff = valuesA[i] - meanA;
            var bDiff = valuesB[i] - meanB;
            numerator += aDiff * bDiff;
            denomA += aDiff * aDiff;
            denomB += bDiff * bDiff;
        }

        if (!denomA || !denomB) return -999;
        return numerator / Math.sqrt(denomA * denomB);
    }

    function correlationColor(value) {
        if (value === -999) return '#d7dee8';

        var clamped = Math.max(-1, Math.min(1, Number(value) || 0));
        var intensity = Math.pow(Math.abs(clamped), 0.85);
        var mix = 0.22 + (intensity * 0.78);

        if (clamped >= 0) {
            var startPos = [220, 232, 248];
            var endPos = [58, 86, 153];
            var rPos = Math.round(startPos[0] + (endPos[0] - startPos[0]) * mix);
            var gPos = Math.round(startPos[1] + (endPos[1] - startPos[1]) * mix);
            var bPos = Math.round(startPos[2] + (endPos[2] - startPos[2]) * mix);
            return 'rgb(' + rPos + ',' + gPos + ',' + bPos + ')';
        }

        var startNeg = [249, 224, 220];
        var endNeg = [179, 59, 47];
        var rNeg = Math.round(startNeg[0] + (endNeg[0] - startNeg[0]) * mix);
        var gNeg = Math.round(startNeg[1] + (endNeg[1] - startNeg[1]) * mix);
        var bNeg = Math.round(startNeg[2] + (endNeg[2] - startNeg[2]) * mix);
        return 'rgb(' + rNeg + ',' + gNeg + ',' + bNeg + ')';
    }

    function renderCorrelationMatrix(strategyResults, onlyChecked) {
        var panel = document.getElementById('correlation-matrix-panel');
        var grid = document.getElementById('correlation-matrix-grid');
        if (!panel || !grid) return;

        var checkedIds = onlyChecked ? getCheckedPortfolioStrategyIds() : [];
        var nameMap = getStrategyNameMap();
        var rows = strategyResults.filter(function (item) {
            return !onlyChecked || checkedIds.indexOf(String(item.item_id)) !== -1;
        }).map(function (item, index) {
            return {
                id: String(item.item_id),
                name: nameMap[String(item.item_id)] || ('Strategy ' + (index + 1)),
                daily: buildDailyPnlMap(item && item.results ? item.results.trades : [])
            };
        });

        panel.hidden = false;

        if (!rows.length) {
            grid.style.gridTemplateColumns = '180px';
            grid.innerHTML = '<div class="correlation-axis-cell is-empty" style="min-height:120px;">No strategies available for correlation.</div>';
            return;
        }

        grid.style.gridTemplateColumns = '180px repeat(' + rows.length + ', minmax(92px, 92px))';
        var html = '<div class="correlation-axis-cell is-corner"></div>';
        rows.forEach(function (row) {
            html += '<div class="correlation-axis-cell is-x">' + escapeHtml(row.name) + '</div>';
        });

        rows.forEach(function (rowY, rowIndex) {
            html += '<div class="correlation-axis-cell">' + escapeHtml(rowY.name) + '</div>';
            rows.forEach(function (rowX, colIndex) {
                if (colIndex > rowIndex) {
                    html += '<div class="correlation-cell is-empty"></div>';
                    return;
                }
                var value = rowIndex === colIndex ? 1 : computeCorrelationFromMaps(rowY.daily, rowX.daily);
                var label = value === -999 ? '-999' : (Math.round(value * 100) / 100).toFixed(2).replace(/\.00$/, '');
                html += '<div class="correlation-cell' + (value === -999 ? ' is-empty' : '') + '" style="background:' + correlationColor(value) + ';">' + label + '</div>';
            });
        });

        grid.innerHTML = html;
    }

    function bindCorrelationMatrixControls() {
        var loadBtn = document.querySelector('[data-testid="correlation-matrix-button"]');
        var hideBtn = document.getElementById('correlation-matrix-hide');
        var onlyChecked = document.getElementById('correlation-only-checked');
        var panel = document.getElementById('correlation-matrix-panel');

        if (loadBtn && !loadBtn.dataset.boundCorrelation) {
            loadBtn.dataset.boundCorrelation = 'true';
            loadBtn.addEventListener('click', function () {
                if (!window._portfolioResult) return;
                renderCorrelationMatrix(getStrategyResults(window._portfolioResult), !!(onlyChecked && onlyChecked.checked));
            });
        }

        if (hideBtn && !hideBtn.dataset.boundCorrelation) {
            hideBtn.dataset.boundCorrelation = 'true';
            hideBtn.addEventListener('click', function () {
                if (panel) panel.hidden = true;
            });
        }

        if (onlyChecked && !onlyChecked.dataset.boundCorrelation) {
            onlyChecked.dataset.boundCorrelation = 'true';
            onlyChecked.addEventListener('change', function () {
                if (!window._portfolioResult || (panel && panel.hidden)) return;
                renderCorrelationMatrix(getStrategyResults(window._portfolioResult), !!onlyChecked.checked);
            });
        }
    }

    function renderPortfolioBacktestReports(payload) {
        if (!window._portfolioBacktestQtySnapshot) {
            if (typeof window.setPortfolioBacktestQtySnapshot === 'function') {
                window.setPortfolioBacktestQtySnapshot(buildPortfolioBacktestQtySnapshot(payload));
            } else {
                window._portfolioBacktestQtySnapshot = buildPortfolioBacktestQtySnapshot(payload);
            }
        }
        var strategyResults = getStrategyResults(payload);
        var allStrategyResults = getAllStrategyResults(payload);
        var visibleTrades = [];
        strategyResults.forEach(function (item) {
            var trades = item && item.results && Array.isArray(item.results.trades) ? item.results.trades : [];
            trades.forEach(function (trade) { visibleTrades.push(trade); });
        });
        window._portfolioVisibleTrades = visibleTrades;
        var combinedPeriods = buildCombinedPeriods(strategyResults);
        renderPortfolioCharts(combinedPeriods);
        renderYearWiseTable(computeYearWiseFromPeriods(combinedPeriods));
        buildSummaryTable(strategyResults, allStrategyResults);
        renderFullReportTable(strategyResults);
        syncPortfolioCostAnalysis();
        var correlationPanel = document.getElementById('correlation-matrix-panel');
        var correlationOnlyChecked = document.getElementById('correlation-only-checked');
        if (correlationPanel && !correlationPanel.hidden) {
            renderCorrelationMatrix(strategyResults, !!(correlationOnlyChecked && correlationOnlyChecked.checked));
        }
        if (typeof window._setPortfolioProgressVisible === 'function') {
            window._setPortfolioProgressVisible(false);
        }
        if (typeof window._setPortfolioResultSectionVisible === 'function') {
            window._setPortfolioResultSectionVisible(true);
        }
    }

    window._renderPortfolioBacktestReports = renderPortfolioBacktestReports;
    window._onPortfolioBacktestResult = renderPortfolioBacktestReports;
    bindCorrelationMatrixControls();
    bindPortfolioCostAnalysis();
    initPortfolioFullReportControls();

    function clearInitialStaticPortfolioMarkup() {
        setTitle('Portfolio');
        setSummary(0);
        buildSummaryTable([], []);
        renderYearWiseTable({});
        renderFullReportTable([]);
        if (typeof window._resetPortfolioBacktestView === 'function') {
            window._resetPortfolioBacktestView();
        }

        var desktopStats = document.querySelector('.hidden.w-full.text-xs.md\\:grid');
        if (desktopStats) {
            desktopStats.remove();
        }
    }

    clearInitialStaticPortfolioMarkup();

    fetch(SAMPLE_RESULT_URL)
        .then(function (response) {
            if (!response.ok) throw new Error('Failed to load sample result');
            return response.json();
        })
        .then(function (payload) {
            if (!window._portfolioResult) {
                window._portfolioResult = payload;
            }
        })
        .catch(function () {
            renderYearWiseTable({});
        });
})();
