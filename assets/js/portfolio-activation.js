function getPortfolioActivationApiUrl(routeName, suffix) {
    if (typeof window.buildNamedApiUrl === 'function') {
        return window.buildNamedApiUrl(routeName, suffix);
    }
    var hostname = String((window.location && window.location.hostname) || '').toLowerCase();
    var baseUrl = (window.APP_CONFIG && window.APP_CONFIG.algoApiBaseUrl)
        || window.APP_ALGO_API_BASE_URL
        || ((hostname === 'finedgealgo.com' || hostname === 'www.finedgealgo.com')
            ? 'https://finedgealgo.com/algo'
            : 'http://localhost:8000/algo');
    var routeMap = window.APP_API_ROUTES || {};
    var routePath = routeMap[routeName] || routeName || '';
    var normalizedRoute = String(routePath).replace(/\/+$/, '');
    var normalizedSuffix = String(suffix || '').replace(/^\/+/, '');
    return baseUrl.replace(/\/+$/, '') + '/' + (normalizedSuffix ? normalizedRoute + '/' + normalizedSuffix : normalizedRoute);
}

(function () {
    var params = new URLSearchParams(window.location.search);
    var portfolioId = params.get('strategy_id');
    var activationMode = (params.get('status') || '').trim() || 'algo-backtest';
    var requestedCurrentDateTime = (params.get('current_datetime') || '').trim();
    document.body.setAttribute('data-activation-mode', activationMode);
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
    var qtyValueEl = document.getElementById('portfolio-qty-value');
    var qtyDecrementBtn = document.getElementById('portfolio-qty-decrement');
    var qtyIncrementBtn = document.getElementById('portfolio-qty-increment');
    var brokerFilter = document.getElementById('portfolio-broker-filter');
    var brokerSelect = document.getElementById('portfolio-broker-select');
    var editPortfolioLinkBtn = document.getElementById('portfolio-edit-link-btn');
    var slippageFilter = document.getElementById('portfolio-slippage-filter');
    var slippageButton = document.querySelector('[data-portfolio-slippage-button]');
    var slippagePopover = document.getElementById('portfolio-slippage-popover');
    var slippageInput = document.getElementById('portfolio-slippage-input');
    var slippageApply = document.getElementById('portfolio-slippage-apply');
    var updatePortfolioBtn = document.querySelector('[data-testid="portfolio-update-btn"]');
    var saveAsNewPortfolioBtn = document.querySelector('[data-testid="portfolio-save-as-new-btn"]');
    var activateSelectedBtn = document.querySelector('[data-testid="portfolio-recent-backtests-btn"]');
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
    var loadedPortfolioData = null;
    var toastTimer = null;
    var availableBrokers = [];
    var DEFAULT_BACKTEST_BROKER_TYPE_ID = '69dcf57211877c164638d2aa';
    var DEFAULT_ACTIVATION_USER_ID = '69dcf52711877c164638d2a7';
    var executionConfigModalState = {
        strategyId: '',
        strategyName: '',
        detail: null,
        workingDetail: null,
        activeLegIndex: 0
    };

    function isExecutionConfigEditableMode() {
        var normalizedMode = String(activationMode || '').trim().toLowerCase();
        return normalizedMode === 'live' || normalizedMode === 'fast-forward' || normalizedMode === 'fast-forwarding';
    }

    function cloneJson(value) {
        if (value === null || value === undefined) {
            return value;
        }
        try {
            return JSON.parse(JSON.stringify(value));
        } catch (error) {
            return value;
        }
    }

    function createDefaultLegExecutionConfig() {
        return {
            ProductType: 'ProductType.NRML',
            ExitOrder: {
                Type: 'OrderType.Limit',
                Value: {
                    Buffer: {
                        Type: 'BufferType.Points',
                        Value: { TriggerBuffer: 0, LimitBuffer: 3 }
                    },
                    Modification: {
                        ModificationFrequency: 5,
                        ContinuousMonitoring: 'True',
                        MarketOrderAfter: 1
                    }
                }
            },
            EntryOrder: {
                Type: 'OrderType.Limit',
                Value: {
                    Buffer: {
                        Type: 'BufferType.Points',
                        Value: { TriggerBuffer: 0, LimitBuffer: 3 }
                    },
                    Modification: {
                        MarketOrderAfter: 40
                    }
                }
            },
            ReferenceForTgtSL: 'PriceReferenceType.Trigger',
            EntryDelay: 0
        };
    }

    function ensureExecutionConfigDetail(detail, strategyState) {
        var normalizedDetail = detail && typeof detail === 'object' ? detail : {};
        var fullConfig = normalizedDetail.full_config && typeof normalizedDetail.full_config === 'object'
            ? normalizedDetail.full_config
            : (normalizedDetail.full_config = {});
        var strategy = fullConfig.strategy && typeof fullConfig.strategy === 'object'
            ? fullConfig.strategy
            : (fullConfig.strategy = {});
        var parentLegs = Array.isArray(strategy.ListOfLegConfigs) ? strategy.ListOfLegConfigs : [];
        var executionBase = normalizedDetail.execution_config_base && typeof normalizedDetail.execution_config_base === 'object'
            ? normalizedDetail.execution_config_base
            : {};
        var executionExtra = normalizedDetail.execution_config_extra && typeof normalizedDetail.execution_config_extra === 'object'
            ? normalizedDetail.execution_config_extra
            : {};
        var legExecutionList = Array.isArray(executionExtra.ListOfLegExecutionConfig)
            ? executionExtra.ListOfLegExecutionConfig.slice()
            : [];

        normalizedDetail.execution_config_base = Object.assign({
            Multiplier: strategyState && strategyState.qty_multiplier ? intOrDefault(strategyState.qty_multiplier, 1) : 1,
            LikeBacktester: activationMode !== 'live',
            MarginAutoSquareOff: true,
            TimeDelta: 0
        }, executionBase);

        while (legExecutionList.length < parentLegs.length) {
            legExecutionList.push(createDefaultLegExecutionConfig());
        }
        if (legExecutionList.length > parentLegs.length) {
            legExecutionList = legExecutionList.slice(0, parentLegs.length);
        }
        legExecutionList = legExecutionList.map(function (item) {
            var mergedItem = cloneJson(createDefaultLegExecutionConfig());
            if (item && typeof item === 'object') {
                if (item.ProductType) {
                    mergedItem.ProductType = item.ProductType;
                }
                if (item.ReferenceForTgtSL || item.Reference) {
                    mergedItem.ReferenceForTgtSL = item.ReferenceForTgtSL || item.Reference;
                }
                if (item.EntryDelay !== undefined) {
                    mergedItem.EntryDelay = intOrDefault(item.EntryDelay, 0);
                }
                if (item.EntryOrder && typeof item.EntryOrder === 'object') {
                    mergedItem.EntryOrder = Object.assign({}, mergedItem.EntryOrder, cloneJson(item.EntryOrder));
                    if (item.EntryOrder.Value && item.EntryOrder.Value.Buffer) {
                        mergedItem.EntryOrder.Value = mergedItem.EntryOrder.Value || {};
                        mergedItem.EntryOrder.Value.Buffer = Object.assign(
                            {},
                            mergedItem.EntryOrder.Value.Buffer || {},
                            cloneJson(item.EntryOrder.Value.Buffer)
                        );
                        mergedItem.EntryOrder.Value.Buffer.Value = Object.assign(
                            {},
                            (mergedItem.EntryOrder.Value.Buffer && mergedItem.EntryOrder.Value.Buffer.Value) || {},
                            cloneJson((item.EntryOrder.Value.Buffer || {}).Value || {})
                        );
                    }
                    if (item.EntryOrder.Value && item.EntryOrder.Value.Modification) {
                        mergedItem.EntryOrder.Value = mergedItem.EntryOrder.Value || {};
                        mergedItem.EntryOrder.Value.Modification = Object.assign(
                            {},
                            mergedItem.EntryOrder.Value.Modification || {},
                            cloneJson(item.EntryOrder.Value.Modification)
                        );
                    }
                }
                if (item.ExitOrder && typeof item.ExitOrder === 'object') {
                    mergedItem.ExitOrder = Object.assign({}, mergedItem.ExitOrder, cloneJson(item.ExitOrder));
                    if (item.ExitOrder.Value && item.ExitOrder.Value.Buffer) {
                        mergedItem.ExitOrder.Value = mergedItem.ExitOrder.Value || {};
                        mergedItem.ExitOrder.Value.Buffer = Object.assign(
                            {},
                            mergedItem.ExitOrder.Value.Buffer || {},
                            cloneJson(item.ExitOrder.Value.Buffer)
                        );
                        mergedItem.ExitOrder.Value.Buffer.Value = Object.assign(
                            {},
                            (mergedItem.ExitOrder.Value.Buffer && mergedItem.ExitOrder.Value.Buffer.Value) || {},
                            cloneJson((item.ExitOrder.Value.Buffer || {}).Value || {})
                        );
                    }
                    if (item.ExitOrder.Value && item.ExitOrder.Value.Modification) {
                        mergedItem.ExitOrder.Value = mergedItem.ExitOrder.Value || {};
                        mergedItem.ExitOrder.Value.Modification = Object.assign(
                            {},
                            mergedItem.ExitOrder.Value.Modification || {},
                            cloneJson(item.ExitOrder.Value.Modification)
                        );
                    }
                }
            }
            return mergedItem;
        });

        normalizedDetail.execution_config_extra = Object.assign({}, executionExtra, {
            ListOfLegExecutionConfig: legExecutionList
        });
        return normalizedDetail;
    }

    function intOrDefault(value, fallback) {
        var parsed = parseInt(value, 10);
        return Number.isFinite(parsed) ? parsed : fallback;
    }

    function getExecutionLegConfigs(detail) {
        var executionExtra = detail && detail.execution_config_extra && typeof detail.execution_config_extra === 'object'
            ? detail.execution_config_extra
            : {};
        return Array.isArray(executionExtra.ListOfLegExecutionConfig) ? executionExtra.ListOfLegExecutionConfig : [];
    }

    function getParentLegConfigs(detail) {
        var fullConfig = detail && detail.full_config && typeof detail.full_config === 'object' ? detail.full_config : {};
        var strategy = fullConfig.strategy && typeof fullConfig.strategy === 'object' ? fullConfig.strategy : {};
        return Array.isArray(strategy.ListOfLegConfigs) ? strategy.ListOfLegConfigs : [];
    }

    function cleanExecutionEnum(value, fallback) {
        var normalized = String(value || fallback || '').trim();
        if (!normalized) {
            return fallback || '';
        }
        return normalized.indexOf('.') !== -1 ? normalized.split('.').pop() : normalized;
    }

    function toExecutionEnum(prefix, value, fallback) {
        var normalized = cleanExecutionEnum(value, fallback);
        return normalized ? prefix + '.' + normalized : (fallback || '');
    }


    function getExecutionConfigModal() {
        var existing = document.getElementById('portfolio-execution-config-modal');
        if (existing) {
            return existing;
        }
        var modal = document.createElement('div');
        modal.id = 'portfolio-execution-config-modal';
        modal.className = 'pec-modal';
        modal.hidden = true;
        modal.innerHTML = ''
            + '<div class="pec-backdrop" data-close-execution-config></div>'
            + '<div class="pec-dialog" role="dialog" aria-modal="true" aria-labelledby="pec-title">'
            + '  <div class="pec-header">'
            + '    <h2 class="pec-title" id="pec-title">Edit execution</h2>'
            + '    <button type="button" class="pec-close" data-close-execution-config aria-label="Close">&times;</button>'
            + '  </div>'
            + '  <div class="pec-body">'
            + '    <div class="pec-warning">'
            + '      <svg class="pec-warning-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor"><path fill-rule="evenodd" d="M9.401 3.003c1.155-2 4.043-2 5.197 0l7.355 12.748c1.154 2-.29 4.5-2.599 4.5H4.645c-2.309 0-3.752-2.5-2.598-4.5L9.4 3.003zM12 8.25a.75.75 0 01.75.75v3.75a.75.75 0 01-1.5 0V9a.75.75 0 01.75-.75zm0 8.25a.75.75 0 100-1.5.75.75 0 000 1.5z" clip-rule="evenodd"/></svg>'
            + '      <span>Your limit and stop-loss limit orders will be monitored for <span id="pec-monitor-seconds">50</span> seconds. If the order remains open after this period, it will be cancelled and the strategy will be marked as an error.</span>'
            + '    </div>'
            + '    <a href="javascript:void(0)" class="pec-learn-more">Learn more about execution settings</a>'
            + '    <div class="pec-settings-row">'
            + '      <div class="pec-setting-group">'
            + '        <span class="pec-setting-label">Auto Sq Off on Margin Error</span>'
            + '        <label class="pec-toggle-switch">'
            + '          <input type="checkbox" id="pec-margin-auto" data-execution-base-field="marginAutoSquareOff">'
            + '          <span class="pec-toggle-track"></span>'
            + '        </label>'
            + '      </div>'
            + '      <div class="pec-setting-group">'
            + '        <span class="pec-setting-label">Trade monitoring</span>'
            + '        <div class="pec-pill-group" id="pec-monitoring-group">'
            + '          <button type="button" class="pec-pill" data-monitoring-mode="ltp">on LTP</button>'
            + '          <button type="button" class="pec-pill" data-monitoring-mode="candle">on Candle Close</button>'
            + '        </div>'
            + '        <a href="javascript:void(0)" class="pec-sub-link">Know more</a>'
            + '      </div>'
            + '      <div class="pec-setting-group">'
            + '        <span class="pec-setting-label">Strategy execution time</span>'
            + '        <input type="time" class="pec-time-input" id="pec-exec-time" step="1" data-execution-base-field="strategyExecutionTime">'
            + '        <a href="javascript:void(0)" class="pec-sub-link">How to use?</a>'
            + '      </div>'
            + '    </div>'
            + '    <div class="pec-tabs" id="pec-tabs"></div>'
            + '    <div id="pec-panel-empty" class="pec-panel-empty" hidden>No parent legs found for this strategy.</div>'
            + '    <div class="pec-panel" id="pec-panel"></div>'
            + '    <div class="pec-json-preview" id="pec-json-preview" hidden>'
            + '      <div class="pec-json-title">Generated JSON</div>'
            + '      <pre class="pec-json-pre" id="pec-json-pre"></pre>'
            + '    </div>'
            + '  </div>'
            + '  <div class="pec-footer">'
            + '    <button type="button" class="pec-btn-cancel" data-close-execution-config>Cancel</button>'
            + '    <button type="button" class="pec-btn-save" data-save-execution-config>Update Execution Settings</button>'
            + '  </div>'
            + '</div>';
        document.body.appendChild(modal);
        return modal;
    }

    function setExecutionConfigModalVisibility(show) {
        var modal = getExecutionConfigModal();
        modal.hidden = !show;
        document.body.classList.toggle('portfolio-execution-config-open', !!show);
    }

    function buildPecFieldRow(labelHtml, controlHtml) {
        return '<div class="pec-field-row">'
            + '<span class="pec-field-label">' + labelHtml + '</span>'
            + '<div class="pec-field-control">' + controlHtml + '</div>'
            + '</div>';
    }

    function buildPecLegPanel(_legConfig, executionConfig, legIndex) {
        var entryBuffer = (((executionConfig.EntryOrder || {}).Value || {}).Buffer || {});
        var entryBufferValue = entryBuffer.Value || {};
        var exitBuffer = (((executionConfig.ExitOrder || {}).Value || {}).Buffer || {});
        var exitBufferValue = exitBuffer.Value || {};
        var exitModification = (((executionConfig.ExitOrder || {}).Value || {}).Modification || {});
        var li = String(legIndex);

        return '<div class="pec-leg-section">'
            + '<div class="pec-leg-header">'
            + '  <strong class="pec-leg-name">Leg ' + (legIndex + 1) + '</strong>'
            + '  <button type="button" class="pec-copy-btn" data-copy-to-legs="' + li + '">'
            + '    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="pec-copy-icon"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>'
            + '    Copy To Legs'
            + '  </button>'
            + '</div>'
            + '<div class="pec-leg-grid">'

            + '<div class="pec-leg-col">'
            + buildPecFieldRow('NRML/MIS', '<select data-execution-field="product" data-leg-index="' + li + '">' + buildExecutionOptions(['NRML', 'MIS'], cleanExecutionEnum(executionConfig.ProductType, 'NRML')) + '</select>')
            + buildPecFieldRow('Tgt/SL Ref Price <span class="pec-info-icon">&#9432;</span>', '<select data-execution-field="reference" data-leg-index="' + li + '">' + buildExecutionOptions(['Trigger', 'LTP'], cleanExecutionEnum(executionConfig.ReferenceForTgtSL, 'Trigger')) + '</select>')
            + buildPecFieldRow('Delay entry by <span class="pec-info-icon">&#9432;</span>', '<div class="pec-input-unit"><input type="number" min="0" step="1" data-execution-field="entryDelay" data-leg-index="' + li + '" value="' + String(intOrDefault(executionConfig.EntryDelay, 0)) + '"><span class="pec-unit">SEC</span></div>')
            + '</div>'

            + '<div class="pec-leg-col">'
            + '<div class="pec-order-heading">Entry Order Type</div>'
            + buildPecFieldRow('Entry Order Type', '<select data-execution-field="entryOrderType" data-leg-index="' + li + '">' + buildExecutionOptions(['MPP', 'Market', 'Limit'], cleanExecutionEnum((executionConfig.EntryOrder || {}).Type, 'Limit')) + '</select>')
            + buildPecFieldRow('Buffer type', '<select data-execution-field="entryBufferType" data-leg-index="' + li + '">' + buildExecutionOptions(['Points', 'Percent'], cleanExecutionEnum(entryBuffer.Type, 'Points')) + '</select>')
            + buildPecFieldRow('Trigger buffer', '<input type="number" step="0.1" data-execution-field="entryTriggerBuffer" data-leg-index="' + li + '" value="' + String(Number(entryBufferValue.TriggerBuffer || 0)) + '">')
            + buildPecFieldRow('Limit buffer', '<input type="number" step="0.1" data-execution-field="entryLimitBuffer" data-leg-index="' + li + '" value="' + String(Number(entryBufferValue.LimitBuffer || 0)) + '">')
            + '</div>'

            + '<div class="pec-leg-col">'
            + '<div class="pec-order-heading">Exit Order Type</div>'
            + buildPecFieldRow('Exit Order Type', '<select data-execution-field="exitOrderType" data-leg-index="' + li + '">' + buildExecutionOptions(['MPP', 'Market', 'Limit'], cleanExecutionEnum((executionConfig.ExitOrder || {}).Type, 'Limit')) + '</select>')
            + buildPecFieldRow('Buffer type <span class="pec-info-icon">&#9432;</span>', '<select data-execution-field="exitBufferType" data-leg-index="' + li + '">' + buildExecutionOptions(['Points', 'Percent'], cleanExecutionEnum(exitBuffer.Type, 'Points')) + '</select>')
            + buildPecFieldRow('Trigger buffer', '<input type="number" step="0.1" data-execution-field="exitTriggerBuffer" data-leg-index="' + li + '" value="' + String(Number(exitBufferValue.TriggerBuffer || 0)) + '">')
            + buildPecFieldRow('Limit buffer', '<input type="number" step="0.1" data-execution-field="exitLimitBuffer" data-leg-index="' + li + '" value="' + String(Number(exitBufferValue.LimitBuffer || 0)) + '">')
            + buildPecFieldRow('Monitoring', '<select data-execution-field="exitMonitoring" data-leg-index="' + li + '">' + buildExecutionOptions(['Continuous', 'Periodic'], String(exitModification.ContinuousMonitoring) === 'True' ? 'Continuous' : 'Periodic') + '</select>')
            + buildPecFieldRow('Frequency', '<div class="pec-input-unit"><input type="number" min="1" step="1" data-execution-field="exitFrequency" data-leg-index="' + li + '" value="' + String(intOrDefault(exitModification.ModificationFrequency, 5)) + '"><span class="pec-unit">sec</span></div>')
            + '</div>'

            + '</div>'
            + '</div>';
    }

    function renderExecutionConfigModal() {
        var modal = getExecutionConfigModal();
        var titleEl = modal.querySelector('#pec-title');
        var marginToggle = modal.querySelector('#pec-margin-auto');
        var monitoringGroup = modal.querySelector('#pec-monitoring-group');
        var execTimeInput = modal.querySelector('#pec-exec-time');
        var tabsContainer = modal.querySelector('#pec-tabs');
        var panelContainer = modal.querySelector('#pec-panel');
        var emptyState = modal.querySelector('#pec-panel-empty');
        var workingDetail = executionConfigModalState.workingDetail;
        var parentLegs = getParentLegConfigs(workingDetail);
        var legExecutionList = getExecutionLegConfigs(workingDetail);
        var activeLegIndex = Math.max(0, Math.min(executionConfigModalState.activeLegIndex, Math.max(parentLegs.length - 1, 0)));
        var baseConfig = workingDetail && workingDetail.execution_config_base && typeof workingDetail.execution_config_base === 'object'
            ? workingDetail.execution_config_base : {};

        executionConfigModalState.activeLegIndex = activeLegIndex;

        if (titleEl) {
            titleEl.textContent = 'Edit execution for ' + (executionConfigModalState.strategyName || 'Strategy');
        }
        if (marginToggle) {
            marginToggle.checked = !!baseConfig.MarginAutoSquareOff;
        }
        if (monitoringGroup) {
            var isCandle = baseConfig.LikeBacktester !== false;
            monitoringGroup.querySelectorAll('[data-monitoring-mode]').forEach(function (btn) {
                var mode = btn.getAttribute('data-monitoring-mode');
                btn.classList.toggle('is-active', mode === (isCandle ? 'candle' : 'ltp'));
            });
        }
        if (execTimeInput) {
            execTimeInput.value = String(baseConfig.StrategyExecutionTime || '');
        }

        if (!parentLegs.length) {
            if (emptyState) emptyState.hidden = false;
            tabsContainer.innerHTML = '';
            panelContainer.innerHTML = '';
            return;
        }
        if (emptyState) emptyState.hidden = true;

        tabsContainer.innerHTML = parentLegs.map(function (_leg, index) {
            return '<button type="button" class="pec-tab' + (index === activeLegIndex ? ' is-active' : '')
                + '" data-execution-config-tab="' + String(index) + '">Leg ' + (index + 1) + '</button>';
        }).join('');

        var legConfig = parentLegs[activeLegIndex] || {};
        var executionConfig = legExecutionList[activeLegIndex] || createDefaultLegExecutionConfig();
        panelContainer.innerHTML = buildPecLegPanel(legConfig, executionConfig, activeLegIndex);
    }

    function buildExecutionOptions(options, selectedValue) {
        return (options || []).map(function (option) {
            var isSelected = String(option) === String(selectedValue);
            return '<option value="' + option + '"' + (isSelected ? ' selected' : '') + '>' + option + '</option>';
        }).join('');
    }

    function normalizeWeekdaysPayload(selectedDays) {
        var normalizedList = Array.isArray(selectedDays) ? selectedDays.map(function (day) {
            return String(day || '').trim().toLowerCase();
        }) : [];
        return {
            friday: normalizedList.indexOf('f') !== -1 || normalizedList.indexOf('friday') !== -1,
            monday: normalizedList.indexOf('m') !== -1 || normalizedList.indexOf('monday') !== -1,
            saturday: normalizedList.indexOf('sa') !== -1 || normalizedList.indexOf('saturday') !== -1,
            sunday: normalizedList.indexOf('su') !== -1 || normalizedList.indexOf('sunday') !== -1,
            thursday: normalizedList.indexOf('th') !== -1 || normalizedList.indexOf('thursday') !== -1,
            tuesday: normalizedList.indexOf('t') !== -1 || normalizedList.indexOf('tuesday') !== -1,
            wednesday: normalizedList.indexOf('w') !== -1 || normalizedList.indexOf('wednesday') !== -1
        };
    }

    function buildExecutionConfigJsonPayload() {
        var strategyId = executionConfigModalState.strategyId;
        var workingDetail = executionConfigModalState.workingDetail;
        var row = strategyId ? document.querySelector('#portfolio-strategy-rows [data-strategy-id="' + CSS.escape(strategyId) + '"]') : null;
        var weekdays = parseRowJsonAttribute(row, 'data-strategy-weekdays', ['M', 'T', 'W', 'Th', 'F']);
        var dte = normalizeDteValues(parseRowJsonAttribute(row, 'data-strategy-dte', []));
        var baseConfig = workingDetail && workingDetail.execution_config_base && typeof workingDetail.execution_config_base === 'object'
            ? cloneJson(workingDetail.execution_config_base)
            : {};
        var extraConfig = workingDetail && workingDetail.execution_config_extra && typeof workingDetail.execution_config_extra === 'object'
            ? cloneJson(workingDetail.execution_config_extra)
            : {};
        var viewConfig = workingDetail && workingDetail.view_config && typeof workingDetail.view_config === 'object'
            ? cloneJson(workingDetail.view_config)
            : { advanced_exec_config_modal: true };

        baseConfig.Multiplier = intOrDefault(baseConfig.Multiplier, 1);
        baseConfig.LikeBacktester = !!baseConfig.LikeBacktester;
        baseConfig.MarginAutoSquareOff = !!baseConfig.MarginAutoSquareOff;
        baseConfig.TimeDelta = intOrDefault(baseConfig.TimeDelta, 0);
        delete baseConfig.StrategyExecutionTime;

        return {
            execution_config_base: baseConfig,
            execution_config_extra: {
                ListOfLegExecutionConfig: getExecutionLegConfigs({ execution_config_extra: extraConfig }).map(function (item) {
                    return cloneJson(item);
                })
            },
            is_weekdays: currentFilterMode === 'Weekdays',
            dte: currentFilterMode === 'Weekdays' ? [] : dte,
            weekdays: normalizeWeekdaysPayload(weekdays),
            view_config: viewConfig
        };
    }

    function renderExecutionConfigJsonPreview(payload) {
        var modal = getExecutionConfigModal();
        var previewWrap = modal.querySelector('#pec-json-preview');
        var previewPre = modal.querySelector('#pec-json-pre');
        if (!previewWrap || !previewPre) {
            return;
        }
        previewPre.textContent = JSON.stringify(payload, null, 4);
        previewWrap.hidden = false;
    }

    function renderExecutionConfigAction(row) {
        if (!row) {
            return;
        }
        row.querySelectorAll('[data-strategy-action]').forEach(function (node) {
            node.remove();
        });
        if (!isExecutionConfigEditableMode()) {
            return;
        }
        var strategyId = String(row.getAttribute('data-strategy-id') || '');
        if (!strategyId) {
            return;
        }
        var actionWrap = document.createElement('div');
        actionWrap.className = 'portfolio-strategy-action';
        actionWrap.setAttribute('data-strategy-action', 'true');
        actionWrap.innerHTML = '<button type="button" class="portfolio-strategy-action-btn" data-edit-execution-config="' + strategyId + '">Edit Configuration</button>';
        var metaEl = row.querySelector('[data-strategy-meta]');
        if (metaEl) {
            metaEl.insertAdjacentElement('afterend', actionWrap);
        } else {
            row.appendChild(actionWrap);
        }
    }

    function updateExecutionConfigField(legIndex, fieldName, rawValue) {
        var workingDetail = executionConfigModalState.workingDetail;
        var legConfigs = getExecutionLegConfigs(workingDetail);
        var legConfig = legConfigs[legIndex];
        if (!legConfig) {
            return;
        }
        if (fieldName === 'product') {
            legConfig.ProductType = toExecutionEnum('ProductType', rawValue, 'ProductType.NRML');
            return;
        }
        if (fieldName === 'reference') {
            legConfig.ReferenceForTgtSL = rawValue === 'LTP' ? 'PriceReferenceType.LTP' : 'PriceReferenceType.Trigger';
            return;
        }
        if (fieldName === 'entryDelay') {
            legConfig.EntryDelay = intOrDefault(rawValue, 0);
            return;
        }
        if (fieldName === 'entryOrderType') {
            legConfig.EntryOrder.Type = toExecutionEnum('OrderType', rawValue, 'OrderType.Limit');
            return;
        }
        if (fieldName === 'entryBufferType') {
            legConfig.EntryOrder.Value.Buffer.Type = toExecutionEnum('BufferType', rawValue, 'BufferType.Points');
            return;
        }
        if (fieldName === 'entryTriggerBuffer') {
            legConfig.EntryOrder.Value.Buffer.Value.TriggerBuffer = Number(rawValue || 0);
            return;
        }
        if (fieldName === 'entryLimitBuffer') {
            legConfig.EntryOrder.Value.Buffer.Value.LimitBuffer = Number(rawValue || 0);
            return;
        }
        if (fieldName === 'exitOrderType') {
            legConfig.ExitOrder.Type = toExecutionEnum('OrderType', rawValue, 'OrderType.Limit');
            return;
        }
        if (fieldName === 'exitBufferType') {
            legConfig.ExitOrder.Value.Buffer.Type = toExecutionEnum('BufferType', rawValue, 'BufferType.Points');
            return;
        }
        if (fieldName === 'exitTriggerBuffer') {
            legConfig.ExitOrder.Value.Buffer.Value.TriggerBuffer = Number(rawValue || 0);
            return;
        }
        if (fieldName === 'exitLimitBuffer') {
            legConfig.ExitOrder.Value.Buffer.Value.LimitBuffer = Number(rawValue || 0);
            return;
        }
        if (fieldName === 'exitMonitoring') {
            legConfig.ExitOrder.Value.Modification.ContinuousMonitoring = rawValue === 'Periodic' ? 'False' : 'True';
            return;
        }
        if (fieldName === 'exitFrequency') {
            legConfig.ExitOrder.Value.Modification.ModificationFrequency = Math.max(1, intOrDefault(rawValue, 5));
        }
    }

    function openExecutionConfigModal(strategyId) {
        if (!strategyId) {
            return;
        }
        var detailMap = window._portfolioStrategyDetails || {};
        var renderedStrategies = window._portfolioRenderedStrategies || {};
        var detail = detailMap[strategyId];
        var strategy = renderedStrategies[strategyId];
        var strategyName = (strategy && strategy.name) || (detail && detail.name) || 'Strategy';
        if (!detail) {
            showToast('Strategy details are not available for configuration.', 'error');
            return;
        }

        executionConfigModalState.strategyId = strategyId;
        executionConfigModalState.strategyName = strategyName;
        executionConfigModalState.detail = detail;
        executionConfigModalState.workingDetail = ensureExecutionConfigDetail(cloneJson(detail), strategy || {});
        executionConfigModalState.activeLegIndex = 0;

        var workingBase = executionConfigModalState.workingDetail.execution_config_base;
        if (!workingBase.StrategyExecutionTime) {
            var rawEntryTime = (strategy && strategy.entry_time) || '';
            var t12 = String(rawEntryTime).trim();
            var m12 = t12.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)$/i);
            if (m12) {
                var h = parseInt(m12[1], 10);
                var mn = m12[2];
                var sc = m12[3] || '00';
                var ampm = (m12[4] || '').toUpperCase();
                if (ampm === 'AM' && h === 12) h = 0;
                if (ampm === 'PM' && h !== 12) h += 12;
                workingBase.StrategyExecutionTime = String(h).padStart(2, '0') + ':' + mn + ':' + sc;
            } else if (/^\d{2}:\d{2}(:\d{2})?$/.test(t12)) {
                workingBase.StrategyExecutionTime = t12.length === 5 ? t12 + ':00' : t12;
            }
        }
        renderExecutionConfigModal();
        setExecutionConfigModalVisibility(true);
    }

    function saveExecutionConfigModal() {
        var strategyId = executionConfigModalState.strategyId;
        var detailMap = window._portfolioStrategyDetails || {};
        if (!strategyId || !detailMap[strategyId] || !executionConfigModalState.workingDetail) {
            setExecutionConfigModalVisibility(false);
            return;
        }
        detailMap[strategyId] = ensureExecutionConfigDetail(cloneJson(executionConfigModalState.workingDetail), window._portfolioRenderedStrategies[strategyId] || {});
        window._portfolioStrategyDetails = detailMap;
        var generatedPayload = buildExecutionConfigJsonPayload();
        return fetch(getPortfolioActivationApiUrl('portfolio/execution-settings/update'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                portfolio_id: portfolioId,
                source_strategy_id: strategyId,
                activation_mode: activationMode,
                execution_settings: generatedPayload
            })
        }).then(function (response) {
            return response.json().catch(function () {
                return {};
            }).then(function (data) {
                if (!response.ok) {
                    throw new Error(data.detail || 'Failed to update execution settings');
                }
                return data;
            });
        }).then(function (data) {
            window._lastExecutionConfigPayload = data && data.execution_settings ? data.execution_settings : generatedPayload;
            console.log('Execution configuration payload', window._lastExecutionConfigPayload);
            renderExecutionConfigJsonPreview(window._lastExecutionConfigPayload);
            showToast('Execution settings updated successfully.');
        }).catch(function (error) {
            window._lastExecutionConfigPayload = generatedPayload;
            console.log('Execution configuration payload', generatedPayload);
            renderExecutionConfigJsonPreview(generatedPayload);
            showToast(error.message || 'Failed to update execution settings', 'error');
        });
    }

    function updateExecutionConfigBaseField(fieldName, rawValue) {
        var workingDetail = executionConfigModalState.workingDetail;
        if (!workingDetail || !workingDetail.execution_config_base) {
            return;
        }
        var baseConfig = workingDetail.execution_config_base;
        if (fieldName === 'marginAutoSquareOff') {
            baseConfig.MarginAutoSquareOff = !!rawValue;
        } else if (fieldName === 'strategyExecutionTime') {
            baseConfig.StrategyExecutionTime = String(rawValue || '');
        }
    }

    function bindExecutionConfigModalEvents() {
        if (document.documentElement.dataset.boundExecutionConfigModal === 'true') {
            return;
        }
        document.documentElement.dataset.boundExecutionConfigModal = 'true';

        document.addEventListener('click', function (event) {
            var closeTrigger = event.target.closest('[data-close-execution-config]');
            if (closeTrigger) {
                setExecutionConfigModalVisibility(false);
                var modal = getExecutionConfigModal();
                var previewWrap = modal.querySelector('#pec-json-preview');
                if (previewWrap) {
                    previewWrap.hidden = true;
                }
                return;
            }

            var editTrigger = event.target.closest('[data-edit-execution-config]');
            if (editTrigger) {
                event.preventDefault();
                openExecutionConfigModal(String(editTrigger.getAttribute('data-edit-execution-config') || ''));
                return;
            }

            var tabTrigger = event.target.closest('[data-execution-config-tab]');
            if (tabTrigger) {
                executionConfigModalState.activeLegIndex = intOrDefault(tabTrigger.getAttribute('data-execution-config-tab'), 0);
                renderExecutionConfigModal();
                return;
            }

            var saveTrigger = event.target.closest('[data-save-execution-config]');
            if (saveTrigger) {
                saveExecutionConfigModal();
                return;
            }

            var monitoringTrigger = event.target.closest('[data-monitoring-mode]');
            if (monitoringTrigger) {
                var mode = monitoringTrigger.getAttribute('data-monitoring-mode');
                var workingDetail = executionConfigModalState.workingDetail;
                if (workingDetail && workingDetail.execution_config_base) {
                    workingDetail.execution_config_base.LikeBacktester = mode === 'candle';
                }
                var group = monitoringTrigger.closest('.pec-pill-group');
                if (group) {
                    group.querySelectorAll('[data-monitoring-mode]').forEach(function (btn) {
                        btn.classList.toggle('is-active', btn.getAttribute('data-monitoring-mode') === mode);
                    });
                }
                return;
            }

            var copyTrigger = event.target.closest('[data-copy-to-legs]');
            if (copyTrigger) {
                var fromIndex = intOrDefault(copyTrigger.getAttribute('data-copy-to-legs'), 0);
                var wd = executionConfigModalState.workingDetail;
                var legConfigs = getExecutionLegConfigs(wd);
                var parentLegs = getParentLegConfigs(wd);
                if (legConfigs[fromIndex]) {
                    var sourceConfig = cloneJson(legConfigs[fromIndex]);
                    parentLegs.forEach(function (_, idx) {
                        if (idx !== fromIndex) {
                            legConfigs[idx] = cloneJson(sourceConfig);
                        }
                    });
                }
                showToast('Configuration copied to all legs.');
                return;
            }
        });

        document.addEventListener('change', function (event) {
            var baseField = event.target && event.target.matches && event.target.matches('[data-execution-base-field]')
                ? event.target : null;
            if (baseField) {
                var fieldName = String(baseField.getAttribute('data-execution-base-field') || '');
                var fieldValue = baseField.type === 'checkbox' ? baseField.checked : baseField.value;
                updateExecutionConfigBaseField(fieldName, fieldValue);
                return;
            }

            var field = event.target && event.target.matches && event.target.matches('[data-execution-field]')
                ? event.target : null;
            if (!field) {
                return;
            }
            updateExecutionConfigField(
                intOrDefault(field.getAttribute('data-leg-index'), 0),
                String(field.getAttribute('data-execution-field') || ''),
                field.value
            );
        });

        document.addEventListener('keydown', function (event) {
            if (event.key === 'Escape') {
                var modal = document.getElementById('portfolio-execution-config-modal');
                if (modal && !modal.hidden) {
                    setExecutionConfigModalVisibility(false);
                }
            }
        });
    }

    bindExecutionConfigModalEvents();

    function getSelectedActivationBrokerId() {
        return brokerSelect ? String(brokerSelect.value || '').trim() : '';
    }

    function renderBrokerOptions(records) {
        availableBrokers = Array.isArray(records) ? records.slice() : [];
        if (!brokerSelect) {
            return;
        }
        brokerSelect.innerHTML = '<option value="">Select Broker</option>' + availableBrokers.map(function (item) {
            var brokerId = String(item && item._id || '').trim();
            var brokerName = String(item && item.name || '').trim() || brokerId;
            return '<option value="' + brokerId.replace(/"/g, '&quot;') + '">' + brokerName + '</option>';
        }).join('');
        brokerSelect.value = '';
    }

    function fetchBrokerConfigurations() {
        var brokerType = String(activationMode || '').trim() || 'algo-backtest';
        if (brokerFilter) {
            brokerFilter.hidden = false;
        }
        return fetch(getPortfolioActivationApiUrl('broker-configurations') + '?broker_type=' + encodeURIComponent(brokerType))
            .then(function (response) {
                return response.json().catch(function () {
                    return {};
                }).then(function (data) {
                    if (!response.ok) {
                        throw new Error(data.detail || 'Failed to load brokers');
                    }
                    return Array.isArray(data && data.records) ? data.records : [];
                });
            })
            .then(function (records) {
                renderBrokerOptions(records);
                return records;
            });
    }

    function padDatePart(value) {
        return String(value).padStart(2, '0');
    }

    function formatDateTime(date, includeMs) {
        if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
            return '';
        }
        var stamp = date.getFullYear()
            + '-' + padDatePart(date.getMonth() + 1)
            + '-' + padDatePart(date.getDate())
            + ' ' + padDatePart(date.getHours())
            + ':' + padDatePart(date.getMinutes())
            + ':' + padDatePart(date.getSeconds());
        if (!includeMs) {
            return stamp;
        }
        return stamp + '.' + String(date.getMilliseconds()).padStart(3, '0') + '000';
    }

    function parseRequestedActivationTime(value) {
        if (!value) {
            return null;
        }
        var normalized = String(value).trim().replace('T', ' ');
        var parts = normalized.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})(?::(\d{2}))?/);
        if (!parts) {
            return null;
        }
        var parsedDate = new Date(
            parseInt(parts[1], 10),
            parseInt(parts[2], 10) - 1,
            parseInt(parts[3], 10),
            parseInt(parts[4], 10),
            parseInt(parts[5], 10),
            parseInt(parts[6] || '0', 10),
            0
        );
        return Number.isNaN(parsedDate.getTime()) ? null : parsedDate;
    }

    function getActivationRequestTime() {
        if (activationMode === 'algo-backtest') {
            var requestedTime = parseRequestedActivationTime(requestedCurrentDateTime);
            if (requestedTime) {
                return requestedTime;
            }
        }
        return new Date();
    }

    function buildStrategyDateTime(displayTime, fallbackDate, isEntry) {
        if (!displayTime || displayTime === '-') {
            return '';
        }
        var hours, minutes;
        var dtStr = String(displayTime).trim();
        var dtMatch = dtStr.match(/^\d{4}-\d{2}-\d{2}\s+(\d{2}):(\d{2}):\d{2}$/);
        if (dtMatch) {
            hours = parseInt(dtMatch[1], 10);
            minutes = parseInt(dtMatch[2], 10);
        } else {
            var ampmMatch = dtStr.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
            if (!ampmMatch) {
                return '';
            }
            hours = parseInt(ampmMatch[1], 10) % 12;
            minutes = parseInt(ampmMatch[2], 10);
            if (ampmMatch[3].toUpperCase() === 'PM') {
                hours += 12;
            }
        }
        if (isEntry) {
            if (minutes === 0) {
                hours = hours - 1;
                minutes = 59;
            } else {
                minutes = minutes - 1;
            }
        }
        var date = fallbackDate instanceof Date ? new Date(fallbackDate.getTime()) : new Date();
        date.setHours(hours, minutes, 59, 0);
        return formatDateTime(date, false);
    }

    function generateExecutionObjectId(seed) {
        var randomPart = Math.random().toString(16).slice(2, 10);
        var suffix = seed ? String(seed).replace(/[^a-zA-Z0-9]/g, '').slice(-8) : randomPart;
        return 'exec_' + Date.now().toString(16) + suffix;
    }

    function detectAppRootPath() {
        var configuredRoot = window.APP_HEADER_ROOT_PATH || window.APP_BASE_PATH || '';
        if (configuredRoot) {
            return configuredRoot;
        }
        var pathname = (window.location && window.location.pathname) || '';
        var normalizedPath = pathname.replace(/\\/g, '/');
        var rootFolderName = window.APP_ROOT_FOLDER_NAME
            || (window.APP_HEADER_ROOT_PATH ? String(window.APP_HEADER_ROOT_PATH).replace(/\/+$/, '').split('/').pop() : '');
        if (!rootFolderName) {
            return normalizedPath;
        }
        var marker = '/' + rootFolderName + '/';
        var markerIndex = normalizedPath.indexOf(marker);
        if (markerIndex !== -1) {
            return normalizedPath.slice(0, markerIndex + marker.length - 1);
        }
        if (normalizedPath.slice(-(rootFolderName.length + 1)) === '/' + rootFolderName) {
            return normalizedPath;
        }
        return '/' + rootFolderName;
    }

    function buildAppUrl(relativePath) {
        if (typeof window.buildNamedPageUrl === 'function' && window.APP_PAGE_ROUTES) {
            var matchedKey = Object.keys(window.APP_PAGE_ROUTES).find(function (key) {
                return window.APP_PAGE_ROUTES[key] === relativePath;
            });
            if (matchedKey) {
                return window.buildNamedPageUrl(matchedKey);
            }
        }
        if (typeof window.buildAppUrl === 'function') {
            return window.buildAppUrl(relativePath);
        }
        var rootPath = detectAppRootPath().replace(/\/+$/, '');
        var normalizedRelativePath = String(relativePath || '').replace(/^\/+/, '');
        if (window.location && window.location.protocol === 'file:') {
            return 'file://' + rootPath + '/' + normalizedRelativePath;
        }
        return rootPath + '/' + normalizedRelativePath;
    }

    function getActivationSuccessRedirectUrl() {
        if (activationMode === 'algo-backtest') {
            return typeof window.buildNamedPageUrl === 'function'
                ? window.buildNamedPageUrl('algoBacktestDashboard')
                : buildAppUrl((window.APP_PAGE_ROUTES && window.APP_PAGE_ROUTES.algoBacktestDashboard) || 'algo-backtest/dashboard.html');
        }
        if (activationMode === 'forward-test') {
            return typeof window.buildNamedPageUrl === 'function'
                ? window.buildNamedPageUrl('forwardTestDashboard')
                : buildAppUrl((window.APP_PAGE_ROUTES && window.APP_PAGE_ROUTES.forwardTestDashboard) || 'forward-test/dashboard.html');
        }
        if (activationMode === 'live') {
            return typeof window.buildNamedPageUrl === 'function'
                ? window.buildNamedPageUrl('liveDashboard')
                : buildAppUrl((window.APP_PAGE_ROUTES && window.APP_PAGE_ROUTES.liveDashboard) || 'fast-forward-clone.html');
        }
        return '';
    }

    function getPortfolioQtyMultiplier() {
        return qtySelect ? parseInt(qtySelect.value || '1', 10) || 1 : 1;
    }

    function updateQtyButtonLabel() {
        var qtyValue = getPortfolioQtyMultiplier();
        if (qtyValueEl) {
            qtyValueEl.textContent = String(qtyValue);
        }
        if (qtyDecrementBtn) {
            qtyDecrementBtn.disabled = qtyValue <= 1;
        }
        if (qtyIncrementBtn) {
            qtyIncrementBtn.disabled = qtyValue >= 100;
        }
        if (qtyButton) {
            var label = qtyButton.querySelector('span');
            if (label) {
                label.textContent = 'Qty Multiplier: ' + String(qtyValue);
            }
        }
    }

    function setPortfolioQtyMultiplier(nextValue) {
        if (!qtySelect) {
            return;
        }
        var normalized = Math.max(1, Math.min(100, parseInt(nextValue || '1', 10) || 1));
        qtySelect.value = String(normalized);
        updateAllRowEffectiveQty();
        updateQtyButtonLabel();
        if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
            window._renderPortfolioBacktestReports(window._portfolioResult);
        }
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
        toast.className = 'portfolio-toast portfolio-toast-success rounded px-4 py-3 text-sm font-medium shadow-lg';
        document.body.appendChild(toast);
        return toast;
    }

    function showToast(message, type) {
        var toast = getToastElement();
        toast.textContent = message;
        toast.className = 'portfolio-toast rounded px-4 py-3 text-sm font-medium shadow-lg';
        if (type === 'error') {
            toast.classList.add('portfolio-toast-error');
        } else {
            toast.classList.add('portfolio-toast-success');
        }
        window.clearTimeout(toastTimer);
        toast.classList.remove('is-visible');
        void toast.offsetWidth;
        requestAnimationFrame(function () {
            toast.classList.add('is-visible');
        });
        toastTimer = window.setTimeout(function () {
            toast.classList.remove('is-visible');
        }, 2600);
    }

    function getRowBaseQty(row) {
        var stored = row ? parseInt(row.getAttribute('data-strategy-qty-base') || '0', 10) : 0;
        if (stored > 0) {
            return stored;
        }
        return 1;
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
        if (!row) {
            return;
        }
        var effectiveQty = getRowBaseQty(row) * getPortfolioQtyMultiplier();
        row.setAttribute('data-strategy-effective-qty', String(effectiveQty));
    }

    function updateAllRowEffectiveQty() {
        rowsContainer.querySelectorAll('[data-strategy-id]').forEach(function (row) {
            updateRowEffectiveQty(row);
            var strategyId = row.getAttribute('data-strategy-id') || '';
            var strategyData = strategyId && window._portfolioRenderedStrategies ? window._portfolioRenderedStrategies[strategyId] : null;
            if (strategyData) {
                renderStrategyMeta(row, strategyData);
            }
        });
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

    function updateSelectionSummary() {
        var rowCheckboxes = rowsContainer.querySelectorAll('input[id^="portfolio-strategy-"]');
        var totalCount = rowCheckboxes.length;
        var selectedCount = 0;

        rowCheckboxes.forEach(function (checkbox) {
            syncRowSelectionState(checkbox);
            if (checkbox.checked) {
                selectedCount += 1;
            }
        });

        selectedCountEl.textContent = String(selectedCount);
        selectedTotalEl.textContent = '/' + String(totalCount) + ' selected';
        overallSelect.checked = totalCount > 0 && selectedCount === totalCount;
        overallSelect.indeterminate = selectedCount > 0 && selectedCount < totalCount;
    }

    function syncRowSelectionState(checkbox) {
        if (!checkbox) {
            return;
        }
        var row = checkbox.closest('[data-strategy-id]');
        if (!row) {
            return;
        }
        row.classList.toggle('is-selected', !!checkbox.checked);
    }

    function updateWeekdayState(row, weekdays) {
        var activeDays = Array.isArray(weekdays) && weekdays.length ? weekdays : ['M', 'T', 'W', 'Th', 'F'];
        if (!row) {
            return;
        }
        row.setAttribute('data-strategy-weekdays', JSON.stringify(activeDays));
    }

    function attachWeekdayToggle(row) {
        return row;
    }

    function getRowQtySelect(row) {
        return null;
    }

    function getRowSlippageInput(row) {
        return null;
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
        return null;
    }

    function padTimePart(value) {
        var num = parseInt(value, 10);
        return num < 10 ? '0' + String(num) : String(num);
    }

    function formatTimeFromIndicatorConfig(node) {
        if (!node) {
            return '';
        }
        if (Array.isArray(node)) {
            for (var i = 0; i < node.length; i += 1) {
                var nestedTime = formatTimeFromIndicatorConfig(node[i]);
                if (nestedTime) {
                    return nestedTime;
                }
            }
            return '';
        }
        if (typeof node !== 'object') {
            return '';
        }
        var indicator = node.Value && node.Value.IndicatorName;
        var params = node.Value && node.Value.Parameters;
        if (indicator === 'IndicatorType.TimeIndicator' && params) {
            var hour = parseInt(params.Hour, 10);
            var minute = parseInt(params.Minute, 10);
            if (Number.isFinite(hour) && Number.isFinite(minute)) {
                var suffix = hour >= 12 ? 'PM' : 'AM';
                var twelveHour = hour % 12 || 12;
                return padTimePart(twelveHour) + ':' + padTimePart(minute) + ' ' + suffix;
            }
        }
        if (node.Value && Array.isArray(node.Value)) {
            return formatTimeFromIndicatorConfig(node.Value);
        }
        return '';
    }

    function getStrategyDisplayDetails(strategy) {
        var strategyId = strategy && (strategy._id || strategy.id) ? String(strategy._id || strategy.id) : '';
        var detail = strategyId && window._portfolioStrategyDetails ? window._portfolioStrategyDetails[strategyId] : null;
        var fullConfig = detail && detail.full_config ? detail.full_config : {};
        var configStrategy = fullConfig.strategy || {};
        var entryTime = strategy && strategy.entry_time ? strategy.entry_time : formatTimeFromIndicatorConfig(configStrategy.EntryIndicators);
        var exitTime = strategy && strategy.exit_time ? strategy.exit_time : formatTimeFromIndicatorConfig(configStrategy.ExitIndicators);

        return {
            name: strategy && strategy.name ? strategy.name : (detail && detail.name ? detail.name : 'Untitled Strategy'),
            underlying: strategy && strategy.underlying ? strategy.underlying : (detail && detail.underlying ? detail.underlying : 'NIFTY'),
            product: strategy && strategy.product ? strategy.product : 'INTRADAY',
            entryTime: entryTime || '-',
            exitTime: exitTime || '-'
        };
    }

    function renderStrategyMeta(row, strategy) {
        if (!row || !strategy) {
            return;
        }

        var existingMeta = row.querySelector('[data-strategy-meta]');
        if (existingMeta) {
            existingMeta.remove();
        }

        var meta = document.createElement('div');
        meta.className = 'portfolio-strategy-meta';
        meta.setAttribute('data-strategy-meta', 'true');

        function createPill(text, isMuted) {
            var pill = document.createElement('span');
            pill.className = 'portfolio-strategy-meta-pill';
            if (isMuted) {
                pill.classList.add('is-muted');
            }
            pill.textContent = text;
            return pill;
        }

        function appendSection(label, content) {
            var section = document.createElement('div');
            section.className = 'portfolio-strategy-meta-section';

            if (typeof content === 'string') {
                var valueEl = document.createElement('span');
                valueEl.className = 'portfolio-strategy-meta-value';
                valueEl.textContent = content;
                section.appendChild(valueEl);
            } else if (content) {
                section.appendChild(content);
            }

            meta.appendChild(section);
        }

        var baseQty = row ? getRowBaseQty(row) : (strategy.qty_multiplier || 1);
        var portfolioQty = getPortfolioQtyMultiplier();
        var effectiveQty = row
            ? parseInt(row.getAttribute('data-strategy-effective-qty') || '0', 10) || (baseQty * portfolioQty)
            : (baseQty * portfolioQty);
        var qtyFormula = document.createElement('div');
        qtyFormula.className = 'portfolio-qty-formula';

        var leftValue = document.createElement('span');
        leftValue.textContent = String(baseQty);
        qtyFormula.appendChild(leftValue);

        var multiplySymbol = document.createElement('span');
        multiplySymbol.textContent = '×';
        qtyFormula.appendChild(multiplySymbol);

        var multiplierValue = document.createElement('span');
        multiplierValue.textContent = String(portfolioQty);
        qtyFormula.appendChild(multiplierValue);

        var equalSymbol = document.createElement('span');
        equalSymbol.textContent = '=';
        qtyFormula.appendChild(equalSymbol);

        var resultValue = document.createElement('span');
        resultValue.className = 'portfolio-qty-formula-result';
        resultValue.textContent = String(effectiveQty);
        qtyFormula.appendChild(resultValue);

        appendSection('Quantity multiplier', qtyFormula);

        if (currentFilterMode === 'Weekdays') {
            var allDays = ['M', 'T', 'W', 'Th', 'F', 'Sa', 'Su'];
            var selectedDays = Array.isArray(strategy.weekdays) && strategy.weekdays.length ? strategy.weekdays : ['M', 'T', 'W', 'Th', 'F'];
            var daysWrap = document.createElement('div');
            daysWrap.className = 'portfolio-strategy-meta-days';
            allDays.forEach(function (day) {
                daysWrap.appendChild(createPill(day, selectedDays.indexOf(day) === -1));
            });
            appendSection('Weekdays', daysWrap);
        } else {
            var dteValues = normalizeDteValues(strategy.dte && strategy.dte.length ? strategy.dte : [0]);
            appendSection('DTE', (dteValues.length ? dteValues.join(', ') : '0') + ' DTE');
        }

        appendSection('Entry', strategy.entry_time || '-');
        appendSection('Exit', strategy.exit_time || '-');
        var actionEl = row.querySelector('[data-strategy-action]');
        if (actionEl && actionEl.parentNode === row) {
            row.insertBefore(meta, actionEl);
        } else {
            row.appendChild(meta);
        }
    }

    function fetchPortfolioStrategyDetails(strategies) {
        var items = Array.isArray(strategies) ? strategies : [];
        var strategyIds = items.map(function (strategy) {
            return strategy && (strategy.id || strategy._id) ? String(strategy.id || strategy._id) : '';
        }).filter(Boolean);

        if (!strategyIds.length) {
            window._portfolioStrategyDetails = {};
            return Promise.resolve(window._portfolioStrategyDetails);
        }

        return Promise.all(strategyIds.map(function (strategyId) {
            return fetch(getPortfolioActivationApiUrl('strategyById', encodeURIComponent(strategyId)))
                .then(function (response) {
                    if (!response.ok) {
                        return null;
                    }
                    return response.json().catch(function () {
                        return null;
                    });
                })
                .catch(function () {
                    return null;
                });
        })).then(function (results) {
            var detailMap = {};
            results.forEach(function (detail, index) {
                if (detail) {
                    detailMap[strategyIds[index]] = ensureExecutionConfigDetail(detail, items[index] || {});
                }
            });
            window._portfolioStrategyDetails = detailMap;
            return detailMap;
        });
    }

    function applyExecutionModeToRow(row) {
        var strategyId = row.getAttribute('data-strategy-id') || '';
        var strategyData = strategyId && window._portfolioRenderedStrategies ? window._portfolioRenderedStrategies[strategyId] : null;
        if (strategyData) {
            renderStrategyMeta(row, strategyData);
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
                    rowsContainer.querySelectorAll('[data-strategy-id]').forEach(function (row) {
                        row.setAttribute('data-strategy-dte', JSON.stringify(normalizeDteValues(selectedDtes)));
                        var strategyId = row.getAttribute('data-strategy-id') || '';
                        var strategyData = strategyId && window._portfolioRenderedStrategies ? window._portfolioRenderedStrategies[strategyId] : null;
                        if (strategyData) {
                            strategyData.dte = normalizeDteValues(selectedDtes);
                            renderStrategyMeta(row, strategyData);
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
        row.classList.add('portfolio-row-shell');
        row.setAttribute('data-strategy-id', strategy && (strategy._id || strategy.id) ? String(strategy._id || strategy.id) : '');

        var displayDetails = getStrategyDisplayDetails(strategy);
        strategy.is_weekdays = currentFilterMode === 'Weekdays';
        strategy.name = displayDetails.name;
        strategy.underlying = displayDetails.underlying;
        strategy.product = displayDetails.product;
        strategy.entry_time = displayDetails.entryTime;
        strategy.exit_time = displayDetails.exitTime;

        var checkbox = row.querySelector('input[type="checkbox"]');
        var checkboxLabel = checkbox ? row.querySelector('label[for="' + checkbox.id + '"]') : null;
        var rowName = row.querySelector('[data-testid="special-text-tooltip"]');
        var detailSpans = row.querySelectorAll('.text-xs.font-normal.text-primaryBlack-700');
        var templateEditButton = row.querySelector('button');

        setRowBaseQty(row, strategy.qty_multiplier || 1);
        row.setAttribute('data-strategy-slippage', String(Number(strategy.slippage || 0) || 0));
        row.setAttribute('data-strategy-dte', JSON.stringify(normalizeDteValues(strategy.dte && strategy.dte.length ? strategy.dte : [0])));
        updateRowEffectiveQty(row);

        if (checkbox) {
            var checkboxId = 'portfolio-strategy-' + index;
            checkbox.id = checkboxId;
            checkbox.checked = strategy.checked !== false;
            if (checkboxLabel) {
                checkboxLabel.setAttribute('for', checkboxId);
            }
            syncRowSelectionState(checkbox);
            checkbox.addEventListener('change', function () {
                syncRowSelectionState(checkbox);
                updateSelectionSummary();
                if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
                    window._renderPortfolioBacktestReports(window._portfolioResult);
                }
            });
        }

        if (rowName) {
            rowName.textContent = displayDetails.name;
        }

        if (detailSpans.length >= 2) {
            detailSpans[0].textContent = displayDetails.underlying;
            detailSpans[1].textContent = displayDetails.product;
        }
        if (templateEditButton) {
            templateEditButton.remove();
        }

        updateWeekdayState(row, strategy.weekdays);
        applyExecutionModeToRow(row);
        renderStrategyMeta(row, strategy);
        renderExecutionConfigAction(row);
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
        window._portfolioRenderedStrategies = {};
        strategies.forEach(function (strategy) {
            if (strategy && (strategy._id || strategy.id)) {
                var strategyId = String(strategy._id || strategy.id);
                window._portfolioStrategyMap[strategyId] = strategy.name || 'Untitled Strategy';
                window._portfolioRenderedStrategies[strategyId] = strategy;
            }
        });
        setTitle(data.name || 'Portfolio');
        setSummary(strategies.length);
        rowsContainer.innerHTML = '';
        strategies.forEach(function (strategy, index) {
            rowsContainer.appendChild(createRow(strategy, index));
        });
        updateSelectionSummary();
        setActiveTopFilter(currentFilterMode === 'DTE' ? dteTab : weekdaysTab);
        renderTopFilterMenu(currentFilterMode);
        if (window._portfolioResult && typeof window._renderPortfolioBacktestReports === 'function') {
            window._renderPortfolioBacktestReports(window._portfolioResult);
        }
    }

    function collectPortfolioStrategiesState() {
        var strategies = [];
        rowsContainer.querySelectorAll('[data-strategy-id]').forEach(function (row) {
            var strategyId = String(row.getAttribute('data-strategy-id') || '');
            if (!strategyId) {
                return;
            }

            var checkbox = row.querySelector('input[id^="portfolio-strategy-"]');
            var weekdays = [];
            var storedDte = [];
            var storedSlippage = Number(row.getAttribute('data-strategy-slippage') || 0) || 0;
            try {
                weekdays = JSON.parse(row.getAttribute('data-strategy-weekdays') || '[]');
            } catch (error) {
                weekdays = [];
            }

            try {
                storedDte = normalizeDteValues(JSON.parse(row.getAttribute('data-strategy-dte') || '[]'));
            } catch (error) {
                storedDte = [];
            }

            strategies.push({
                id: strategyId,
                checked: !!(checkbox && checkbox.checked),
                dte: storedDte.length ? storedDte : normalizeDteValues(selectedTopFilters.DTE || [0]),
                qty_multiplier: getRowBaseQty(row),
                slippage: storedSlippage,
                weekdays: weekdays.length ? weekdays : ['M', 'T', 'W', 'Th', 'F']
            });
        });
        return strategies;
    }

    function savePortfolioState(options) {
        var strategyStates = collectPortfolioStrategiesState();
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

        return fetch(getPortfolioActivationApiUrl('portfolioSave'), {
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

    function getSelectedStrategyRows() {
        var selectedRows = [];
        rowsContainer.querySelectorAll('[data-strategy-id]').forEach(function (row) {
            var checkbox = row.querySelector('input[id^="portfolio-strategy-"]');
            if (checkbox && checkbox.checked) {
                selectedRows.push(row);
            }
        });
        return selectedRows;
    }

    function parseRowJsonAttribute(row, attributeName, fallbackValue) {
        if (!row) {
            return fallbackValue;
        }
        try {
            var rawValue = row.getAttribute(attributeName);
            if (!rawValue) {
                return fallbackValue;
            }
            return JSON.parse(rawValue);
        } catch (error) {
            return fallbackValue;
        }
    }

    function buildPrepareActivationPayload(trades) {
        var selectedBrokerId = getSelectedActivationBrokerId();
        var tradeMap = {};
        var renderedStrategies = window._portfolioRenderedStrategies || {};
        var strategyDetailsMap = window._portfolioStrategyDetails || {};
        (Array.isArray(trades) ? trades : []).forEach(function (trade) {
            if (trade && trade.strategy_id) {
                tradeMap[String(trade.strategy_id)] = trade;
            }
        });

        return getSelectedStrategyRows().map(function (row) {
            var strategyId = String(row.getAttribute('data-strategy-id') || '');
            var strategy = strategyId ? renderedStrategies[strategyId] : null;
            var detail = strategyId ? strategyDetailsMap[strategyId] : null;
            if (detail) {
                detail = ensureExecutionConfigDetail(cloneJson(detail), strategy || {});
                strategyDetailsMap[strategyId] = detail;
            }
            var trade = tradeMap[strategyId] || {};
            return {
                strategy_id: strategyId,
                name: (strategy && strategy.name) || (detail && detail.name) || '',
                qty_multiplier: getRowBaseQty(row),
                dte: normalizeDteValues(parseRowJsonAttribute(row, 'data-strategy-dte', [0])),
                weekdays: parseRowJsonAttribute(row, 'data-strategy-weekdays', ['M', 'T', 'W', 'Th', 'F']),
                is_weekdays: currentFilterMode === 'Weekdays',
                slippage: Number(row.getAttribute('data-strategy-slippage') || 0) || 0,
                broker: selectedBrokerId || trade.broker || (detail && detail.broker) || (strategy && strategy.broker) || null,
                broker_type: trade.broker_type || (detail && detail.broker_type) || (strategy && strategy.broker_type) || (activationMode === 'algo-backtest' ? 'algo-backtest' : 'Broker.FlatTrade'),
                user_id: trade.user_id || (detail && detail.user_id) || (strategy && strategy.user_id) || DEFAULT_ACTIVATION_USER_ID,
                ticker: trade.ticker || ((detail && detail.underlying) || (strategy && strategy.underlying) || 'NIFTY'),
                strategy_detail: cloneJson(detail || strategy || {})
            };
        });
    }

    function prepareActivationRecords(trades) {
        return fetch(getPortfolioActivationApiUrl('portfolio/prepare-activation'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                portfolio_id: portfolioId,
                activation_mode: activationMode,
                strategies: buildPrepareActivationPayload(trades)
            })
        }).then(function (response) {
            return response.json().catch(function () {
                return {};
            }).then(function (data) {
                if (!response.ok) {
                    throw new Error(data.detail || 'Failed to prepare activation strategies');
                }
                return data;
            });
        });
    }

    function applyPreparedStrategyIds(trades, preparedData) {
        var preparedMap = {};
        var preparedRows = Array.isArray(preparedData && preparedData.executed_strategies)
            ? preparedData.executed_strategies
            : [];

        preparedRows.forEach(function (item) {
            if (item && item.source_strategy_id) {
                preparedMap[String(item.source_strategy_id)] = item;
            }
        });

        (Array.isArray(trades) ? trades : []).forEach(function (trade) {
            if (!trade || !trade.strategy_id) {
                return;
            }
            var sourceStrategyId = String(trade.strategy_id);
            var prepared = preparedMap[sourceStrategyId];
            if (!prepared || !prepared.assigned_strategy_id) {
                return;
            }
            trade.source_strategy_id = sourceStrategyId;
            trade.executed_strategy_id = prepared.assigned_strategy_id;
            trade.strategy_id = prepared.assigned_strategy_id;
            trade.number_of_executions = prepared.number_of_executions;
        });
        return trades;
    }

    function buildInitialLiveExecutionPayload() {
        var activationTime = getActivationRequestTime();
        var activationBatchId = generateExecutionObjectId('group');
        var portfolioGroupName = loadedPortfolioName || 'Portfolio Activation';
        var renderedStrategies = window._portfolioRenderedStrategies || {};
        var strategyDetailsMap = window._portfolioStrategyDetails || {};
        var selectedBrokerId = getSelectedActivationBrokerId();

        return getSelectedStrategyRows().map(function (row) {
            var strategyId = String(row.getAttribute('data-strategy-id') || '');
            var strategy = strategyId ? renderedStrategies[strategyId] : null;
            var detail = strategyId ? strategyDetailsMap[strategyId] : null;
            var displayDetails = getStrategyDisplayDetails(strategy || detail || {});
            var executionId = generateExecutionObjectId(strategyId || displayDetails.name);
            var ticker = displayDetails.underlying || 'NIFTY';
            var defaultBrokerType = activationMode === 'algo-backtest' ? 'algo-backtest' : 'Broker.FlatTrade';
            var brokerType = (detail && detail.broker_type) || (strategy && strategy.broker_type) || defaultBrokerType;
            var broker = selectedBrokerId || (detail && detail.broker) || (strategy && strategy.broker) || null;
            var defaultStatus = 'StrategyStatus.Live_Running';

            return {
                _id: executionId,
                active_on_server: true,
                activation_mode: activationMode,
                broker: broker,
                broker_type: brokerType,
                check_after_ts: buildStrategyDateTime(displayDetails.entryTime, activationTime, true) || formatDateTime(activationTime, false),
                config: {
                    Ticker: ticker
                },
                creation_ts: formatDateTime(activationTime, true),
                entry_time: buildStrategyDateTime(displayDetails.entryTime, activationTime, true) || formatDateTime(activationTime, false),
                exit_time: buildStrategyDateTime(displayDetails.exitTime, activationTime, true),
                external_start: null,
                free_execution: false,
                is_ra_algo: false,
                is_shared: false,
                last_activation_ts: formatDateTime(activationTime, true),
                legs: [],
                name: displayDetails.name || 'Untitled Strategy',
                portfolio: {
                    group_id: activationBatchId,
                    group_name: portfolioGroupName,
                    portfolio: portfolioId
                },
                reentry_time_restriction: null,
                skip_initial_candles: 0,
                status: defaultStatus,
                trade_status: 1,
                strategy_id: strategyId,
                ticker: ticker
            };
        });
    }

    function handleActivateSelectedClick(event) {
        if (event) {
            event.preventDefault();
        }
        if (!getSelectedActivationBrokerId()) {
            showToast('Please select broker.', 'error');
            return;
        }
        var payload = buildInitialLiveExecutionPayload();
        if (!payload.length) {
            showToast('Select at least one strategy to activate.', 'error');
            console.warn('No strategies selected for activation.');
            return;
        }
        prepareActivationRecords(payload)
            .then(function (preparedData) {
                applyPreparedStrategyIds(payload, preparedData);
                return fetch(getPortfolioActivationApiUrl('portfolioActivate'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        portfolio_id: portfolioId,
                        activation_mode: activationMode,
                        current_datetime: activationMode === 'algo-backtest' && requestedCurrentDateTime ? requestedCurrentDateTime : undefined,
                        trades: payload
                    })
                });
            })
            .then(function (response) {
                return response.json().catch(function () {
                    return {};
                }).then(function (data) {
                    if (!response.ok) {
                        throw new Error(data.detail || 'Failed to activate selected strategies');
                    }
                    return data;
                });
            }).then(function (data) {
                var records = Array.isArray(data && data.records) ? data.records : payload;
                var groupId = String(
                    (data && data.group_id)
                    || ((((records[0] || {}).portfolio || {}).group_id) || '')
                ).trim();
                if (!groupId) {
                    throw new Error('Activated group_id was not returned');
                }
                console.log('Initial live execution payload stored', {
                    activation_mode: activationMode,
                    collection_name: data && data.collection_name,
                    group_id: groupId,
                    records: records
                });
                console.table(records.map(function (item) {
                    return {
                        name: item.name,
                        strategy_id: item.strategy_id,
                        source_strategy_id: item.source_strategy_id,
                        activation_mode: item.activation_mode || activationMode,
                        active_on_server: item.active_on_server,
                        status: item.status,
                        entry_time: item.entry_time,
                        exit_time: item.exit_time,
                        legs_count: Array.isArray(item.legs) ? item.legs.length : 0
                    };
                }));
                return fetch(
                    getPortfolioActivationApiUrl('portfolioStartGroup', encodeURIComponent(groupId))
                    + '?activation_mode=' + encodeURIComponent(activationMode)
                )
                    .then(function (startResponse) {
                        return startResponse.json().catch(function () {
                            return {};
                        }).then(function (startData) {
                            if (!startResponse.ok) {
                                throw new Error(startData.detail || 'Failed to start activated group');
                            }
                            return {
                                records: records,
                                groupId: groupId,
                                startData: startData
                            };
                        });
                    });
            }).then(function (result) {
                var records = result.records || [];
                window._latestLiveExecutionPayload = records;
                showToast('Selected strategies activated successfully.');
            }).catch(function (error) {
                console.error('Failed to store activation payload', error);
                showToast(error.message || 'Failed to activate selected strategies', 'error');
            });
    }

    function renderError(message) {
        setTitle('Portfolio');
        setSummary(0);
        rowsContainer.innerHTML = '<div class="rounded border border-red-200 bg-red-50 p-4 text-sm text-red-600">' + message + '</div>';
    }

    fetchBrokerConfigurations().catch(function (error) {
        showToast(error.message || 'Failed to load brokers', 'error');
        return [];
    });

    fetch(getPortfolioActivationApiUrl('portfolioById', encodeURIComponent(portfolioId)))
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
            loadedPortfolioData = data;
            return fetchPortfolioStrategyDetails(data && data.strategies).then(function () {
                return data;
            });
        })
        .then(renderPortfolio)
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
        if (slippageFilter && !slippageFilter.contains(event.target) && slippagePopover) {
            slippagePopover.hidden = true;
            if (slippageButton) {
                slippageButton.setAttribute('aria-expanded', 'false');
            }
        }
    });

    if (qtyDecrementBtn) {
        qtyDecrementBtn.addEventListener('click', function () {
            setPortfolioQtyMultiplier(getPortfolioQtyMultiplier() - 1);
        });
    }

    if (qtyIncrementBtn) {
        qtyIncrementBtn.addEventListener('click', function () {
            setPortfolioQtyMultiplier(getPortfolioQtyMultiplier() + 1);
        });
    }

    if (qtySelect) {
        qtySelect.addEventListener('change', function () {
            updateQtyButtonLabel();
        });
    }

    if (editPortfolioLinkBtn) {
        editPortfolioLinkBtn.addEventListener('click', function () {
            if (!portfolioId) {
                return;
            }
            var portfolioPageUrl = typeof window.buildNamedPageUrl === 'function'
                ? window.buildNamedPageUrl('portfolio') + '?strategy_id=' + encodeURIComponent(portfolioId)
                : buildAppUrl('portfolio.html') + '?strategy_id=' + encodeURIComponent(portfolioId);
            window.location.href = portfolioPageUrl;
        });
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
                row.setAttribute('data-strategy-slippage', selectedSlippage);
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

    if (activateSelectedBtn) {
        activateSelectedBtn.addEventListener('click', handleActivateSelectedClick);
    }

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

            setBtnState('Starting...', true);

            fetch(getPortfolioActivationApiUrl('portfolioBacktestStart'), {
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

                    var pollInterval = setInterval(function () {
                        fetch(getPortfolioActivationApiUrl('backtestStatus', jobId))
                            .then(function (r) { return r.json(); })
                            .then(function (s) {
                                if (s.status === 'running') {
                                    var pct = s.percent || 0;
                                    var done = s.completed !== undefined ? s.completed : '?';
                                    var sname = s.strategy_name ? ' — ' + s.strategy_name : '';
                                    setBtnState(
                                        pct.toFixed(1) + '% (' + done + '/' + strategyCount + sname + ')',
                                        true
                                    );
                                } else if (s.status === 'done') {
                                    clearInterval(pollInterval);
                                    setBtnState('Fetching result...', true);
                                    fetch(getPortfolioActivationApiUrl('backtestResult', jobId))
                                        .then(function (r) { return r.json(); })
                                        .then(function (result) {
                                            setBtnState('Backtest', false);
                                            window._portfolioResult = result;
                                            if (typeof window.capturePortfolioBacktestQtySnapshot === 'function') {
                                                window.capturePortfolioBacktestQtySnapshot();
                                            }
                                            if (typeof window._onPortfolioBacktestResult === 'function') {
                                                window._onPortfolioBacktestResult(result);
                                            }
                                            var resultSection = document.querySelector('[data-testid="result-chart"]');
                                            if (resultSection) {
                                                resultSection.scrollIntoView({ behavior: 'smooth' });
                                            }
                                        })
                                        .catch(function (err) {
                                            setBtnState('Backtest', false);
                                            alert('Error fetching result: ' + err.message);
                                        });
                                } else if (s.status === 'error') {
                                    clearInterval(pollInterval);
                                    setBtnState('Backtest', false);
                                    alert('Backtest error: ' + (s.error || 'Unknown error'));
                                }
                            })
                            .catch(function (err) {
                                clearInterval(pollInterval);
                                setBtnState('Backtest', false);
                                alert('Poll error: ' + err.message);
                            });
                    }, 1000);
                })
                .catch(function (err) {
                    setBtnState('Backtest', false);
                    alert('Error: ' + err.message);
                });
        });
    }
})();

(function () {
    var SAMPLE_RESULT_URL = getPortfolioActivationApiUrl('backtestSampleResult');
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
            var values = [];
            try {
                values = normalizeDteValues(JSON.parse(row.getAttribute('data-strategy-dte') || '[]'));
            } catch (error) {
                values = [];
            }
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
            try {
                values = JSON.parse(row.getAttribute('data-strategy-weekdays') || '[]');
            } catch (error) {
                values = [];
            }
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
            var slippage = Number(row.getAttribute('data-strategy-slippage') || 0) || 0;
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
        window._portfolioBacktestQtySnapshot = getPortfolioEffectiveQtyMap();
    }
    window.capturePortfolioBacktestQtySnapshot = capturePortfolioBacktestQtySnapshot;

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
        if (!root || !Array.isArray(root.results)) return [];
        return root.results.filter(function (item) {
            return item
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
        var losing = pnls.filter(function (pnl) { return pnl < 0; });
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
        var drawdownStartCandidate = trades[0] ? trades[0].date : null;
        var maxDrawdown = 0;
        var maxDrawdownStart = trades[0] ? trades[0].date : null;
        var maxDrawdownEnd = trades[0] ? trades[0].date : null;
        var activeDrawdownTrades = 0;
        var maxTradesInDrawdown = 0;
        var winStreak = 0;
        var maxWinStreak = 0;
        var lossStreak = 0;
        var maxLossStreak = 0;

        trades.forEach(function (trade) {
            var pnl = toTradePnl(trade);
            if (pnl > 0) {
                winStreak += 1;
                lossStreak = 0;
            } else if (pnl < 0) {
                lossStreak += 1;
                winStreak = 0;
            } else {
                winStreak = 0;
                lossStreak = 0;
            }
            if (winStreak > maxWinStreak) maxWinStreak = winStreak;
            if (lossStreak > maxLossStreak) maxLossStreak = lossStreak;

            cumulative += pnl;
            if (cumulative >= peak) {
                peak = cumulative;
                drawdownStartCandidate = trade.date;
                activeDrawdownTrades = 0;
            }
            var drawdown = cumulative - peak;
            if (drawdown < 0) {
                activeDrawdownTrades += 1;
                if (activeDrawdownTrades > maxTradesInDrawdown) {
                    maxTradesInDrawdown = activeDrawdownTrades;
                }
            }
            if (drawdown < maxDrawdown) {
                maxDrawdown = drawdown;
                maxDrawdownStart = drawdownStartCandidate;
                maxDrawdownEnd = trade.date;
            }
        });

        var expectancy = periods ? ((winPct / 100) * avgWin) + ((lossPct / 100) * avgLoss) : 0;
        var rewardToRisk = avgLoss !== 0 ? Math.abs(avgWin / avgLoss) : 0;
        var expectancyRatio = avgLoss !== 0 ? Math.abs(expectancy / avgLoss) : 0;
        var returnToMaxDD = maxDrawdown !== 0 ? Math.abs(totalProfit / maxDrawdown) : 0;
        var durationDays = maxDrawdownStart && maxDrawdownEnd
            ? Math.round((new Date(maxDrawdownEnd) - new Date(maxDrawdownStart)) / 86400000) + 1
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
            + '<th class="border-2 border-zinc-300 p-1 text-sm dark:border-primaryBlack-500">Leg Type</th>'
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
            var cmp = 0;
            if (portfolioFrSortBy === 'ProfitLoss') {
                cmp = getPortfolioTradePnlForSort(a) - getPortfolioTradePnlForSort(b);
            } else {
                var ka = String((a && a.date) || '') + ' ' + String((a && a.entry_time) || '');
                var kb = String((b && b.date) || '') + ' ' + String((b && b.entry_time) || '');
                cmp = ka < kb ? -1 : ka > kb ? 1 : 0;
            }
            return portfolioFrSortDir === 'Desc' ? -cmp : cmp;
        });
        return copy;
    }

    function renderPortfolioFullReport() {
        var tbody = document.getElementById('full-report-tbody');
        var totalEl = document.getElementById('full-report-total');
        var rangeEl = document.getElementById('full-report-range');
        var pagingEl = document.getElementById('fr-pagination');
        if (!tbody) return;

        var trades = sortPortfolioFullReportTrades();
        var totalTrades = trades.length;
        var totalPages = Math.ceil(totalTrades / PORTFOLIO_FR_PAGE_SIZE);
        if (portfolioFrCurrentPage > totalPages) portfolioFrCurrentPage = 1;

        var startIdx = (portfolioFrCurrentPage - 1) * PORTFOLIO_FR_PAGE_SIZE;
        var pageTrades = trades.slice(startIdx, startIdx + PORTFOLIO_FR_PAGE_SIZE);
        var cellCls = 'border-2 border-zinc-300 p-1 text-xs font-medium dark:border-primaryBlack-500';
        var html = '';

        pageTrades.forEach(function (trade, i) {
            var globalIdx = startIdx + i + 1;
            var tradePnl = getPortfolioTradePnlForSort(trade);
            var pnlCls = tradePnl < 0 ? 'text-red-500' : 'text-green-600';

            html += '<tr class="border-b border-b-white bg-primaryBlack-100">'
                + '<td class="' + cellCls + ' bg-primaryBlack-350 font-bold">' + globalIdx + '</td>'
                + '<td class="' + cellCls + '">' + escapeHtml(trade.date || '') + '</td>'
                + '<td class="' + cellCls + '">' + escapeHtml(trade.entry_time || '') + '</td>'
                + '<td class="' + cellCls + '">' + escapeHtml(trade.date || '') + '</td>'
                + '<td class="' + cellCls + '">' + escapeHtml(trade.exit_time || '') + '</td>'
                + '<td colspan="12" class="' + cellCls + ' text-center font-semibold text-primaryBlack-750">' + escapeHtml(trade.strategy_name || '') + '</td>'
                + '<td class="' + cellCls + ' ' + pnlCls + ' font-bold">' + formatReportPnl(tradePnl) + '</td>'
                + '</tr>';

            var detailIdx = 1;
            (trade.legs || []).forEach(function (leg) {
                var qty = Number(leg.lots || 0) * Number(leg.lot_size || 0);
                var subTrades = leg.sub_trades && leg.sub_trades.length ? leg.sub_trades : [null];
                subTrades.forEach(function (st) {
                    var rawStPnl = st && st.pnl !== undefined ? st.pnl : leg.pnl;
                    var detailBrokerage = getPortfolioDetailBrokerage(leg, st);
                    var stPnl = Number(rawStPnl || 0) - Number(detailBrokerage || 0);
                    var stPnlCls = Number(stPnl || 0) < 0 ? 'text-red-500' : 'text-green-600';
                    html += '<tr class="border-b border-b-white">'
                        + '<td class="' + cellCls + ' bg-primaryBlack-350">' + globalIdx + '.' + detailIdx + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(st && st.entry_date ? st.entry_date : (trade.date || '')) + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(st && st.entry_time ? st.entry_time : (leg.entry_time || '')) + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(st && st.exit_date ? st.exit_date : (trade.date || '')) + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(st && st.exit_time ? st.exit_time : (leg.exit_time || '')) + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(st && st.option_type ? st.option_type : (leg.type || '')) + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(st && st.strike ? st.strike : (leg.strike || '')) + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(leg.position || '') + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(qty || '') + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(st && st.entry_action ? st.entry_action : '') + '</td>'
                        + '<td class="' + cellCls + '">' + formatReportPrice(st && st.entry_price !== undefined ? st.entry_price : leg.entry_price) + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(st && st.exit_action ? st.exit_action : '') + '</td>'
                        + '<td class="' + cellCls + '">' + formatReportPrice(st && st.exit_price !== undefined ? st.exit_price : leg.exit_price) + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(st && st.reentry_type ? st.reentry_type : '') + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(st && st.reentry_number !== undefined && st.reentry_number !== null ? st.reentry_number : '') + '</td>'
                        + '<td class="' + cellCls + '">' + escapeHtml(st && st.exit_reason ? st.exit_reason : (leg.exit_reason || '')) + '</td>'
                        + '<td class="' + cellCls + '"></td>'
                        + '<td class="' + cellCls + ' ' + stPnlCls + '">' + formatReportPnl(stPnl) + '</td>'
                        + '</tr>';
                    detailIdx += 1;
                });
            });
        });

        tbody.innerHTML = html || '<tr><td colspan="19" class="' + cellCls + ' p-3 text-center text-primaryBlack-600">No data.</td></tr>';
        var from = totalTrades ? startIdx + 1 : 0;
        var to = Math.min(startIdx + PORTFOLIO_FR_PAGE_SIZE, totalTrades);
        if (totalEl) totalEl.textContent = String(totalTrades);
        if (rangeEl) rangeEl.textContent = from + ' - ' + to;

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
            capturePortfolioBacktestQtySnapshot();
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
    }

    window._renderPortfolioBacktestReports = renderPortfolioBacktestReports;
    window._onPortfolioBacktestResult = renderPortfolioBacktestReports;
    bindCorrelationMatrixControls();
    bindPortfolioCostAnalysis();
    initPortfolioFullReportControls();

    fetch(SAMPLE_RESULT_URL)
        .then(function (response) {
            if (!response.ok) throw new Error('Failed to load sample result');
            return response.json();
        })
        .then(function (payload) {
            if (!window._portfolioResult) {
                window._portfolioResult = payload;
            }
            renderPortfolioBacktestReports(window._portfolioResult || payload);
        })
        .catch(function () {
            renderYearWiseTable({});
        });

})();
