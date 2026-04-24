(function () {
    var localApiRouteMap = {
        strategyList: 'strategy/list',
        portfolioList: 'portfolio/list',
        tradesList: 'trades/list',
        executions: 'executions'
    };
    var localPageRouteMap = {
        portfolio: 'portfolio.html',
        portfolioActivation: 'portfolio-activation.html'
    };

    function buildAlgoApiUrl(path) {
        if (window.APP_CONFIG && typeof window.APP_CONFIG.buildAlgoApiUrl === 'function') {
            return window.APP_CONFIG.buildAlgoApiUrl(path);
        }
        if (typeof window.buildAlgoApiUrl === 'function') {
            return window.buildAlgoApiUrl(path);
        }
        var baseUrl = (window.APP_CONFIG && window.APP_CONFIG.algoApiBaseUrl)
            || window.APP_ALGO_API_BASE_URL
            || ((((window.location && window.location.hostname || '').toLowerCase() === 'finedgealgo.com'
                || (window.location && window.location.hostname || '').toLowerCase() === 'www.finedgealgo.com')
                ? 'https://finedgealgo.com/algo'
                : 'http://localhost:8000/algo'));
        return baseUrl.replace(/\/+$/, '') + '/' + String(path || '').replace(/^\/+/, '');
    }

    function resolveCurrentUserId() {
        // 1. APP_CONFIG
        if (window.APP_CONFIG && String(window.APP_CONFIG.user_id || window.APP_CONFIG.userId || '').trim()) {
            return String(window.APP_CONFIG.user_id || window.APP_CONFIG.userId).trim();
        }
        // 2. Global window variable
        if (String(window.APP_USER_ID || window.user_id || '').trim()) {
            return String(window.APP_USER_ID || window.user_id).trim();
        }
        // 3. URL query param
        try {
            var params = new URLSearchParams(window.location.search || '');
            var fromUrl = String(params.get('user_id') || params.get('userId') || '').trim();
            if (fromUrl) return fromUrl;
        } catch (e) { /* ignore */ }
        // 4. localStorage
        try {
            var keys = ['user_id', 'userId', 'algo_user_id'];
            for (var i = 0; i < keys.length; i++) {
                var val = String(window.localStorage.getItem(keys[i]) || '').trim();
                if (val) return val;
            }
        } catch (e) { /* ignore */ }
        return '';
    }

    function buildNamedApiUrl(routeName, suffix) {
        if (window.APP_CONFIG && typeof window.APP_CONFIG.buildNamedApiUrl === 'function') {
            return window.APP_CONFIG.buildNamedApiUrl(routeName, suffix);
        }
        if (typeof window.buildNamedApiUrl === 'function') {
            return window.buildNamedApiUrl(routeName, suffix);
        }
        var routeMap = (window.APP_CONFIG && window.APP_CONFIG.apiRoutes) || window.APP_API_ROUTES || localApiRouteMap;
        var routePath = routeMap[routeName] || routeName || '';
        var normalizedRoute = String(routePath).replace(/\/+$/, '');
        var normalizedSuffix = String(suffix || '').replace(/^\/+/, '');
        return buildAlgoApiUrl(normalizedSuffix ? normalizedRoute + '/' + normalizedSuffix : normalizedRoute);
    }

    function buildNamedPageUrl(routeName, suffix) {
        if (window.APP_CONFIG && typeof window.APP_CONFIG.buildNamedPageUrl === 'function') {
            var basePageUrl = window.APP_CONFIG.buildNamedPageUrl(routeName);
            return basePageUrl + (suffix || '');
        }
        if (typeof window.buildNamedPageUrl === 'function') {
            return window.buildNamedPageUrl(routeName) + (suffix || '');
        }
        var routeMap = (window.APP_CONFIG && window.APP_CONFIG.pageRoutes) || window.APP_PAGE_ROUTES || localPageRouteMap;
        var routePath = routeMap[routeName] || routeName || '';
        if (typeof window.buildAppUrl === 'function') {
            return window.buildAppUrl(routePath) + (suffix || '');
        }
        return './' + String(routePath || '').replace(/^\/+/, '') + (suffix || '');
    }

    var table = document.querySelector('.ff-strategy-table');
    var rowsHost = document.getElementById('ff-strategy-rows');
    var countLabel = document.getElementById('ff-strategy-count');
    var paginationHost = document.getElementById('ff-strategy-pagination');
    var searchInput = document.getElementById('ff-list-search');
    var marketCountdownEl = document.getElementById('ff-market-countdown');
    var listeningDateInput = document.getElementById('ff-listening-date');
    var listeningDateTrigger = document.getElementById('ff-listening-date-trigger');
    var listeningDateLabel = document.getElementById('ff-listening-date-label');
    var calendarPopup = document.getElementById('ff-calendar-popup');
    var calendarGrid = document.getElementById('ff-calendar-grid');
    var calendarWeekdays = document.getElementById('ff-calendar-weekdays');
    var calendarMonthToggle = document.getElementById('ff-calendar-month-toggle');
    var calendarYearToggle = document.getElementById('ff-calendar-year-toggle');
    var calendarPrevBtn = document.getElementById('ff-calendar-prev-btn');
    var calendarNextBtn = document.getElementById('ff-calendar-next-btn');
    var calendarConfirmBtn = document.getElementById('ff-calendar-confirm-btn');
    var listeningBehindTimeSelect = document.getElementById('ff-listening-behind-time');
    var startListeningBtn = document.getElementById('ff-start-listening-btn');
    var pauseListeningBtn = document.getElementById('ff-pause-listening-btn');
    var stopListeningBtn = document.getElementById('ff-stop-listening-btn');
    var socketStatusEl = document.getElementById('ff-socket-status');
    var socketStatusLabel = document.getElementById('ff-socket-status-label');
    var secondsPanel = document.getElementById('ff-seconds-panel');
    var secondsSlider = document.getElementById('ff-seconds-slider');
    var secondsStartLabel = document.getElementById('ff-seconds-start-label');
    var secondsEndLabel = document.getElementById('ff-seconds-end-label');
    var secondsDateChip = document.getElementById('ff-seconds-date-chip');
    var secondsTimeInput = document.getElementById('ff-seconds-time-input');
    var secondsAutoplayBtn = document.getElementById('ff-seconds-autoplay-btn');
    var secondsSpeedSelect = document.getElementById('ff-seconds-speed');
    var importExportBtn = document.getElementById('ff-import-export-btn');
    var manualRunBtn = document.getElementById('ff-manual-run-btn');
    var autoloadToggleBtn = document.getElementById('ff-autoload-toggle-btn');
    var autoloadStatusEl = document.getElementById('ff-autoload-status');
    var headerBrokerListHost = document.getElementById('ff-header-broker-list');
    var seekButtons = Array.prototype.slice.call(document.querySelectorAll('[data-seek-minutes], [data-seek-target]'));
    var deployedRowsHost = document.getElementById('ff-deployed-rows');
    var tabButtons = Array.prototype.slice.call(document.querySelectorAll('[data-ff-tab]'));
    var setupModal = document.getElementById('ff-setup-modal');
    var setupStrategyName = document.getElementById('ff-setup-strategy-name');
    var brokerSettingsModal = document.getElementById('ff-broker-settings-modal');
    var brokerSettingsEditBtn = document.querySelector('[data-open-broker-settings]');
    var brokerStopLossInput = document.getElementById('ff-broker-stop-loss-input');
    var brokerTargetProfitInput = document.getElementById('ff-broker-target-profit-input');
    var brokerStopLossDisplay = document.getElementById('ff-broker-stop-loss-display');
    var brokerTargetProfitDisplay = document.getElementById('ff-broker-target-profit-display');
    var cancelDeploymentModal = document.getElementById('ff-cancel-deployment-modal');
    var confirmCancelDeploymentBtn = document.getElementById('ff-confirm-cancel-deployment-btn');
    var squareOffModal = document.getElementById('ff-square-off-modal');
    var confirmSquareOffBtn = document.getElementById('ff-confirm-square-off-btn');
    var weekdayPanel = document.getElementById('ff-weekday-panel');
    var dtePanel = document.getElementById('ff-dte-panel');
    var dteSelectAll = document.getElementById('ff-dte-select-all');
    var marketCountdownEndTime = '09:15';
    var backtestMarketOpenTime = '09:15';
    var marketCountdownTimer = null;
    var backtestCountdownState = null;
    var listeningPollTimer = null;
    var listeningFetchInFlight = false;
    var listeningState = 'idle';
    var deployedGroupExpandedState = {};
    var calendarMonthNames = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
    var calendarMonthNamesShort = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    var calendarState = {
        view: 'date',
        displayDate: null,
        selectedDate: null,
        draftDate: null,
        yearPageStart: 0
    };
    function resolveListeningModeKey() {
        var explicitMode = String(
            window.ALGO_ACTIVATION_MODE
            || (window.APP_CONFIG && window.APP_CONFIG.activation_mode)
            || ''
        ).trim();
        if (explicitMode) {
            return explicitMode;
        }
        var pathName = String(window.location && window.location.pathname || '').toLowerCase();
        if (pathName.indexOf('/live/') !== -1 || /\/live$/.test(pathName)) {
            return 'live';
        }
        if (pathName.indexOf('/forward-test/') !== -1 || /\/forward-test$/.test(pathName)) {
            return 'fast-forward';
        }
        return 'algo-backtest';
    }

    var listeningModeKey = resolveListeningModeKey();
    var executeOrdersSocketClient = null;
    var updateSocketClient = null;
    var socketManuallyPaused = false;
    var initialPositionSnapshotRequested = false;
    var currentListenTimeEl = null;
    var latestListenTimestamp = '';
    var timelineScrubValue = '';
    var timelineAutoplayTimer = null;
    var backtestAutoloadEnabled = true;
    var executeOrdersGroupId = String((new URLSearchParams(window.location.search).get('group_id') || '')).trim();
    var perPage = 20;
    var currentPage = 1;
    var currentDeployedRecords = [];
    var latestLtpSnapshot = [];
    var latestLtpSnapshotMap = {};
    var defaultBrowserTitle = String(document.title || '').trim() || 'Options Simulator';
    var ltpRenderFrameId = 0;
    var ltpRenderTimerId = 0;
    var ltpRenderHeartbeatId = 0;
    var lastLtpRenderAt = 0;
    var LTP_RENDER_INTERVAL_MS = 500;
    // Map of record._id → record (with legs) for LTP-based PnL calculation
    var executionRecordsMap = {};
    // Map of broker_id → broker settings (SL / Target / LockAndTrail state) from backend
    var brokerSettingsCache = {};
    var strategyPnlCache = {};
    var recordPriceLinkIndex = {};
    var aggregateCountsCache = null;
    var aggregateCountsListenSecondKey = '';
    var pendingChangedLtpKeys = {};
    var forceFullLtpRender = true;

    function buildExecuteOrdersSubscriptionPayload(selectedDate) {
        var payload = {
            trade_date: selectedDate,
            activation_mode: listeningModeKey,
            status: listeningModeKey
        };
        var currentUserId = resolveCurrentUserId();
        if (currentUserId) {
            payload.user_id = currentUserId;
        }
        if (executeOrdersGroupId) {
            payload.group_id = executeOrdersGroupId;
        }
        return payload;
    }

    // Cross-tab relay:
    // 1. execute-orders socket messages from any tab are relayed here via AlgoStreamSockets broadcast
    // 2. group_activated from portfolio-activation.html keeps this tab in sync without
    //    actively sending trade refresh requests from this dashboard script
    (function initCrossTabRelay() {
        // Relay: socket messages from other tabs (e.g. execute_order responses)
        if (window.AlgoStreamSockets && typeof window.AlgoStreamSockets.onBroadcast === 'function') {
            window.AlgoStreamSockets.onBroadcast('execute-orders', function (payload) {
                if (!payload || typeof payload !== 'object') return;
                handleExecuteOrdersSocketMessage(payload);
            });
        }

        // group_activated: portfolio-activation page finished activating a group
        // → dashboard updates its local date/socket state only
        if (typeof BroadcastChannel !== 'undefined') {
            try {
                var bc = new BroadcastChannel('algo_execute_orders');
                bc.onmessage = function (event) {
                    var msg = event && event.data;
                    // Skip relay envelopes (handled above by onBroadcast)
                    if (!msg || msg._bc_relayed) return;
                    if (msg.type !== 'group_activated') return;

                    var groupId = String(msg.group_id || '').trim();
                    var tradeDate = String(msg.trade_date || '').trim();
                    var activationMode = String(msg.activation_mode || listeningModeKey).trim();
                    if (!groupId || !tradeDate) return;

                    // Sync date picker
                    if (listeningDateInput && listeningDateInput.value !== tradeDate) {
                        listeningDateInput.value = tradeDate;
                        if (listeningDateLabel) listeningDateLabel.textContent = tradeDate;
                    }

                    // Ensure only execute-orders socket is connected (not update) when
                    // responding to a group_activated broadcast from portfolio-activation.html.
                    if (!executeOrdersSocketClient) {
                        if (window.AlgoStreamSockets) {
                            executeOrdersSocketClient = window.AlgoStreamSockets.createChannel({
                                channel: 'execute-orders',
                                userId: resolveCurrentUserId(),
                                onStatusChange: function (state, meta) {
                                    updateSocketStatus(state, meta || {});
                                },
                                onMessage: function (payload) {
                                    handleExecuteOrdersSocketMessage(payload);
                                }
                            });
                            executeOrdersSocketClient.connect();
                        }
                    }
                };
            } catch (e) { /* BroadcastChannel not supported */ }
        }
    }());

    function syncExecutionRecordsMap(records) {
        executionRecordsMap = {};
        (records || []).forEach(function (record) {
            if (record && record._id) {
                executionRecordsMap[String(record._id)] = record;
            }
        });
        rebuildRecordPriceLinkIndex();
        strategyPnlCache = {};
        aggregateCountsCache = null;
        aggregateCountsListenSecondKey = '';
        forceFullLtpRender = true;
    }

    function isLikelyObjectId(value) {
        return /^[a-f0-9]{24}$/i.test(String(value || '').trim());
    }

    function mergeExecutionRecordPreservingBrokerMeta(nextRecord, existingRecord) {
        var mergedRecord = Object.assign({}, existingRecord || {}, nextRecord || {});
        var nextBrokerDetails = nextRecord && typeof nextRecord.broker_details === 'object' ? nextRecord.broker_details : null;
        var existingBrokerDetails = existingRecord && typeof existingRecord.broker_details === 'object' ? existingRecord.broker_details : null;
        if (!nextBrokerDetails && existingBrokerDetails) {
            mergedRecord.broker_details = existingBrokerDetails;
        }

        var nextBrokerLabel = String(nextRecord && nextRecord.broker_label || '').trim();
        var existingBrokerLabel = String(existingRecord && existingRecord.broker_label || '').trim();
        if ((!nextBrokerLabel || isLikelyObjectId(nextBrokerLabel)) && existingBrokerLabel && !isLikelyObjectId(existingBrokerLabel)) {
            mergedRecord.broker_label = existingBrokerLabel;
        }

        // Execute-order refreshes can briefly send empty legs for active strategies.
        // Preserve the previous legs so MTM does not flash to 0 between socket updates.
        var nextLegs = Array.isArray(nextRecord && nextRecord.legs) ? nextRecord.legs : null;
        var existingLegs = Array.isArray(existingRecord && existingRecord.legs) ? existingRecord.legs : [];
        var isClosedRecord = mergedRecord.active_on_server === false || mergedRecord.status === 'StrategyStatus.SquaredOff';
        if ((!nextLegs || !nextLegs.length) && existingLegs.length && !isClosedRecord) {
            mergedRecord.legs = existingLegs;
        }

        var nextPendingFeatureLegs = Array.isArray(nextRecord && nextRecord.pending_feature_legs)
            ? nextRecord.pending_feature_legs
            : null;
        var existingPendingFeatureLegs = Array.isArray(existingRecord && existingRecord.pending_feature_legs)
            ? existingRecord.pending_feature_legs
            : [];
        if ((!nextPendingFeatureLegs || !nextPendingFeatureLegs.length) && existingPendingFeatureLegs.length && !isClosedRecord) {
            mergedRecord.pending_feature_legs = existingPendingFeatureLegs;
        }

        if ((nextRecord == null || typeof nextRecord.open_legs_count !== 'number')
            && existingRecord != null
            && typeof existingRecord.open_legs_count === 'number'
            && !isClosedRecord) {
            mergedRecord.open_legs_count = existingRecord.open_legs_count;
        }
        return mergedRecord;
    }

    function getLtpSnapshotKey(item) {
        if (!item || typeof item !== 'object') {
            return '';
        }
        var tokenKey = String(item.token || '').trim();
        if (tokenKey) {
            return 'token:' + tokenKey;
        }
        var underlyingKey = String(item.underlying || '').trim().toUpperCase();
        if (underlyingKey && String(item.option_type || '').trim().toUpperCase() === 'SPOT') {
            return 'spot:' + underlyingKey;
        }
        var strikeKey = String(item.strike || '').trim();
        var expiryKey = String(item.expiry || item.expiry_date || '').trim().slice(0, 10);
        var optionKey = String(item.option_type || item.option || '').trim().toUpperCase();
        if (strikeKey && expiryKey && optionKey) {
            return 'contract:' + strikeKey + '_' + expiryKey + '_' + optionKey;
        }
        return '';
    }

    function mergeLatestLtpSnapshot(items) {
        var incoming = Array.isArray(items) ? items : [];
        incoming.forEach(function (item) {
            var key = getLtpSnapshotKey(item);
            if (!key) {
                return;
            }
            latestLtpSnapshotMap[key] = Object.assign({}, latestLtpSnapshotMap[key] || {}, item);
        });
        latestLtpSnapshot = Object.keys(latestLtpSnapshotMap).map(function (key) {
            return latestLtpSnapshotMap[key];
        });
    }

    function rememberChangedLtpKeys(items) {
        var incoming = Array.isArray(items) ? items : [];
        incoming.forEach(function (item) {
            var key = getLtpSnapshotKey(item);
            if (key) {
                pendingChangedLtpKeys[key] = true;
            }
        });
    }

    function resetLatestLtpSnapshot() {
        latestLtpSnapshotMap = {};
        latestLtpSnapshot = [];
        pendingChangedLtpKeys = {};
        forceFullLtpRender = true;
    }

    function buildLegContractKey(leg) {
        if (!leg || typeof leg !== 'object') {
            return '';
        }
        var strike = String(leg.strike || '').trim();
        var expiry10 = String(leg.expiry_date || leg.expiry || '').trim().slice(0, 10);
        var optionType = String(leg.option || leg.option_type || '').trim().toUpperCase();
        if (!strike || !expiry10 || !optionType || optionType === 'SPOT') {
            return '';
        }
        return 'contract:' + strike + '_' + expiry10 + '_' + optionType;
    }

    function collectLegPriceKeys(leg, keys) {
        if (!leg || typeof leg !== 'object' || !keys) {
            return;
        }
        var token = String(leg.token || '').trim();
        if (token) {
            keys['token:' + token] = true;
        }
        var entryTrade = leg.entry_trade;
        var entryToken = String(entryTrade && entryTrade.instrument_token || '').trim();
        if (entryToken) {
            keys['token:' + entryToken] = true;
        }
        var contractKey = buildLegContractKey(leg);
        if (contractKey) {
            keys[contractKey] = true;
        }
    }

    function rebuildRecordPriceLinkIndex() {
        recordPriceLinkIndex = {};
        Object.keys(executionRecordsMap).forEach(function (recordId) {
            var rec = executionRecordsMap[recordId];
            var keys = {};
            var legs = Array.isArray(rec && rec.legs) ? rec.legs : [];
            var pendingFeatureLegs = Array.isArray(rec && rec.pending_feature_legs) ? rec.pending_feature_legs : [];
            legs.forEach(function (leg) {
                collectLegPriceKeys(leg, keys);
            });
            pendingFeatureLegs.forEach(function (leg) {
                collectLegPriceKeys(leg, keys);
            });
            Object.keys(keys).forEach(function (key) {
                if (!recordPriceLinkIndex[key]) {
                    recordPriceLinkIndex[key] = {};
                }
                recordPriceLinkIndex[key][recordId] = true;
            });
        });
    }

    function parseExecutionTimestamp(tsStr) {
        if (!tsStr) {
            return null;
        }
        var normalized = String(tsStr).replace(' ', 'T');
        var dt = new Date(normalized);
        return isNaN(dt.getTime()) ? null : dt;
    }

    function resolveLegLtpFromSnapshot(leg) {
        if (!leg || typeof leg !== 'object') {
            return null;
        }
        var token = String(leg.token || '').trim();
        if (token && latestLtpSnapshotMap['token:' + token] && latestLtpSnapshotMap['token:' + token].ltp != null) {
            return parseFloat(latestLtpSnapshotMap['token:' + token].ltp) || 0;
        }
        var entryTrade = leg.entry_trade;
        var entryToken = String(entryTrade && entryTrade.instrument_token || '').trim();
        if (
            entryToken
            && latestLtpSnapshotMap['token:' + entryToken]
            && latestLtpSnapshotMap['token:' + entryToken].ltp != null
        ) {
            return parseFloat(latestLtpSnapshotMap['token:' + entryToken].ltp) || 0;
        }
        var contractKey = buildLegContractKey(leg);
        if (contractKey && latestLtpSnapshotMap[contractKey] && latestLtpSnapshotMap[contractKey].ltp != null) {
            return parseFloat(latestLtpSnapshotMap[contractKey].ltp) || 0;
        }
        return null;
    }

    function countOpenLegsForRecord(record, currentListenDt) {
        var totalOpenLegs = 0;
        var legs = Array.isArray(record && record.legs) ? record.legs : [];
        legs.forEach(function (leg) {
            if (!leg || typeof leg !== 'object') {
                return;
            }
            var exitTrade = leg.exit_trade;
            if (!exitTrade || typeof exitTrade !== 'object') {
                totalOpenLegs += 1;
                return;
            }
            var exitDt = parseExecutionTimestamp(exitTrade.traded_timestamp || exitTrade.trigger_timestamp || '');
            if (!exitDt || isNaN(exitDt.getTime())) {
                totalOpenLegs += 1;
                return;
            }
            if (currentListenDt && currentListenDt.getTime() < exitDt.getTime()) {
                totalOpenLegs += 1;
            }
        });
        return totalOpenLegs;
    }

    function calculateStrategyPnl(recordId, currentListenDt) {
        var rec = executionRecordsMap[recordId];
        if (!rec || typeof rec !== 'object') {
            return 0;
        }
        var isSquaredOff = rec.active_on_server === false || rec.status === 'StrategyStatus.SquaredOff';
        var legs = Array.isArray(rec.legs) ? rec.legs : [];
        var strategyPnl = 0;

        if (legs.length === 0) {
            if (isSquaredOff && squaredOffMtmCache[recordId] !== undefined) {
                strategyPnl = squaredOffMtmCache[recordId];
            }
            return strategyPnl;
        }

        legs.forEach(function (leg) {
            if (!leg || typeof leg !== 'object') {
                return;
            }
            var entryTrade = leg.entry_trade;
            if (!entryTrade || typeof entryTrade !== 'object') {
                return;
            }
            var legEntryDt = parseExecutionTimestamp(
                leg.entry_timestamp
                || entryTrade.traded_timestamp
                || entryTrade.trigger_timestamp
                || ''
            );
            if (currentListenDt && legEntryDt && currentListenDt.getTime() < legEntryDt.getTime()) {
                return;
            }

            var entryPrice = parseFloat(entryTrade.price || entryTrade.trigger_price) || 0;
            var lotSize = parseInt(leg.lot_size, 10) || 1;
            var lotQty = parseInt(leg.quantity, 10) || 0;
            var qty = lotQty * lotSize;
            var isSell = String(leg.position || '').toLowerCase().indexOf('sell') !== -1;
            var pnl = 0;
            var exitTrade = leg.exit_trade;

            if (exitTrade && typeof exitTrade === 'object') {
                var exitPrice = parseFloat(exitTrade.price || exitTrade.trigger_price) || 0;
                if (exitPrice > 0) {
                    pnl = isSell ? (entryPrice - exitPrice) * qty : (exitPrice - entryPrice) * qty;
                } else {
                    var exitDt = parseExecutionTimestamp(exitTrade.traded_timestamp || exitTrade.trigger_timestamp || '');
                    if (currentListenDt && exitDt && currentListenDt.getTime() >= exitDt.getTime()) {
                        pnl = 0;
                    } else {
                        var exitLtp = resolveLegLtpFromSnapshot(leg);
                        if (exitLtp === null) {
                            return;
                        }
                        pnl = isSell ? (entryPrice - exitLtp) * qty : (exitLtp - entryPrice) * qty;
                    }
                }
            } else {
                if (isSquaredOff) {
                    return;
                }
                var openLtp = resolveLegLtpFromSnapshot(leg);
                if (openLtp === null) {
                    return;
                }
                leg.last_saw_price = openLtp;
                leg.mark_price = openLtp;
                leg.ltp = openLtp;
                pnl = isSell ? (entryPrice - openLtp) * qty : (openLtp - entryPrice) * qty;
            }

            leg.pnl = pnl;
            strategyPnl += pnl;
        });

        var pendingFeatureLegs = Array.isArray(rec.pending_feature_legs) ? rec.pending_feature_legs : [];
        pendingFeatureLegs.forEach(function (leg) {
            if (!leg || typeof leg !== 'object') {
                return;
            }
            var pendingLtp = resolveLegLtpFromSnapshot(leg);
            if (pendingLtp === null) {
                return;
            }
            leg.last_saw_price = pendingLtp;
            leg.mark_price = pendingLtp;
            leg.ltp = pendingLtp;
        });

        if (isSquaredOff) {
            squaredOffMtmCache[recordId] = strategyPnl;
        }
        return strategyPnl;
    }

    function updateStrategyMtmCell(recordId, strategyPnl) {
        var mtmEl = document.querySelector('.ff-deployed-mtm[data-record-id="' + recordId + '"]');
        if (!mtmEl) {
            return;
        }
        var rounded = Math.round(strategyPnl * 100) / 100;
        mtmEl.textContent = '\u20b9 ' + rounded.toLocaleString('en-IN', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });
        mtmEl.style.color = rounded >= 0 ? '#16a34a' : '#ef4444';
    }

    function rebuildAggregateCountsCache(currentListenDt, forceRefresh) {
        var secondKey = getTimestampSecondKey(latestListenTimestamp);
        if (!forceRefresh && aggregateCountsCache && aggregateCountsListenSecondKey === secondKey) {
            return aggregateCountsCache;
        }
        var brokerStaticMap = {};
        var totalOpenLegs = 0;
        var totalStrategies = 0;
        Object.keys(executionRecordsMap).forEach(function (recordId) {
            var rec = executionRecordsMap[recordId];
            var brokerLabel = getBrokerDisplayName(rec);
            var brokerId = String(rec && rec.broker || '').trim();
            var brokerKey = String(brokerId || brokerLabel || 'unknown').trim().toLowerCase();
            if (!brokerStaticMap[brokerKey]) {
                brokerStaticMap[brokerKey] = {
                    key: brokerKey,
                    label: brokerLabel,
                    icon: getBrokerIconPath(rec),
                    brokerId: brokerId,
                    userId: String(rec && rec.user_id || '').trim(),
                    strategyCount: 0,
                    openLegCount: 0
                };
            }
            var openLegCount = countOpenLegsForRecord(rec, currentListenDt);
            brokerStaticMap[brokerKey].strategyCount += 1;
            brokerStaticMap[brokerKey].openLegCount += openLegCount;
            totalOpenLegs += openLegCount;
            totalStrategies += 1;
        });
        aggregateCountsCache = {
            brokerStaticMap: brokerStaticMap,
            totalOpenLegs: totalOpenLegs,
            totalStrategies: totalStrategies
        };
        aggregateCountsListenSecondKey = secondKey;
        return aggregateCountsCache;
    }

    function renderAggregateMtmFromCache(currentListenDt, forceCountRefresh) {
        var groupPnl = {};
        var brokerSummaryMap = {};
        var countsCache = rebuildAggregateCountsCache(currentListenDt, forceCountRefresh);
        Object.keys(countsCache.brokerStaticMap || {}).forEach(function (brokerKey) {
            var item = countsCache.brokerStaticMap[brokerKey];
            brokerSummaryMap[brokerKey] = {
                key: brokerKey,
                label: item.label,
                icon: item.icon,
                brokerId: item.brokerId,
                userId: item.userId,
                mtm: 0,
                strategyCount: item.strategyCount,
                openLegCount: item.openLegCount
            };
        });

        Object.keys(executionRecordsMap).forEach(function (recordId) {
            var rec = executionRecordsMap[recordId];
            var strategyPnl = parseFloat(strategyPnlCache[recordId]) || 0;
            var groupId = String((rec && rec.portfolio && rec.portfolio.group_id) || '').trim();
            if (groupId) {
                groupPnl[groupId] = (groupPnl[groupId] || 0) + strategyPnl;
            }
            var brokerLabel = getBrokerDisplayName(rec);
            var brokerId = String(rec && rec.broker || '').trim();
            var brokerKey = String(brokerId || brokerLabel || 'unknown').trim().toLowerCase();
            if (brokerSummaryMap[brokerKey]) {
                brokerSummaryMap[brokerKey].mtm += strategyPnl;
            }
        });

        Object.keys(groupPnl).forEach(function (groupId) {
            var groupEl = document.querySelector('.ff-deployed-mtm[data-group-mtm="' + groupId + '"]');
            if (groupEl) {
                var rounded = Math.round(groupPnl[groupId] * 100) / 100;
                groupEl.textContent = '\u20b9 ' + rounded.toLocaleString('en-IN', {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2
                });
                groupEl.style.color = rounded >= 0 ? '#16a34a' : '#ef4444';
            }
        });

        var totalPnl = 0;
        Object.keys(groupPnl).forEach(function (groupId) {
            totalPnl += groupPnl[groupId];
        });
        var totalRounded = Math.round(totalPnl * 100) / 100;
        var totalFormatted = '\u20b9 ' + totalRounded.toLocaleString('en-IN', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });
        var totalColor = totalRounded >= 0 ? '#16a34a' : '#ef4444';
        var countStr = countsCache.totalOpenLegs + '/' + countsCache.totalStrategies;
        updateBrowserTabTitle(totalRounded);

        ['ff-header-total-mtm', 'ff-toolbar-total-mtm'].forEach(function (id) {
            var el = document.getElementById(id);
            if (el) {
                el.textContent = totalFormatted;
                el.style.color = totalColor;
            }
        });
        var headerPos = document.getElementById('ff-header-open-pos');
        if (headerPos) {
            headerPos.textContent = countStr;
        }
        var toolbarRunning = document.getElementById('ff-toolbar-running-count');
        if (toolbarRunning) {
            toolbarRunning.textContent = countStr;
        }
        var toolbarOpenPos = document.getElementById('ff-toolbar-open-pos-count');
        if (toolbarOpenPos) {
            toolbarOpenPos.textContent = String(countsCache.totalOpenLegs);
        }
        renderHeaderBrokerSummary(brokerSummaryMap);
    }

    function applyLtpToAffectedRecords(changedKeys) {
        var currentTsStr = latestListenTimestamp ? String(latestListenTimestamp).replace(' ', 'T') : '';
        var currentListenDt = (currentTsStr && !isNaN(new Date(currentTsStr).getTime())) ? new Date(currentTsStr) : null;
        var affectedRecordIds = {};
        (changedKeys || []).forEach(function (key) {
            var linked = recordPriceLinkIndex[key];
            if (!linked) {
                return;
            }
            Object.keys(linked).forEach(function (recordId) {
                affectedRecordIds[recordId] = true;
            });
        });
        Object.keys(affectedRecordIds).forEach(function (recordId) {
            var strategyPnl = calculateStrategyPnl(recordId, currentListenDt);
            strategyPnlCache[recordId] = strategyPnl;
            updateStrategyMtmCell(recordId, strategyPnl);
            updateStrategyDetailPanel(recordId, executionRecordsMap[recordId] || {});
        });
        renderAggregateMtmFromCache(currentListenDt, getTimestampSecondKey(latestListenTimestamp) !== aggregateCountsListenSecondKey);
    }

    function getLiveTabTitleSuffix() {
        if (listeningModeKey === 'fast-forward') {
            return 'Forward Test';
        }
        if (listeningModeKey === 'live') {
            return 'Live';
        }
        return 'Algo Backtest';
    }

    function updateBrowserTabTitle(mtmValue) {
        var numericMtm = parseFloat(mtmValue);
        if (!isFinite(numericMtm)) {
            document.title = defaultBrowserTitle;
            return;
        }
        var rounded = Math.round(numericMtm * 100) / 100;
        document.title = '\u20b9 ' + rounded.toLocaleString('en-IN', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }) + ' | ' + getLiveTabTitleSuffix();
    }


    function ensureCurrentListenTimeEl() {
        if (currentListenTimeEl && currentListenTimeEl.parentNode) {
            return currentListenTimeEl;
        }
        if (!socketStatusEl || !socketStatusEl.parentNode) {
            return null;
        }
        var el = document.getElementById('ff-current-listen-time');
        if (!el) {
            el = document.createElement('div');
            el.id = 'ff-current-listen-time';
            el.className = 'ff-socket-status';
            el.setAttribute('data-state', 'connected');
            el.style.minWidth = '220px';
            el.innerHTML = '<span class="ff-socket-status__dot" aria-hidden="true"></span><span>Current time: -</span>';
            socketStatusEl.parentNode.appendChild(el);
        }
        currentListenTimeEl = el;
        return currentListenTimeEl;
    }

    function updateCurrentListenTimeDisplay(value) {
        var el = ensureCurrentListenTimeEl();
        if (!el) {
            return;
        }
        var label = el.querySelector('span:last-child');
        if (!label) {
            return;
        }
        latestListenTimestamp = String(value || '').trim();
        label.textContent = 'Current time: ' + String(value || '-');
        syncListeningTimeline(latestListenTimestamp, false);
    }

    function getTimestampSecondKey(value) {
        var normalized = String(value || '').trim().replace('T', ' ');
        return normalized ? normalized.slice(0, 19) : '';
    }

    function flushScheduledLtpRender() {
        if (ltpRenderTimerId) {
            window.clearTimeout(ltpRenderTimerId);
            ltpRenderTimerId = 0;
        }
        if (ltpRenderFrameId) {
            window.cancelAnimationFrame(ltpRenderFrameId);
            ltpRenderFrameId = 0;
        }
        ltpRenderFrameId = window.requestAnimationFrame(function () {
            ltpRenderFrameId = 0;
            lastLtpRenderAt = Date.now();
            var changedKeys = Object.keys(pendingChangedLtpKeys);
            pendingChangedLtpKeys = {};
            if (!forceFullLtpRender && changedKeys.length && Object.keys(strategyPnlCache).length) {
                applyLtpToAffectedRecords(changedKeys);
                return;
            }
            forceFullLtpRender = false;
            applyLtpToMtm(latestLtpSnapshot);
        });
    }

    function scheduleLtpRender() {
        var now = Date.now();
        var elapsed = now - lastLtpRenderAt;
        if (elapsed >= LTP_RENDER_INTERVAL_MS) {
            flushScheduledLtpRender();
            return;
        }
        if (ltpRenderTimerId) {
            return;
        }
        ltpRenderTimerId = window.setTimeout(function () {
            ltpRenderTimerId = 0;
            flushScheduledLtpRender();
        }, Math.max(0, LTP_RENDER_INTERVAL_MS - elapsed));
    }

    function getCurrentBacktestListenTimestamp() {
        var rawValue = String(latestListenTimestamp || '').trim() || formatBacktestActivationDateTime();
        return rawValue ? String(rawValue).replace(' ', 'T') : '';
    }

    function getInitialBacktestListenTimestamp(behindTimeOverride) {
        var selectedDate = String(listeningDateInput && listeningDateInput.value || '').trim();
        if (!selectedDate) {
            return '';
        }
        var baseDate = parseIsoDate(selectedDate);
        if (!baseDate) {
            return '';
        }
        var normalizedBehindTime = Math.max(0, parseInt(behindTimeOverride || '0', 10) || 0);
        var timeParts = String(backtestMarketOpenTime || '09:15').split(':');
        var marketOpenHour = parseInt(timeParts[0] || '9', 10);
        var marketOpenMinute = parseInt(timeParts[1] || '15', 10);
        var totalMinutes = (marketOpenHour * 60) + marketOpenMinute - normalizedBehindTime;
        var listenDate = new Date(baseDate.getFullYear(), baseDate.getMonth(), baseDate.getDate(), 0, 0, 0, 0);
        listenDate.setMinutes(totalMinutes);
        return listenDate.getFullYear() + '-'
            + String(listenDate.getMonth() + 1).padStart(2, '0') + '-'
            + String(listenDate.getDate()).padStart(2, '0') + 'T'
            + String(listenDate.getHours()).padStart(2, '0') + ':'
            + String(listenDate.getMinutes()).padStart(2, '0') + ':'
            + String(listenDate.getSeconds()).padStart(2, '0');
    }

    function getListeningBounds() {
        var selectedDate = String(listeningDateInput && listeningDateInput.value || '').trim();
        if (!selectedDate) {
            return null;
        }
        return {
            start: new Date(selectedDate + 'T09:15:00'),
            end: new Date(selectedDate + 'T15:30:00')
        };
    }

    function clampTimelineDate(date) {
        var bounds = getListeningBounds();
        if (!bounds || !(date instanceof Date) || isNaN(date.getTime())) {
            return date;
        }
        if (date.getTime() < bounds.start.getTime()) {
            return new Date(bounds.start.getTime());
        }
        if (date.getTime() > bounds.end.getTime()) {
            return new Date(bounds.end.getTime());
        }
        return date;
    }

    function formatTimelineDateChip(date) {
        if (!(date instanceof Date) || isNaN(date.getTime())) {
            return 'Mon 03 Nov 25';
        }
        var weekdayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
        return weekdayNames[date.getDay()] + ' '
            + String(date.getDate()).padStart(2, '0') + ' '
            + calendarMonthNamesShort[date.getMonth()] + ' '
            + String(date.getFullYear()).slice(-2);
    }

    function formatTimelineTime(date) {
        if (!(date instanceof Date) || isNaN(date.getTime())) {
            return '09:15:00';
        }
        return String(date.getHours()).padStart(2, '0') + ':'
            + String(date.getMinutes()).padStart(2, '0') + ':'
            + String(date.getSeconds()).padStart(2, '0');
    }

    function syncListeningTimeline(rawTimestamp, preserveScrubValue) {
        if (!secondsPanel || !secondsSlider || !secondsTimeInput || !secondsDateChip) {
            return;
        }
        var bounds = getListeningBounds();
        if (!bounds) {
            return;
        }
        if (secondsStartLabel) secondsStartLabel.textContent = formatTimelineTime(bounds.start).slice(0, 5);
        if (secondsEndLabel) secondsEndLabel.textContent = formatTimelineTime(bounds.end).slice(0, 5);
        var parsedDate = rawTimestamp ? new Date(String(rawTimestamp).replace(' ', 'T')) : new Date(bounds.start.getTime());
        if (isNaN(parsedDate.getTime())) {
            parsedDate = new Date(bounds.start.getTime());
        }
        parsedDate = clampTimelineDate(parsedDate);
        var totalSeconds = Math.max(1, Math.round((bounds.end.getTime() - bounds.start.getTime()) / 1000));
        var elapsedSeconds = Math.max(0, Math.min(totalSeconds, Math.round((parsedDate.getTime() - bounds.start.getTime()) / 1000)));
        secondsSlider.max = String(totalSeconds);
        if (!preserveScrubValue) {
            secondsSlider.value = String(elapsedSeconds);
            timelineScrubValue = String(elapsedSeconds);
        }
        secondsTimeInput.value = formatTimelineTime(parsedDate);
        secondsDateChip.textContent = formatTimelineDateChip(parsedDate);
    }

    function buildListenTimestampFromSliderValue(rawValue) {
        var bounds = getListeningBounds();
        if (!bounds) {
            return '';
        }
        var totalSeconds = Math.max(0, parseInt(rawValue, 10) || 0);
        var nextDate = new Date(bounds.start.getTime() + (totalSeconds * 1000));
        nextDate = clampTimelineDate(nextDate);
        return nextDate.getFullYear() + '-'
            + String(nextDate.getMonth() + 1).padStart(2, '0') + '-'
            + String(nextDate.getDate()).padStart(2, '0') + 'T'
            + formatTimelineTime(nextDate);
    }

    function stopTimelineAutoplay() {
        if (timelineAutoplayTimer) {
            window.clearInterval(timelineAutoplayTimer);
            timelineAutoplayTimer = null;
        }
        if (secondsAutoplayBtn) {
            secondsAutoplayBtn.classList.remove('is-active');
        }
    }

    function updateAutoloadUi() {
        if (autoloadToggleBtn) {
            autoloadToggleBtn.textContent = 'Autoload: ' + (backtestAutoloadEnabled ? 'true' : 'false');
        }
        if (autoloadStatusEl) {
            autoloadStatusEl.innerHTML = 'autoload=<strong>' + (backtestAutoloadEnabled ? 'true' : 'false') + '</strong>';
        }
    }

    function runManualSimulatorStep(reason, listenTimestampOverride) {
        var listenTimestamp = String(listenTimestampOverride || latestListenTimestamp || '').trim() || getInitialBacktestListenTimestamp(listeningBehindTimeSelect ? listeningBehindTimeSelect.value : '0');
        if (!listenTimestamp) {
            updateSocketStatus('error', { message: 'Listen timestamp missing' });
            return Promise.resolve(null);
        }
        stopTimelineAutoplay();
        ensureStreamSocketConnections();
        var url = buildAlgoApiUrl('algo-backtest-simulator')
            + '?listen_timestamp=' + encodeURIComponent(listenTimestamp)
            + '&autoload=' + encodeURIComponent('false');
        updateSocketStatus('connecting', { message: 'Manual one-time run started' });
        return fetch(url)
            .then(function (response) {
                if (!response.ok) {
                    throw new Error('Manual simulator request failed');
                }
                return response.json();
            })
            .then(function (payload) {
                var broadcast = payload && payload.socket_broadcast ? payload.socket_broadcast : {};
                var executeOrdersBroadcasted = !!(broadcast && broadcast['execute-orders']);
                var updateBroadcasted = !!(broadcast && broadcast.update);
                if (!executeOrdersBroadcasted && payload && payload.execute_order_event) {
                    handleExecuteOrdersSocketMessage(payload.execute_order_event);
                }
                if (!updateBroadcasted && payload && payload.update_event) {
                    handleUpdateSocketMessage(payload.update_event);
                }
                if (!updateBroadcasted && payload && payload.ltp_event) {
                    handleUpdateSocketMessage(payload.ltp_event);
                }
                updateSocketStatus('connected', {
                    message: 'Manual one-time run completed (' + String(reason || 'manual') + ')'
                });
                return payload;
            })
            .catch(function (error) {
                console.error('[MANUAL SIMULATOR ERROR]', error);
                updateSocketStatus('error', { message: error && error.message ? error.message : 'Manual simulator failed' });
                return null;
            });
    }

    function sendTimelineSeek(listenTimestamp, reason) {
        var normalizedTs = String(listenTimestamp || '').trim();
        if (!normalizedTs) {
            return;
        }
        updateCurrentListenTimeDisplay(normalizedTs);
        refreshDeployedStrategyTimers();
        if (!backtestAutoloadEnabled) {
            persistListeningSession();
            runManualSimulatorStep(reason || 'timeline_seek', normalizedTs);
            return;
        }
        if (updateSocketClient) {
            updateSocketClient.send({
                trade_date: String(listeningDateInput && listeningDateInput.value || '').trim(),
                activation_mode: listeningModeKey,
                status: listeningModeKey,
                reason: reason || 'timeline_seek',
                listen_timestamp: normalizedTs
            });
            requestInitialPositionSnapshot(reason || 'timeline_seek', normalizedTs);
        }
        persistListeningSession();
    }

    function applyTimelineDelta(minutes) {
        var baseTimestamp = String(latestListenTimestamp || '').trim() || getInitialBacktestListenTimestamp(listeningBehindTimeSelect ? listeningBehindTimeSelect.value : '0');
        var baseDate = new Date(String(baseTimestamp).replace(' ', 'T'));
        if (isNaN(baseDate.getTime())) {
            return;
        }
        baseDate.setMinutes(baseDate.getMinutes() + (parseInt(minutes, 10) || 0));
        baseDate = clampTimelineDate(baseDate);
        sendTimelineSeek(
            baseDate.getFullYear() + '-'
            + String(baseDate.getMonth() + 1).padStart(2, '0') + '-'
            + String(baseDate.getDate()).padStart(2, '0') + 'T'
            + formatTimelineTime(baseDate),
            'timeline_jump'
        );
    }

    function applyTimelineTarget(target) {
        var bounds = getListeningBounds();
        if (!bounds) {
            return;
        }
        var nextDate = target === 'eod' ? bounds.end : bounds.start;
        sendTimelineSeek(
            nextDate.getFullYear() + '-'
            + String(nextDate.getMonth() + 1).padStart(2, '0') + '-'
            + String(nextDate.getDate()).padStart(2, '0') + 'T'
            + formatTimelineTime(nextDate),
            'timeline_' + String(target || 'seek')
        );
    }

    function updateMarketCountdown() {
        if (!marketCountdownEl) {
            return;
        }

        if (backtestCountdownState) {
            var isPaused = backtestCountdownState.paused;
            var tsStr = latestListenTimestamp ? String(latestListenTimestamp).replace(' ', 'T') : '';
            var currentListenDt = (tsStr && !isNaN(new Date(tsStr).getTime())) ? new Date(tsStr) : null;

            if (currentListenDt) {
                var selectedDate2 = String(listeningDateInput && listeningDateInput.value || '').trim();
                var baseDate2 = selectedDate2 ? parseIsoDate(selectedDate2) : null;
                if (baseDate2) {
                    var mktParts = String(backtestMarketOpenTime || '09:15').split(':');
                    var marketOpenDt = new Date(
                        baseDate2.getFullYear(), baseDate2.getMonth(), baseDate2.getDate(),
                        parseInt(mktParts[0] || '9', 10), parseInt(mktParts[1] || '15', 10), 0, 0
                    );
                    var hhmm = tsStr.slice(11, 16);
                    if (currentListenDt.getTime() < marketOpenDt.getTime()) {
                        // Before market open — show countdown to 09:15
                        var diffMs2 = marketOpenDt.getTime() - currentListenDt.getTime();
                        marketCountdownEl.textContent = (isPaused ? 'Paused • ' : '') + 'Market opens in ' + formatEntryCountdown(diffMs2);
                    } else {
                        // Market is open — show current listen time
                        backtestCountdownState.marketOpenReached = true;
                        marketCountdownEl.textContent = (isPaused ? 'Paused • ' : 'Market is open • ') + hhmm;
                    }
                    if (!isPaused) persistListeningSession();
                    refreshDeployedStrategyTimers();
                    return;
                }
            }

            // Fallback: minute-based (no latestListenTimestamp yet)
            if (isPaused) {
                var pausedMinutesLeft = Math.max(0, backtestCountdownState.remainingMinutes);
                var pausedHours = Math.floor(pausedMinutesLeft / 60);
                var pausedMins = pausedMinutesLeft % 60;
                marketCountdownEl.textContent = 'Paused • ' + pausedHours + 'h ' + String(pausedMins).padStart(2, '0') + 'm';
                refreshDeployedStrategyTimers();
                return;
            }
            var simulatedMinutesLeft = Math.max(0, backtestCountdownState.remainingMinutes);
            if (simulatedMinutesLeft <= 0) {
                marketCountdownEl.textContent = 'Market is open';
                backtestCountdownState.remainingMinutes = 0;
                backtestCountdownState.marketOpenReached = true;
                if (marketCountdownTimer) {
                    window.clearInterval(marketCountdownTimer);
                    marketCountdownTimer = null;
                }
                persistListeningSession();
                refreshDeployedStrategyTimers();
                return;
            }
            var simulatedHours = Math.floor(simulatedMinutesLeft / 60);
            var simulatedMinutes2 = simulatedMinutesLeft % 60;
            marketCountdownEl.textContent = simulatedHours + 'h '
                + String(simulatedMinutes2).padStart(2, '0') + 'm till market opens';
            backtestCountdownState.remainingMinutes -= 1;
            persistListeningSession();
            refreshDeployedStrategyTimers();
            return;
        }

        var now = new Date();
        var timeParts = String(marketCountdownEndTime || '09:15').split(':');
        var targetHour = parseInt(timeParts[0] || '9', 10);
        var targetMinute = parseInt(timeParts[1] || '15', 10);
        var target = new Date(now);
        target.setHours(targetHour, targetMinute, 0, 0);

        var diffMs = target.getTime() - now.getTime();
        if (diffMs <= 0) {
            marketCountdownEl.textContent = 'Market is open';
            return;
        }

        var totalSeconds = Math.floor(diffMs / 1000);
        var hours = Math.floor(totalSeconds / 3600);
        var minutes = Math.floor((totalSeconds % 3600) / 60);

        marketCountdownEl.textContent = hours + 'h '
            + String(minutes).padStart(2, '0') + 'm till market opens';
        refreshDeployedStrategyTimers();
    }

    function resetMarketCountdownTimer() {
        if (marketCountdownTimer) {
            window.clearInterval(marketCountdownTimer);
            marketCountdownTimer = null;
        }
    }

    function getListeningManager() {
        if (window.APP_CONFIG && window.APP_CONFIG.listeningManager) {
            return window.APP_CONFIG.listeningManager;
        }
        if (window.APP_LISTENING_MANAGER) {
            return window.APP_LISTENING_MANAGER;
        }
        return {
            load: function (mode) {
                try {
                    var prefix = window.APP_LISTENING_STORAGE_PREFIX || 'option_algo_listening';
                    var rawValue = window.localStorage.getItem(prefix + ':' + String(mode || 'default'));
                    return rawValue ? JSON.parse(rawValue) : null;
                } catch (error) {
                    return null;
                }
            },
            save: function (mode, payload) {
                try {
                    var prefix = window.APP_LISTENING_STORAGE_PREFIX || 'option_algo_listening';
                    window.localStorage.setItem(
                        prefix + ':' + String(mode || 'default'),
                        JSON.stringify(Object.assign({}, payload || {}, { updated_at: Date.now() }))
                    );
                    return true;
                } catch (error) {
                    return false;
                }
            },
            clear: function (mode) {
                try {
                    var prefix = window.APP_LISTENING_STORAGE_PREFIX || 'option_algo_listening';
                    window.localStorage.removeItem(prefix + ':' + String(mode || 'default'));
                    return true;
                } catch (error) {
                    return false;
                }
            }
        };
    }

    function syncCalendarStateFromIsoDate(isoDate) {
        var parsedDate = parseIsoDate(isoDate) || new Date();
        calendarState.selectedDate = new Date(parsedDate.getTime());
        calendarState.draftDate = new Date(parsedDate.getTime());
        calendarState.displayDate = new Date(parsedDate.getTime());
        calendarState.yearPageStart = calendarState.displayDate.getFullYear() - 7;
        listeningDateInput.value = formatIsoDate(parsedDate);
        syncListeningDateLabel();
        renderCalendar();
    }

    function persistListeningSession() {
        var manager = getListeningManager();
        if (!manager) {
            return;
        }
        if (listeningState !== 'running' && listeningState !== 'paused') {
            manager.clear(listeningModeKey);
            return;
        }
        manager.save(listeningModeKey, {
            mode: listeningModeKey,
            listening_state: listeningState,
            selected_date: String(listeningDateInput.value || '').trim(),
            autoload: !!backtestAutoloadEnabled,
            behind_time: String(listeningBehindTimeSelect ? listeningBehindTimeSelect.value || '5' : '5'),
            remaining_minutes: backtestCountdownState ? Math.max(0, parseInt(backtestCountdownState.remainingMinutes || 0, 10) || 0) : 0,
            current_listen_timestamp: getCurrentBacktestListenTimestamp(),
            paused: !!(backtestCountdownState && backtestCountdownState.paused)
        });
    }

    function renderListeningControls() {
        if (!startListeningBtn || !pauseListeningBtn || !stopListeningBtn) {
            return;
        }
        if (listeningState === 'running') {
            startListeningBtn.style.display = 'none';
            pauseListeningBtn.style.display = '';
            stopListeningBtn.style.display = '';
        } else if (listeningState === 'paused') {
            startListeningBtn.style.display = '';
            startListeningBtn.textContent = 'Re-Start Listening';
            startListeningBtn.disabled = false;
            pauseListeningBtn.style.display = 'none';
            stopListeningBtn.style.display = '';
        } else {
            startListeningBtn.style.display = '';
            startListeningBtn.textContent = 'Start Listening';
            startListeningBtn.disabled = false;
            pauseListeningBtn.style.display = 'none';
            stopListeningBtn.style.display = 'none';
        }
        if (secondsPanel) {
            secondsPanel.style.opacity = listeningState === 'idle' ? '0.92' : '1';
        }
    }

    function resetListeningPollTimer() {
        if (listeningPollTimer) {
            window.clearInterval(listeningPollTimer);
            listeningPollTimer = null;
        }
    }

    function stopLtpRenderHeartbeat() {
        if (ltpRenderHeartbeatId) {
            window.clearInterval(ltpRenderHeartbeatId);
            ltpRenderHeartbeatId = 0;
        }
    }

    function startLtpRenderHeartbeat() {
        stopLtpRenderHeartbeat();
        ltpRenderHeartbeatId = window.setInterval(function () {
            if (listeningState !== 'running') {
                return;
            }
            flushScheduledLtpRender();
        }, LTP_RENDER_INTERVAL_MS);
    }



    function buildExecutionListUrl(selectedDate) {
        var executionEnvironment = listeningModeKey === 'fast-forward' ? 'forward-test' : listeningModeKey;
        var query = [
            'environment=' + encodeURIComponent(executionEnvironment),
            'is_signal=false',
            'trade_status=1'
        ];
        var normalizedDate = String(selectedDate || '').trim();
        if (normalizedDate) {
            query.push('date=' + encodeURIComponent(normalizedDate));
        }
        return buildNamedApiUrl('executions') + '?' + query.join('&');
    }

    function startRealTimeMarketCountdown() {
        resetMarketCountdownTimer();
        resetListeningPollTimer();
        stopLtpRenderHeartbeat();
        backtestCountdownState = null;
        listeningState = 'idle';
        updateMarketCountdown();
        marketCountdownTimer = window.setInterval(updateMarketCountdown, 1000);
        persistListeningSession();
        renderListeningControls();
    }

    function startBacktestCountdown(behindTime) {
        var normalizedBehindTime = Math.max(0, parseInt(behindTime || '0', 10) || 0);
        var timeParts = String(backtestMarketOpenTime || '09:16').split(':');
        var marketOpenHour = parseInt(timeParts[0] || '9', 10);
        var marketOpenMinute = parseInt(timeParts[1] || '16', 10);
        var totalOpenMinutes = (marketOpenHour * 60) + marketOpenMinute;
        var simulatedCurrentMinutes = Math.max(0, totalOpenMinutes - normalizedBehindTime);

        backtestCountdownState = {
            remainingMinutes: Math.max(0, totalOpenMinutes - simulatedCurrentMinutes),
            paused: false
        };

        resetMarketCountdownTimer();
        resetListeningPollTimer();
        listeningState = 'running';
        startLtpRenderHeartbeat();
        updateMarketCountdown();
        if (backtestCountdownState) {
            marketCountdownTimer = window.setInterval(updateMarketCountdown, 1000);
        }
        persistListeningSession();
        renderListeningControls();
    }

    function closeExecutionSocket(markPaused) {
        if (markPaused) {
            socketManuallyPaused = true;
        }
        if (markPaused) {
            stopLtpRenderHeartbeat();
        }
        initialPositionSnapshotRequested = false;
        if (executeOrdersSocketClient) {
            executeOrdersSocketClient.close();
            executeOrdersSocketClient = null;
        }
        if (updateSocketClient) {
            updateSocketClient.close();
            updateSocketClient = null;
        }
        updateSocketStatus(markPaused ? 'paused' : 'disconnected');
    }

    function pauseBacktestListening() {
        if (!backtestCountdownState) {
            return;
        }
        stopTimelineAutoplay();
        backtestCountdownState.paused = true;
        listeningState = 'paused';
        resetMarketCountdownTimer();
        resetListeningPollTimer();
        closeExecutionSocket(true);
        updateMarketCountdown();
        persistListeningSession();
        renderListeningControls();
    }

    function resumeBacktestListening() {
        if (!backtestCountdownState) {
            return;
        }
        stopTimelineAutoplay();
        backtestCountdownState.paused = false;
        listeningState = 'running';
        socketManuallyPaused = false;
        startLtpRenderHeartbeat();
        resetMarketCountdownTimer();
        resetListeningPollTimer();
        if (backtestAutoloadEnabled) {
            activateAutoloadSockets(getCurrentBacktestListenTimestamp(), 'resume_backtest_listening');
        } else {
            activateManualSockets(getCurrentBacktestListenTimestamp(), 'resume_manual_listening');
        }
        updateMarketCountdown();
        if (backtestCountdownState) {
            marketCountdownTimer = window.setInterval(updateMarketCountdown, 1000);
        }
        persistListeningSession();
        renderListeningControls();
    }

    function stopBacktestListening() {
        stopTimelineAutoplay();
        closeExecutionSocket(false);
        resetMarketCountdownTimer();
        resetListeningPollTimer();
        stopLtpRenderHeartbeat();
        backtestCountdownState = null;
        listeningState = 'idle';
        socketManuallyPaused = false;
        latestListenTimestamp = '';
        resetLatestLtpSnapshot();
        updateBrowserTabTitle(null);
        updateCurrentListenTimeDisplay('-');
        startRealTimeMarketCountdown();
        persistListeningSession();
        renderListeningControls();
    }

    var activeTab = 'strategies';
    var allItems = [];
    if (!table || !rowsHost || !countLabel || !paginationHost || !searchInput || !tabButtons.length || !setupModal || !setupStrategyName || !cancelDeploymentModal || !confirmCancelDeploymentBtn || !squareOffModal || !confirmSquareOffBtn || !weekdayPanel || !dtePanel || !dteSelectAll || !listeningDateInput || !listeningDateTrigger || !listeningDateLabel || !calendarPopup || !calendarGrid || !calendarWeekdays || !calendarMonthToggle || !calendarYearToggle || !calendarPrevBtn || !calendarNextBtn || !calendarConfirmBtn || !startListeningBtn || !pauseListeningBtn || !stopListeningBtn || !deployedRowsHost || !socketStatusEl || !socketStatusLabel) {
        return;
    }

    var dteOptions = Array.prototype.slice.call(setupModal.querySelectorAll('.ff-dte-option'));

    document.body.appendChild(setupModal);
    document.body.appendChild(cancelDeploymentModal);
    document.body.appendChild(squareOffModal);

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function parseIsoDate(value) {
        var parts = String(value || '').split('-');
        if (parts.length !== 3) {
            return null;
        }
        var year = parseInt(parts[0], 10);
        var month = parseInt(parts[1], 10) - 1;
        var day = parseInt(parts[2], 10);
        if (Number.isNaN(year) || Number.isNaN(month) || Number.isNaN(day)) {
            return null;
        }
        return new Date(year, month, day);
    }

    function formatIsoDate(date) {
        if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
            return '';
        }
        return date.getFullYear() + '-'
            + String(date.getMonth() + 1).padStart(2, '0') + '-'
            + String(date.getDate()).padStart(2, '0');
    }

    function formatDisplayDate(date) {
        if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
            return '';
        }
        return String(date.getDate()).padStart(2, '0') + '/'
            + String(date.getMonth() + 1).padStart(2, '0') + '/'
            + date.getFullYear();
    }

    function isSameDate(left, right) {
        return !!left && !!right
            && left.getFullYear() === right.getFullYear()
            && left.getMonth() === right.getMonth()
            && left.getDate() === right.getDate();
    }

    function setCalendarWeekdaysVisible(visible) {
        calendarWeekdays.hidden = !visible;
    }

    function syncListeningDateLabel() {
        listeningDateLabel.textContent = formatDisplayDate(calendarState.selectedDate || calendarState.draftDate || new Date());
        syncListeningTimeline(latestListenTimestamp || getInitialBacktestListenTimestamp(listeningBehindTimeSelect ? listeningBehindTimeSelect.value : '0'), false);
    }

    function closeCalendarPopup() {
        calendarPopup.hidden = true;
    }

    function openCalendarPopup() {
        calendarState.view = 'date';
        calendarState.draftDate = new Date((calendarState.selectedDate || new Date()).getTime());
        calendarState.displayDate = new Date(calendarState.draftDate.getTime());
        calendarState.yearPageStart = calendarState.displayDate.getFullYear() - 7;
        renderCalendar();
        calendarPopup.hidden = false;
    }

    function renderDateView() {
        var baseDate = calendarState.displayDate || new Date();
        var year = baseDate.getFullYear();
        var month = baseDate.getMonth();
        var firstDay = new Date(year, month, 1);
        var firstWeekday = firstDay.getDay();
        var totalDays = new Date(year, month + 1, 0).getDate();
        var cells = [];
        var todaySelected = calendarState.draftDate || calendarState.selectedDate || new Date();

        for (var i = 0; i < firstWeekday; i += 1) {
            cells.push('<span class="ff-calendar-cell is-muted"></span>');
        }

        for (var day = 1; day <= totalDays; day += 1) {
            var candidateDate = new Date(year, month, day);
            var isSelected = isSameDate(candidateDate, todaySelected);
            cells.push('<button type="button" class="ff-calendar-cell' + (isSelected ? ' is-selected' : '') + '" data-calendar-day="' + day + '">' + day + '</button>');
        }

        calendarGrid.className = 'ff-calendar-grid';
        calendarGrid.innerHTML = cells.join('');
        calendarMonthToggle.textContent = calendarMonthNames[month];
        calendarYearToggle.textContent = String(year);
        calendarMonthToggle.classList.add('is-active');
        calendarYearToggle.classList.remove('is-active');
        setCalendarWeekdaysVisible(true);
    }

    function renderMonthView() {
        var baseDate = calendarState.displayDate || new Date();
        var selectedMonth = baseDate.getMonth();
        calendarGrid.className = 'ff-calendar-grid is-month-view';
        calendarGrid.innerHTML = calendarMonthNamesShort.map(function (monthName, index) {
            var selectedClass = index === selectedMonth ? ' is-selected' : '';
            return '<button type="button" class="ff-calendar-cell' + selectedClass + '" data-calendar-month="' + index + '">' + monthName + '</button>';
        }).join('');
        calendarMonthToggle.textContent = calendarMonthNames[selectedMonth];
        calendarYearToggle.textContent = String(baseDate.getFullYear());
        calendarMonthToggle.classList.add('is-active');
        calendarYearToggle.classList.remove('is-active');
        setCalendarWeekdaysVisible(false);
    }

    function renderYearView() {
        var startYear = calendarState.yearPageStart || ((calendarState.displayDate || new Date()).getFullYear() - 7);
        var selectedYear = (calendarState.displayDate || new Date()).getFullYear();
        var items = [];
        for (var year = startYear; year < startYear + 8; year += 1) {
            items.push('<button type="button" class="ff-calendar-cell' + (year === selectedYear ? ' is-selected' : '') + '" data-calendar-year="' + year + '">' + year + '</button>');
        }
        calendarGrid.className = 'ff-calendar-grid is-year-view';
        calendarGrid.innerHTML = items.join('');
        calendarMonthToggle.textContent = calendarMonthNames[(calendarState.displayDate || new Date()).getMonth()];
        calendarYearToggle.textContent = String(selectedYear);
        calendarMonthToggle.classList.remove('is-active');
        calendarYearToggle.classList.add('is-active');
        setCalendarWeekdaysVisible(false);
    }

    function renderCalendar() {
        if (calendarState.view === 'month') {
            renderMonthView();
            return;
        }
        if (calendarState.view === 'year') {
            renderYearView();
            return;
        }
        renderDateView();
    }

    function commitCalendarDateSelection() {
        var previousDate = String(listeningDateInput.value || '').trim();
        var pickedDate = calendarState.draftDate || calendarState.selectedDate || new Date();
        calendarState.selectedDate = new Date(pickedDate.getTime());
        calendarState.displayDate = new Date(pickedDate.getTime());
        listeningDateInput.value = formatIsoDate(calendarState.selectedDate);
        if (String(listeningDateInput.value || '').trim() !== previousDate) {
            resetLatestLtpSnapshot();
        }
        syncListeningDateLabel();
        closeCalendarPopup();
    }

    function restoreListeningSession() {
        var manager = getListeningManager();
        var savedState = manager ? manager.load(listeningModeKey) : null;
        if (!savedState || !savedState.selected_date) {
            return false;
        }

        syncCalendarStateFromIsoDate(savedState.selected_date);
        if (listeningBehindTimeSelect && savedState.behind_time) {
            listeningBehindTimeSelect.value = String(savedState.behind_time);
        }
        if (typeof savedState.autoload === 'boolean') {
            backtestAutoloadEnabled = !!savedState.autoload;
        }
        updateAutoloadUi();

        var remainingMinutes = Math.max(0, parseInt(savedState.remaining_minutes || 0, 10) || 0);
        var updatedAt = parseInt(savedState.updated_at || 0, 10) || 0;
        if (savedState.listening_state === 'running' && updatedAt) {
            var elapsedSeconds = Math.max(0, Math.floor((Date.now() - updatedAt) / 1000));
            remainingMinutes = Math.max(0, remainingMinutes - elapsedSeconds);
        }

        if (remainingMinutes <= 0) {
            if (manager) {
                manager.clear(listeningModeKey);
            }
            return false;
        }

        backtestCountdownState = {
            remainingMinutes: remainingMinutes,
            paused: savedState.listening_state === 'paused'
        };
        listeningState = savedState.listening_state === 'paused' ? 'paused' : 'running';
        if (listeningState === 'running') {
            startLtpRenderHeartbeat();
        } else {
            stopLtpRenderHeartbeat();
        }
        resetMarketCountdownTimer();
        updateMarketCountdown();
        if (listeningState === 'running' && backtestCountdownState) {
            marketCountdownTimer = window.setInterval(updateMarketCountdown, 1000);
        }
        renderListeningControls();
        syncListeningTimeline(savedState.current_listen_timestamp || getInitialBacktestListenTimestamp(savedState.behind_time), false);
        // API called once on restore — updates come via execute_order socket events
        fetchDeployedPortfolios(savedState.selected_date, false);
        return true;
    }

    function inferUnderlying(name) {
        var upper = String(name || '').toUpperCase();
        if (upper.indexOf('SENSEX') !== -1 || upper.indexOf('SNX') !== -1) {
            return 'SENSEX';
        }
        if (upper.indexOf('BANKNIFTY') !== -1 || upper.indexOf('BANKNIFT') !== -1) {
            return 'BANKNIFTY';
        }
        if (upper.indexOf('FINNIFTY') !== -1 || upper.indexOf('FIN') !== -1) {
            return 'FINNIFTY';
        }
        if (upper.indexOf('MIDCP') !== -1) {
            return 'MIDCPNIFTY';
        }
        if (upper.indexOf('BANKEX') !== -1) {
            return 'BANKEX';
        }
        return 'NIFTY';
    }

    var initialCalendarDate = parseIsoDate(listeningDateInput.value) || new Date();
    calendarState.selectedDate = new Date(initialCalendarDate.getTime());
    calendarState.draftDate = new Date(initialCalendarDate.getTime());
    calendarState.displayDate = new Date(initialCalendarDate.getTime());
    calendarState.yearPageStart = calendarState.displayDate.getFullYear() - 7;
    syncListeningDateLabel();
    updateAutoloadUi();
    renderCalendar();
    if (!restoreListeningSession()) {
        startRealTimeMarketCountdown();
    }

    function setActiveTabUi() {
        tabButtons.forEach(function (button) {
            var isActive = button.getAttribute('data-ff-tab') === activeTab;
            button.classList.toggle('!bg-secondaryBlue-50', isActive);
            button.classList.toggle('!font-bold', isActive);
            button.classList.toggle('!text-secondaryBlue-500', isActive);
        });
        searchInput.placeholder = activeTab === 'portfolios' ? 'Search Portfolios' : 'Search Strategies';
    }

    function renderPagination(totalItems) {
        var totalPages = Math.ceil(totalItems / perPage);
        if (totalPages <= 1) {
            paginationHost.innerHTML = '';
            return;
        }

        var items = [];
        items.push('<button class="ff-pagination-btn" data-page="' + (currentPage - 1) + '"' + (currentPage === 1 ? ' disabled' : '') + '>Prev</button>');
        for (var page = 1; page <= totalPages; page += 1) {
            items.push('<button class="ff-pagination-btn' + (page === currentPage ? ' active' : '') + '" data-page="' + page + '">' + page + '</button>');
        }
        items.push('<button class="ff-pagination-btn" data-page="' + (currentPage + 1) + '"' + (currentPage === totalPages ? ' disabled' : '') + '>Next</button>');
        paginationHost.innerHTML = items.join('');
    }

    function renderRows() {
        var itemLabel = activeTab === 'portfolios' ? 'Portfolio' : 'Strategy';
        countLabel.textContent = itemLabel + ' (' + allItems.length + ')';

        if (!allItems.length) {
            rowsHost.innerHTML = '<div class="ff-strategy-row"><div class="ff-strategy-info"><div class="ff-strategy-title"><span class="ff-strategy-title-text">No saved ' + itemLabel.toLowerCase() + 's found</span></div></div><div></div><div></div><div></div><div></div></div>';
            paginationHost.innerHTML = '';
            return;
        }

        var start = (currentPage - 1) * perPage;
        var visibleItems = allItems.slice(start, start + perPage);

        rowsHost.innerHTML = visibleItems.map(function (item) {
            var itemName = escapeHtml(item.name || 'Untitled ' + itemLabel);
            var metaPrimary = activeTab === 'portfolios'
                ? 'PORTFOLIO'
                : escapeHtml(item.underlying || inferUnderlying(item.name));
            var metaSecondary = activeTab === 'portfolios'
                ? ((item.strategy_ids && item.strategy_ids.length) ? item.strategy_ids.length + ' Strategies' : '0 Strategies')
                : 'INTRADAY';
            return '' +
                '<div class="ff-strategy-row">' +
                '    <div class="ff-strategy-info">' +
                '        <div class="ff-strategy-title">' +
                '            <span class="ff-strategy-title-text">' + itemName + '</span>' +
                '            <svg class="ff-edit-icon" viewBox="0 0 24 24" fill="none" aria-hidden="true" data-edit-icon="' + (activeTab === 'portfolios' ? 'portfolio' : 'strategy') + '" data-item-id="' + encodeURIComponent(item._id || '') + '">' +
                '                <path d="M16.862 3.487 20.513 7.138M9 20H5v-4L16.044 4.956a2.25 2.25 0 1 1 3.182 3.182L9 20Z" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"></path>' +
                '            </svg>' +
                '        </div>' +
                '        <div class="ff-strategy-meta">' +
                '            <span>' + metaPrimary + '</span>' +
                '            <span>&bull;</span>' +
                '            <span>' + escapeHtml(metaSecondary) + '</span>' +
                '        </div>' +
                '    </div>' +
                '    <div><div class="ff-status-pill">Ready to deploy</div></div>' +
                '    <div><div class="ff-instance-chip">0 Deployed</div></div>' +
                '    <div class="ff-days">' +
                '        <span class="ff-day">M</span>' +
                '        <span class="ff-day">T</span>' +
                '        <span class="ff-day">W</span>' +
                '        <span class="ff-day th">TH</span>' +
                '        <span class="ff-day">F</span>' +
                '        <span class="ff-day">-</span>' +
                '        <span class="ff-day">-</span>' +
                '    </div>' +
                '    <div class="ff-actions">' +
                '        <a class="ff-btn ff-btn-secondary" href="#" data-edit-setup="true" data-strategy-name="' + itemName + '" data-strategy-id="' + encodeURIComponent(item._id || '') + '">Edit Setup</a>' +
                '        <button class="ff-btn ff-btn-primary" data-activate-item="' + (activeTab === 'portfolios' ? 'portfolio' : 'strategy') + '" data-item-id="' + encodeURIComponent(item._id || '') + '">Activate</button>' +
                '    </div>' +
                '</div>';
        }).join('');

        renderPagination(allItems.length);
    }

    function renderError(message) {
        countLabel.textContent = (activeTab === 'portfolios' ? 'Portfolio' : 'Strategy') + ' (0)';
        rowsHost.innerHTML = '<div class="ff-strategy-row"><div class="ff-strategy-info"><div class="ff-strategy-title"><span class="ff-strategy-title-text">' + escapeHtml(message) + '</span></div></div><div></div><div></div><div></div><div></div></div>';
        paginationHost.innerHTML = '';
    }

    function normalizeExecutionStatus(record) {
        var normalizedStatus = String(record && record.status || '').trim();
        if (normalizedStatus === 'StrategyStatus.Stopped') {
            return {
                text: 'STOPPED',
                className: 'stopped'
            };
        }
        if (normalizedStatus === 'StrategyStatus.SquaredOff') {
            return {
                text: 'SQD OFF',
                className: 'squared-off'
            };
        }
        if (normalizedStatus === 'StrategyStatus.Paused') {
            return {
                text: 'PAUSED',
                className: 'paused'
            };
        }
        if (record && record.active_on_server === false) {
            return {
                text: 'SQD OFF',
                className: 'squared-off'
            };
        }
        if (normalizedStatus === 'StrategyStatus.Live_Running' || (record && record.active_on_server)) {
            return {
                text: 'RUNNING',
                className: 'running'
            };
        }
        return {
            text: 'STOPPED',
            className: 'stopped'
        };
    }

    function getBrokerLabel(record) {
        var mode = String(record && record.activation_mode || listeningModeKey || 'algo-backtest');
        if (mode === 'forward-test' || mode === 'fast-forward' || mode === 'fast-forwarding') {
            return { badge: 'FT', label: 'Forward Test' };
        }
        if (mode === 'live') {
            return { badge: 'LV', label: 'Live' };
        }
        return { badge: 'AB', label: 'Backtest' };
    }

    function buildBrokerDisplayHtml(record) {
        var brokerMeta = getBrokerLabel(record);
        var brokerName = getBrokerDisplayName(record);
        var brokerIcon = getBrokerIconPath(record);
        var iconHtml = brokerIcon
            ? '<img class="ff-deployed-broker-icon" src="' + escapeHtml(brokerIcon) + '" alt="' + escapeHtml(brokerName || 'Broker') + '" onerror="this.onerror=null;this.src=\'../assets/brokers/' + escapeHtml(String(brokerIcon || '').split('/').pop() || '') + '\';">'
            : '<span class="ff-deployed-broker-badge">' + escapeHtml(brokerMeta.badge) + '</span>';
        return '' +
            '<div class="ff-deployed-broker">' +
            iconHtml +
            '<span class="ff-deployed-broker-label">' + escapeHtml(brokerName || brokerMeta.label) + '</span>' +
            '</div>';
    }

    function getBrokerDisplayName(record) {
        if (!record || typeof record !== 'object') {
            return 'Unknown Broker';
        }
        var brokerDetails = record.broker_details && typeof record.broker_details === 'object'
            ? record.broker_details
            : {};
        function humanizeBrokerValue(value) {
            var normalized = String(value || '').trim();
            if (!normalized) {
                return '';
            }
            normalized = normalized.replace(/^Broker\./i, '');
            normalized = normalized.replace(/[_-]+/g, ' ');
            normalized = normalized.replace(/([a-z])([A-Z])/g, '$1 $2');
            normalized = normalized.replace(/\s+/g, ' ').trim();
            if (!normalized) {
                return '';
            }
            return normalized.split(' ').map(function (part) {
                if (!part) {
                    return '';
                }
                return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
            }).join(' ');
        }

        function humanizeBrokerIconName(value) {
            var normalized = String(value || '').trim();
            if (!normalized) {
                return '';
            }
            normalized = normalized.replace(/\.[a-z0-9]+$/i, '');
            if (normalized.toLowerCase() === 'flattrade') {
                return 'FlatTrade';
            }
            return humanizeBrokerValue(normalized);
        }

        var candidates = [
            humanizeBrokerValue(brokerDetails.broker_name),
            humanizeBrokerValue(brokerDetails.display_name),
            humanizeBrokerValue(brokerDetails.name),
            humanizeBrokerValue(brokerDetails.title),
            humanizeBrokerIconName(brokerDetails.broker_icon),
            (!isLikelyObjectId(record.broker_label) ? record.broker_label : ''),
            humanizeBrokerValue(brokerDetails.broker),
            humanizeBrokerValue(brokerDetails.provider),
            humanizeBrokerValue(brokerDetails.vendor),
            (!isLikelyObjectId(record.broker) ? humanizeBrokerValue(record.broker) : '')
        ];
        for (var i = 0; i < candidates.length; i += 1) {
            var value = String(candidates[i] || '').trim();
            if (value) {
                return value;
            }
        }
        return 'Unknown Broker';
    }

    function getBrokerIconPath(record) {
        if (!record || typeof record !== 'object') {
            return '';
        }
        var brokerDetails = record.broker_details && typeof record.broker_details === 'object'
            ? record.broker_details
            : {};
        var brokerIcon = String(brokerDetails.broker_icon || '').trim();
        if (!brokerIcon) {
            return '';
        }
        return '../assets/images/brokers/' + encodeURIComponent(brokerIcon);
    }

    function fmtRs(v) {
        var n = Math.round((parseFloat(v) || 0) * 100) / 100;
        return '\u20b9\u202f' + n.toLocaleString('en-IN', { minimumFractionDigits: 0, maximumFractionDigits: 2 });
    }

    function buildBrokerSettingsStrip(bs) {
        if (!bs) return '';
        var chips = [];

        // SL chip
        if (bs.stop_loss != null) {
            var effSl = bs.state && bs.state.effective_sl != null ? bs.state.effective_sl : bs.stop_loss;
            var slLabel = (effSl !== bs.stop_loss)
                ? 'SL ' + fmtRs(effSl) + ' <span class="ff-bss-orig">(' + fmtRs(bs.stop_loss) + ')</span>'
                : 'SL ' + fmtRs(bs.stop_loss);
            chips.push('<span class="ff-bss-chip ff-bss-sl">' + slLabel + '</span>');
        }

        // TGT chip
        if (bs.target != null) {
            chips.push('<span class="ff-bss-chip ff-bss-tgt">TGT ' + fmtRs(bs.target) + '</span>');
        }

        // Lock & Trail chip
        var lat = bs.lock_and_trail;
        if (lat && lat.instrument_move != null && lat.stop_loss_move != null) {
            var state = bs.state || {};
            var activated = state.lock_activated;
            if (activated) {
                var floor = state.current_lock_floor || lat.stop_loss_move;
                chips.push(
                    '<span class="ff-bss-chip ff-bss-lock ff-bss-lock-on">' +
                    'Lock \u2713 Floor ' + fmtRs(floor) +
                    '</span>'
                );
            } else {
                chips.push(
                    '<span class="ff-bss-chip ff-bss-lock">' +
                    'Lock @ ' + fmtRs(lat.instrument_move) + ' \u2192 ' + fmtRs(lat.stop_loss_move) +
                    '</span>'
                );
            }
        }

        // Trail SL chip (standalone — when LockAndTrail not used)
        var trail = bs.trail_sl;
        if (trail && trail.instrument_move != null && trail.stop_loss_move != null && !lat) {
            chips.push(
                '<span class="ff-bss-chip ff-bss-trail">' +
                'Trail ' + fmtRs(trail.stop_loss_move) + '/\u20b9' + fmtRs(trail.instrument_move) +
                '</span>'
            );
        }

        if (!chips.length) return '';
        return (
            '<div class="ff-header-broker-settings-strip">' +
            chips.join('') +
            '</div>'
        );
    }

    function formatBrokerMtm(value) {
        var rounded = Math.round((parseFloat(value) || 0) * 100) / 100;
        return '\u20b9 ' + rounded.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function renderHeaderBrokerSummary(brokerSummaryMap) {
        if (!headerBrokerListHost) {
            return;
        }
        var entries = Object.keys(brokerSummaryMap || {}).map(function (key) {
            return brokerSummaryMap[key];
        }).filter(Boolean);
        var selectedBrokerId = '';
        if (window.ffSelectedBrokerSettingsContext && typeof window.ffSelectedBrokerSettingsContext === 'object') {
            selectedBrokerId = String(window.ffSelectedBrokerSettingsContext.broker || '').trim();
        }

        if (!entries.length) {
            headerBrokerListHost.innerHTML = '<div class="ff-header-broker-empty">No brokers loaded.</div>';
            if (selectedBrokerId && typeof window.ffUpdateSelectedBrokerSummary === 'function') {
                window.ffUpdateSelectedBrokerSummary(null);
            }
            return;
        }

        entries.sort(function (left, right) {
            return String(left.label || '').localeCompare(String(right.label || ''));
        });

        // Remove stale "no brokers" placeholder if present
        var emptyEl = headerBrokerListHost.querySelector('.ff-header-broker-empty');
        if (emptyEl) emptyEl.remove();

        entries.forEach(function (entry) {
            var mtmColor = (parseFloat(entry.mtm) || 0) >= 0 ? '#16a34a' : '#ef4444';
            var strategyCount = parseInt(entry.strategyCount || 0, 10) || 0;
            var openLegCount  = parseInt(entry.openLegCount  || 0, 10) || 0;
            var bid = escapeHtml(entry.brokerId || '');

            var card = headerBrokerListHost.querySelector('.ff-header-broker-card[data-broker-id="' + bid + '"]');

            if (!card) {
                // First time — create the full card including the icon (loaded only once)
                var iconHtml = entry.icon
                    ? '<img class="ff-header-broker-icon" src="' + escapeHtml(entry.icon) + '" alt="' + escapeHtml(entry.label || 'Broker') + '" onerror="this.onerror=null;this.src=\'../assets/brokers/' + escapeHtml(String(entry.icon || '').split('/').pop() || '') + '\';this.style.display=\'block\';">'
                    : '';
                var settingsStrip = buildBrokerSettingsStrip(brokerSettingsCache[entry.brokerId] || null);
                var cardHtml =
                    '<div class="ff-header-broker-card" data-broker-summary-card="true" data-broker-id="' + bid + '" data-user-id="' + escapeHtml(entry.userId || '') + '" data-broker-name="' + escapeHtml(entry.label || '') + '">' +
                    '  <div class="ff-header-broker-main">' +
                    iconHtml +
                    '    <div class="ff-header-broker-copy">' +
                    '      <div class="ff-header-broker-name-row">' +
                    '        <p class="ff-header-broker-name">' + escapeHtml(entry.label || 'Unknown Broker') + '</p>' +
                    '        <p class="ff-header-broker-mtm" data-broker-mtm="' + bid + '" style="color:' + mtmColor + ';">' + escapeHtml(formatBrokerMtm(entry.mtm)) + '</p>' +
                    '      </div>' +
                    '      <p class="ff-header-broker-meta" data-broker-meta="' + bid + '">' + strategyCount + ' strategies \u2022 ' + openLegCount + ' open legs</p>' +
                    '    </div>' +
                    '  </div>' +
                    (settingsStrip ? settingsStrip : '') +
                    '</div>';
                headerBrokerListHost.insertAdjacentHTML('beforeend', cardHtml);
            } else {
                // Card exists — patch only MTM and meta text; icon stays untouched
                var mtmEl  = card.querySelector('[data-broker-mtm="' + bid + '"]');
                var metaEl = card.querySelector('[data-broker-meta="' + bid + '"]');
                if (mtmEl) {
                    mtmEl.textContent = formatBrokerMtm(entry.mtm);
                    mtmEl.style.color = mtmColor;
                }
                if (metaEl) {
                    metaEl.textContent = strategyCount + ' strategies \u2022 ' + openLegCount + ' open legs';
                }
            }

            if (
                selectedBrokerId
                && entry.brokerId === selectedBrokerId
                && typeof window.ffUpdateSelectedBrokerSummary === 'function'
            ) {
                window.ffUpdateSelectedBrokerSummary({
                    brokerId: entry.brokerId,
                    label: entry.label,
                    mtm: entry.mtm,
                    strategyCount: strategyCount,
                    openLegCount: openLegCount
                });
            }
        });

        // Remove cards for brokers that are no longer in the list
        var allCards = headerBrokerListHost.querySelectorAll('.ff-header-broker-card[data-broker-id]');
        var activeIds = entries.map(function (e) { return e.brokerId || ''; });
        allCards.forEach(function (c) {
            if (activeIds.indexOf(c.getAttribute('data-broker-id')) === -1) c.remove();
        });
    }

    function parseRecordDateTime(value) {
        if (!value) {
            return null;
        }
        var normalized = String(value).trim().replace('T', ' ');
        var match = normalized.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})(?::(\d{2}))?/);
        if (!match) {
            return null;
        }
        var parsed = new Date(
            parseInt(match[1], 10),
            parseInt(match[2], 10) - 1,
            parseInt(match[3], 10),
            parseInt(match[4], 10),
            parseInt(match[5], 10),
            parseInt(match[6] || '0', 10),
            0
        );
        return Number.isNaN(parsed.getTime()) ? null : parsed;
    }

    function getCurrentListeningDateTime() {
        if (!backtestCountdownState || !listeningDateInput || listeningState === 'idle') {
            return null;
        }
        var selectedDate = String(listeningDateInput.value || '').trim();
        var baseDate = parseIsoDate(selectedDate);
        if (!baseDate) {
            return null;
        }
        var timeParts = String(backtestMarketOpenTime || '09:16').split(':');
        var marketOpenDate = new Date(
            baseDate.getFullYear(),
            baseDate.getMonth(),
            baseDate.getDate(),
            parseInt(timeParts[0] || '9', 10),
            parseInt(timeParts[1] || '16', 10),
            0,
            0
        );
        var remainingMinutes = Math.max(0, parseInt(backtestCountdownState.remainingMinutes || 0, 10) || 0);
        return new Date(marketOpenDate.getTime() - (remainingMinutes * 60000));
    }

    function formatEntryCountdown(diffMs) {
        var totalSeconds = Math.max(0, Math.floor(diffMs / 1000));
        var hours = Math.floor(totalSeconds / 3600);
        var minutes = Math.floor((totalSeconds % 3600) / 60);
        var seconds = totalSeconds % 60;
        return String(hours).padStart(2, '0') + ':'
            + String(minutes).padStart(2, '0') + ':'
            + String(seconds).padStart(2, '0');
    }

    function hasRecordTradeStarted(record, currentListeningDateTime) {
        var legCount = Array.isArray(record && record.legs) ? record.legs.length : 0;
        if (legCount > 0) {
            return true;
        }
        var entryTime = parseRecordDateTime(record && record.entry_time ? record.entry_time : '');
        if (!entryTime || !currentListeningDateTime) {
            return false;
        }
        return currentListeningDateTime.getTime() >= entryTime.getTime();
    }

    function buildDeploymentActionButton(openPositionCount, strategyId, groupId) {
        var hasOpenPositions = Number(openPositionCount || 0) > 0;
        var label = hasOpenPositions ? 'Square Off' : 'Cancel Deployment';
        var stateClass = hasOpenPositions ? 'ff-deployed-btn-warning' : 'ff-deployed-btn-danger';
        var action = hasOpenPositions ? 'square-off' : 'cancel-deployment';
        return '<button type="button" class="ff-deployed-btn ' + stateClass + '" data-deployment-action="' + escapeHtml(action) + '" data-strategy-id="' + escapeHtml(strategyId || '') + '" data-group-id="' + escapeHtml(groupId || '') + '">' + escapeHtml(label) + '</button>';
    }

    function refreshDeployedStrategyTimers() {
        // Use latestListenTimestamp (updated every second from ltp_update) for second-precision countdown.
        // Falls back to getCurrentListeningDateTime() if no socket timestamp yet.
        var tsStr = latestListenTimestamp ? String(latestListenTimestamp).replace(' ', 'T') : '';
        var parsedTs = tsStr ? new Date(tsStr) : null;
        var currentListeningDateTime = (parsedTs && !isNaN(parsedTs.getTime()))
            ? parsedTs
            : getCurrentListeningDateTime();

        deployedRowsHost.querySelectorAll('[data-entry-target]').forEach(function (node) {
            var legCount = parseInt(node.getAttribute('data-leg-count') || '0', 10) || 0;
            // Strategies with legs: MTM is managed by applyLtpToMtm — never reset here
            if (legCount > 0) return;

            // Pre-entry: show countdown or ₹ 0 once entry time passes
            var entryTime = parseRecordDateTime(node.getAttribute('data-entry-target') || '');
            if (!entryTime || !currentListeningDateTime || currentListeningDateTime.getTime() >= entryTime.getTime()) {
                var hasCountdownPill = !!node.querySelector('.ff-deployed-entry-pill');
                var currentText = String(node.textContent || '').trim();
                if (hasCountdownPill || !currentText) {
                    node.innerHTML = '₹ 0';
                }
                return;
            }
            var diffMs = entryTime.getTime() - currentListeningDateTime.getTime();
            node.innerHTML = '<span class="ff-deployed-entry-pill">entry in ' + formatEntryCountdown(diffMs) + '</span>';
        });
    }

    function groupDeployedRecords(records) {
        var groupedMap = {};
        (records || []).forEach(function (record) {
            var portfolioMeta = record && record.portfolio && typeof record.portfolio === 'object' ? record.portfolio : {};
            var groupId = String(portfolioMeta.group_id || portfolioMeta.portfolio || record._id || '');
            if (!groupedMap[groupId]) {
                groupedMap[groupId] = {
                    group_id: groupId,
                    group_name: portfolioMeta.group_name || record.name || 'Portfolio',
                    portfolio_id: portfolioMeta.portfolio || '',
                    items: []
                };
            }
            groupedMap[groupId].items.push(record);
        });
        return Object.keys(groupedMap).map(function (key) {
            return groupedMap[key];
        });
    }

    function captureDeployedGroupExpandedState() {
        if (!deployedRowsHost) {
            return {};
        }
        var nextState = {};
        deployedRowsHost.querySelectorAll('.ff-deployed-group[data-group-id]').forEach(function (groupNode) {
            var groupId = String(groupNode.getAttribute('data-group-id') || '').trim();
            if (!groupId) {
                return;
            }
            nextState[groupId] = groupNode.getAttribute('data-expanded') === 'true';
        });
        deployedGroupExpandedState = nextState;
        return nextState;
    }

    function applyDeployedGroupExpandedState(groupedRecords, previousState) {
        if (!deployedRowsHost) {
            return;
        }
        var records = Array.isArray(groupedRecords) ? groupedRecords : [];
        var restoredState = {};
        records.forEach(function (group) {
            var groupId = String(group && group.group_id ? group.group_id : '').trim();
            if (!groupId) {
                return;
            }
            var isExpanded = !!(previousState && previousState[groupId]);
            var groupNode = deployedRowsHost.querySelector('[data-group-id="' + groupId + '"]');
            if (!groupNode) {
                return;
            }
            groupNode.setAttribute('data-expanded', isExpanded ? 'true' : 'false');
            restoredState[groupId] = isExpanded;
        });
        deployedGroupExpandedState = restoredState;
    }

    var deployedStrategyDetailExpandedState = {};

    function formatDetailMoney(value, fallback) {
        var numeric = Number(value);
        if (!isFinite(numeric)) {
            return fallback || '-';
        }
        return '₹ ' + numeric.toLocaleString('en-IN', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });
    }

    function getOpenStrategyLegs(record) {
        var legs = Array.isArray(record && record.legs) ? record.legs : [];
        return legs.filter(function (leg) {
            return leg && typeof leg === 'object' && Number(leg.status) === 1;
        });
    }

    function getPendingFeatureLegs(record) {
        var legs = Array.isArray(record && record.pending_feature_legs) ? record.pending_feature_legs : [];
        return legs.filter(function (leg) {
            return leg && leg.is_pending_feature_leg;
        });
    }

    function getLegFeatureRow(leg, featureKey) {
        var featureMap = leg && typeof leg.feature_status_map === 'object' ? leg.feature_status_map : {};
        return featureMap && typeof featureMap === 'object' ? featureMap[featureKey] : null;
    }

    function resolveLegDisplayMeta(leg, record) {
        var legId = String(leg && (leg.id || leg.leg_id) || '').trim();
        var lazyLegRef = String(leg && leg.lazy_leg_ref || '').trim();
        var triggeredBy = String(leg && leg.triggered_by || '').trim();
        var parentLegId = String(leg && leg.parent_leg_id || '').trim();
        var legType = String(leg && leg.leg_type || '').trim();
        var hasParentReference = !!(triggeredBy || parentLegId);
        var isOverallParentReentry = /overall_reentry/i.test(legType) || /^overall_/i.test(triggeredBy);
        var isOverallParentReentryDescendant = isOverallParentReentry || /-overall_reentry-/i.test(legType) || /-ore-/i.test(triggeredBy);
        var isTrueLazyLeg = !!(lazyLegRef && hasParentReference && !isOverallParentReentry && lazyLegRef !== parentLegId);
        var isLazy = isTrueLazyLeg;
        var isReentry = !!(leg && (leg.is_reentered_leg || triggeredBy));
        var baseLabel = 'Parent Leg';
        var overallReason = String(record && record.last_overall_event_reason || '').trim().toLowerCase();

        function buildOverallSourceLabel() {
            var sourceLabel = 'Overall Re-entry';
            if (overallReason === 'overall_sl') {
                sourceLabel = 'Overall SL Re-entry';
            } else if (overallReason === 'overall_target') {
                sourceLabel = 'Overall Target Re-entry';
            }
            return sourceLabel;
        }

        if (isOverallParentReentryDescendant) {
            var overallCount = Math.max(
                Number(record && record.overall_sl_reentry_done || 0),
                Number(record && record.overall_tgt_reentry_done || 0),
                0
            );
            var overallReentryType = String(leg && leg.reentry_type || '').trim();
            var overallSourceLabel = buildOverallSourceLabel();
            if (/LikeOriginal/i.test(overallReentryType)) {
                overallSourceLabel = overallSourceLabel.replace(/Re-entry/i, 'Re-momentum');
            }
            var descendantReentryMatch = legType.match(/reentry_(\d+)/i);
            var descendantReentryCount = descendantReentryMatch && descendantReentryMatch[1]
                ? descendantReentryMatch[1]
                : (overallCount > 0 ? String(overallCount) : '');
            baseLabel = 'Parent Leg (' + overallSourceLabel + (descendantReentryCount ? ' ' + descendantReentryCount : '') + ')';
        } else if (isLazy) {
            baseLabel = 'Lazy Leg';
            if (lazyLegRef || legId) {
                baseLabel += ' (' + (lazyLegRef || legId) + ')';
            }
        }

        var reentryCount = '';
        var reentryMatch = legType.match(/reentry_(\d+)/i);
        if (reentryMatch && reentryMatch[1]) {
            reentryCount = reentryMatch[1];
        } else if (/_re_/i.test(legId)) {
            reentryCount = '1';
        }

        // AtCost pending: entry_trade is null, reentry_type is AtCost, not yet entered
        var isAtCostPending = !!(
            String(leg && leg.reentry_type || '').trim() === 'AtCost' &&
            leg && leg.is_lazy &&
            !(leg.entry_trade && leg.entry_trade.price)
        );

        // AtCost triggered (already entered): show as "Reentry N (re-cost)"
        var isAtCostTriggered = !!(
            String(leg && leg.reentry_type || '').trim() === 'AtCost' &&
            leg && !leg.is_lazy &&
            leg.entry_trade && leg.entry_trade.price
        );

        if (isAtCostPending) {
            baseLabel += ' • RE-COST Waiting';
        } else if (isAtCostTriggered && reentryCount) {
            baseLabel += ' • Reentry ' + reentryCount + ' (re-cost)';
        } else if (isReentry && reentryCount) {
            baseLabel += ' • Reentry ' + reentryCount;
        }

        return {
            legId: legId,
            isLazy: isLazy,
            isReentry: isReentry,
            isOverallParentReentry: isOverallParentReentry,
            isAtCostPending: isAtCostPending,
            isAtCostTriggered: isAtCostTriggered,
            label: baseLabel
        };
    }

    function buildDetailMetricHtml(label, value) {
        return '' +
            '<div class="ff-deployed-detail-metric">' +
            '    <div class="ff-deployed-detail-label">' + escapeHtml(label) + '</div>' +
            '    <div class="ff-deployed-detail-value">' + escapeHtml(value) + '</div>' +
            '</div>';
    }

    function buildFeatureMetricHtml(label, value, enabled) {
        var stateClass = enabled ? 'enabled' : 'disabled';
        var stateLabel = enabled ? 'Enabled' : 'Disabled';
        return '' +
            '<div class="ff-deployed-detail-metric">' +
            '    <div class="ff-deployed-detail-label">' + escapeHtml(label) + ' <span class="ff-deployed-detail-state ' + stateClass + '">(' + stateLabel + ')</span></div>' +
            '    <div class="ff-deployed-detail-value">' + escapeHtml(value) + '</div>' +
            '</div>';
    }

    function buildPnlMetricHtml(label, value) {
        var numeric = Number(value);
        var formattedValue = formatDetailMoney(value, '-');
        var colorStyle = '';
        if (isFinite(numeric)) {
            colorStyle = ' style="color:' + (numeric >= 0 ? '#16a34a' : '#ef4444') + '"';
        }
        return '' +
            '<div class="ff-deployed-detail-metric">' +
            '    <div class="ff-deployed-detail-label">' + escapeHtml(label) + '</div>' +
            '    <div class="ff-deployed-detail-value"' + colorStyle + '>' + escapeHtml(formattedValue) + '</div>' +
            '</div>';
    }

    function buildStrategyFeatureHtml(record) {
        var rows = Array.isArray(record && record.strategy_feature_status_rows) ? record.strategy_feature_status_rows : [];
        if (!rows.length) {
            return '';
        }
        return rows.map(function (row) {
            var featureKey = String(row && row.feature || '').trim();
            var title = featureKey === 'overall_sl' ? 'Overall Stop Loss' : 'Overall Target';
            var currentValue = formatDetailMoney(row && row.trigger_value, '-');
            var nextValue = formatDetailMoney(row && row.next_trigger_value, '-');
            var cycleText = 'Cycle ' + String(row && row.cycle_number != null ? row.cycle_number : 1);
            var reentryType = String(row && row.reentry_type || 'None');
            var reentryDone = String(row && row.reentry_done != null ? row.reentry_done : 0);
            var reentryCount = String(row && row.reentry_count != null ? row.reentry_count : 0);
            var statusText = String(row && row.status || 'active');
            var description = String(row && row.trigger_description || '').trim();
            return '' +
                '<div class="ff-deployed-detail-card">' +
                '    <div class="ff-deployed-detail-leg-title">' + escapeHtml(title) + '</div>' +
                '    <div class="ff-deployed-detail-grid">' +
                buildDetailMetricHtml('Current Value', currentValue) +
                buildDetailMetricHtml('Next Value', nextValue) +
                buildDetailMetricHtml('Cycle', cycleText) +
                buildDetailMetricHtml('Re-entry Type', reentryType) +
                buildDetailMetricHtml('Re-entry Used', reentryDone + '/' + reentryCount) +
                buildDetailMetricHtml('Status', statusText) +
                '    </div>' +
                '    <div class="ff-deployed-feature-status-title">Feature Status</div>' +
                (description
                    ? '<div class="ff-deployed-feature-status-list"><div class="ff-deployed-feature-status-item">' + escapeHtml(description) + '</div></div>'
                    : '<div class="ff-deployed-detail-empty">No overall feature status available.</div>') +
                '</div>';
        }).join('');
    }

    function buildStrategyDetailHtml(record) {
        var openLegs = getOpenStrategyLegs(record);
        var pendingLegs = getPendingFeatureLegs(record);
        var strategyFeaturesHtml = buildStrategyFeatureHtml(record);
        var detailLegs = openLegs.concat(pendingLegs);
        if (!detailLegs.length && !strategyFeaturesHtml) {
            return '<div class="ff-deployed-detail-empty">No open leg details available.</div>';
        }

        var legCardsHtml = detailLegs.map(function (leg, index) {
            var isPendingFeatureLeg = !!(leg && leg.is_pending_feature_leg);
            var entryTrade = leg && typeof leg.entry_trade === 'object' ? leg.entry_trade : {};
            var optionLabel = String(leg.option || '').toUpperCase();
            var displayMeta = resolveLegDisplayMeta(leg, record);
            var legTitle = String(leg.strike || '-')
                + (optionLabel ? ' ' + optionLabel : '')
                + ' • ' + displayMeta.label;

            var slRow = getLegFeatureRow(leg, 'sl');
            var targetRow = getLegFeatureRow(leg, 'target');
            var trailRow = getLegFeatureRow(leg, 'trailSL');
            var momentumRow = getLegFeatureRow(leg, 'momentum_pending');

            var slValue = slRow
                ? formatDetailMoney(slRow.current_sl_price != null ? slRow.current_sl_price : slRow.trigger_price, '-')
                : '-';
            var targetValue = targetRow
                ? formatDetailMoney(targetRow.trigger_price, '-')
                : '-';
            var trailValue = trailRow
                ? formatDetailMoney(trailRow.current_sl_price != null ? trailRow.current_sl_price : trailRow.trigger_price, '-')
                : '-';

            var triggerDescriptions = Array.isArray(leg.active_trigger_descriptions)
                ? leg.active_trigger_descriptions.filter(Boolean)
                : [];

            // ── AtCost pending card (waiting for price to return to cost) ─────────
            if (displayMeta.isAtCostPending) {
                var recostQueuedAt = String((leg.queued_at || '')).replace('T', ' ') || '-';
                var recostCostPrice = formatDetailMoney(leg.momentum_base_price, '-');
                var recostLtp = formatDetailMoney(
                    leg.last_saw_price != null ? leg.last_saw_price : (leg.ltp != null ? leg.ltp : leg.mark_price),
                    '-'
                );
                var recostRow = getLegFeatureRow(leg, 'reCost');
                var recostStatus = recostRow && recostRow.status
                    ? String(recostRow.status).charAt(0).toUpperCase() + String(recostRow.status).slice(1)
                    : 'Pending';

                return '' +
                    '<div class="ff-deployed-detail-card">' +
                    '    <div class="ff-deployed-detail-leg-title">' + escapeHtml(legTitle) + '</div>' +
                    '    <div class="ff-deployed-detail-grid">' +
                    buildDetailMetricHtml('Queued At', recostQueuedAt) +
                    buildDetailMetricHtml('Cost Price', recostCostPrice) +
                    buildDetailMetricHtml('Current LTP', recostLtp) +
                    buildDetailMetricHtml('Status', recostStatus) +
                    '    </div>' +
                    '    <div class="ff-deployed-feature-status-title">Feature Status</div>' +
                    (triggerDescriptions.length
                        ? '<div class="ff-deployed-feature-status-list">' + triggerDescriptions.map(function (description) {
                            return '<div class="ff-deployed-feature-status-item">' + escapeHtml(description) + '</div>';
                        }).join('') + '</div>'
                        : '<div class="ff-deployed-detail-empty">No active feature status available.</div>') +
                    '</div>';
            }

            if (isPendingFeatureLeg) {
                var queuedAt = String((leg.queued_at || '')).replace('T', ' ') || '-';
                var armedAt = String((leg.armed_at || '')).replace('T', ' ') || '-';
                var pendingLtpValue = formatDetailMoney(
                    leg.last_saw_price != null ? leg.last_saw_price : (leg.ltp != null ? leg.ltp : leg.mark_price),
                    '-'
                );
                var baseValue = formatDetailMoney(leg.momentum_base_price, '-');
                var pendingTargetValue = formatDetailMoney(leg.momentum_target_price, '-');
                var statusValue = momentumRow && momentumRow.status
                    ? String(momentumRow.status).replace(/_/g, ' ')
                    : 'active';
                var statusLabel = statusValue.charAt(0).toUpperCase() + statusValue.slice(1);

                return '' +
                    '<div class="ff-deployed-detail-card">' +
                    '    <div class="ff-deployed-detail-leg-title">' + escapeHtml(legTitle + ' • Momentum Queue') + '</div>' +
                    '    <div class="ff-deployed-detail-grid">' +
                    buildDetailMetricHtml('Queued At', queuedAt) +
                    buildDetailMetricHtml('Armed At', armedAt) +
                    buildDetailMetricHtml('LTP', pendingLtpValue) +
                    buildDetailMetricHtml('Base Price', baseValue) +
                    buildDetailMetricHtml('Target Price', pendingTargetValue) +
                    buildDetailMetricHtml('Status', statusLabel) +
                    '    </div>' +
                    '    <div class="ff-deployed-feature-status-title">Feature Status</div>' +
                    (triggerDescriptions.length
                        ? '<div class="ff-deployed-feature-status-list">' + triggerDescriptions.map(function (description) {
                            return '<div class="ff-deployed-feature-status-item">' + escapeHtml(description) + '</div>';
                        }).join('') + '</div>'
                        : '<div class="ff-deployed-detail-empty">No active feature status available.</div>') +
                    '</div>';
            }

            return '' +
                '<div class="ff-deployed-detail-card">' +
                '    <div class="ff-deployed-detail-leg-title">' + escapeHtml(legTitle) + '</div>' +
                '    <div class="ff-deployed-detail-grid">' +
                '        <div class="ff-deployed-detail-metric">' +
                '            <div class="ff-deployed-detail-label">Entry Price</div>' +
                '            <div class="ff-deployed-detail-value">' + escapeHtml(formatDetailMoney(entryTrade.price, '-')) + '</div>' +
                '        </div>' +
                '        <div class="ff-deployed-detail-metric">' +
                '            <div class="ff-deployed-detail-label">LTP</div>' +
                '            <div class="ff-deployed-detail-value">' + escapeHtml(formatDetailMoney(leg.last_saw_price, '-')) + '</div>' +
                '        </div>' +
                buildPnlMetricHtml('P&L', leg.pnl) +
                buildFeatureMetricHtml('SL', slValue, !!slRow) +
                buildFeatureMetricHtml('TG', targetValue, !!targetRow) +
                buildFeatureMetricHtml('TSL', trailValue, !!trailRow) +
                '    </div>' +
                '    <div class="ff-deployed-feature-status-title">Feature Status</div>' +
                (triggerDescriptions.length
                    ? '<div class="ff-deployed-feature-status-list">' + triggerDescriptions.map(function (description) {
                        return '<div class="ff-deployed-feature-status-item">' + escapeHtml(description) + '</div>';
                    }).join('') + '</div>'
                    : '<div class="ff-deployed-detail-empty">No active feature status available.</div>') +
                '</div>';
        }).join('');

        return strategyFeaturesHtml + legCardsHtml;
    }

    function updateStrategyDetailPanel(recordId, record, forceRefresh) {
        if (!deployedRowsHost) {
            return;
        }
        var row = deployedRowsHost.querySelector('.ff-deployed-child-row[data-record-id="' + recordId + '"]');
        if (!row) {
            return;
        }
        var isExpanded = !!deployedStrategyDetailExpandedState[recordId];
        row.setAttribute('data-details-expanded', isExpanded ? 'true' : 'false');
        var toggleBtn = row.querySelector('[data-strategy-details-toggle]');
        if (toggleBtn) {
            toggleBtn.textContent = isExpanded ? 'Hide Details' : 'Show Details';
        }
        if (!isExpanded && !forceRefresh) {
            return;
        }
        var detailsEl = row.querySelector('.ff-deployed-strategy-details');
        if (detailsEl) {
            detailsEl.innerHTML = buildStrategyDetailHtml(record);
        }
    }

    function renderDeployedRows(records, selectedDate) {
        if (!deployedRowsHost) {
            return;
        }
        var previousExpandedState = captureDeployedGroupExpandedState();
        if (!Array.isArray(records) || !records.length) {
            deployedGroupExpandedState = {};
            deployedRowsHost.innerHTML = '<div class="ff-deployed-empty">No deployed portfolios found for ' + escapeHtml(selectedDate) + '.</div>';
            renderHeaderBrokerSummary({});
            return;
        }

        var groupedRecords = groupDeployedRecords(records);
        var currentListeningDateTime = getCurrentListeningDateTime();
        deployedRowsHost.innerHTML = groupedRecords.map(function (group) {
            var items = Array.isArray(group.items) ? group.items : [];
            var activeCount = items.filter(function (item) { return !!item.active_on_server; }).length;
            var primaryRecord = items[0] || {};
            var primaryStatus;
            if (activeCount > 0) {
                primaryStatus = { text: 'RUNNING', className: 'running' };
            } else if (items.every(function (item) {
                return item.active_on_server === false || item.status === 'StrategyStatus.SquaredOff';
            })) {
                primaryStatus = { text: 'SQD OFF', className: 'squared-off' };
            } else {
                primaryStatus = normalizeExecutionStatus(primaryRecord);
            }
            var groupTradeStarted = items.some(function (item) {
                return hasRecordTradeStarted(item, currentListeningDateTime);
            });
            var primaryOpenPositions = items.reduce(function (sum, item) {
                var count = typeof item.open_legs_count === 'number'
                    ? item.open_legs_count
                    : (Array.isArray(item && item.legs) ? item.legs.length : 0);
                return sum + count;
            }, 0);
            var openUrl = group.portfolio_id
                ? buildNamedPageUrl('portfolioActivation', '?strategy_id=' + encodeURIComponent(group.portfolio_id) + '&status=' + encodeURIComponent(listeningModeKey))
                : '#';

            var childRowsHtml = items.map(function (record) {
                var statusMeta = normalizeExecutionStatus(record);
                var legCount = typeof record.open_legs_count === 'number'
                    ? record.open_legs_count
                    : (Array.isArray(record && record.legs) ? record.legs.length : 0);
                var childIsActive = !!record.active_on_server;
                var recordId = String(record._id || '');
                var isDetailExpanded = !!deployedStrategyDetailExpandedState[recordId];
                // Hide entry countdown for cancelled (active_on_server=false) strategies
                var entryTarget = childIsActive ? escapeHtml(record.entry_time || '') : '';
                var childOpenUrl = group.portfolio_id
                    ? buildNamedPageUrl('portfolioActivation', '?strategy_id=' + encodeURIComponent(group.portfolio_id) + '&status=' + encodeURIComponent(listeningModeKey))
                    : '#';
                return '' +
                    '<div class="ff-deployed-child-row" data-record-id="' + escapeHtml(recordId) + '" data-details-expanded="' + (isDetailExpanded ? 'true' : 'false') + '">' +
                    '    <div>' +
                    '        <div class="ff-deployed-name-wrap">' +
                    '            <input type="checkbox" class="ff-deployed-checkbox" data-group-child-checkbox="' + escapeHtml(group.group_id) + '" data-strategy-id="' + escapeHtml(record._id || '') + '"' + (childIsActive ? '' : ' disabled') + '>' +
                    '            <div>' +
                    '                <div class="ff-deployed-name-wrap">' +
                    '                    <span class="ff-deployed-strategy-icon">&#10038;</span>' +
                    '                    <div class="ff-deployed-child-name">' + escapeHtml(record.name || 'Untitled Strategy') + '</div>' +
                    '                </div>' +
                    '                <div class="ff-deployed-child-sub">' + escapeHtml(record.ticker || 'NIFTY') + ' • Open Pos: ' + legCount + '</div>' +
                    '            </div>' +
                    '        </div>' +
                    '    </div>' +
                    '    ' + buildBrokerDisplayHtml(record) +
                    '    <div><span class="ff-deployed-status ' + statusMeta.className + '">' + statusMeta.text + '</span></div>' +
                    '    <div class="ff-deployed-mtm" data-record-id="' + escapeHtml(record._id || '') + '" data-entry-target="' + entryTarget + '" data-leg-count="' + legCount + '">₹ 0</div>' +
                    '    <div class="ff-deployed-actions">' +
                    '        <button type="button" class="ff-deployed-btn ff-deployed-btn-outline" data-strategy-details-toggle="' + escapeHtml(recordId) + '">' + (isDetailExpanded ? 'Hide Details' : 'Show Details') + '</button>' +
                    (childIsActive ? '        ' + buildDeploymentActionButton(legCount, record._id || '', group.group_id) : '') +
                    '        <a class="ff-deployed-btn ff-deployed-btn-solid" href="' + childOpenUrl + '">View</a>' +
                    '    </div>' +
                    '    <div class="ff-deployed-strategy-details">' + buildStrategyDetailHtml(record) + '</div>' +
                    '</div>';
            }).join('');

            return '' +
                '<div class="ff-deployed-group" data-group-id="' + escapeHtml(group.group_id) + '" data-expanded="false">' +
                '    <div class="ff-deployed-row" data-group-toggle="' + escapeHtml(group.group_id) + '">' +
                '        <div>' +
                '            <div class="ff-deployed-name-wrap">' +
                '                <input type="checkbox" class="ff-deployed-checkbox" data-group-parent-checkbox="' + escapeHtml(group.group_id) + '"' + (activeCount > 0 ? '' : ' disabled') + '>' +
                '                <div>' +
                '                    <div class="ff-deployed-group-toggle">' +
                '                        <svg class="ff-deployed-group-caret" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="m6 9 6 6 6-6"></path></svg>' +
                '                        <div class="ff-deployed-name">' + escapeHtml(group.group_name || 'Portfolio') + '</div>' +
                '                    </div>' +
                '                    <div class="ff-deployed-sub ff-deployed-group-open-pos" data-group-id="' + escapeHtml(group.group_id) + '">Open Pos: ' + primaryOpenPositions + '</div>' +
                '                </div>' +
                '            </div>' +
                '        </div>' +
                '        ' + buildBrokerDisplayHtml(primaryRecord) +
                '        <div><span class="ff-deployed-status ' + primaryStatus.className + '">' + primaryStatus.text + ' ' + activeCount + '/' + items.length + '</span></div>' +
                '        <div class="ff-deployed-mtm" data-group-mtm="' + escapeHtml(group.group_id) + '">₹ 0</div>' +
                '        <div class="ff-deployed-actions">' +
                (activeCount > 0 ? '            ' + buildDeploymentActionButton(primaryOpenPositions, '', group.group_id) : '') +
                '            <a class="ff-deployed-btn ff-deployed-btn-solid" href="' + openUrl + '">View</a>' +
                '        </div>' +
                '    </div>' +
                '    <div class="ff-deployed-children">' + childRowsHtml + '</div>' +
                '</div>';
        }).join('');
        applyDeployedGroupExpandedState(groupedRecords, previousExpandedState);
        refreshDeployedStrategyTimers();
    }

    function formatBacktestActivationDateTime() {
        var selectedDate = String(listeningDateInput.value || '').trim();
        if (!selectedDate) {
            return '';
        }
        var baseDate = parseIsoDate(selectedDate);
        if (!baseDate) {
            return '';
        }
        var timeParts = String(backtestMarketOpenTime || '09:16').split(':');
        var marketOpenHour = parseInt(timeParts[0] || '9', 10);
        var marketOpenMinute = parseInt(timeParts[1] || '16', 10);
        var displayedRemainingMinutes = 0;

        if (backtestCountdownState) {
            displayedRemainingMinutes = Math.max(0, parseInt(backtestCountdownState.remainingMinutes || 0, 10) || 0);
            if (!backtestCountdownState.paused && listeningState === 'running') {
                displayedRemainingMinutes += 1;
            }
        } else {
            displayedRemainingMinutes = Math.max(0, parseInt(listeningBehindTimeSelect ? listeningBehindTimeSelect.value || '0' : '0', 10) || 0);
        }

        var totalMinutes = (marketOpenHour * 60) + marketOpenMinute - displayedRemainingMinutes;
        var activationDate = new Date(baseDate.getFullYear(), baseDate.getMonth(), baseDate.getDate(), 0, 0, 0, 0);
        activationDate.setMinutes(totalMinutes);

        return activationDate.getFullYear() + '-'
            + String(activationDate.getMonth() + 1).padStart(2, '0') + '-'
            + String(activationDate.getDate()).padStart(2, '0') + ' '
            + String(activationDate.getHours()).padStart(2, '0') + ':'
            + String(activationDate.getMinutes()).padStart(2, '0') + ':'
            + String(activationDate.getSeconds()).padStart(2, '0');
    }

    function fetchDeployedPortfolios(selectedDate, showLoader) {
        var normalizedDate = String(selectedDate || '').trim();
        if (!normalizedDate) {
            resetLatestLtpSnapshot();
            renderDeployedRows([], 'the selected date');
            return Promise.resolve([]);
        }
        if (showLoader !== false) {
            deployedRowsHost.innerHTML = '<div class="ff-deployed-empty">Loading deployed portfolios for ' + escapeHtml(normalizedDate) + '...</div>';
        }
        if (listeningFetchInFlight) {
            return Promise.resolve([]);
        }
        listeningFetchInFlight = true;
        return fetch(buildExecutionListUrl(normalizedDate))
            .then(function (response) {
                if (!response.ok) {
                    throw new Error('Failed to load deployed portfolios');
                }
                return response.json();
            })
            .then(function (data) {
                var records = Array.isArray(data && data.records) ? data.records : [];
                currentDeployedRecords = records.slice();
                syncExecutionRecordsMap(currentDeployedRecords);
                renderDeployedRows(records, normalizedDate);
                applyLtpToMtm(latestLtpSnapshot);
                return records;
            })
            .catch(function (error) {
                deployedRowsHost.innerHTML = '<div class="ff-deployed-empty">Unable to load deployed portfolios for ' + escapeHtml(normalizedDate) + '.</div>';
                renderHeaderBrokerSummary({});
                throw error;
            })
            .finally(function () {
                listeningFetchInFlight = false;
            });
    }

    function setListeningButtonState(label, disabled) {
        if (!startListeningBtn) {
            return;
        }
        startListeningBtn.textContent = label;
        startListeningBtn.disabled = !!disabled;
    }

    function updateSocketStatus(state, detail) {
        if (!socketStatusEl || !socketStatusLabel) {
            return;
        }
        var normalizedState = String(state || 'disconnected').toLowerCase();
        if (socketManuallyPaused && normalizedState === 'disconnected') {
            normalizedState = 'paused';
        }
        var textMap = {
            connecting: 'Sockets connecting...',
            reconnecting: 'Sockets reconnecting...',
            connected: 'Sockets connected',
            disconnected: 'Sockets disconnected',
            error: 'Socket error',
            paused: 'Sockets paused'
        };
        var baseLabel = textMap[normalizedState] || 'Socket disconnected';
        var suffix = detail && detail.message ? ' - ' + detail.message : '';
        socketStatusEl.setAttribute('data-state', normalizedState);
        socketStatusLabel.textContent = baseLabel + suffix;
    }

    function requestPositionSnapshot(reason, listenTimestampOverride) {
        if (!updateSocketClient) {
            return false;
        }
        var selectedDate = String(listeningDateInput && listeningDateInput.value || '').trim();
        var normalizedReason = String(reason || 'execute_order');
        var normalizedListenTimestamp = String(listenTimestampOverride || getCurrentBacktestListenTimestamp() || '').trim();
        console.info('[POSITION REFRESH TRIGGER][FRONTEND]', {
            trade_date: selectedDate,
            activation_mode: listeningModeKey,
            reason: normalizedReason,
            listen_timestamp: normalizedListenTimestamp
        });
        var requestSent = updateSocketClient.send({
            action: 'get-position',
            trade_date: selectedDate,
            user_id: resolveCurrentUserId(),
            activation_mode: listeningModeKey,
            status: listeningModeKey,
            autoload: !!backtestAutoloadEnabled,
            reason: normalizedReason,
            listen_timestamp: normalizedListenTimestamp
        });
        updateSocketStatus('connected', {
            message: 'Position refresh ' + (requestSent ? 'sent' : 'queued') + ' (' + normalizedReason + ')'
        });
        return requestSent;
    }

    function requestInitialPositionSnapshot(reason, listenTimestampOverride) {
        if (initialPositionSnapshotRequested) {
            return false;
        }
        var queued = requestPositionSnapshot(reason || 'socket_open_initial_load', listenTimestampOverride);
        initialPositionSnapshotRequested = true;
        return queued;
    }

    function sendExecuteOrdersSubscription(reason) {
        if (!executeOrdersSocketClient) {
            return false;
        }
        var selectedDate = String(listeningDateInput && listeningDateInput.value || '').trim();
        if (!selectedDate) {
            return false;
        }
        var payload = buildExecuteOrdersSubscriptionPayload(selectedDate);
        payload.autoload = !!backtestAutoloadEnabled;
        if (reason) {
            payload.reason = reason;
        }
        return executeOrdersSocketClient.send(payload);
    }

    function sendUpdateSubscription(reason, listenTimestampOverride) {
        if (!updateSocketClient) {
            return false;
        }
        var selectedDate = String(listeningDateInput && listeningDateInput.value || '').trim();
        if (!selectedDate) {
            return false;
        }
        return updateSocketClient.send({
            trade_date: selectedDate,
            user_id: resolveCurrentUserId(),
            activation_mode: listeningModeKey,
            status: listeningModeKey,
            autoload: !!backtestAutoloadEnabled,
            listen_timestamp: String(listenTimestampOverride || getCurrentBacktestListenTimestamp() || '').trim(),
            reason: reason || 'socket_subscription'
        });
    }

    function handleExecuteOrdersSocketMessage(payload) {
        if (!payload || typeof payload !== 'object') {
            updateSocketStatus('connected', { message: 'Message received' });
            return;
        }
        if (payload.type === 'connection_established') {
            sendExecuteOrdersSubscription('connection_established');
            updateSocketStatus('connected', { message: 'Handshake ready' });
            return;
        }
        if (payload.type === 'subscription_ack') {
            updateSocketStatus('connected', { message: 'Subscribed' });
            return;
        }
        if (payload.type === 'countdown_update') {
            var countdownData = payload.data || {};
            var currentListenTime = countdownData.listen_timestamp || countdownData.listen_time || payload.server_time || '';
            console.info('[LISTEN TIME][FRONTEND]', {
                listen_time: countdownData.listen_time || '',
                listen_timestamp: countdownData.listen_timestamp || ''
            });
            updateCurrentListenTimeDisplay(currentListenTime);
            updateSocketStatus('connected', {
                message: 'Listening time ' + String(countdownData.listen_time || currentListenTime || '-')
            });
            return;
        }
        if (payload.type === 'ltp_update') {
            handleUpdateSocketMessage(payload);
            return;
        }
        if (payload.type === 'broker-settings') {
            var bsList = (payload.data && Array.isArray(payload.data.brokers)) ? payload.data.brokers : [];
            bsList.forEach(function (bs) {
                var bid = String(bs && bs.broker_id || '').trim();
                if (!bid) return;
                brokerSettingsCache[bid] = bs;
                // Patch existing broker card if already rendered
                var card = headerBrokerListHost && headerBrokerListHost.querySelector(
                    '.ff-header-broker-card[data-broker-id="' + bid + '"]'
                );
                if (card) {
                    var strip = card.querySelector('.ff-header-broker-settings-strip');
                    var newStrip = buildBrokerSettingsStrip(bs);
                    if (strip) {
                        strip.outerHTML = newStrip;
                    } else {
                        card.insertAdjacentHTML('beforeend', newStrip);
                    }
                }
            });
            return;
        }
        if (payload.type === 'execute_order') {
            var strategyRecords = Array.isArray(payload.data)
                ? payload.data
                : (payload.data && Array.isArray(payload.data.records) ? payload.data.records : []);
            var payloadGroupId = String(
                (payload.data && payload.data.group_id)
                || payload.group_id
                || ''
            ).trim();
            var isCancelDeploymentTrigger = strategyRecords.some(function (r) {
                var t = String(r && r.position_refresh_trigger || '').trim();
                return t === 'cancel-deployment' || t === 'squared-off';
            });
            var needsFullRender = isCancelDeploymentTrigger;
            strategyRecords.forEach(function (strategyRecord) {
                var recordId = String(strategyRecord && (strategyRecord._id || strategyRecord.trade_id) || '');
                if (!recordId) return;

                // Update in-memory map
                var existingRecord = executionRecordsMap[recordId];
                var mergedStrategyRecord = mergeExecutionRecordPreservingBrokerMeta(strategyRecord, existingRecord);
                executionRecordsMap[recordId] = mergedStrategyRecord;
                // Keep currentDeployedRecords in sync (replace or append)
                var replaced = false;
                currentDeployedRecords = currentDeployedRecords.map(function (r) {
                    if (String(r && (r._id || r.trade_id) || '') === recordId) {
                        replaced = true;
                        return mergeExecutionRecordPreservingBrokerMeta(strategyRecord, r);
                    }
                    return r;
                });
                if (!replaced) {
                    currentDeployedRecords.push(mergedStrategyRecord);
                    needsFullRender = true; // genuinely new record — DOM needs to add its row
                }

                var newLegs = Array.isArray(mergedStrategyRecord.legs) ? mergedStrategyRecord.legs : [];
                var newLegCount = newLegs.length;
                var openCount = typeof mergedStrategyRecord.open_legs_count === 'number'
                    ? mergedStrategyRecord.open_legs_count
                    : newLegs.filter(function (l) { return l && l.status === 1; }).length;

                // Patch existing DOM row — no re-render
                var mtmEl = document.querySelector('.ff-deployed-mtm[data-record-id="' + recordId + '"]');
                if (mtmEl) {
                    mtmEl.setAttribute('data-leg-count', String(newLegCount));
                }
                var childRow = mtmEl ? mtmEl.closest('.ff-deployed-child-row') : null;
                if (childRow) {
                    var subEl = childRow.querySelector('.ff-deployed-child-sub');
                    if (subEl) {
                        subEl.textContent = String(mergedStrategyRecord.ticker || 'NIFTY') + ' \u2022 Open Pos: ' + openCount;
                    }
                    updateStrategyDetailPanel(recordId, mergedStrategyRecord);
                    // Update action button based on current open leg count
                    var existingActionBtn = childRow.querySelector('[data-deployment-action]');
                    if (existingActionBtn && mergedStrategyRecord.active_on_server !== false) {
                        var newAction = openCount > 0 ? 'square-off' : 'cancel-deployment';
                        var currentAction = existingActionBtn.getAttribute('data-deployment-action');
                        if (currentAction !== newAction) {
                            var groupIdForBtn = existingActionBtn.getAttribute('data-group-id') || '';
                            existingActionBtn.outerHTML = buildDeploymentActionButton(openCount, recordId, groupIdForBtn);
                        }
                    }

                    // Patch status badge, action button, and entry countdown for cancel-deployment
                    var isCancelled = mergedStrategyRecord.active_on_server === false
                        || mergedStrategyRecord.status === 'StrategyStatus.SquaredOff';
                    if (isCancelled) {
                        // Status badge → SQD OFF
                        var statusEl = childRow.querySelector('.ff-deployed-status');
                        if (statusEl) {
                            statusEl.textContent = 'SQD OFF';
                            statusEl.className = 'ff-deployed-status squared-off';
                        }
                        // Remove Cancel Deployment / Square Off button, keep only View
                        var actionBtn = childRow.querySelector('[data-deployment-action]');
                        if (actionBtn) actionBtn.remove();
                        // Clear entry countdown
                        if (mtmEl) {
                            mtmEl.setAttribute('data-entry-target', '');
                            if (mtmEl.querySelector('.ff-deployed-entry-pill')) {
                                mtmEl.innerHTML = '\u20b9 0.00';
                            }
                        }
                    }
                }

                // Update group open-pos label and group status badge
                var groupId = String((mergedStrategyRecord.portfolio && mergedStrategyRecord.portfolio.group_id) || payloadGroupId || '').trim();
                if (groupId) {
                    var groupOpenPos = 0;
                    var groupTotalCount = 0;
                    var groupActiveCount = 0;
                    Object.keys(executionRecordsMap).forEach(function (id) {
                        var r = executionRecordsMap[id];
                        if (((r.portfolio && r.portfolio.group_id) || '') !== groupId) return;
                        groupTotalCount++;
                        var isActive = r.active_on_server !== false && r.status !== 'StrategyStatus.SquaredOff';
                        if (isActive) groupActiveCount++;
                        groupOpenPos += (typeof r.open_legs_count === 'number'
                            ? r.open_legs_count
                            : (Array.isArray(r.legs) ? r.legs.filter(function (l) { return l && l.status === 1; }).length : 0));
                    });
                    var groupSubEl = document.querySelector('.ff-deployed-group-open-pos[data-group-id="' + groupId + '"]');
                    if (groupSubEl) groupSubEl.textContent = 'Open Pos: ' + groupOpenPos;

                    // Update group status badge and action button
                    var groupRow = deployedRowsHost.querySelector('.ff-deployed-group[data-group-id="' + groupId + '"] .ff-deployed-row');
                    var groupStatusEl = groupRow ? groupRow.querySelector('.ff-deployed-status') : null;
                    if (groupStatusEl) {
                        var allSqdOff = groupActiveCount === 0 && groupTotalCount > 0;
                        if (allSqdOff) {
                            groupStatusEl.textContent = 'SQD OFF ' + groupTotalCount + '/' + groupTotalCount;
                            groupStatusEl.className = 'ff-deployed-status squared-off';
                            var groupActionBtn = groupRow.querySelector('[data-deployment-action]');
                            if (groupActionBtn) groupActionBtn.remove();
                        } else {
                            groupStatusEl.textContent = 'RUNNING ' + groupActiveCount + '/' + groupTotalCount;
                            groupStatusEl.className = 'ff-deployed-status running';
                            // Update group action button: Square Off if any open legs, else Cancel Deployment
                            var groupActionsEl = groupRow.querySelector('.ff-deployed-actions');
                            if (groupActionsEl) {
                                var existingGroupBtn = groupActionsEl.querySelector('[data-deployment-action]');
                                var expectedGroupAction = groupOpenPos > 0 ? 'square-off' : 'cancel-deployment';
                                if (!existingGroupBtn || existingGroupBtn.getAttribute('data-deployment-action') !== expectedGroupAction) {
                                    var newGroupBtnHtml = buildDeploymentActionButton(groupOpenPos, '', groupId);
                                    if (existingGroupBtn) {
                                        existingGroupBtn.outerHTML = newGroupBtnHtml;
                                    } else {
                                        var viewLink = groupActionsEl.querySelector('a');
                                        if (viewLink) groupActionsEl.insertAdjacentHTML('afterbegin', newGroupBtnHtml);
                                    }
                                }
                            }
                        }
                    }
                }
            });

            // Only do a full re-render when new groups/records appear in DOM for the first time
            if (needsFullRender) {
                renderDeployedRows(
                    currentDeployedRecords,
                    String(listeningDateInput && listeningDateInput.value || 'the selected date').trim() || 'the selected date'
                );
            }
            rebuildRecordPriceLinkIndex();
            aggregateCountsCache = null;
            aggregateCountsListenSecondKey = '';
            forceFullLtpRender = true;
            // Recalculate MTM — active legs use LTP, closed/squared-off legs use exit price
            scheduleLtpRender();
            updateSocketStatus('connected', {
                message: 'Updated ' + String(strategyRecords.length || 0) + ' strategy' + (strategyRecords.length === 1 ? '' : 'ies')
            });
            return;
        }
        if (payload.type === 'listening_started' || payload.type === 'listening_stopped') {
            updateSocketStatus('connected', {
                message: String(payload.message || payload.type || 'Execute orders active')
            });
            return;
        }
        updateSocketStatus('connected', { message: payload.type || 'Execute order message' });
    }

    // Cache squared-off MTM so it survives re-renders with empty legs
    var squaredOffMtmCache = {};

    function applyLtpToMtm(ltpList) {
        if (!ltpList) {
            ltpList = [];
        }
        var currentTsStr = latestListenTimestamp ? String(latestListenTimestamp).replace(' ', 'T') : '';
        var currentListenDt = (currentTsStr && !isNaN(new Date(currentTsStr).getTime())) ? new Date(currentTsStr) : null;
        Object.keys(executionRecordsMap).forEach(function (recordId) {
            var strategyPnl = calculateStrategyPnl(recordId, currentListenDt);
            strategyPnlCache[recordId] = strategyPnl;
            updateStrategyMtmCell(recordId, strategyPnl);
            updateStrategyDetailPanel(recordId, executionRecordsMap[recordId] || {});
        });
        renderAggregateMtmFromCache(currentListenDt, true);
    }

    function handleUpdateSocketMessage(payload) {
        if (!payload || typeof payload !== 'object') {
            updateSocketStatus('connected', { message: 'Update received' });
            return;
        }
        if (payload.type === 'ltp_update') {
            var ltpData = payload.data || {};
            mergeLatestLtpSnapshot(ltpData.ltp);
            rememberChangedLtpKeys(ltpData.ltp);
            var previousListenSecond = getTimestampSecondKey(latestListenTimestamp);
            var ltpListenTs = ltpData.listen_timestamp || ltpData.listen_time || '';
            if (ltpListenTs) {
                updateCurrentListenTimeDisplay(ltpListenTs);
                if (getTimestampSecondKey(ltpListenTs) !== previousListenSecond) {
                    refreshDeployedStrategyTimers();
                }
                // Keep market-open label time in sync (timer stops after market opens)
                if (marketCountdownEl && backtestCountdownState && backtestCountdownState.marketOpenReached) {
                    var hhmm = String(ltpListenTs).replace('T', ' ').slice(11, 16);
                    if (hhmm) marketCountdownEl.textContent = 'Market is open \u2022 ' + hhmm;
                }
            }
            scheduleLtpRender();
            return;
        }
        if (payload.type === 'listen_time_update') {
            var timeData = payload.data || {};
            var currentListenTime = timeData.listen_timestamp || timeData.listen_time || payload.server_time || '';
            console.info('[LISTEN TIME][UPDATE SOCKET]', {
                listen_time: timeData.listen_time || '',
                listen_timestamp: timeData.listen_timestamp || ''
            });
            updateCurrentListenTimeDisplay(currentListenTime);
            return;
        }
        if (payload.type === 'connection_established' || payload.type === 'subscription_ack') {
            if (payload.type === 'connection_established') {
                sendUpdateSubscription('connection_established');
            }
            if (payload.type === 'subscription_ack' && !initialPositionSnapshotRequested) {
                requestInitialPositionSnapshot('socket_open_initial_load');
            }
            updateSocketStatus('connected', { message: 'Update stream ready' });
            return;
        }
        if (payload.type === 'update') {
            var d = payload.data || {};
            var openPositions = Array.isArray(d.open_positions) ? d.open_positions : [];
            openPositions.forEach(function (position) {
                var tradeId = String(position && position.trade_id || '').trim();
                var legId = String(position && position.leg_id || '').trim();
                if (!tradeId || !legId || !executionRecordsMap[tradeId]) {
                    return;
                }
                var record = executionRecordsMap[tradeId];
                var legs = Array.isArray(record.legs) ? record.legs : [];
                legs.forEach(function (leg) {
                    var currentLegId = String((leg && (leg.leg_id || leg.id)) || '').trim();
                    if (!leg || currentLegId !== legId) {
                        return;
                    }
                    if (position.current_price != null) {
                        leg.last_saw_price = position.current_price;
                        leg.mark_price = position.current_price;
                        leg.ltp = position.current_price;
                    }
                    if (position.sl_price != null) {
                        leg.current_sl_price = position.sl_price;
                        leg.display_sl_value = position.sl_price;
                    }
                    if (position.tp_price != null) {
                        leg.display_target_value = position.tp_price;
                    }
                });
                updateStrategyDetailPanel(tradeId, record);
            });
            console.info('[POSITION REFRESH STATUS][FRONTEND]', {
                trade_date: d.trade_date || '',
                reason: d.trigger_reason || 'unknown',
                count: d.count || 0,
                status: d.refresh_status || 'completed'
            });
            updateSocketStatus('connected', {
                message: 'Positions refreshed (' + String(d.trigger_reason || 'unknown') + ') | Open positions ' + String(d.count || 0)
            });
            renderLivePositions({
                trade_date: d.trade_date || '',
                checked_at: payload.server_time || '',
                current_ist_time: '',
                total_running: 0,
                total_open_legs: d.count || 0,
                running_strategies: [],
                open_positions: openPositions.map(function (item) {
                    return {
                        strategy_name: item.strategy_name,
                        leg_id: item.leg_id,
                        token: item.token,
                        symbol: item.symbol,
                        position: item.position,
                        option: item.option,
                        strike: item.strike,
                        expiry_date: item.expiry_date,
                        entry_price: item.entry_price,
                        current_price: item.ltp,
                        pnl: item.pnl || 0,
                        snapshot: {
                            close: item.ltp
                        }
                    };
                }),
                actions_taken: []
            });
            return;
        }
        updateSocketStatus('connected', { message: payload.type || 'Update message' });
    }

    function renderLivePositions(data) {
        var positions = (data && data.open_positions) || [];
        var strategies = (data && data.running_strategies) || [];
        var actions = (data && data.actions_taken) || [];

        // Emit a custom event so the page can handle the data
        var event = new CustomEvent('algoLiveTick', {
            detail: {
                trade_date: (data && data.trade_date) || '',
                checked_at: (data && data.checked_at) || '',
                current_ist_time: (data && data.current_ist_time) || '',
                total_running: (data && data.total_running) || 0,
                total_open_legs: (data && data.total_open_legs) || 0,
                running_strategies: strategies,
                open_positions: positions,
                actions_taken: actions
            }
        });
        document.dispatchEvent(event);

        // Also render into a dedicated container if present
        var liveContainer = document.getElementById('ff-live-positions');
        if (!liveContainer) {
            return;
        }
        if (!positions.length) {
            liveContainer.innerHTML = '<p class="text-sm text-gray-400 px-4 py-2">No open positions</p>';
            return;
        }
        var html = '<div class="overflow-x-auto">'
            + '<table class="w-full text-xs border-collapse">'
            + '<thead><tr class="bg-gray-100 dark:bg-gray-800 text-left">'
            + '<th class="px-2 py-1">Strategy</th>'
            + '<th class="px-2 py-1">Leg</th>'
            + '<th class="px-2 py-1">Symbol</th>'
            + '<th class="px-2 py-1">Pos</th>'
            + '<th class="px-2 py-1">Strike</th>'
            + '<th class="px-2 py-1">Expiry</th>'
            + '<th class="px-2 py-1">Entry</th>'
            + '<th class="px-2 py-1">LTP</th>'
            + '<th class="px-2 py-1">SL</th>'
            + '<th class="px-2 py-1">PnL</th>'
            + '<th class="px-2 py-1">IV</th>'
            + '<th class="px-2 py-1">Delta</th>'
            + '<th class="px-2 py-1">Spot</th>'
            + '</tr></thead><tbody>';
        positions.forEach(function (p) {
            var snap = p.snapshot || {};
            var pnlClass = (p.pnl || 0) >= 0 ? 'text-green-600' : 'text-red-500';
            var posLabel = String(p.position || '').replace('PositionType.', '');
            html += '<tr class="border-b border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800">'
                + '<td class="px-2 py-1">' + escapeHtml(String(p.strategy_name || '-')) + '</td>'
                + '<td class="px-2 py-1">' + escapeHtml(String(p.leg_id || '-')) + '</td>'
                + '<td class="px-2 py-1">' + escapeHtml(String(p.token || p.symbol || '-')) + '</td>'
                + '<td class="px-2 py-1">' + escapeHtml(posLabel) + ' ' + escapeHtml(String(p.option || '')) + '</td>'
                + '<td class="px-2 py-1 text-right">' + escapeHtml(String(p.strike || '-')) + '</td>'
                + '<td class="px-2 py-1">' + escapeHtml(String(p.expiry_date || '-')) + '</td>'
                + '<td class="px-2 py-1 text-right">' + escapeHtml(String(p.entry_price || '-')) + '</td>'
                + '<td class="px-2 py-1 text-right font-medium">' + escapeHtml(String(p.current_price || snap.close || '-')) + '</td>'
                + '<td class="px-2 py-1 text-right text-orange-500">' + escapeHtml(String(p.sl_price || '-')) + '</td>'
                + '<td class="px-2 py-1 text-right font-bold ' + pnlClass + '">' + escapeHtml(String(p.pnl || 0)) + '</td>'
                + '<td class="px-2 py-1 text-right">' + escapeHtml(String(snap.iv != null ? (+snap.iv).toFixed(4) : '-')) + '</td>'
                + '<td class="px-2 py-1 text-right">' + escapeHtml(String(snap.delta != null ? (+snap.delta).toFixed(4) : '-')) + '</td>'
                + '<td class="px-2 py-1 text-right">' + escapeHtml(String(snap.spot_price || '-')) + '</td>'
                + '</tr>';
        });
        html += '</tbody></table></div>';
        if (actions.length) {
            html += '<div class="text-xs text-gray-400 px-2 py-1">Actions: ' + escapeHtml(actions.join(' | ')) + '</div>';
        }
        liveContainer.innerHTML = html;
    }

    function initializeStreamSockets() {
        if (!window.AlgoStreamSockets) {
            return null;
        }
        var currentUserId = resolveCurrentUserId();
        if (!executeOrdersSocketClient) {
            executeOrdersSocketClient = window.AlgoStreamSockets.createChannel({
                channel: 'execute-orders',
                userId: currentUserId,
                activationMode: listeningModeKey,
                onStatusChange: function (state, meta) {
                    updateSocketStatus(state, meta || {});
                },
                onMessage: function (payload) {
                    handleExecuteOrdersSocketMessage(payload);
                }
            });
        }
        if (!updateSocketClient) {
            updateSocketClient = window.AlgoStreamSockets.createChannel({
                channel: 'update',
                userId: currentUserId,
                activationMode: listeningModeKey,
                onStatusChange: function (state, meta) {
                    updateSocketStatus(state, meta || {});
                },
                onMessage: function (payload) {
                    handleUpdateSocketMessage(payload);
                }
            });
        }
        return {
            executeOrders: executeOrdersSocketClient,
            update: updateSocketClient
        };
    }

    function ensureStreamSocketConnections() {
        var clients = initializeStreamSockets();
        if (!clients) {
            return null;
        }
        socketManuallyPaused = false;
        clients.executeOrders.connect();
        clients.update.connect();
        return clients;
    }

    function activateAutoloadSockets(listenTimestamp, reason) {
        var selectedDate = String(listeningDateInput && listeningDateInput.value || '').trim();
        var normalizedTimestamp = String(listenTimestamp || '').trim() || getCurrentBacktestListenTimestamp();
        var clients = ensureStreamSocketConnections();
        if (!clients || !selectedDate) {
            return null;
        }
        initialPositionSnapshotRequested = false;
        if (executeOrdersSocketClient) {
            var executionPayload = buildExecuteOrdersSubscriptionPayload(selectedDate);
            executionPayload.autoload = true;
            executeOrdersSocketClient.send(executionPayload);
        }
        if (updateSocketClient) {
            updateSocketClient.send({
                trade_date: selectedDate,
                user_id: resolveCurrentUserId(),
                activation_mode: listeningModeKey,
                status: listeningModeKey,
                autoload: true,
                listen_timestamp: normalizedTimestamp,
                reason: reason || 'autoload_start'
            });
            requestInitialPositionSnapshot(reason || 'autoload_start', normalizedTimestamp);
        }
        return clients;
    }

    function activateManualSockets(listenTimestamp, reason) {
        var selectedDate = String(listeningDateInput && listeningDateInput.value || '').trim();
        var normalizedTimestamp = String(listenTimestamp || '').trim() || getCurrentBacktestListenTimestamp();
        var clients = ensureStreamSocketConnections();
        if (!clients || !selectedDate) {
            return null;
        }
        initialPositionSnapshotRequested = false;
        if (executeOrdersSocketClient) {
            var executionPayload = buildExecuteOrdersSubscriptionPayload(selectedDate);
            executionPayload.autoload = false;
            executeOrdersSocketClient.send(executionPayload);
        }
        if (updateSocketClient) {
            updateSocketClient.send({
                trade_date: selectedDate,
                user_id: resolveCurrentUserId(),
                activation_mode: listeningModeKey,
                status: listeningModeKey,
                autoload: false,
                listen_timestamp: normalizedTimestamp,
                reason: reason || 'manual_socket_start'
            });
            requestInitialPositionSnapshot(reason || 'manual_socket_start', normalizedTimestamp);
        }
        updateCurrentListenTimeDisplay(normalizedTimestamp);
        return clients;
    }


    function loadDeployedPortfoliosForDate() {
        var selectedDate = String(listeningDateInput.value || '').trim();
        var behindTime = listeningBehindTimeSelect ? String(listeningBehindTimeSelect.value || '5') : '5';
        var savedListeningState = getListeningManager() ? getListeningManager().load(listeningModeKey) : null;
        if (!selectedDate) {
            renderDeployedRows([], 'the selected date');
            return;
        }

        if (listeningState === 'paused' && backtestCountdownState) {
            var resumeListenTimestamp = String(
                savedListeningState && savedListeningState.current_listen_timestamp
                    ? savedListeningState.current_listen_timestamp
                    : getCurrentBacktestListenTimestamp()
            ).trim();
            resumeBacktestListening();
            setListeningButtonState('Listening...', true);
            if (backtestAutoloadEnabled) {
                activateAutoloadSockets(resumeListenTimestamp, 'resume_listening_initial_load');
            }
            fetchDeployedPortfolios(selectedDate, false)
                .finally(function () {
                    startListeningBtn.textContent = 'Start Listening';
                    renderListeningControls();
                });
            return;
        }

        startBacktestCountdown(behindTime);
        setListeningButtonState('Listening...', true);
        var startListenTimestamp = getInitialBacktestListenTimestamp(behindTime);
        if (backtestAutoloadEnabled) {
            activateAutoloadSockets(startListenTimestamp, 'start_listening_initial_load');
        } else {
            activateManualSockets(startListenTimestamp, 'start_listening_initial_load');
        }
        // API called once only — updates come via execute_order socket events after that
        fetchDeployedPortfolios(selectedDate, behindTime, true)
            .finally(function () {
                startListeningBtn.textContent = 'Start Listening';
                renderListeningControls();
            });
    }

    function openSetupModal(strategyName) {
        setupStrategyName.textContent = strategyName || 'Strategy';
        weekdayPanel.hidden = false;
        dtePanel.hidden = true;
        setupModal.querySelectorAll('[data-execution-mode]').forEach(function (button) {
            button.classList.toggle('active', button.getAttribute('data-execution-mode') === 'weekdays');
        });
        setupModal.hidden = false;
        document.body.style.overflow = 'hidden';
        document.documentElement.style.overflow = 'hidden';
    }

    function closeSetupModal() {
        setupModal.hidden = true;
        document.body.style.overflow = '';
        document.documentElement.style.overflow = '';
    }

    function openBrokerSettingsModal() {
        if (!brokerSettingsModal) {
            return;
        }
        brokerSettingsModal.hidden = false;
        document.body.style.overflow = 'hidden';
        document.documentElement.style.overflow = 'hidden';
    }

    function closeBrokerSettingsModal() {
        if (!brokerSettingsModal) {
            return;
        }
        brokerSettingsModal.hidden = true;
        document.body.style.overflow = '';
        document.documentElement.style.overflow = '';
    }

    function formatBrokerCurrency(value) {
        var numericValue = parseInt(String(value || '').replace(/[^\d]/g, ''), 10);
        if (isNaN(numericValue)) {
            numericValue = 0;
        }
        return '\u20b9 ' + numericValue.toLocaleString('en-IN');
    }

    function syncBrokerSettingsDisplay() {
        if (brokerStopLossDisplay && brokerStopLossInput) {
            brokerStopLossDisplay.textContent = formatBrokerCurrency(brokerStopLossInput.value);
        }
        if (brokerTargetProfitDisplay && brokerTargetProfitInput) {
            brokerTargetProfitDisplay.textContent = formatBrokerCurrency(brokerTargetProfitInput.value);
        }
    }

    function adjustBrokerFieldValue(fieldName, direction) {
        var input = null;
        if (fieldName === 'stop-loss') {
            input = brokerStopLossInput;
        } else if (fieldName === 'target-profit') {
            input = brokerTargetProfitInput;
        }

        if (!input) {
            return;
        }

        var currentValue = parseInt(String(input.value || '').replace(/[^\d]/g, ''), 10);
        if (isNaN(currentValue)) {
            currentValue = 0;
        }
        var nextValue = Math.max(0, currentValue + (direction * 100));
        input.value = String(nextValue);
    }

    var pendingCancelDeployment = { strategy_id: '', group_id: '' };
    var pendingSquareOff = { strategy_id: '', group_id: '' };

    function openSquareOffModal(strategyId, groupId) {
        pendingSquareOff = { strategy_id: strategyId || '', group_id: groupId || '' };
        squareOffModal.hidden = false;
        document.body.style.overflow = 'hidden';
        document.documentElement.style.overflow = 'hidden';
    }

    function closeSquareOffModal() {
        pendingSquareOff = { strategy_id: '', group_id: '' };
        squareOffModal.hidden = true;
        document.body.style.overflow = '';
        document.documentElement.style.overflow = '';
    }

    function openCancelDeploymentModal(strategyId, groupId) {
        pendingCancelDeployment = { strategy_id: strategyId || '', group_id: groupId || '' };
        cancelDeploymentModal.hidden = false;
        document.body.style.overflow = 'hidden';
        document.documentElement.style.overflow = 'hidden';
    }

    function closeCancelDeploymentModal() {
        pendingCancelDeployment = { strategy_id: '', group_id: '' };
        cancelDeploymentModal.hidden = true;
        document.body.style.overflow = '';
        document.documentElement.style.overflow = '';
    }

    paginationHost.addEventListener('click', function (event) {
        var button = event.target.closest('[data-page]');
        if (!button || button.disabled) {
            return;
        }
        currentPage = Number(button.getAttribute('data-page')) || 1;
        renderRows();
    });

    rowsHost.addEventListener('click', function (event) {
        var editIcon = event.target.closest('[data-edit-icon="portfolio"]');
        if (editIcon) {
            var portfolioId = editIcon.getAttribute('data-item-id');
            if (portfolioId) {
                window.open(buildNamedPageUrl('portfolio', '?strategy_id=' + encodeURIComponent(portfolioId)), '_blank');
            }
            return;
        }
        var activateButton = event.target.closest('[data-activate-item="portfolio"]');
        if (activateButton) {
            var activationPortfolioId = activateButton.getAttribute('data-item-id');
            if (activationPortfolioId) {
                var query = '?strategy_id=' + encodeURIComponent(activationPortfolioId) + '&status=' + encodeURIComponent(listeningModeKey);
                var currentDateTime = formatBacktestActivationDateTime();
                if (currentDateTime) {
                    query += '&current_datetime=' + encodeURIComponent(currentDateTime);
                }
                window.open(buildNamedPageUrl('portfolioActivation', query), '_blank');
            }
            return;
        }
        var setupButton = event.target.closest('[data-edit-setup]');
        if (!setupButton) {
            return;
        }
        event.preventDefault();
        openSetupModal(setupButton.getAttribute('data-strategy-name'));
    });

    if (brokerSettingsEditBtn) {
        brokerSettingsEditBtn.addEventListener('click', function (event) {
            event.preventDefault();
            openBrokerSettingsModal();
        });
    }

    setupModal.addEventListener('click', function (event) {
        var modeButton = event.target.closest('[data-execution-mode]');
        if (modeButton) {
            var mode = modeButton.getAttribute('data-execution-mode');
            weekdayPanel.hidden = mode !== 'weekdays';
            dtePanel.hidden = mode !== 'dte';
            setupModal.querySelectorAll('[data-execution-mode]').forEach(function (button) {
                button.classList.toggle('active', button === modeButton);
            });
            return;
        }
        if (event.target.closest('[data-close-setup]')) {
            closeSetupModal();
        }
    });

    if (brokerSettingsModal) {
        brokerSettingsModal.addEventListener('click', function (event) {
            var adjustButton = event.target.closest('[data-adjust-broker-value]');
            if (adjustButton) {
                event.preventDefault();
                adjustBrokerFieldValue(
                    adjustButton.getAttribute('data-adjust-broker-value'),
                    Number(adjustButton.getAttribute('data-adjust-direction')) || 0
                );
                return;
            }

            if (event.target.closest('[data-save-broker-settings]')) {
                syncBrokerSettingsDisplay();
                closeBrokerSettingsModal();
                return;
            }

            if (event.target.closest('[data-close-broker-settings]')) {
                closeBrokerSettingsModal();
            }
        });
    }

    document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape' && !setupModal.hidden) {
            closeSetupModal();
            return;
        }
        if (brokerSettingsModal && event.key === 'Escape' && !brokerSettingsModal.hidden) {
            closeBrokerSettingsModal();
            return;
        }
        if (event.key === 'Escape' && !cancelDeploymentModal.hidden) {
            closeCancelDeploymentModal();
        }
        if (event.key === 'Escape' && !squareOffModal.hidden) {
            closeSquareOffModal();
        }
    });

    dteSelectAll.addEventListener('change', function () {
        dteOptions.forEach(function (option) {
            option.checked = dteSelectAll.checked;
        });
    });

    dteOptions.forEach(function (option) {
        option.addEventListener('change', function () {
            dteSelectAll.checked = dteOptions.every(function (item) {
                return item.checked;
            });
        });
    });

    listeningDateTrigger.addEventListener('click', function (event) {
        event.preventDefault();
        event.stopPropagation();
        if (calendarPopup.hidden) {
            openCalendarPopup();
        } else {
            closeCalendarPopup();
        }
    });

    calendarPopup.addEventListener('click', function (event) {
        event.stopPropagation();
    });

    calendarMonthToggle.addEventListener('click', function () {
        calendarState.view = 'month';
        renderCalendar();
    });

    calendarYearToggle.addEventListener('click', function () {
        calendarState.view = 'year';
        calendarState.yearPageStart = (calendarState.displayDate || new Date()).getFullYear() - 7;
        renderCalendar();
    });

    calendarPrevBtn.addEventListener('click', function () {
        if (calendarState.view === 'year') {
            calendarState.yearPageStart -= 8;
        } else if (calendarState.view === 'month') {
            calendarState.displayDate.setFullYear(calendarState.displayDate.getFullYear() - 1);
        } else {
            calendarState.displayDate.setMonth(calendarState.displayDate.getMonth() - 1);
        }
        renderCalendar();
    });

    calendarNextBtn.addEventListener('click', function () {
        if (calendarState.view === 'year') {
            calendarState.yearPageStart += 8;
        } else if (calendarState.view === 'month') {
            calendarState.displayDate.setFullYear(calendarState.displayDate.getFullYear() + 1);
        } else {
            calendarState.displayDate.setMonth(calendarState.displayDate.getMonth() + 1);
        }
        renderCalendar();
    });

    calendarGrid.addEventListener('click', function (event) {
        var dayButton = event.target.closest('[data-calendar-day]');
        if (dayButton) {
            var selectedDay = parseInt(dayButton.getAttribute('data-calendar-day') || '1', 10);
            calendarState.draftDate = new Date(
                calendarState.displayDate.getFullYear(),
                calendarState.displayDate.getMonth(),
                selectedDay
            );
            renderCalendar();
            return;
        }

        var monthButton = event.target.closest('[data-calendar-month]');
        if (monthButton) {
            var selectedMonth = parseInt(monthButton.getAttribute('data-calendar-month') || '0', 10);
            calendarState.displayDate.setMonth(selectedMonth);
            calendarState.view = 'date';
            if (calendarState.draftDate) {
                calendarState.draftDate.setFullYear(calendarState.displayDate.getFullYear(), selectedMonth, Math.min(calendarState.draftDate.getDate(), new Date(calendarState.displayDate.getFullYear(), selectedMonth + 1, 0).getDate()));
            }
            renderCalendar();
            return;
        }

        var yearButton = event.target.closest('[data-calendar-year]');
        if (yearButton) {
            var selectedYear = parseInt(yearButton.getAttribute('data-calendar-year') || String(new Date().getFullYear()), 10);
            calendarState.displayDate.setFullYear(selectedYear);
            if (calendarState.draftDate) {
                calendarState.draftDate.setFullYear(selectedYear);
            }
            calendarState.view = 'month';
            renderCalendar();
        }
    });

    calendarConfirmBtn.addEventListener('click', function () {
        commitCalendarDateSelection();
    });

    if (secondsSlider) {
        secondsSlider.addEventListener('input', function (event) {
            timelineScrubValue = String(event.target.value || '0');
            syncListeningTimeline(buildListenTimestampFromSliderValue(timelineScrubValue), true);
        });
        secondsSlider.addEventListener('change', function (event) {
            var nextTimestamp = buildListenTimestampFromSliderValue(event.target.value);
            if (nextTimestamp) {
                sendTimelineSeek(nextTimestamp, 'timeline_slider');
            }
        });
    }

    if (secondsTimeInput) {
        secondsTimeInput.addEventListener('change', function () {
            var selectedDate = String(listeningDateInput && listeningDateInput.value || '').trim();
            var selectedTime = String(secondsTimeInput.value || '').trim();
            if (!selectedDate || !selectedTime) {
                return;
            }
            sendTimelineSeek(selectedDate + 'T' + selectedTime, 'timeline_time_input');
        });
    }

    if (secondsAutoplayBtn) {
        secondsAutoplayBtn.addEventListener('click', function () {
            if (timelineAutoplayTimer) {
                stopTimelineAutoplay();
                return;
            }
            var autoplaySeconds = Math.max(1, parseInt(secondsSpeedSelect && secondsSpeedSelect.value || '60', 10) || 60);
            secondsAutoplayBtn.classList.add('is-active');
            timelineAutoplayTimer = window.setInterval(function () {
                applyTimelineDelta(Math.max(1, Math.round(autoplaySeconds / 60)));
            }, 1000);
        });
    }

    seekButtons.forEach(function (button) {
        button.addEventListener('click', function () {
            var seekTarget = button.getAttribute('data-seek-target');
            if (seekTarget) {
                applyTimelineTarget(seekTarget);
                return;
            }
            applyTimelineDelta(button.getAttribute('data-seek-minutes'));
        });
    });

    if (importExportBtn) {
        importExportBtn.addEventListener('click', function () {
            updateSocketStatus('connected', { message: 'Import/Export options coming next' });
        });
    }

    if (manualRunBtn) {
        manualRunBtn.addEventListener('click', function () {
            runManualSimulatorStep('manual_button');
        });
    }

    if (autoloadToggleBtn) {
        autoloadToggleBtn.addEventListener('click', function () {
            backtestAutoloadEnabled = !backtestAutoloadEnabled;
            updateAutoloadUi();
            if (backtestAutoloadEnabled) {
                if (listeningState === 'running') {
                    activateAutoloadSockets(getCurrentBacktestListenTimestamp(), 'autoload_toggle_enable');
                }
            } else {
                closeExecutionSocket(true);
            }
            persistListeningSession();
        });
    }

    document.addEventListener('click', function (event) {
        if (calendarPopup.hidden) {
            return;
        }
        if (event.target.closest('.ff-calendar-wrap')) {
            return;
        }
        closeCalendarPopup();
    });

    startListeningBtn.addEventListener('click', function () {
        loadDeployedPortfoliosForDate();
    });

    deployedRowsHost.addEventListener('click', function (event) {
        var detailsToggleButton = event.target.closest('[data-strategy-details-toggle]');
        if (detailsToggleButton) {
            event.preventDefault();
            event.stopPropagation();
            var detailRecordId = String(detailsToggleButton.getAttribute('data-strategy-details-toggle') || '').trim();
            if (!detailRecordId) {
                return;
            }
            deployedStrategyDetailExpandedState[detailRecordId] = !deployedStrategyDetailExpandedState[detailRecordId];
            updateStrategyDetailPanel(detailRecordId, executionRecordsMap[detailRecordId] || {}, true);
            return;
        }
        var deploymentActionButton = event.target.closest('[data-deployment-action]');
        if (deploymentActionButton) {
            var deploymentAction = deploymentActionButton.getAttribute('data-deployment-action');
            if (deploymentAction === 'cancel-deployment') {
                event.preventDefault();
                event.stopPropagation();
                var cancelStrategyId = deploymentActionButton.getAttribute('data-strategy-id') || '';
                var cancelGroupId = deploymentActionButton.getAttribute('data-group-id') || '';
                openCancelDeploymentModal(cancelStrategyId, cancelGroupId);
                return;
            }
            if (deploymentAction === 'square-off') {
                event.preventDefault();
                event.stopPropagation();
                var sqStrategyId = deploymentActionButton.getAttribute('data-strategy-id') || '';
                var sqGroupId = deploymentActionButton.getAttribute('data-group-id') || '';
                openSquareOffModal(sqStrategyId, sqGroupId);
                return;
            }
        }
        if (event.target.closest('.ff-deployed-btn')) {
            return;
        }
        if (event.target.closest('.ff-deployed-checkbox')) {
            return;
        }
        var toggleRow = event.target.closest('[data-group-toggle]');
        if (!toggleRow) {
            return;
        }
        var groupId = toggleRow.getAttribute('data-group-toggle');
        var groupNode = groupId ? deployedRowsHost.querySelector('[data-group-id="' + groupId + '"]') : null;
        if (!groupNode) {
            return;
        }
        var isExpanded = groupNode.getAttribute('data-expanded') === 'true';
        groupNode.setAttribute('data-expanded', isExpanded ? 'false' : 'true');
        deployedGroupExpandedState[groupId] = !isExpanded;
    });

    cancelDeploymentModal.addEventListener('click', function (event) {
        if (event.target.closest('[data-close-cancel-deployment]')) {
            closeCancelDeploymentModal();
        }
    });

    confirmCancelDeploymentBtn.addEventListener('click', function () {
        var payload = {
            type: 'cancel-deployment',
            strategy_id: pendingCancelDeployment.strategy_id,
            group_id: pendingCancelDeployment.group_id
        };
        var currentUserId = resolveCurrentUserId();
        if (currentUserId) {
            payload.user_id = currentUserId;
        }
        if (executeOrdersSocketClient) {
            executeOrdersSocketClient.send(payload);
        }
        closeCancelDeploymentModal();
    });

    squareOffModal.addEventListener('click', function (event) {
        if (event.target.closest('[data-close-square-off]')) {
            closeSquareOffModal();
        }
    });

    confirmSquareOffBtn.addEventListener('click', function () {
        var payload = {
            type: 'squared-off',
            strategy_id: pendingSquareOff.strategy_id,
            group_id: pendingSquareOff.group_id,
            listen_timestamp: latestListenTimestamp || ''
        };
        var currentUserId = resolveCurrentUserId();
        if (currentUserId) {
            payload.user_id = currentUserId;
        }
        if (executeOrdersSocketClient) {
            executeOrdersSocketClient.send(payload);
        }
        closeSquareOffModal();
    });

    deployedRowsHost.addEventListener('change', function (event) {
        var parentCheckbox = event.target.closest('[data-group-parent-checkbox]');
        if (parentCheckbox) {
            var parentGroupId = parentCheckbox.getAttribute('data-group-parent-checkbox');
            deployedRowsHost.querySelectorAll('[data-group-child-checkbox="' + parentGroupId + '"]').forEach(function (checkbox) {
                if (!checkbox.disabled) {
                    checkbox.checked = !!parentCheckbox.checked;
                }
            });
            return;
        }

        var childCheckbox = event.target.closest('[data-group-child-checkbox]');
        if (!childCheckbox) {
            return;
        }
        var childGroupId = childCheckbox.getAttribute('data-group-child-checkbox');
        var childCheckboxes = Array.prototype.slice.call(
            deployedRowsHost.querySelectorAll('[data-group-child-checkbox="' + childGroupId + '"]')
        ).filter(function (checkbox) {
            return !checkbox.disabled;
        });
        var checkedCount = childCheckboxes.filter(function (checkbox) {
            return checkbox.checked;
        }).length;
        var groupParentCheckbox = deployedRowsHost.querySelector('[data-group-parent-checkbox="' + childGroupId + '"]');
        if (groupParentCheckbox) {
            groupParentCheckbox.checked = childCheckboxes.length > 0 && checkedCount === childCheckboxes.length;
        }
    });

    pauseListeningBtn.addEventListener('click', function () {
        pauseBacktestListening();
    });

    stopListeningBtn.addEventListener('click', function () {
        stopBacktestListening();
    });

    function fetchActiveTabData() {
        var url = activeTab === 'portfolios'
            ? buildNamedApiUrl('portfolioList')
            : buildNamedApiUrl('strategyList');
        fetch(url)
            .then(function (response) {
                if (!response.ok) {
                    throw new Error('Failed to load items');
                }
                return response.json();
            })
            .then(function (data) {
                allItems = activeTab === 'portfolios'
                    ? (Array.isArray(data && data.portfolios) ? data.portfolios : [])
                    : (Array.isArray(data && data.strategies) ? data.strategies : []);
                currentPage = 1;
                renderRows();
            })
            .catch(function () {
                renderError('Unable to load saved ' + (activeTab === 'portfolios' ? 'portfolios' : 'strategies'));
            });
    }

    tabButtons.forEach(function (button) {
        button.addEventListener('click', function () {
            var nextTab = button.getAttribute('data-ff-tab');
            if (!nextTab || nextTab === activeTab) {
                return;
            }
            activeTab = nextTab;
            setActiveTabUi();
            fetchActiveTabData();
        });
    });

    setActiveTabUi();
    fetchActiveTabData();

    // ── Live monitor public API ────────────────────────────────────────────────
    window.startAlgoLiveMonitor = function (tradeDate) {
        ensureStreamSocketConnections();
        if (!updateSocketClient) {
            return;
        }
        var date = tradeDate || (function () {
            var now = new Date();
            var y = now.getFullYear();
            var m = String(now.getMonth() + 1).padStart(2, '0');
            var d = String(now.getDate()).padStart(2, '0');
            return y + '-' + m + '-' + d;
        }());
        updateSocketClient.send({ trade_date: date, activation_mode: listeningModeKey });
    };

    window.stopAlgoLiveMonitor = function () {
        if (updateSocketClient) {
            updateSocketClient.close();
            updateSocketClient = null;
        }
        if (executeOrdersSocketClient) {
            executeOrdersSocketClient.close();
            executeOrdersSocketClient = null;
        }
    };
})();
