(function () {
    var _livePayload = null;
    var _liveTradeId = '';
    var _liveLtpMap = {};
    var _liveRefreshTimer = null;
    var _tradeHistoryRequestToken = 0;
    var _tradeHistoryRefreshTimer = null;
    var _tradeHistorySocketKey = '';

    function normalizeBaseUrl(value) {
        return String(value || '').replace(/\/+$/, '');
    }

    function ensureAlgoApiBase() {
        var baseUrl = normalizeBaseUrl(
            (window.APP_CONFIG && window.APP_CONFIG.algoApiBaseUrl)
            || window.APP_ALGO_API_BASE_URL
            || window.APP_LOCAL_ALGO_API_BASE_URL
            || 'http://localhost:8000/algo'
        );

        window.APP_LOCAL_ALGO_API_BASE_URL = window.APP_LOCAL_ALGO_API_BASE_URL || baseUrl;
        window.APP_ALGO_API_BASE_URL = baseUrl;
        window.getBackendUrl = window.getBackendUrl || function () {
            return baseUrl;
        };
        window.buildAlgoApiUrl = window.buildAlgoApiUrl || function (path) {
            return baseUrl + '/' + String(path || '').replace(/^\/+/, '');
        };
        window.APP_CONFIG = window.APP_CONFIG || {};
        if (!window.APP_CONFIG.algoApiBaseUrl) {
            window.APP_CONFIG.algoApiBaseUrl = baseUrl;
        }
        if (typeof window.APP_CONFIG.buildAlgoApiUrl !== 'function') {
            window.APP_CONFIG.buildAlgoApiUrl = window.buildAlgoApiUrl;
        }
    }

    function getAnalysePathParams() {
        var pathname = String((window.location && window.location.pathname) || '').replace(/\\/g, '/');
        var segments = pathname.split('/').filter(Boolean);
        var analyseIndex = -1;
        var index;

        for (index = 0; index < segments.length; index += 1) {
            if (String(segments[index] || '').trim().toLowerCase() === 'analyse') {
                analyseIndex = index;
            }
        }

        if (analyseIndex === -1 || segments.length < analyseIndex + 3) {
            return {
                entityType: '',
                entityId: ''
            };
        }

        return {
            entityType: String(segments[analyseIndex + 1] || '').trim().toLowerCase(),
            entityId: decodeURIComponent(String(segments[analyseIndex + 2] || '').trim())
        };
    }

    function getTradeQuery() {
        var params = new URLSearchParams(window.location.search || '');
        var pathParams = getAnalysePathParams();
        var pathEntityId = pathParams.entityId;
        var pathEntityType = pathParams.entityType;
        return {
            strategyId: String(params.get('strategy_id') || (pathEntityType === 'strategy' ? pathEntityId : '') || '').trim(),
            groupId: String(params.get('group_id') || (pathEntityType === 'group' ? pathEntityId : '') || '').trim(),
            portfolioId: String(params.get('portfolio') || (pathEntityType === 'portfolio' ? pathEntityId : '') || '').trim(),
            status: String(params.get('status') || 'algo-backtest').trim() || 'algo-backtest'
        };
    }

    function normalizeOptionType(value) {
        var normalized = String(value || '').trim().toUpperCase();
        if (normalized === 'CE' || normalized === 'CALL') {
            return 'Call';
        }
        if (normalized === 'PE' || normalized === 'PUT') {
            return 'Put';
        }
        return 'Call';
    }

    function normalizePositionType(value) {
        var normalized = String(value || '').trim().toLowerCase();
        return normalized.indexOf('buy') !== -1 ? 'Buy' : 'Sell';
    }

    function normalizeDateTime(value, fallback) {
        var raw = String(value || '').trim();
        if (!raw) {
            return fallback;
        }
        var isoLike = raw.replace(' ', 'T');
        var dt = new Date(isoLike);
        if (isNaN(dt.getTime())) {
            return fallback;
        }
        return dt.toISOString();
    }

    function normalizeTradeObject(value) {
        return value && typeof value === 'object' ? value : {};
    }

    function scheduleSimulatorRefresh() {
        if (_liveRefreshTimer) {
            return;
        }
        _liveRefreshTimer = window.setTimeout(function () {
            _liveRefreshTimer = null;
            if (_livePayload) {
                applyPayloadToSimulator(_livePayload);
            }
        }, 80);
    }

    function buildSimulatorLegs(payload) {
        var legs = (payload && payload.legs) || {};
        var sourceLegs = [];
        var seenLegIds = {};

        function appendLegs(list) {
            if (!Array.isArray(list) || !list.length) {
                return;
            }
            list.forEach(function (leg) {
                var legId = String(leg && (leg.id || leg.leg_id || leg.token) || '').trim();
                var dedupeKey = legId || JSON.stringify([
                    leg && leg.strike,
                    leg && (leg.option || leg.option_type),
                    leg && (leg.expiry_date || leg.expiry),
                    leg && (leg.entry_timestamp || ((leg.entry_trade || {}).traded_timestamp) || ((leg.entry_trade || {}).trigger_timestamp)),
                    !!(leg && leg.exit_trade)
                ]);
                if (seenLegIds[dedupeKey]) {
                    return;
                }
                seenLegIds[dedupeKey] = true;
                sourceLegs.push(leg);
            });
        }

        appendLegs(legs.open);
        appendLegs(legs.pending_feature_legs);
        appendLegs(legs.closed);

        if (!sourceLegs.length) {
            appendLegs(legs.all);
        }

        return sourceLegs.map(function (leg) {
            var entryTrade = normalizeTradeObject(leg && leg.entry_trade);
            var quantity = Number(leg.effective_quantity || leg.quantity || 0) || 0;
            var exitTrade = normalizeTradeObject(leg && leg.exit_trade);
            var entryPrice = Number(
                entryTrade.price != null ? entryTrade.price
                    : (entryTrade.trigger_price != null ? entryTrade.trigger_price : (leg.entry_price || 0))
            ) || 0;
            var exitPrice = Number(
                exitTrade.price != null ? exitTrade.price
                    : (exitTrade.trigger_price != null ? exitTrade.trigger_price : (leg.exit_price || 0))
            ) || 0;
            var isExited = !!(exitTrade && Object.keys(exitTrade).length && exitPrice > 0);
            var isQueued = !!(leg && (leg.is_pending_feature_leg || leg.status === 0));
            var lotValue = Number(leg.lot_config_value || leg.lots || 0) || 0;

            return {
                type: normalizePositionType(leg.position_side || leg.position),
                optionType: normalizeOptionType(leg.option || leg.option_type),
                strike: Number(leg.strike || 0) || 0,
                premium: isQueued ? 0 : entryPrice,
                quantity: quantity,
                expiry: String(leg.expiry_date || leg.expiry || '').trim().slice(0, 10),
                entryDate: normalizeDateTime(
                    leg.queued_at || entryTrade.traded_timestamp || entryTrade.trigger_timestamp || leg.entry_timestamp || leg.entry_time || leg.created_at,
                    new Date().toISOString()
                ),
                exitDate: isExited ? normalizeDateTime(
                    exitTrade.traded_timestamp || exitTrade.trigger_timestamp || leg.exit_timestamp,
                    ''
                ) : '',
                exitPrice: isExited ? exitPrice : null,
                exited: isExited,
                isQueued: isQueued,
                queuedAt: isQueued ? normalizeDateTime(leg.queued_at || leg.armed_at || leg.created_at, '') : '',
                queueLotValue: lotValue,
                includeInPnl: !isQueued,
                sourceLegId: String(leg.id || leg.leg_id || '').trim(),
                sourceToken: String(leg.token || '').trim(),
                sourcePnl: Number(leg.pnl || 0) || 0,
                lastPrice: Number(leg.last_saw_price || leg.mark_price || 0) || null,
                liveLtp: isExited ? exitPrice : (Number(leg.last_saw_price || leg.mark_price || 0) || null),
                liveDelta: leg.delta != null ? (Number(leg.delta) || 0) : null
            };
        }).filter(function (leg) {
            if (leg.isQueued) {
                return leg.strike > 0 && leg.expiry;
            }
            return leg.strike > 0 && leg.quantity > 0 && leg.premium >= 0 && leg.expiry;
        });
    }

    function applyPayloadToSimulator(payload) {
        if (!payload) {
            return;
        }

        var simulatorReady = typeof renderPositionTable === 'function'
            && typeof updateSummaryStats === 'function'
            && typeof updateChart === 'function'
            && typeof renderLegs === 'function'
            && typeof window._setSimulatorLegs === 'function';
        if (!simulatorReady) {
            setTimeout(function () {
                applyPayloadToSimulator(payload);
            }, 150);
            return;
        }

        var mappedLegs = buildSimulatorLegs(payload);
        if (!mappedLegs.length) {
            return;
        }

        window._setSimulatorLegs(mappedLegs);

        var spotPrice = Number(payload.summary && payload.summary.spot_price || 0) || 0;
        if (spotPrice > 0 && typeof currentSpotPrice !== 'undefined') {
            currentSpotPrice = spotPrice;
            var spotValueEl = document.getElementById('spotPriceValue');
            var currentPriceEl = document.getElementById('currentPriceDisplay');
            var sliderEl = document.getElementById('spotPriceSlider');
            if (spotValueEl) {
                spotValueEl.textContent = currentSpotPrice.toFixed(2);
            }
            if (currentPriceEl) {
                currentPriceEl.textContent = currentSpotPrice.toFixed(2);
            }
            if (sliderEl) {
                sliderEl.value = currentSpotPrice;
            }
        }

        renderLegs();
        renderPositionTable();
        updateSummaryStats();
        updateChart();
    }

    function handleTradeHistorySocketMessage(message) {
        if (!_livePayload || !_liveTradeId) {
            return;
        }

        var type = String(message && message.type || '').trim();
        if (type === 'ltp_update') {
            var ltpData = message.data || {};
            var ltpItems = Array.isArray(ltpData.ltp) ? ltpData.ltp : [];
            ltpItems.forEach(function (item) {
                var token = String(item && item.token || '').trim();
                if (!token) {
                    return;
                }
                _liveLtpMap[token] = Number(item && item.ltp || 0) || 0;
            });

            var openLegs = Array.isArray(_livePayload.legs && _livePayload.legs.open) ? _livePayload.legs.open : [];
            openLegs.forEach(function (leg) {
                var token = String(leg && leg.token || '').trim();
                if (!token || _liveLtpMap[token] == null) {
                    return;
                }

                var ltp = Number(_liveLtpMap[token]) || 0;
                var entryTrade = normalizeTradeObject(leg && leg.entry_trade);
                var entryPrice = Number(
                    entryTrade.price != null ? entryTrade.price
                        : (entryTrade.trigger_price != null ? entryTrade.trigger_price : (leg.entry_price || 0))
                ) || 0;
                var qty = Number(leg.effective_quantity || leg.quantity || 0) || 0;
                var isSell = normalizePositionType(leg.position_side || leg.position) === 'Sell';

                leg.last_saw_price = ltp;
                leg.mark_price = ltp;
                if (entryPrice > 0 && qty > 0) {
                    leg.pnl = isSell ? (entryPrice - ltp) * qty : (ltp - entryPrice) * qty;
                }
            });

            var spotItem = ltpItems.find(function (item) {
                return String(item && item.option_type || '').trim().toUpperCase() === 'SPOT';
            });
            if (spotItem) {
                _livePayload.summary = _livePayload.summary || {};
                _livePayload.summary.spot_price = Number(spotItem.ltp || 0) || 0;
            }

            var closedLegs = Array.isArray(_livePayload.legs && _livePayload.legs.closed) ? _livePayload.legs.closed : [];
            var openPnl = openLegs.reduce(function (sum, leg) { return sum + (Number(leg && leg.pnl || 0) || 0); }, 0);
            var closedPnl = closedLegs.reduce(function (sum, leg) { return sum + (Number(leg && leg.pnl || 0) || 0); }, 0);
            _livePayload.summary = _livePayload.summary || {};
            _livePayload.summary.mtm = openPnl + closedPnl;
            scheduleSimulatorRefresh();
            return;
        }

        if (type === 'execute_order') {
            var messageData = message && message.data ? message.data : {};
            var records = Array.isArray(messageData)
                ? messageData
                : (Array.isArray(messageData.records) ? messageData.records : []);
            var matchedCurrentTrade = false;
            var query = getTradeQuery();
            records.forEach(function (record) {
                var rid = String(record && (record._id || record.trade_id) || '').trim();
                if (rid !== _liveTradeId) {
                    return;
                }
                matchedCurrentTrade = true;

                if (Array.isArray(record.legs) && _livePayload.legs) {
                    var incomingLegs = record.legs;
                    ['open', 'closed', 'all'].forEach(function (bucket) {
                        var targetLegs = Array.isArray(_livePayload.legs[bucket]) ? _livePayload.legs[bucket] : [];
                        targetLegs.forEach(function (leg) {
                            var legId = String(leg && (leg.id || leg.leg_id) || '').trim();
                            var match = incomingLegs.find(function (item) {
                                return String(item && (item.id || item.leg_id) || '').trim() === legId;
                            });
                            if (match) {
                                Object.keys(match).forEach(function (key) {
                                    leg[key] = match[key];
                                });
                            }
                        });
                    });
                }

                if (record.summary) {
                    _livePayload.summary = Object.assign({}, _livePayload.summary || {}, record.summary || {});
                }
            });
            scheduleSimulatorRefresh();
            var matchedCurrentGroup = !!(
                query.groupId && (
                    String(messageData.group_id || '').trim() === query.groupId
                    || records.some(function (record) {
                        return String(record && record.portfolio && record.portfolio.group_id || '').trim() === query.groupId;
                    })
                )
            );
            var matchedCurrentPortfolio = !!(
                query.portfolioId && records.some(function (record) {
                    return String(record && record.portfolio && record.portfolio.portfolio || '').trim() === query.portfolioId;
                })
            );
            var matchedCurrentStrategy = !!(
                query.strategyId && records.some(function (record) {
                    return String(record && (record.strategy_id || record._id || record.trade_id) || '').trim() === query.strategyId;
                })
            );
            if (matchedCurrentTrade || matchedCurrentGroup || matchedCurrentPortfolio || matchedCurrentStrategy) {
                scheduleTradeHistoryReload();
            }
        }
    }

    function connectTradeHistorySockets(payload) {
        var trade = (payload && payload.trade) || {};
        var userId = String(trade.user_id || '').trim();
        var activationMode = String(payload.activation_mode || trade.activation_mode || 'algo-backtest').trim() || 'algo-backtest';
        if (!userId || !window.AlgoStreamSockets || typeof window.AlgoStreamSockets.createChannel !== 'function') {
            return;
        }

        var channels = ['execute-orders', 'update'];
        var nextSocketKey = [userId, activationMode].join('::');
        var existingSockets = window.StrategyTradeSimulatorSockets || {};
        if (_tradeHistorySocketKey === nextSocketKey && Object.keys(existingSockets).length) {
            return;
        }
        Object.keys(existingSockets).forEach(function (key) {
            var existing = existingSockets[key];
            if (existing && typeof existing.close === 'function') {
                existing.close();
            }
        });

        var socketRegistry = {};
        channels.forEach(function (channelName) {
            var channelSocket = window.AlgoStreamSockets.createChannel({
                channel: channelName,
                userId: userId,
                activationMode: activationMode
            });

            channelSocket.on('status', function (status, meta) {
                window.dispatchEvent(new CustomEvent('strategy-trade-simulator-socket-status', {
                    detail: {
                        channel: channelName,
                        status: status,
                        meta: meta || {}
                    }
                }));
            });

            channelSocket.on('message', function (message) {
                handleTradeHistorySocketMessage(message);
                window.dispatchEvent(new CustomEvent('strategy-trade-simulator-socket-message', {
                    detail: {
                        channel: channelName,
                        message: message
                    }
                }));
            });

            channelSocket.connect();
            socketRegistry[channelName] = channelSocket;
        });

        window.StrategyTradeSimulatorSockets = socketRegistry;
        _tradeHistorySocketKey = nextSocketKey;
    }

    function fetchTradeHistory(options) {
        var config = options || {};
        ensureAlgoApiBase();

        var query = getTradeQuery();
        if (!query.strategyId && !query.groupId && !query.portfolioId) {
            return;
        }

        var requestUrl = query.portfolioId
            ? window.buildAlgoApiUrl(
                'strategy-trade-history/portfolio/' + encodeURIComponent(query.portfolioId) + '?status=' + encodeURIComponent(query.status)
              )
            : (query.strategyId
                ? window.buildAlgoApiUrl(
                    'strategy-trade-history/' + encodeURIComponent(query.strategyId) + '?status=' + encodeURIComponent(query.status)
                  )
                : window.buildAlgoApiUrl(
                    'strategy-trade-history/group/' + encodeURIComponent(query.groupId) + '?status=' + encodeURIComponent(query.status)
                  ));

        window.strategyTradeSimulatorRequest = {
            strategy_id: query.strategyId,
            group_id: query.groupId,
            portfolio_id: query.portfolioId,
            status: query.status,
            url: requestUrl
        };

        var requestToken = ++_tradeHistoryRequestToken;
        fetch(requestUrl)
            .then(function (response) {
                if (!response.ok) {
                    throw new Error('Failed to load strategy trade history');
                }
                return response.json();
            })
            .then(function (payload) {
                if (requestToken !== _tradeHistoryRequestToken) {
                    return;
                }
                window.strategyTradeSimulatorPayload = payload || {};
                _livePayload = payload || {};
                _liveTradeId = String(((payload || {}).trade || {})._id || (payload || {}).strategy_id || '').trim();
                applyPayloadToSimulator(payload || {});
                connectTradeHistorySockets(payload || {});
                window.dispatchEvent(new CustomEvent('strategy-trade-simulator:data-loaded', {
                    detail: {
                        strategy_id: query.strategyId,
                        status: query.status,
                        payload: payload || {}
                    }
                }));
            })
            .catch(function (error) {
                if (requestToken !== _tradeHistoryRequestToken) {
                    return;
                }
                window.strategyTradeSimulatorError = error;
                window.dispatchEvent(new CustomEvent('strategy-trade-simulator:data-error', {
                    detail: {
                        strategy_id: query.strategyId,
                        status: query.status,
                        error: error
                    }
                }));
                console.error('[strategy-trade-simulator]', error);
            });
    }

    function scheduleTradeHistoryReload() {
        if (_tradeHistoryRefreshTimer) {
            return;
        }
        _tradeHistoryRefreshTimer = window.setTimeout(function () {
            _tradeHistoryRefreshTimer = null;
            fetchTradeHistory({ silent: true });
        }, 120);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function () {
            fetchTradeHistory();
        });
    } else {
        fetchTradeHistory();
    }

    window.addEventListener('beforeunload', function () {
        var sockets = window.StrategyTradeSimulatorSockets || {};
        Object.keys(sockets).forEach(function (key) {
            var channelSocket = sockets[key];
            if (channelSocket && typeof channelSocket.close === 'function') {
                channelSocket.close();
            }
        });
        _tradeHistorySocketKey = '';
    });
}());
