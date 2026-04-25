(function () {
    var _livePayload = null;
    var _liveTradeId = '';
    var _liveLtpMap = {};
    var _liveRefreshTimer = null;
    var _liveSyncCosts = null;

    function getApiBaseUrl() {
        return (window.APP_CONFIG && window.APP_CONFIG.algoApiBaseUrl)
            || (typeof window.getBackendUrl === 'function' ? window.getBackendUrl() : '')
            || window.APP_ALGO_API_BASE_URL
            || '';
    }

    function buildApiUrl(path) {
        if (typeof window.buildAlgoApiUrl === 'function') {
            return window.buildAlgoApiUrl(path);
        }
        return String(getApiBaseUrl() || '').replace(/\/+$/, '') + '/' + String(path || '').replace(/^\/+/, '');
    }

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function formatMoney(value) {
        var amount = Number(value || 0);
        var formatted = Math.abs(amount).toLocaleString('en-IN', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });
        return '₹ ' + (amount < 0 ? '-' : '') + formatted;
    }

    function getMoneyClass(value) {
        return Number(value || 0) >= 0 ? 'text-green-500' : 'text-red-500';
    }

    function getStatusPillStyle(status) {
        var normalized = String(status || '').trim().toLowerCase();
        if (normalized.indexOf('squared') !== -1 || normalized.indexOf('sqd') !== -1) {
            return 'border:#d1d5db;background:#fff;color:#6b7280;';
        }
        if (normalized.indexOf('running') !== -1 || normalized.indexOf('active') !== -1) {
            return 'border:#86efac;background:#f0fdf4;color:#16a34a;';
        }
        if (normalized.indexOf('pending') !== -1 || normalized.indexOf('import') !== -1) {
            return 'border:#bfdbfe;background:#eff6ff;color:#2563eb;';
        }
        return 'border:#d1d5db;background:#fff;color:#374151;';
    }

    function formatCompactMoney(value, fallback) {
        if (value == null || value === '') {
            return fallback || '₹ 0';
        }
        var amount = Number(value);
        if (!isFinite(amount)) {
            return String(value);
        }
        return '₹ ' + (amount < 0 ? '-' : '') + Math.abs(amount).toLocaleString('en-IN', {
            maximumFractionDigits: 2
        });
    }

    function formatDateTime(value) {
        var raw = String(value || '').trim();
        if (!raw) {
            return '-';
        }
        var normalized = raw.replace(' ', 'T');
        var dt = new Date(normalized);
        if (isNaN(dt.getTime())) {
            return raw;
        }
        return dt.toLocaleString('en-IN', {
            year: 'numeric',
            month: 'short',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: true
        });
    }

    function normalizeLegTime(leg, type) {
        var trade = type === 'entry' ? leg.entry_trade : leg.exit_trade;
        return formatDateTime((trade && (trade.traded_timestamp || trade.trigger_timestamp)) || '');
    }

    function normalizeLegPrice(leg, type) {
        var trade = type === 'entry' ? leg.entry_trade : leg.exit_trade;
        if (!trade) {
            return '-';
        }
        var price = trade.price != null ? trade.price : trade.trigger_price;
        return price == null || price === '' ? '-' : Number(price).toFixed(2);
    }

    function normalizeInstrumentLabel(leg) {
        var ticker = String(leg.ticker || leg.underlying || '').trim();
        var expiry = String(leg.expiry_date || leg.expiry || '').trim().slice(0, 10);
        var strike = String(leg.strike || '').trim();
        var option = String(leg.option || leg.option_type || '').trim();
        return [ticker, expiry, strike, option].filter(Boolean).join(' ');
    }

    function getExecutionPrice(trade) {
        if (!trade) {
            return 0;
        }
        var price = trade.price != null ? trade.price : trade.trigger_price;
        return Number(price || 0) || 0;
    }

    function getLegExecutionCount(leg) {
        if (!leg) {
            return 0;
        }
        if (Array.isArray(leg.sub_trades) && leg.sub_trades.length) {
            return leg.sub_trades.length;
        }
        return (getExecutionPrice(leg.entry_trade) > 0
            || getExecutionPrice(leg.exit_trade) > 0
            || Number(leg.entry_price || 0) > 0
            || Number(leg.exit_price || 0) > 0) ? 1 : 0;
    }

    function getLegLotsValue(leg) {
        var qty = Number(leg.effective_quantity || leg.quantity || 0) || 0;
        var lots = Number(leg.lots || leg.total_lots || leg.no_of_lots || 0) || 0;
        if (lots > 0) {
            return lots;
        }
        var lotSize = Number(leg.lot_size || leg.lotsize || leg.quantity_per_lot || leg.freeze_qty || 0) || 0;
        if (qty > 0 && lotSize > 0) {
            return Math.max(qty / lotSize, 1);
        }
        return qty > 0 ? 1 : 0;
    }

    function getLegLotSizeValue(leg) {
        var qty = Number(leg.effective_quantity || leg.quantity || 0) || 0;
        var lots = Number(leg.lots || leg.total_lots || leg.no_of_lots || 0) || 0;
        var explicitLotSize = Number(leg.lot_size || leg.lotsize || leg.quantity_per_lot || leg.freeze_qty || 0) || 0;
        if (explicitLotSize > 0) {
            return explicitLotSize;
        }
        if (qty > 0 && lots > 0) {
            return Math.max(qty / lots, 1);
        }
        return qty > 0 ? qty : 1;
    }

    function normalizeChargeLeg(leg) {
        return {
            lots: getLegLotsValue(leg),
            lot_size: getLegLotSizeValue(leg),
            position: String(leg.position_side || leg.position || 'sell'),
            entry_price: getExecutionPrice(leg.entry_trade) || Number(leg.entry_price || 0) || 0,
            exit_price: getExecutionPrice(leg.exit_trade) || Number(leg.exit_price || 0) || 0,
            sub_trades: Array.isArray(leg.sub_trades) ? leg.sub_trades.map(function (subTrade) {
                return {
                    lots: Number(subTrade && subTrade.lots || 0) || undefined,
                    entry_price: Number(subTrade && (subTrade.entry_price != null ? subTrade.entry_price : subTrade.price) || 0) || undefined,
                    exit_price: Number(subTrade && subTrade.exit_price || 0) || undefined
                };
            }) : []
        };
    }

    function buildChargeTrades(payload) {
        var trade = payload.trade || {};
        var legs = payload.legs || {};
        var orders = Array.isArray(payload.broker_orders) ? payload.broker_orders : [];
        var allLegs = []
            .concat(Array.isArray(legs.open) ? legs.open : [])
            .concat(Array.isArray(legs.closed) ? legs.closed : []);
        var fallbackLegs = allLegs.map(normalizeChargeLeg);
        return [{
            _id: trade._id || payload.strategy_id || 'trade-history',
            orders: orders,
            legs: fallbackLegs
        }];
    }

    function createSwitchMarkup(id, checked) {
        return '' +
            '<button class="relative inline-flex h-4 w-8 items-center rounded-full ' + (checked ? 'bg-secondaryBlue-500' : 'bg-primaryBlack-600') + '" id="' + escapeHtml(id) + '" role="switch" type="button" aria-checked="' + escapeHtml(String(checked)) + '">' +
            '  <span class="inline-block h-3 w-3 transform rounded-full bg-primaryBlack-0 transition ' + (checked ? 'translate-x-4' : 'translate-x-1') + '"></span>' +
            '</button>';
    }

    function formatCostNumber(value, withCurrency) {
        var amount = Number(value || 0) || 0;
        var formatted = amount.toLocaleString('en-IN', {
            minimumFractionDigits: 0,
            maximumFractionDigits: 2
        });
        return withCurrency ? '₹ ' + formatted : formatted;
    }

    function initTradeHistoryCostControls(payload) {
        var brokerageToggle = document.getElementById('toggle--result-brokerage_include_trade_history');
        var taxesToggle = document.getElementById('toggle--result_include_trade_history');
        var brokerageTrigger = document.getElementById('brokerage-cost-trigger');
        var brokerageEditTrigger = document.getElementById('brokerage-edit-trigger');
        var brokeragePopup = document.getElementById('brokerage-popup');
        var brokerageRateInput = document.getElementById('brokerage-rate-input');
        var brokerageRateType = document.getElementById('brokerage-rate-type');
        var brokerageDone = document.getElementById('brokerage-popup-done');
        var brokerageCostValue = document.getElementById('brokerage-cost-value');
        var taxesTrigger = document.getElementById('taxes-cost-trigger');
        var taxesEditTrigger = document.getElementById('taxes-edit-trigger');
        var taxesPopup = document.getElementById('taxes-popup');
        var taxesCostValue = document.getElementById('taxes-cost-value');
        var netMtmValue = document.getElementById('trade-history-net-mtm');
        var brokerageValueWrap = document.getElementById('brokerage-cost-trigger');
        var taxesValueWrap = document.getElementById('taxes-cost-trigger');
        var chargeTrades = buildChargeTrades(payload);
        var summary = payload.summary || {};

        var NSE_STT = 0.001;
        var NSE_EXCHANGE = 0.00053;
        var NSE_STAMP = 0.00003;
        var NSE_SEBI = 0.0000010;
        var NSE_GST = 0.18;

        function isEnabled(toggle) {
            return !!toggle && toggle.getAttribute('aria-checked') === 'true';
        }

        function setSwitchState(toggle, enabled) {
            if (!toggle) {
                return;
            }
            toggle.setAttribute('aria-checked', String(enabled));
            toggle.classList.toggle('bg-secondaryBlue-500', enabled);
            toggle.classList.toggle('bg-primaryBlack-600', !enabled);
            var knob = toggle.querySelector('span');
            if (knob) {
                knob.classList.toggle('translate-x-4', enabled);
                knob.classList.toggle('translate-x-1', !enabled);
            }
        }

        function getTradeLotCount(trade) {
            var orderLots = (trade.orders || []).reduce(function (sum, order) {
                var explicitLots = Number(order && (order.lots || order.lot || order.no_of_lots) || 0) || 0;
                if (explicitLots > 0) {
                    return sum + explicitLots;
                }
                var quantity = Number(order && (order.filled_quantity || order.fill_qty || order.quantity || order.qty) || 0) || 0;
                var symbol = String(order && (order.trading_symbol || order.symbol || order.instrument) || '').trim().toUpperCase();
                var matchedLeg = (trade.legs || []).find(function (leg) {
                    var legLabel = normalizeInstrumentLabel(leg).toUpperCase();
                    var legTicker = String(leg.ticker || leg.underlying || '').trim().toUpperCase();
                    return (symbol && legLabel && symbol.indexOf(legLabel) !== -1)
                        || (symbol && legTicker && symbol.indexOf(legTicker) !== -1);
                });
                var lotSize = matchedLeg ? getLegLotSizeValue(matchedLeg) : 0;
                if (quantity > 0 && lotSize > 0) {
                    return sum + Math.max(quantity / lotSize, 1);
                }
                return sum + (quantity > 0 ? 1 : 0);
            }, 0);
            if (orderLots > 0) {
                return orderLots;
            }
            return (trade.legs || []).reduce(function (sum, leg) {
                return sum + (Number(leg.lots || 0) || 0);
            }, 0) || 1;
        }

        function getTradeOrderCount(trade) {
            var orders = Array.isArray(trade.orders) ? trade.orders.filter(function (order) { return !!order; }) : [];
            if (orders.length) {
                return orders.length;
            }
            return (trade.legs || []).reduce(function (sum, leg) {
                return sum + getLegExecutionCount(leg);
            }, 0) || 1;
        }

        function getTradeBrokerage(trade) {
            if (!isEnabled(brokerageToggle)) {
                return 0;
            }
            var rate = Number(brokerageRateInput && brokerageRateInput.value) || 0;
            if (rate <= 0) {
                return 0;
            }
            var rateType = brokerageRateType ? brokerageRateType.value : 'per_order';
            return rateType === 'per_lot'
                ? rate * getTradeLotCount(trade)
                : rate * getTradeOrderCount(trade);
        }

        function computeTotalBrokerage(trades) {
            return (trades || []).reduce(function (sum, trade) {
                return sum + getTradeBrokerage(trade);
            }, 0);
        }

        function calcTradeTaxes(trade) {
            var stt = 0;
            var exchange = 0;
            var stamp = 0;
            var sebi = 0;
            var orders = Array.isArray(trade.orders) ? trade.orders : [];
            if (orders.length) {
                orders.forEach(function (order) {
                    var qty = Number(order && (order.filled_quantity || order.fill_qty || order.quantity || order.qty) || 0) || 0;
                    var price = Number(order && (order.average_price || order.avg_price || order.fill_price || order.price || order.trigger_price) || 0) || 0;
                    var side = String(order && (order.transaction_type || order.side || order.order_side) || '').trim().toLowerCase();
                    if (!qty || !price) {
                        return;
                    }
                    var turnover = qty * price;
                    var isSell = side === 'sell' || side === 's';
                    if (isSell) {
                        stt += turnover * NSE_STT;
                    } else {
                        stamp += turnover * NSE_STAMP;
                    }
                    exchange += turnover * NSE_EXCHANGE;
                    sebi += turnover * NSE_SEBI;
                });
            } else {
                (trade.legs || []).forEach(function (leg) {
                    var lots = Number(leg.lots || 1) || 1;
                    var lotSize = Number(leg.lot_size || 1) || 1;
                    var subList = leg.sub_trades && leg.sub_trades.length ? leg.sub_trades : [null];
                    var isSell = String(leg.position || 'sell').toLowerCase() !== 'buy';
                    subList.forEach(function (subTrade) {
                        var subLots = Number(subTrade && subTrade.lots || 0) || lots;
                        var qty = subLots * lotSize;
                        var entryPx = Number(subTrade && subTrade.entry_price != null ? subTrade.entry_price : leg.entry_price || 0) || 0;
                        var exitPx = Number(subTrade && subTrade.exit_price != null ? subTrade.exit_price : leg.exit_price || 0) || 0;
                        var entryTurnover = entryPx * qty;
                        if (isSell) {
                            stt += entryTurnover * NSE_STT;
                        } else {
                            stamp += entryTurnover * NSE_STAMP;
                        }
                        exchange += entryTurnover * NSE_EXCHANGE;
                        sebi += entryTurnover * NSE_SEBI;
                        var exitTurnover = exitPx * qty;
                        if (!isSell) {
                            stt += exitTurnover * NSE_STT;
                        } else {
                            stamp += exitTurnover * NSE_STAMP;
                        }
                        exchange += exitTurnover * NSE_EXCHANGE;
                        sebi += exitTurnover * NSE_SEBI;
                    });
                });
            }
            var gst = (exchange + sebi) * NSE_GST;
            return {
                stt: stt,
                exchange: exchange,
                stamp: stamp,
                sebi: sebi,
                gst: gst,
                total: stt + exchange + stamp + sebi + gst
            };
        }

        function aggregateTaxes(trades) {
            return (trades || []).reduce(function (agg, trade) {
                var bucket = calcTradeTaxes(trade);
                agg.stt += bucket.stt;
                agg.exchange += bucket.exchange;
                agg.stamp += bucket.stamp;
                agg.sebi += bucket.sebi;
                agg.gst += bucket.gst;
                agg.total += bucket.total;
                return agg;
            }, { stt: 0, exchange: 0, stamp: 0, sebi: 0, gst: 0, total: 0 });
        }

        function syncBrokerageTriggerState() {
            var enabled = isEnabled(brokerageToggle);
            if (brokerageTrigger) {
                brokerageTrigger.classList.toggle('opacity-50', !enabled);
                brokerageTrigger.classList.toggle('opacity-100', enabled);
            }
            if (brokerageEditTrigger) {
                brokerageEditTrigger.classList.toggle('pointer-events-none', !enabled);
                brokerageEditTrigger.setAttribute('aria-disabled', String(!enabled));
            }
        }

        function syncTaxesTriggerState() {
            var enabled = isEnabled(taxesToggle);
            if (taxesTrigger) {
                taxesTrigger.classList.toggle('opacity-50', !enabled);
                taxesTrigger.classList.toggle('opacity-100', enabled);
            }
            if (taxesEditTrigger) {
                taxesEditTrigger.classList.toggle('pointer-events-none', !enabled);
                taxesEditTrigger.setAttribute('aria-disabled', String(!enabled));
            }
        }

        function closeBrokeragePopup() {
            if (!brokeragePopup) {
                return;
            }
            brokeragePopup.classList.add('hidden');
            brokeragePopup.style.display = 'none';
        }

        function closeTaxesPopup() {
            if (!taxesPopup) {
                return;
            }
            taxesPopup.classList.add('hidden');
            taxesPopup.style.display = 'none';
        }

        function syncSummaryCosts() {
            var totalBrokerage = computeTotalBrokerage(chargeTrades);
            var taxAgg = aggregateTaxes(chargeTrades);
            var enabledBrokerage = isEnabled(brokerageToggle);
            var enabledTaxes = isEnabled(taxesToggle);
            if (brokerageCostValue) {
                brokerageCostValue.textContent = enabledBrokerage ? formatCostNumber(totalBrokerage, false) : '0';
            }
            if (taxesCostValue) {
                taxesCostValue.textContent = enabledTaxes ? formatCostNumber(taxAgg.total, false) : '0';
            }
            if (brokerageValueWrap) {
                brokerageValueWrap.classList.toggle('text-primaryBlack-800', enabledBrokerage);
                brokerageValueWrap.classList.toggle('text-primaryBlack-500', !enabledBrokerage);
            }
            if (taxesValueWrap) {
                taxesValueWrap.classList.toggle('text-primaryBlack-800', enabledTaxes);
                taxesValueWrap.classList.toggle('text-primaryBlack-500', !enabledTaxes);
            }
            ['stt', 'exchange', 'stamp', 'sebi', 'gst', 'total'].forEach(function (key) {
                var el = document.getElementById('taxes-' + key);
                if (el) {
                    el.textContent = enabledTaxes ? formatCostNumber(taxAgg[key], true) : '₹0';
                }
            });
            if (netMtmValue) {
                var netMtm = Number(summary.mtm || 0) - (enabledBrokerage ? totalBrokerage : 0) - (enabledTaxes ? taxAgg.total : 0);
                netMtmValue.textContent = formatMoney(netMtm);
                netMtmValue.className = 'text-sm font-bold ' + getMoneyClass(netMtm);
            }
        }

        if (brokerageEditTrigger) {
            brokerageEditTrigger.addEventListener('click', function (event) {
                event.preventDefault();
                event.stopPropagation();
                if (!isEnabled(brokerageToggle) || !brokeragePopup) {
                    return;
                }
                var open = brokeragePopup.style.display === 'block';
                brokeragePopup.classList.toggle('hidden', open);
                brokeragePopup.style.display = open ? 'none' : 'block';
            });
        }

        if (taxesEditTrigger) {
            taxesEditTrigger.addEventListener('click', function (event) {
                event.preventDefault();
                event.stopPropagation();
                if (!isEnabled(taxesToggle) || !taxesPopup) {
                    return;
                }
                var open = taxesPopup.style.display === 'block';
                taxesPopup.classList.toggle('hidden', open);
                taxesPopup.style.display = open ? 'none' : 'block';
            });
        }

        if (brokerageDone) {
            brokerageDone.addEventListener('click', function (event) {
                event.preventDefault();
                closeBrokeragePopup();
                syncSummaryCosts();
            });
        }

        if (brokerageRateInput) {
            brokerageRateInput.addEventListener('input', syncSummaryCosts);
        }
        if (brokerageRateType) {
            brokerageRateType.addEventListener('change', syncSummaryCosts);
        }
        if (brokerageToggle) {
            brokerageToggle.addEventListener('click', function () {
                var nextState = !isEnabled(brokerageToggle);
                setSwitchState(brokerageToggle, nextState);
                if (!nextState) {
                    closeBrokeragePopup();
                }
                syncBrokerageTriggerState();
                syncSummaryCosts();
            });
        }
        if (taxesToggle) {
            taxesToggle.addEventListener('click', function () {
                var nextState = !isEnabled(taxesToggle);
                setSwitchState(taxesToggle, nextState);
                if (!nextState) {
                    closeTaxesPopup();
                }
                syncTaxesTriggerState();
                syncSummaryCosts();
            });
        }

        document.addEventListener('click', function (event) {
            if (brokeragePopup && !brokeragePopup.classList.contains('hidden')) {
                if (!(brokeragePopup.contains(event.target) || (brokerageEditTrigger && brokerageEditTrigger.contains(event.target)))) {
                    closeBrokeragePopup();
                }
            }
            if (taxesPopup && !taxesPopup.classList.contains('hidden')) {
                if (!(taxesPopup.contains(event.target) || (taxesEditTrigger && taxesEditTrigger.contains(event.target)))) {
                    closeTaxesPopup();
                }
            }
        });

        syncBrokerageTriggerState();
        syncTaxesTriggerState();
        syncSummaryCosts();
        _liveSyncCosts = syncSummaryCosts;
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
                var token = String(leg.token || '').trim();
                if (!token || _liveLtpMap[token] == null) {
                    return;
                }
                var ltp = Number(_liveLtpMap[token]);
                var entryTrade = leg.entry_trade && typeof leg.entry_trade === 'object' ? leg.entry_trade : null;
                if (!entryTrade) {
                    return;
                }
                var entryPrice = Number(entryTrade.price != null ? entryTrade.price : (entryTrade.trigger_price || 0)) || 0;
                if (!entryPrice) {
                    return;
                }
                var qty = Number(leg.effective_quantity || leg.quantity || 0) || 0;
                if (!qty) {
                    return;
                }
                var isSell = String(leg.position_side || leg.position || 'sell').toLowerCase() !== 'buy';
                leg.pnl = isSell ? (entryPrice - ltp) * qty : (ltp - entryPrice) * qty;
                leg.last_saw_price = ltp;
            });
            var spotItem = ltpItems.find(function (item) {
                return String(item && item.option_type || '').trim().toUpperCase() === 'SPOT';
            });
            if (spotItem) {
                _livePayload.summary = _livePayload.summary || {};
                _livePayload.summary.spot_price = Number(spotItem.ltp || 0) || 0;
            }
            var openPnl = openLegs.reduce(function (s, l) { return s + (Number(l.pnl) || 0); }, 0);
            var closedLegs = Array.isArray(_livePayload.legs && _livePayload.legs.closed) ? _livePayload.legs.closed : [];
            var closedPnl = closedLegs.reduce(function (s, l) { return s + (Number(l.pnl) || 0); }, 0);
            _livePayload.summary.mtm = openPnl + closedPnl;
            scheduleRefreshLiveMtm();
            return;
        }

        if (type === 'execute_order') {
            var records = Array.isArray(message.data)
                ? message.data
                : (message.data && Array.isArray(message.data.records) ? message.data.records : []);
            records.forEach(function (record) {
                var rid = String(record && (record._id || record.trade_id) || '').trim();
                if (rid !== _liveTradeId) {
                    return;
                }
                var incomingLegs = Array.isArray(record.legs) ? record.legs : [];
                var openLegs = Array.isArray(_livePayload.legs && _livePayload.legs.open) ? _livePayload.legs.open : [];
                openLegs.forEach(function (leg) {
                    var legId = String(leg.id || leg.leg_id || '').trim();
                    var match = incomingLegs.find(function (il) {
                        return String(il && (il.id || il.leg_id) || '') === legId;
                    });
                    if (match) {
                        if (match.pnl != null) { leg.pnl = match.pnl; }
                        if (match.last_saw_price != null) { leg.last_saw_price = match.last_saw_price; }
                    }
                });
                if (record.summary && record.summary.mtm != null) {
                    _livePayload.summary.mtm = record.summary.mtm;
                } else {
                    var openPnl = openLegs.reduce(function (s, l) { return s + (Number(l.pnl) || 0); }, 0);
                    var closedLegs = Array.isArray(_livePayload.legs && _livePayload.legs.closed) ? _livePayload.legs.closed : [];
                    var closedPnl = closedLegs.reduce(function (s, l) { return s + (Number(l.pnl) || 0); }, 0);
                    _livePayload.summary.mtm = openPnl + closedPnl;
                }
                scheduleRefreshLiveMtm();
            });
        }

        if (type === 'update') {
            var d = message.data || {};
            var openPositions = Array.isArray(d.open_positions) ? d.open_positions : [];
            var openLegs = Array.isArray(_livePayload.legs && _livePayload.legs.open) ? _livePayload.legs.open : [];
            var updated = false;
            openPositions.forEach(function (position) {
                var tradeId = String(position && position.trade_id || '').trim();
                if (tradeId !== _liveTradeId) {
                    return;
                }
                var legId = String(position && position.leg_id || '').trim();
                openLegs.forEach(function (leg) {
                    var currentLegId = String(leg.id || leg.leg_id || '').trim();
                    if (currentLegId !== legId) {
                        return;
                    }
                    if (position.pnl != null) { leg.pnl = position.pnl; }
                    var ltp = position.ltp != null ? position.ltp : position.last_saw_price;
                    if (ltp != null) { leg.last_saw_price = ltp; }
                    updated = true;
                });
            });
            if (updated) {
                if (d.mtm != null) {
                    _livePayload.summary.mtm = d.mtm;
                } else {
                    var openPnl = openLegs.reduce(function (s, l) { return s + (Number(l.pnl) || 0); }, 0);
                    var closedLegs = Array.isArray(_livePayload.legs && _livePayload.legs.closed) ? _livePayload.legs.closed : [];
                    var closedPnl = closedLegs.reduce(function (s, l) { return s + (Number(l.pnl) || 0); }, 0);
                    _livePayload.summary.mtm = openPnl + closedPnl;
                }
                scheduleRefreshLiveMtm();
            }
        }
    }

    function scheduleRefreshLiveMtm() {
        if (_liveRefreshTimer) {
            return;
        }
        _liveRefreshTimer = setTimeout(function () {
            _liveRefreshTimer = null;
            refreshLiveMtm();
        }, 150);
    }

    function refreshLiveMtm() {
        if (!_livePayload) {
            return;
        }
        var spotEl = document.getElementById('trade-history-underlying-price');
        if (spotEl && _livePayload.summary && _livePayload.summary.spot_price != null) {
            spotEl.textContent = Number(_livePayload.summary.spot_price || 0).toLocaleString('en-IN', {
                maximumFractionDigits: 2
            });
        }
        var openLegsHost = document.getElementById('trade-history-open-legs-table');
        if (openLegsHost && _livePayload.legs && Array.isArray(_livePayload.legs.open)) {
            openLegsHost.innerHTML = renderLegRows(_livePayload.legs.open);
        }
        if (typeof _liveSyncCosts === 'function') {
            _liveSyncCosts();
        } else {
            var netMtmEl = document.getElementById('trade-history-net-mtm');
            if (netMtmEl) {
                var mtm = Number(_livePayload.summary && _livePayload.summary.mtm || 0);
                netMtmEl.textContent = formatMoney(mtm);
                netMtmEl.className = 'text-sm font-bold ' + getMoneyClass(mtm);
            }
        }
    }

    function renderLegRows(legs) {
        if (!legs.length) {
            return '<div class="rounded border border-primaryBlack-350 bg-primaryBlack-0 px-4 py-6 text-sm text-primaryBlack-700">No legs found.</div>';
        }
        return '' +
            '<div class="overflow-auto rounded border border-primaryBlack-350 bg-primaryBlack-0">' +
            '  <table class="w-full min-w-[960px] text-left text-sm">' +
            '    <thead class="bg-primaryBlack-200 text-primaryBlack-750">' +
            '      <tr>' +
            '        <th class="px-4 py-3 font-medium">Instrument</th>' +
            '        <th class="px-4 py-3 font-medium">Qty</th>' +
            '        <th class="px-4 py-3 font-medium">Entry Price</th>' +
            '        <th class="px-4 py-3 font-medium">Entry Time</th>' +
            '        <th class="px-4 py-3 font-medium">Exit Price</th>' +
            '        <th class="px-4 py-3 font-medium">Exit Time</th>' +
            '        <th class="px-4 py-3 font-medium">Status</th>' +
            '        <th class="px-4 py-3 font-medium">MTM</th>' +
            '      </tr>' +
            '    </thead>' +
            '    <tbody>' +
            legs.map(function (leg) {
                return '' +
                    '<tr class="border-t border-primaryBlack-350">' +
                    '  <td class="px-4 py-3 font-medium">' + escapeHtml(normalizeInstrumentLabel(leg) || '-') + '</td>' +
                    '  <td class="px-4 py-3">' + escapeHtml(String((leg.effective_quantity || leg.quantity || 0))) + '</td>' +
                    '  <td class="px-4 py-3">' + escapeHtml(normalizeLegPrice(leg, 'entry')) + '</td>' +
                    '  <td class="px-4 py-3">' + escapeHtml(normalizeLegTime(leg, 'entry')) + '</td>' +
                    '  <td class="px-4 py-3">' + escapeHtml(normalizeLegPrice(leg, 'exit')) + '</td>' +
                    '  <td class="px-4 py-3">' + escapeHtml(normalizeLegTime(leg, 'exit')) + '</td>' +
                    '  <td class="px-4 py-3">' + escapeHtml(String(leg.status || '-')) + '</td>' +
                    '  <td class="px-4 py-3 font-semibold ' + getMoneyClass(leg.pnl) + '">' + escapeHtml(formatMoney(leg.pnl)) + '</td>' +
                    '</tr>';
            }).join('') +
            '    </tbody>' +
            '  </table>' +
            '</div>';
    }

    function renderOrders(orders) {
        if (!orders.length) {
            return '<div class="rounded border border-primaryBlack-350 bg-primaryBlack-0 px-4 py-6 text-sm text-primaryBlack-700">No broker orders found.</div>';
        }
        return '' +
            '<div class="overflow-auto rounded border border-primaryBlack-350 bg-primaryBlack-0">' +
            '  <table class="w-full min-w-[900px] text-left text-sm">' +
            '    <thead class="bg-primaryBlack-200 text-primaryBlack-750">' +
            '      <tr>' +
            '        <th class="px-4 py-3 font-medium">Instrument</th>' +
            '        <th class="px-4 py-3 font-medium">Side</th>' +
            '        <th class="px-4 py-3 font-medium">Qty</th>' +
            '        <th class="px-4 py-3 font-medium">Status</th>' +
            '        <th class="px-4 py-3 font-medium">Order ID</th>' +
            '        <th class="px-4 py-3 font-medium">Placed At</th>' +
            '      </tr>' +
            '    </thead>' +
            '    <tbody>' +
            orders.map(function (order) {
                return '' +
                    '<tr class="border-t border-primaryBlack-350">' +
                    '  <td class="px-4 py-3">' + escapeHtml(String(order.trading_symbol || order.symbol || order.instrument || '-')) + '</td>' +
                    '  <td class="px-4 py-3">' + escapeHtml(String(order.order_side || order.transaction_type || '-')) + '</td>' +
                    '  <td class="px-4 py-3">' + escapeHtml(String(order.quantity || order.qty || '-')) + '</td>' +
                    '  <td class="px-4 py-3">' + escapeHtml(String(order.status || '-')) + '</td>' +
                    '  <td class="px-4 py-3">' + escapeHtml(String(order.order_id || order.broker_order_id || order._id || '-')) + '</td>' +
                    '  <td class="px-4 py-3">' + escapeHtml(formatDateTime(order.placed_at || order.order_timestamp || order.created_at || '')) + '</td>' +
                    '</tr>';
            }).join('') +
            '    </tbody>' +
            '  </table>' +
            '</div>';
    }

    function toTitleCase(value) {
        return String(value || '')
            .replace(/[_-]+/g, ' ')
            .replace(/\s+/g, ' ')
            .trim()
            .replace(/\b\w/g, function (char) { return char.toUpperCase(); });
    }

    function parseNotificationData(value) {
        if (!value) {
            return {};
        }
        if (typeof value === 'object') {
            return value;
        }
        try {
            return JSON.parse(value);
        } catch (error) {
            return {};
        }
    }

    function getNotificationSeverity(item) {
        var eventType = String(item.event_type || item.type || '').trim().toLowerCase();
        if (['error', 'rejected', 'failed'].indexOf(eventType) !== -1) {
            return 'error';
        }
        if (['sl_hit', 'stop_loss_hit', 'square_off', 'squared_off', 'cancelled'].indexOf(eventType) !== -1) {
            return 'critical';
        }
        return 'info';
    }

    function getNotificationTypeLabel(item) {
        var eventType = String(item.event_type || item.type || 'notification').trim();
        var labelMap = {
            entry_taken: 'Entry Taken',
            trail_sl_changed: 'Trail SL Changed',
            sl_hit: 'SL Hit',
            target_hit: 'Target Hit',
            reentry_queued: 'Reentry Queued',
            square_off: 'Square Off',
            squared_off: 'Squared Off',
            pending_entry: 'Pending Entry',
            momentum_pending: 'Momentum Pending'
        };
        return labelMap[eventType] || toTitleCase(eventType || 'notification');
    }

    function buildNotificationMessage(item) {
        var payload = parseNotificationData(item.data);
        var eventType = String(item.event_type || item.type || '').trim().toLowerCase();
        var directMessage = String(
            item.trigger_description
            || payload.trigger_description
            ||
            item.what_happened
            || payload.what_happened
            || item.message
            || item.description
            || ''
        ).trim();
        if (directMessage) {
            return directMessage;
        }
        return toTitleCase(eventType || 'notification');
    }

    function enrichNotifications(notifications) {
        return (notifications || []).map(function (item) {
            var enriched = Object.assign({}, item || {});
            enriched.severity = getNotificationSeverity(enriched);
            enriched.typeLabel = getNotificationTypeLabel(enriched);
            enriched.messageText = buildNotificationMessage(enriched);
            enriched.timestampText = formatDateTime(enriched.timestamp || enriched.created_at || '');
            return enriched;
        }).sort(function (a, b) {
            var aTime = new Date(String(a.timestamp || a.created_at || '').replace(' ', 'T')).getTime();
            var bTime = new Date(String(b.timestamp || b.created_at || '').replace(' ', 'T')).getTime();
            aTime = isNaN(aTime) ? 0 : aTime;
            bTime = isNaN(bTime) ? 0 : bTime;
            return bTime - aTime;
        });
    }

    function renderNotifications(notifications) {
        if (!notifications.length) {
            return '<div class="rounded border border-primaryBlack-350 bg-primaryBlack-0 px-4 py-6 text-sm text-primaryBlack-700">No notifications found.</div>';
        }
        return '' +
            '<div class="rounded border border-primaryBlack-350 bg-primaryBlack-0 overflow-hidden">' +
            '  <div style="display:grid;grid-template-columns:220px 220px minmax(0,1fr);column-gap:16px;align-items:center;" class="border-b border-primaryBlack-350 bg-primaryBlack-200 px-4 py-3 text-xs font-medium text-primaryBlack-750">' +
            '    <span style="white-space:nowrap;">Time</span>' +
            '    <span style="white-space:nowrap;">Status</span>' +
            '    <span style="white-space:nowrap;">Message</span>' +
            '  </div>' +
            notifications.map(function (item) {
                return '' +
                    '<div style="display:grid;grid-template-columns:220px 220px minmax(0,1fr);column-gap:16px;align-items:center;" class="border-b border-primaryBlack-350 px-4 py-3 last:border-b-0 text-sm text-primaryBlack-700">' +
                    '  <div style="white-space:nowrap;">' + escapeHtml(item.timestampText) + '</div>' +
                    '  <div style="display:flex;align-items:center;gap:8px;white-space:nowrap;">' +
                    '    <span class="font-medium text-primaryBlack-800">' + escapeHtml(item.typeLabel) + '</span>' +
                    '    <span class="rounded-full border border-primaryBlack-350 bg-primaryBlack-0 px-2 py-0.5 text-[10px] uppercase tracking-wide text-primaryBlack-600">' + escapeHtml(toTitleCase(item.severity)) + '</span>' +
                    '  </div>' +
                    '  <div style="white-space:normal;word-break:break-word;">' + escapeHtml(item.messageText) +
                    '  </div>' +
                    '</div>';
            }).join('') +
            '</div>';
    }

    function connectTradeHistorySockets(payload) {
        var trade = payload.trade || {};
        var userId = String(trade.user_id || '').trim();
        var activationMode = String(payload.activation_mode || trade.activation_mode || 'algo-backtest').trim() || 'algo-backtest';
        if (!userId || !window.AlgoStreamSockets || typeof window.AlgoStreamSockets.createChannel !== 'function') {
            return;
        }

        var channels = ['execute-orders', 'update'];
        var existingSockets = window.StrategyTradeHistorySockets || {};
        Object.keys(existingSockets).forEach(function (key) {
            var existing = existingSockets[key];
            if (existing && typeof existing.close === 'function') {
                existing.close();
            }
        });

        var socketRegistry = {};
        var socketMessages = window.__strategyTradeHistorySocketMessages || {};

        channels.forEach(function (channelName) {
            var channelSocket = window.AlgoStreamSockets.createChannel({
                channel: channelName,
                userId: userId,
                activationMode: activationMode
            });

            socketMessages[channelName] = Array.isArray(socketMessages[channelName]) ? socketMessages[channelName] : [];

            channelSocket.on('status', function (status, meta) {
                window.dispatchEvent(new CustomEvent('strategy-trade-history-socket-status', {
                    detail: {
                        channel: channelName,
                        status: status,
                        meta: meta || {}
                    }
                }));
            });

            channelSocket.on('message', function (message) {
                socketMessages[channelName].push(message);
                handleTradeHistorySocketMessage(message);
                window.dispatchEvent(new CustomEvent('strategy-trade-history-socket-message', {
                    detail: {
                        channel: channelName,
                        message: message
                    }
                }));
                try {
                    console.log('[StrategyTradeHistorySocket]', channelName, message);
                } catch (error) { /* ignore */ }
            });

            channelSocket.connect();
            socketRegistry[channelName] = channelSocket;
        });

        window.__strategyTradeHistorySocketMessages = socketMessages;
        window.StrategyTradeHistorySockets = socketRegistry;
    }

    function renderPage(payload) {
        var host = document.getElementById('body-content');
        if (!host) {
            return;
        }
        var trade = payload.trade || {};
        var summary = payload.summary || {};
        var legs = payload.legs || {};
        var notifications = enrichNotifications(Array.isArray(payload.notifications) ? payload.notifications : []);
        var orders = Array.isArray(payload.broker_orders) ? payload.broker_orders : [];
        var notificationStatus = payload.notification_status || {};
        var brokerLabel = ((trade.broker_details || {}).broker_name)
            || ((trade.broker_details || {}).display_name)
            || trade.broker_label
            || trade.broker
            || '-';
        var notificationChips = Object.keys(notificationStatus).length
            ? Object.keys(notificationStatus).map(function (key) {
                return '<span class="rounded-full border border-primaryBlack-350 bg-primaryBlack-0 px-3 py-1 text-xs font-medium">' + escapeHtml(getNotificationTypeLabel({ event_type: key })) + ': ' + escapeHtml(String(notificationStatus[key])) + '</span>';
            }).join('')
            : '<span class="text-sm text-primaryBlack-700">No notification status found.</span>';
        var notificationCounts = {
            all: notifications.length,
            info: notifications.filter(function (item) { return item.severity === 'info'; }).length,
            critical: notifications.filter(function (item) { return item.severity === 'critical'; }).length,
            error: notifications.filter(function (item) { return item.severity === 'error'; }).length
        };
        var brokerIcon = String(((trade.broker_details || {}).broker_icon) || '').trim();
        var brokerIconUrl = brokerIcon ? escapeHtml(brokerIcon) : '';
        var statusLabel = String(trade.status || '-').replace(/^StrategyStatus\./, '').replace(/_/g, ' ');
        var underlyingLabel = String(trade.ticker || trade.underlying || 'Underlying').trim();
        var underlyingPrice = summary.spot_price != null ? Number(summary.spot_price).toLocaleString('en-IN', {
            maximumFractionDigits: 2
        }) : '-';
        var underlyingChange = summary.spot_change_text || '';
        var marginAmount = summary.margin_blocked != null ? formatCompactMoney(summary.margin_blocked) : '₹ 0';
        var strategyLink = './test.html?strategy_id=' + encodeURIComponent(payload.strategy_id || '');
        _livePayload = payload;
        _liveTradeId = String(payload.strategy_id || (trade && trade._id) || '').trim();

        host.innerHTML = '' +
            '<section class="px-4 py-8 pt-16 md:px-6 md:pt-20">' +
            '  <div class="mx-auto max-w-[1400px]">' +
            '    <div class="overflow-hidden rounded border border-primaryBlack-350 bg-primaryBlack-0">' +
            '      <div class="flex flex-col gap-2 bg-primaryBlack-900 px-4 py-3 text-xs text-primaryBlack-0 md:flex-row md:items-center md:justify-between">' +
            '        <div class="flex items-center gap-2">' +
            '          <span class="text-primaryBlack-200">Underlying:</span>' +
            '          <span class="font-medium uppercase">' + escapeHtml(underlyingLabel) + '</span>' +
            '          <span class="tabular-nums" id="trade-history-underlying-price">' + escapeHtml(String(underlyingPrice)) + '</span>' +
            (underlyingChange ? '<span class="tabular-nums text-green-300">' + escapeHtml(String(underlyingChange)) + '</span>' : '') +
            '        </div>' +
            '      </div>' +
            '      <div class="border-b border-primaryBlack-350 px-4 py-4">' +
            '        <div class="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">' +
            '          <div class="flex flex-wrap items-center gap-3">' +
            '            <div class="flex items-center gap-3">' +
            '              <span class="text-2xl leading-none text-primaryBlack-700">&#8592;</span>' +
            '              <span class="text-2xl font-semibold text-primaryBlack-800">' + escapeHtml(trade.name || trade._id || 'Strategy') + '</span>' +
            '            </div>' +
            '            <a href="' + escapeHtml(strategyLink) + '" target="_blank" rel="noopener noreferrer" class="inline-flex items-center gap-2 rounded border border-primaryBlack-350 px-3 py-2 text-xs font-medium text-primaryBlue-500">' +
            '              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:16px;height:16px"><path stroke-linecap="round" stroke-linejoin="round" d="M2.036 12.322a1.012 1.012 0 010-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178z"></path><path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>' +
            '              <span>View Strategy</span>' +
            '            </a>' +
            '          </div>' +
            '          <div class="flex flex-wrap items-center gap-2">' +
            '            <button type="button" class="rounded border border-primaryBlack-350 px-3 py-2 text-xs font-medium text-primaryBlack-700">Screenshot</button>' +
            '            <button type="button" class="rounded border border-secondaryBlue-500 px-3 py-2 text-xs font-medium text-secondaryBlue-500">Today\'s MTM Graph</button>' +
            '            <button type="button" class="rounded border border-secondaryBlue-500 px-3 py-2 text-xs font-medium text-secondaryBlue-500">Analyse</button>' +
            '            <button type="button" class="rounded border border-secondaryBlue-500 px-3 py-2 text-xs font-medium text-secondaryBlue-500">Replay</button>' +
            '          </div>' +
            '        </div>' +
            '      </div>' +
            '      <div class="px-4 py-4">' +
            '        <div class="flex flex-wrap items-start gap-x-10 gap-y-5 text-xs font-normal leading-4 md:leading-6">' +
            '        <p class="flex flex-col gap-1"><span>Status</span><span style="' + getStatusPillStyle(statusLabel) + '" class="inline-flex w-fit items-center rounded-sm border px-2 py-0.5 text-xs font-medium uppercase">' + escapeHtml(statusLabel) + '</span></p>' +
            '        <p class="flex flex-col gap-1"><span>Open Position</span><span class="text-sm font-medium text-primaryBlack-800">' + escapeHtml(String(summary.open_positions || 0)) + '</span></p>' +
            '        <div class="flex flex-col gap-1"><span>Broker</span><div class="flex items-center gap-2 text-sm font-medium text-primaryBlack-800">' +
            (brokerIconUrl ? '<img src="' + brokerIconUrl + '" alt="' + escapeHtml(brokerLabel) + '" style="width:20px;height:20px;object-fit:contain;" onerror="this.style.display=\'none\'">' : '') +
            '<span>' + escapeHtml(String(brokerLabel)) + '</span></div></div>' +
            '        <div class="relative flex flex-col gap-2"><div class="flex items-center gap-2"><span>Include Brokerage</span>' + createSwitchMarkup('toggle--result-brokerage_include_trade_history', false) + '</div><div id="brokerage-cost-trigger" class="flex items-center gap-2 px-1 py-1 opacity-50 text-primaryBlack-500"><span id="brokerage-cost-value" class="text-sm font-medium">0</span><button type="button" id="brokerage-edit-trigger" class="pointer-events-none text-primaryBlack-600" aria-expanded="false" aria-disabled="true"><svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" style="width:14px;height:14px"><path stroke-linecap="round" stroke-linejoin="round" d="M16.862 4.487l1.687-1.688a1.875 1.875 0 112.652 2.652L10.582 16.07a4.5 4.5 0 01-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 011.13-1.897l8.932-8.931zm0 0L19.5 7.125"></path></svg></button></div><div id="brokerage-popup" class="hidden absolute left-0 top-full z-20 mt-2 w-[260px] rounded-xl border border-primaryBlack-350 bg-white p-4 shadow-lg"><p class="text-sm font-semibold text-primaryBlack-800">Brokerage</p><div class="mt-3 grid gap-3"><input id="brokerage-rate-input" type="number" min="0" step="0.01" value="20" class="w-full rounded border border-primaryBlack-350 px-3 py-2 text-sm" placeholder="Amount"><select id="brokerage-rate-type" class="w-full rounded border border-primaryBlack-350 px-3 py-2 text-sm"><option value="per_order" selected>per order</option><option value="per_lot">per lot</option></select><button type="button" id="brokerage-popup-done" class="rounded bg-secondaryBlue-500 px-3 py-2 text-sm font-medium text-white">Done</button></div></div></div>' +
            '        <div class="relative flex flex-col gap-2"><div class="flex items-center gap-2"><span>Taxes &amp; charges</span>' + createSwitchMarkup('toggle--result_include_trade_history', false) + '</div><div id="taxes-cost-trigger" class="flex items-center gap-2 px-1 py-1 opacity-50 text-primaryBlack-500"><span id="taxes-cost-value" class="text-sm font-medium">0</span><button type="button" id="taxes-edit-trigger" class="pointer-events-none text-primaryBlack-600" aria-disabled="true"><svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" style="width:14px;height:14px"><path stroke-linecap="round" stroke-linejoin="round" d="M16.862 4.487l1.687-1.688a1.875 1.875 0 112.652 2.652L10.582 16.07a4.5 4.5 0 01-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 011.13-1.897l8.932-8.931zm0 0L19.5 7.125"></path></svg></button></div><div id="taxes-popup" class="hidden absolute left-0 top-full z-20 mt-2 w-[280px] rounded-xl border border-primaryBlack-350 bg-white p-4 shadow-lg"><p class="text-sm font-semibold text-primaryBlack-800">Taxes &amp; charges</p><div class="mt-3 space-y-2 text-sm text-primaryBlack-700"><div class="flex items-center justify-between"><span>STT</span><span id="taxes-stt">₹0</span></div><div class="flex items-center justify-between"><span>Exchange transaction charges</span><span id="taxes-exchange">₹0</span></div><div class="flex items-center justify-between"><span>Stamp charges</span><span id="taxes-stamp">₹0</span></div><div class="flex items-center justify-between"><span>SEBI turnover fees</span><span id="taxes-sebi">₹0</span></div><div class="flex items-center justify-between"><span>GST</span><span id="taxes-gst">₹0</span></div><div class="flex items-center justify-between border-t border-primaryBlack-350 pt-2 font-semibold text-primaryBlack-800"><span>Total</span><span id="taxes-total">₹0</span></div></div></div></div>' +
            '        <p class="flex flex-col gap-1"><span>Margin Blocked (approx)</span><span class="text-sm font-medium text-primaryBlack-800">' + escapeHtml(String(marginAmount)) + '</span></p>' +
            '        <p class="flex flex-col gap-1"><span>MTM</span><span id="trade-history-net-mtm" class="text-sm font-bold ' + getMoneyClass(summary.mtm) + '">' + escapeHtml(formatMoney(summary.mtm)) + '</span></p>' +
            '      </div>' +
            '    </div>' +
            '    <div class="mt-8">' +
            '      <div class="mb-3 flex items-center justify-between gap-3"><h2 class="text-xl font-medium">Open Legs</h2><p class="text-sm text-primaryBlack-600">Count: ' + escapeHtml(String(summary.open_positions || 0)) + '</p></div>' +
            '      <div id="trade-history-open-legs-table">' + renderLegRows(Array.isArray(legs.open) ? legs.open : []) + '</div>' +
            '    </div>' +
            '    <div class="mt-8">' +
            '      <div class="mb-3 flex items-center justify-between gap-3"><h2 class="text-xl font-medium">Closed Legs</h2><p class="text-sm text-primaryBlack-600">Count: ' + escapeHtml(String(summary.closed_positions || 0)) + ' | Pending: ' + escapeHtml(String(summary.pending_positions || 0)) + '</p></div>' +
            renderLegRows(Array.isArray(legs.closed) ? legs.closed : []) +
            '    </div>' +
            '    <div class="mt-8">' +
            '      <div class="mb-3 flex items-center justify-between gap-3"><h2 class="text-xl font-medium">Broker Orders</h2><p class="text-sm text-primaryBlack-600">Total: ' + escapeHtml(String(summary.broker_orders_count || 0)) + '</p></div>' +
            renderOrders(orders) +
            '    </div>' +
            '    <div class="mt-8">' +
            '      <div class="mb-3 flex flex-wrap items-center justify-between gap-3"><h2 class="text-xl font-medium">Notification Status</h2><div class="flex flex-wrap gap-2">' + notificationChips + '</div></div>' +
            '      <div class="mb-4 flex flex-wrap gap-2" id="notification-filter-tabs">' +
            '        <button type="button" class="rounded-full border border-blue-500 bg-blue-50 px-4 py-1.5 text-sm font-medium text-blue-600" data-notification-filter="all">All (' + escapeHtml(String(notificationCounts.all)) + ')</button>' +
            '        <button type="button" class="rounded-full border border-primaryBlack-350 bg-primaryBlack-0 px-4 py-1.5 text-sm font-medium text-primaryBlack-700" data-notification-filter="info">Info (' + escapeHtml(String(notificationCounts.info)) + ')</button>' +
            '        <button type="button" class="rounded-full border border-primaryBlack-350 bg-primaryBlack-0 px-4 py-1.5 text-sm font-medium text-primaryBlack-700" data-notification-filter="critical">Critical (' + escapeHtml(String(notificationCounts.critical)) + ')</button>' +
            '        <button type="button" class="rounded-full border border-primaryBlack-350 bg-primaryBlack-0 px-4 py-1.5 text-sm font-medium text-primaryBlack-700" data-notification-filter="error">Error (' + escapeHtml(String(notificationCounts.error)) + ')</button>' +
            '      </div>' +
            '      <div id="notification-list-root" data-notifications=\'' + escapeHtml(JSON.stringify(notifications)) + '\'>' + renderNotifications(notifications) + '</div>' +
            '    </div>' +
            '  </div>' +
            '</section>';

        var filterHost = document.getElementById('notification-filter-tabs');
        var listRoot = document.getElementById('notification-list-root');
        if (filterHost && listRoot) {
            filterHost.addEventListener('click', function (event) {
                var button = event.target.closest('[data-notification-filter]');
                if (!button) {
                    return;
                }
                var selected = String(button.getAttribute('data-notification-filter') || 'all');
                var filtered = selected === 'all'
                    ? notifications
                    : notifications.filter(function (item) { return item.severity === selected; });
                listRoot.innerHTML = renderNotifications(filtered);
                Array.prototype.forEach.call(filterHost.querySelectorAll('[data-notification-filter]'), function (tab) {
                    var isActive = tab === button;
                    tab.className = isActive
                        ? 'rounded-full border border-blue-500 bg-blue-50 px-4 py-1.5 text-sm font-medium text-blue-600'
                        : 'rounded-full border border-primaryBlack-350 bg-primaryBlack-0 px-4 py-1.5 text-sm font-medium text-primaryBlack-700';
                });
            });
        }
        initTradeHistoryCostControls(payload);
        connectTradeHistorySockets(payload);
    }

    function renderState(message) {
        var host = document.getElementById('body-content');
        if (!host) {
            return;
        }
        host.innerHTML = '<section class="px-4 py-10 md:px-6"><div class="mx-auto max-w-[900px] rounded border border-primaryBlack-350 bg-primaryBlack-0 px-6 py-10 text-center text-sm text-primaryBlack-700">' + escapeHtml(message) + '</div></section>';
    }

    function init() {
        var params = new URLSearchParams(window.location.search || '');
        var strategyId = String(params.get('strategy_id') || '').trim();
        var status = String(params.get('status') || 'algo-backtest').trim() || 'algo-backtest';

        if (!strategyId) {
            renderState('strategy_id is missing in the URL.');
            return;
        }

        renderState('Loading strategy trade history...');
        fetch(buildApiUrl('strategy-trade-history/' + encodeURIComponent(strategyId) + '?status=' + encodeURIComponent(status)))
            .then(function (response) {
                if (!response.ok) {
                    throw new Error('Failed to load strategy trade history');
                }
                return response.json();
            })
            .then(function (payload) {
                renderPage(payload || {});
            })
            .catch(function (error) {
                renderState(error && error.message ? error.message : 'Failed to load strategy trade history.');
            });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    window.addEventListener('beforeunload', function () {
        var sockets = window.StrategyTradeHistorySockets || {};
        Object.keys(sockets).forEach(function (key) {
            var channelSocket = sockets[key];
            if (channelSocket && typeof channelSocket.close === 'function') {
                channelSocket.close();
            }
        });
    });
}());
