function getResolvedStrategyApiBaseUrl() {
    if (typeof window.resolveAlgoApiBaseUrl === 'function') {
        return window.resolveAlgoApiBaseUrl();
    }
    var baseUrl = (window.APP_CONFIG && window.APP_CONFIG.algoApiBaseUrl)
        || window.APP_ALGO_API_BASE_URL
        || window.APP_LOCAL_ALGO_API_BASE_URL
        || '';
    var normalizedBaseUrl = String(baseUrl || '').trim().replace(/\/+$/, '');
    if (normalizedBaseUrl) {
        return normalizedBaseUrl;
    }
    if (window.location && window.location.protocol === 'file:') {
        return 'http://localhost:8000/algo';
    }
    if (window.location && /^https?:$/i.test(window.location.protocol || '') && window.location.origin) {
        return window.location.origin.replace(/\/+$/, '') + '/algo';
    }
    return 'http://localhost:8000/algo';
}

function getStrategyApiUrl(routeName, suffix) {
    if (typeof window.buildNamedApiUrl === 'function') {
        return window.buildNamedApiUrl(routeName, suffix);
    }
    var routeMap = window.APP_API_ROUTES || {};
    var routePath = routeMap[routeName] || routeName || '';
    var normalizedRoute = String(routePath).replace(/\/+$/, '');
    var normalizedSuffix = String(suffix || '').replace(/^\/+/, '');
    return getResolvedStrategyApiBaseUrl() + '/' + (normalizedSuffix ? normalizedRoute + '/' + normalizedSuffix : normalizedRoute);
}

(function () {
    const toggleLabels = Array.from(document.querySelectorAll('label[for^="toggle--input__"]'));
    const groupMap = new Map();
    const rangeBreakoutSwitch = document.getElementById('handle_orb_non_modal_to0lxiq1');
    const rangeBreakoutModal = document.getElementById('range-breakout-modal');
    const rangeBreakoutConfirm = document.getElementById('range-breakout-confirm');
    const rangeBreakoutCancel = document.getElementById('range-breakout-cancel');
    const rangeBreakoutClose = document.getElementById('range-breakout-close');
    const rangeBreakoutEndTime = document.getElementById('range-breakout-end-time');
    const mainRangeBreakoutTime = document.getElementById('main-range-breakout-time');
    const mainRangeBreakoutTracking = document.getElementById('main-range-breakout-tracking');
    const mainRangeBreakoutHigh = document.getElementById('toggle--input__0_01KMJMN7ZS6DPN25Y0XYS923HG');
    const mainRangeBreakoutLow = document.getElementById('toggle--input__1_01KMJMN7ZS6DPN25Y0XYS923HG');
    const rangeBreakoutStartTime = document.getElementById('range-breakout-start-time');
    const rangeBreakoutEntryButtons = [
        document.getElementById('range-breakout-entry-high'),
        document.getElementById('range-breakout-entry-low')
    ].filter(Boolean);
    const rangeBreakoutTrackingButtons = [
        document.getElementById('range-breakout-tracking-strike'),
        document.getElementById('range-breakout-tracking-underlying')
    ].filter(Boolean);

    toggleLabels.forEach((label) => {
        const input = document.getElementById(label.htmlFor);
        if (!input || input.type !== 'radio') return;

        const group = label.parentElement;
        if (!groupMap.has(group)) {
            groupMap.set(group, []);
        }

        groupMap.get(group).push({ label, input });
    });

    groupMap.forEach((items, groupIndex) => {
        const groupName = 'toggle-group-' + groupIndex;

        items.forEach(({ input }) => {
            if (!input.name) {
                input.name = groupName;
            }
        });

        function syncGroupState() {
            items.forEach(({ label, input }) => {
                const isChecked = input.checked;
                const isRangeBreakoutButton = label.dataset.testid === 'High-button' || label.dataset.testid === 'Low-button';

                if (isRangeBreakoutButton) {
                    label.classList.toggle('!bg-secondaryBlue-50', isChecked);
                    label.classList.toggle('!font-bold', isChecked);
                    label.classList.toggle('!text-secondaryBlue-500', isChecked);
                    return;
                }

                label.classList.toggle('bg-tertiaryBlue-500', isChecked);
                label.classList.toggle('text-white', isChecked);
            });
        }

        items.forEach(({ label, input }) => {
            label.addEventListener('click', function () {
                items.forEach((item) => {
                    item.input.checked = item.input === input;
                });
                syncGroupState();
            });

            input.addEventListener('change', syncGroupState);
        });

        syncGroupState();
    });

    const switches = Array.from(document.querySelectorAll('button[role="switch"]'));

    function getControlledBlock(button) {
        let current = button.parentElement;

        while (current && current !== document.body) {
            const nextBlock = current.nextElementSibling;
            if (nextBlock) {
                return nextBlock;
            }
            current = current.parentElement;
        }

        return null;
    }

    function syncSwitchState(button) {
        const enabled = button.getAttribute('aria-checked') === 'true';
        const knob = button.querySelector('span');
        const controlledBlock = getControlledBlock(button);

        button.classList.toggle('bg-secondaryBlue-500', enabled);
        button.classList.toggle('bg-primaryBlack-600', !enabled);
        button.setAttribute('data-headlessui-state', enabled ? 'checked' : '');

        if (knob) {
            knob.classList.toggle('translate-x-5', enabled);
            knob.classList.toggle('translate-x-1', !enabled);
        }

        if (controlledBlock) {
            controlledBlock.classList.toggle('pointer-events-none', !enabled);
            controlledBlock.classList.toggle('opacity-40', !enabled);
        }
    }

    function setSwitchState(button, enabled) {
        button.setAttribute('aria-checked', String(enabled));
        syncSwitchState(button);
    }

    function openRangeBreakoutModal() {
        if (rangeBreakoutModal) {
            rangeBreakoutModal.classList.remove('hidden');
            rangeBreakoutModal.style.display = 'flex';
        }
    }

    function closeRangeBreakoutModal() {
        if (rangeBreakoutModal) {
            rangeBreakoutModal.classList.add('hidden');
            rangeBreakoutModal.style.display = 'none';
        }
    }

    function syncRangeBreakoutOptionButtons(buttons) {
        buttons.forEach((button) => {
            const selected = button.getAttribute('data-selected') === 'true';
            button.style.background = selected ? '#dbeeff' : '#ffffff';
            button.style.color = selected ? '#1171cc' : '#2f2f2f';
            button.style.fontWeight = selected ? '600' : '400';
        });
    }

    function setupRangeBreakoutOptionButtons(buttons) {
        syncRangeBreakoutOptionButtons(buttons);
        buttons.forEach((button) => {
            button.addEventListener('click', function () {
                buttons.forEach((item) => {
                    item.setAttribute('data-selected', String(item === button));
                });
                syncRangeBreakoutOptionButtons(buttons);
            });
        });
    }

    if (rangeBreakoutStartTime) {
        rangeBreakoutStartTime.disabled = true;
    }

    setupRangeBreakoutOptionButtons(rangeBreakoutEntryButtons);
    setupRangeBreakoutOptionButtons(rangeBreakoutTrackingButtons);

    const brokerageToggle = document.getElementById('toggle--result-brokerage_include_backtest');
    const taxesToggle = document.getElementById('toggle--result_include_backtest');
    const brokerageTrigger = document.getElementById('brokerage-cost-trigger');
    const brokerageEditTrigger = document.getElementById('brokerage-edit-trigger');
    const brokeragePopup = document.getElementById('brokerage-popup');
    const brokerageRateInput = document.getElementById('brokerage-rate-input');
    const brokerageRateType = document.getElementById('brokerage-rate-type');
    const brokerageDone = document.getElementById('brokerage-popup-done');
    const brokerageCostValue = document.getElementById('brokerage-cost-value');

    function getCurrentVisibleTrades() {
        if (Array.isArray(window.frAllTrades) && window.frAllTrades.length) return window.frAllTrades;
        if (Array.isArray(window._rawTrades)) return window._rawTrades;
        return [];
    }

    function getLegExecutionCount(leg) {
        if (!leg) return 0;
        if (Array.isArray(leg.sub_trades) && leg.sub_trades.length) return leg.sub_trades.length;
        return (leg.entry_time || leg.exit_time || leg.entry_price !== undefined || leg.exit_price !== undefined) ? 1 : 0;
    }

    function getLegLotsTraded(leg) {
        if (!leg) return 0;
        var baseLots = Number(leg.lots || 0) || 0;
        var executions = getLegExecutionCount(leg);
        if (Array.isArray(leg.sub_trades) && leg.sub_trades.length) {
            return leg.sub_trades.reduce(function (sum, subTrade) {
                var subLots = Number(subTrade && subTrade.lots || 0) || baseLots || 1;
                return sum + subLots;
            }, 0);
        }
        if (executions > 0) return baseLots || 1;
        return 0;
    }

    function getTradeLotCount(trade) {
        var totalLots = (trade.legs || []).reduce(function (sum, leg) {
            return sum + getLegLotsTraded(leg);
        }, 0);
        return totalLots || 1;
    }

    function getTradeOrderCount(trade) {
        var totalOrders = (trade.legs || []).reduce(function (sum, leg) {
            return sum + getLegExecutionCount(leg);
        }, 0);
        return totalOrders || 1;
    }

    function getDetailBrokerage(leg, subTrade) {
        if (!brokerageToggle || brokerageToggle.getAttribute('aria-checked') !== 'true') return 0;
        var rate = Number(brokerageRateInput && brokerageRateInput.value) || 0;
        if (rate <= 0) return 0;
        var rateType = brokerageRateType ? brokerageRateType.value : 'per_order';
        if (rateType === 'per_lot') {
            var detailLots = Number(subTrade && subTrade.lots || 0) || Number(leg && leg.lots || 0) || 1;
            return rate * detailLots;
        }
        return rate;
    }

    function getTradeBrokerage(trade) {
        if (!brokerageToggle || brokerageToggle.getAttribute('aria-checked') !== 'true') return 0;
        var rate = Number(brokerageRateInput && brokerageRateInput.value) || 0;
        if (rate <= 0) return 0;
        var rateType = brokerageRateType ? brokerageRateType.value : 'per_order';
        if (rateType === 'per_lot') {
            return rate * getTradeLotCount(trade);
        }
        return rate * getTradeOrderCount(trade);
    }

    function computeTotalBrokerage(trades) {
        return (trades || []).reduce(function (sum, trade) {
            return sum + getTradeBrokerage(trade);
        }, 0);
    }

    window._getTradeBrokerage = getTradeBrokerage;
    window._getDetailBrokerage = getDetailBrokerage;
    window._syncBrokerageCost = syncBrokerageCost;

    function closeBrokeragePopup() {
        if (!brokeragePopup) return;
        brokeragePopup.classList.add('hidden');
        brokeragePopup.style.display = 'none';
        if (brokerageEditTrigger) brokerageEditTrigger.setAttribute('aria-expanded', 'false');
    }

    function openBrokeragePopup() {
        if (!brokeragePopup || !brokerageToggle) return;
        if (brokerageToggle.getAttribute('aria-checked') !== 'true') return;
        brokeragePopup.classList.remove('hidden');
        brokeragePopup.style.display = 'block';
        if (brokerageEditTrigger) brokerageEditTrigger.setAttribute('aria-expanded', 'true');
    }

    function syncBrokerageTriggerState() {
        if (!brokerageToggle || !brokerageTrigger) return;
        var enabled = brokerageToggle.getAttribute('aria-checked') === 'true';
        brokerageTrigger.classList.toggle('opacity-50', !enabled);
        brokerageTrigger.classList.toggle('opacity-100', enabled);
        if (brokerageEditTrigger) {
            brokerageEditTrigger.classList.toggle('pointer-events-none', !enabled);
            brokerageEditTrigger.setAttribute('aria-disabled', String(!enabled));
        }
    }

    function syncBrokerageCost() {
        if (!brokerageCostValue) return;
        var totalCost = computeTotalBrokerage(getCurrentVisibleTrades());
        brokerageCostValue.textContent = Number(totalCost || 0).toLocaleString('en-IN', {
            minimumFractionDigits: Number.isInteger(totalCost || 0) ? 0 : 2,
            maximumFractionDigits: 2
        });
    }

    if (brokerageEditTrigger) {
        brokerageEditTrigger.addEventListener('click', function (event) {
            event.preventDefault();
            event.stopPropagation();
            if (!brokerageToggle || brokerageToggle.getAttribute('aria-checked') !== 'true') return;
            if (brokeragePopup && brokeragePopup.style.display === 'block') {
                closeBrokeragePopup();
            } else {
                openBrokeragePopup();
            }
        });
    }

    if (brokerageDone) {
        brokerageDone.addEventListener('click', function (event) {
            event.preventDefault();
            syncBrokerageTriggerState();
            closeBrokeragePopup();
            if (typeof window._refreshAllSections === 'function') {
                window._refreshAllSections();
            }
            syncBrokerageCost();
        });
    }

    if (brokerageRateInput) {
        brokerageRateInput.addEventListener('input', syncBrokerageCost);
    }

    if (brokerageRateType) {
        brokerageRateType.addEventListener('change', function () {
            syncBrokerageCost();
            if (typeof window._refreshAllSections === 'function') {
                window._refreshAllSections();
            }
        });
    }

    document.addEventListener('click', function (event) {
        if (!brokeragePopup || !brokerageEditTrigger) return;
        if (brokeragePopup.classList.contains('hidden')) return;
        if (brokeragePopup.contains(event.target) || brokerageEditTrigger.contains(event.target)) return;
        closeBrokeragePopup();
    });

    switches.forEach((button) => {
        syncSwitchState(button);

        button.addEventListener('click', function () {
            const nextState = button.getAttribute('aria-checked') !== 'true';

            if (button === rangeBreakoutSwitch && nextState) {
                openRangeBreakoutModal();
                return;
            }

            setSwitchState(button, nextState);

            if (button === brokerageToggle) {
                syncBrokerageTriggerState();
                if (!nextState) {
                    closeBrokeragePopup();
                }
                if (typeof window._refreshAllSections === 'function') {
                    window._refreshAllSections();
                }
                syncBrokerageCost();
            }
            if (button === taxesToggle) {
                syncTaxesTriggerState();
                if (!nextState) closeTaxesPopup();
                if (typeof window._refreshAllSections === 'function') {
                    window._refreshAllSections();
                }
                syncTaxesCost();
            }
        });
    });

    // ── TAXES ─────────────────────────────────────────────────────────────
    const taxesTrigger = document.getElementById('taxes-cost-trigger');
    const taxesEditTrigger = document.getElementById('taxes-edit-trigger');
    const taxesPopup = document.getElementById('taxes-popup');
    const taxesCostVal = document.getElementById('taxes-cost-value');

    // NSE F&O standard rates
    var NSE_STT = 0.001;     // 0.1%  sell side
    var NSE_EXCHANGE = 0.00053;   // 0.053% both sides
    var NSE_STAMP = 0.00003;   // 0.003% buy side
    var NSE_SEBI = 0.0000010; // 0.0001% both sides
    var NSE_GST = 0.18;      // 18% on exchange + SEBI

    function calcTradeTexes(trade) {
        var stt = 0, exchange = 0, stamp = 0, sebi = 0, gst = 0;
        (trade.legs || []).forEach(function (leg) {
            var lots = Number(leg.lots || 1);
            var lotSize = Number(leg.lot_size || 1);
            var subList = leg.sub_trades && leg.sub_trades.length ? leg.sub_trades : [null];
            var isSell = (leg.position || 'Sell').toLowerCase() !== 'buy';

            subList.forEach(function (st) {
                var stLots = st && st.lots ? Number(st.lots) : lots;
                var qty = stLots * lotSize;
                var entryPx = Number(st && st.entry_price !== undefined ? st.entry_price : (leg.entry_price || 0));
                var exitPx = Number(st && st.exit_price !== undefined ? st.exit_price : (leg.exit_price || 0));

                // Entry side
                var entryTov = entryPx * qty;
                if (isSell) stt += entryTov * NSE_STT;
                else stamp += entryTov * NSE_STAMP;
                exchange += entryTov * NSE_EXCHANGE;
                sebi += entryTov * NSE_SEBI;

                // Exit side (opposite of entry)
                var exitTov = exitPx * qty;
                if (!isSell) stt += exitTov * NSE_STT;
                else stamp += exitTov * NSE_STAMP;
                exchange += exitTov * NSE_EXCHANGE;
                sebi += exitTov * NSE_SEBI;
            });
        });
        gst = (exchange + sebi) * NSE_GST;
        return {
            stt: stt, exchange: exchange, stamp: stamp, sebi: sebi, gst: gst,
            total: stt + exchange + stamp + sebi + gst
        };
    }

    function getTradeTexes(trade) {
        if (!taxesToggle || taxesToggle.getAttribute('aria-checked') !== 'true') return 0;
        return calcTradeTexes(trade).total;
    }
    window._getTradeTexes = getTradeTexes;

    function fmtTax(v) {
        return '₹' + Number(v || 0).toLocaleString('en-IN', { minimumFractionDigits: 0, maximumFractionDigits: 2 });
    }

    function syncTaxesCost() {
        var trades = (window.frAllTrades && window.frAllTrades.length)
            ? window.frAllTrades
            : (window._rawTrades || []);
        var agg = { stt: 0, exchange: 0, stamp: 0, sebi: 0, gst: 0, total: 0 };
        trades.forEach(function (t) {
            var b = calcTradeTexes(t);
            agg.stt += b.stt;
            agg.exchange += b.exchange;
            agg.stamp += b.stamp;
            agg.sebi += b.sebi;
            agg.gst += b.gst;
            agg.total += b.total;
        });
        var enabled = taxesToggle && taxesToggle.getAttribute('aria-checked') === 'true';
        if (taxesCostVal) {
            taxesCostVal.textContent = enabled
                ? Number(agg.total).toLocaleString('en-IN', { minimumFractionDigits: 0, maximumFractionDigits: 0 })
                : '0';
        }
        var ids = {
            'taxes-stt': agg.stt, 'taxes-exchange': agg.exchange,
            'taxes-stamp': agg.stamp, 'taxes-sebi': agg.sebi,
            'taxes-gst': agg.gst, 'taxes-total': agg.total
        };
        Object.keys(ids).forEach(function (id) {
            var el = document.getElementById(id);
            if (el) el.textContent = enabled ? fmtTax(ids[id]) : '₹0';
        });
    }
    window._syncTaxesCost = syncTaxesCost;

    function syncTaxesTriggerState() {
        if (!taxesToggle || !taxesTrigger) return;
        var enabled = taxesToggle.getAttribute('aria-checked') === 'true';
        taxesTrigger.classList.toggle('opacity-50', !enabled);
        taxesTrigger.classList.toggle('opacity-100', enabled);
        if (taxesEditTrigger) {
            taxesEditTrigger.classList.toggle('pointer-events-none', !enabled);
            taxesEditTrigger.setAttribute('aria-disabled', String(!enabled));
        }
    }

    function openTaxesPopup() {
        if (!taxesPopup) return;
        syncTaxesCost();
        taxesPopup.classList.remove('hidden');
        taxesPopup.style.display = 'block';
    }
    function closeTaxesPopup() {
        if (!taxesPopup) return;
        taxesPopup.classList.add('hidden');
        taxesPopup.style.display = 'none';
    }

    if (taxesEditTrigger) {
        taxesEditTrigger.addEventListener('click', function (e) {
            e.preventDefault();
            e.stopPropagation();
            if (!taxesToggle || taxesToggle.getAttribute('aria-checked') !== 'true') return;
            taxesPopup && taxesPopup.style.display === 'block' ? closeTaxesPopup() : openTaxesPopup();
        });
    }

    document.addEventListener('click', function (e) {
        if (!taxesPopup || taxesPopup.classList.contains('hidden')) return;
        if (taxesEditTrigger && taxesEditTrigger.contains(e.target)) return;
        if (taxesPopup.contains(e.target)) return;
        closeTaxesPopup();
    });

    syncTaxesCost();

    // ── SLIPPAGE ──────────────────────────────────────────────────────────
    var slippageInput = document.getElementById('slippage-input');
    var slippageRecalcBtn = document.getElementById('slippage-recalculate-btn');

    function getTradeSlippage(trade) {
        var rate = Number(slippageInput ? slippageInput.value : 0) || 0;
        if (rate <= 0) return 0;
        var slipRate = rate / 100;
        var cost = 0;
        (trade.legs || []).forEach(function (leg) {
            var lots = Number(leg.lots || 1);
            var lotSize = Number(leg.lot_size || 1);
            var subList = leg.sub_trades && leg.sub_trades.length ? leg.sub_trades : [null];
            subList.forEach(function (st) {
                var stLots = st && st.lots ? Number(st.lots) : lots;
                var qty = stLots * lotSize;
                var entryPx = Number(st && st.entry_price !== undefined ? st.entry_price : (leg.entry_price || 0));
                var exitPx = Number(st && st.exit_price !== undefined ? st.exit_price : (leg.exit_price || 0));
                // slippage applies to both buy and sell sides: (entry + exit) × rate × qty
                cost += (entryPx + exitPx) * slipRate * qty;
            });
        });
        return cost;
    }
    window._getTradeSlippage = getTradeSlippage;

    if (slippageRecalcBtn) {
        slippageRecalcBtn.addEventListener('click', function () {
            if (typeof window._refreshAllSections === 'function') {
                window._refreshAllSections();
            }
        });
    }

    syncBrokerageCost();

    if (rangeBreakoutConfirm && rangeBreakoutSwitch) {
        rangeBreakoutConfirm.addEventListener('click', function () {
            applyRangeBreakoutPopupValues();
            setSwitchState(rangeBreakoutSwitch, true);
            closeRangeBreakoutModal();
        });
    }

    function applyRangeBreakoutPopupValues() {
        if (mainRangeBreakoutTime && rangeBreakoutEndTime) {
            mainRangeBreakoutTime.value = rangeBreakoutEndTime.value;
        }

        const entrySelection = rangeBreakoutEntryButtons.find((button) => button.getAttribute('data-selected') === 'true');
        if (entrySelection) {
            if (entrySelection.id === 'range-breakout-entry-high' && mainRangeBreakoutHigh) {
                mainRangeBreakoutHigh.checked = true;
            }
            if (entrySelection.id === 'range-breakout-entry-low' && mainRangeBreakoutLow) {
                mainRangeBreakoutLow.checked = true;
            }
        }

        const trackingSelection = rangeBreakoutTrackingButtons.find((button) => button.getAttribute('data-selected') === 'true');
        if (trackingSelection && mainRangeBreakoutTracking) {
            mainRangeBreakoutTracking.value = trackingSelection.id === 'range-breakout-tracking-underlying' ? 'true' : 'false';
        }

        const rangeBreakoutLabels = Array.from(document.querySelectorAll('[data-testid="High-button"], [data-testid="Low-button"]'));
        rangeBreakoutLabels.forEach((label) => {
            const input = document.getElementById(label.htmlFor);
            if (!input) return;
            const isChecked = input.checked;
            label.classList.toggle('!bg-secondaryBlue-50', isChecked);
            label.classList.toggle('!font-bold', isChecked);
            label.classList.toggle('!text-secondaryBlue-500', isChecked);
        });
    }

    function cancelRangeBreakout() {
        if (!rangeBreakoutSwitch) return;
        setSwitchState(rangeBreakoutSwitch, false);
        closeRangeBreakoutModal();
    }

    if (rangeBreakoutCancel) {
        rangeBreakoutCancel.addEventListener('click', cancelRangeBreakout);
    }

    if (rangeBreakoutClose) {
        rangeBreakoutClose.addEventListener('click', cancelRangeBreakout);
    }

    if (rangeBreakoutModal) {
        rangeBreakoutModal.addEventListener('click', function (event) {
            if (event.target === rangeBreakoutModal) {
                cancelRangeBreakout();
            }
        });
    }
})();

(function () {
    const addLegButton = document.getElementById('add-leg');
    const totalLotInput = document.getElementById('total-lot-input');
    const legTemplate = document.getElementById('backtest');
    const legsHeading = document.getElementById('legs-heading');
    const legBuilder = document.getElementById('leg-builder');
    if (!addLegButton || !legTemplate || !legsHeading || !legBuilder) return;

    const legsContainer = document.createElement('div');
    legsContainer.id = 'legs-container';
    legsContainer.style.display = 'flex';
    legsContainer.style.flexDirection = 'column';
    legsContainer.style.gap = '16px';
    legTemplate.parentNode.insertBefore(legsContainer, legTemplate.nextSibling);

    let legCount = 0;

    function rewireCloneIds(clone, suffix) {
        const idMap = new Map();
        clone.querySelectorAll('[id]').forEach((node) => {
            const oldId = node.id;
            const newId = oldId + suffix;
            idMap.set(oldId, newId);
            node.id = newId;
        });

        clone.querySelectorAll('[for]').forEach((node) => {
            const oldFor = node.getAttribute('for');
            if (idMap.has(oldFor)) {
                node.setAttribute('for', idMap.get(oldFor));
            }
        });
    }

    function syncLegFromBuilder(clone) {
        const builderSelected = {
            position: legBuilder.querySelector('#radio_Position input[type="radio"]:checked'),
            optionType: legBuilder.querySelector('#radio_OptionType input[type="radio"]:checked'),
            expiry: legBuilder.querySelector('#select_Expiry select'),
            strikeCriteria: legBuilder.querySelector('#select_StrikeType select'),
            strikeType: legBuilder.querySelector('#select_ select'),
        };

        const clonePositionInputs = clone.querySelectorAll('#radio_Position input[type="radio"]');
        clonePositionInputs.forEach((input) => {
            input.checked = !!builderSelected.position && input.value === builderSelected.position.value;
        });

        const cloneOptionTypeInputs = clone.querySelectorAll('#radio_OptionType input[type="radio"]');
        cloneOptionTypeInputs.forEach((input) => {
            input.checked = !!builderSelected.optionType && input.value === builderSelected.optionType.value;
        });

        const cloneExpiry = clone.querySelector('#select_Expiry select');
        if (cloneExpiry && builderSelected.expiry) {
            cloneExpiry.value = builderSelected.expiry.value;
        }

        const cloneStrikeCriteria = clone.querySelector('select[title="Select Strike Criteria"]');
        if (cloneStrikeCriteria && builderSelected.strikeCriteria) {
            cloneStrikeCriteria.value = builderSelected.strikeCriteria.value;
        }

        const cloneStrikeType = clone.querySelector('select[title=""]');
        if (cloneStrikeType && builderSelected.strikeType) {
            cloneStrikeType.value = builderSelected.strikeType.value;
        }
    }

    function initializeCloneInteractions(clone) {
        const radioLabels = Array.from(clone.querySelectorAll('label[for^="toggle--input__"]'));
        const radioGroups = new Map();

        radioLabels.forEach((label) => {
            const input = clone.querySelector('#' + CSS.escape(label.htmlFor));
            if (!input || input.type !== 'radio') return;
            const group = label.parentElement;
            if (!radioGroups.has(group)) {
                radioGroups.set(group, []);
            }
            radioGroups.get(group).push({ label, input });
        });

        radioGroups.forEach((items, index) => {
            const groupName = 'clone-toggle-group-' + legCount + '-' + index;
            items.forEach(({ input }) => {
                input.name = groupName;
            });

            function syncGroupState() {
                items.forEach(({ label, input }) => {
                    const checked = input.checked;
                    label.classList.toggle('bg-tertiaryBlue-500', checked);
                    label.classList.toggle('text-white', checked);
                });
            }

            items.forEach(({ label, input }) => {
                label.addEventListener('click', function () {
                    items.forEach((item) => {
                        item.input.checked = item.input === input;
                    });
                    syncGroupState();
                });
                input.addEventListener('change', syncGroupState);
            });

            syncGroupState();
        });

        function getCloneControlledBlock(button) {
            let current = button.parentElement;
            while (current && current !== clone) {
                const nextBlock = current.nextElementSibling;
                if (nextBlock) {
                    return nextBlock;
                }
                current = current.parentElement;
            }
            return null;
        }

        clone.querySelectorAll('button[role="switch"]').forEach((button) => {
            const knob = button.querySelector('span');
            const controlledBlock = getCloneControlledBlock(button);

            function syncCloneSwitchState() {
                const current = button.getAttribute('aria-checked') === 'true';
                button.classList.toggle('bg-secondaryBlue-500', current);
                button.classList.toggle('bg-primaryBlack-600', !current);
                if (knob) {
                    knob.classList.toggle('translate-x-5', current);
                    knob.classList.toggle('translate-x-1', !current);
                }
                if (controlledBlock) {
                    controlledBlock.classList.toggle('pointer-events-none', !current);
                    controlledBlock.classList.toggle('opacity-40', !current);
                }
            }

            syncCloneSwitchState();

            button.addEventListener('click', function () {
                const enabled = button.getAttribute('aria-checked') !== 'true';
                button.setAttribute('aria-checked', String(enabled));
                syncCloneSwitchState();
            });
        });
    }

    function copyLegValues(source, clone) {
        // Copy input values (.value and .checked are not cloned by cloneNode)
        const srcInputs = Array.from(source.querySelectorAll('input'));
        const clnInputs = Array.from(clone.querySelectorAll('input'));
        srcInputs.forEach(function (src, i) {
            if (!clnInputs[i]) return;
            if (src.type === 'radio' || src.type === 'checkbox') {
                clnInputs[i].checked = src.checked;
            } else {
                clnInputs[i].value = src.value;
            }
        });
        // Copy select values
        const srcSelects = Array.from(source.querySelectorAll('select'));
        const clnSelects = Array.from(clone.querySelectorAll('select'));
        srcSelects.forEach(function (src, i) {
            if (clnSelects[i]) clnSelects[i].value = src.value;
        });
    }

    addLegButton.addEventListener('click', function () {
        legCount += 1;
        const clone = legTemplate.cloneNode(true);
        clone.style.display = '';
        clone.id = 'backtest-leg-' + legCount;
        rewireCloneIds(clone, '__leg' + legCount);

        const title = clone.querySelector('h3');
        if (title) {
            title.textContent = '# ' + legCount;
        }

        const lotInput = clone.querySelector('input[type="number"]');
        if (lotInput) {
            lotInput.value = totalLotInput && totalLotInput.value ? totalLotInput.value : '1';
        }

        syncLegFromBuilder(clone);
        initializeCloneInteractions(clone);
        legsHeading.style.display = '';
        legsContainer.appendChild(clone);
    });

    legsContainer.addEventListener('click', function (e) {
        // Remove button: bg-red-500 rounded-full
        const removeBtn = e.target.closest('button.bg-red-500.rounded-full');
        if (removeBtn) {
            const leg = removeBtn.closest('[id^="backtest-leg-"]');
            if (leg && leg.parentNode === legsContainer) {
                leg.remove();
                if (legsContainer.children.length === 0) {
                    legsHeading.style.display = 'none';
                }
            }
            return;
        }

        // Copy button: bg-primaryBlack-600 with border-2 (distinguishes from toggle switches)
        const copyBtn = e.target.closest('button.bg-primaryBlack-600.border-2');
        if (copyBtn) {
            const sourceLeg = copyBtn.closest('[id^="backtest-leg-"]');
            if (!sourceLeg) return;
            legCount += 1;
            const randomName = Math.floor(Math.random() * 9000) + 1000;
            const clone = sourceLeg.cloneNode(true);
            clone.id = 'backtest-leg-' + legCount;
            rewireCloneIds(clone, '__leg' + legCount);
            const title = clone.querySelector('h3');
            if (title) title.textContent = '# ' + randomName;
            copyLegValues(sourceLeg, clone);
            initializeCloneInteractions(clone);
            legsHeading.style.display = '';
            legsContainer.appendChild(clone);
        }
    });
})();

(function () {
    /* ── Trailing Options select → show/hide sub-sections ── */
    var trailEl = document.getElementById('TrailingOptions_trailing-options');
    if (!trailEl) return;
    var trailSel = trailEl.querySelector('select');
    if (!trailSel) return;

    var lockGrid = document.getElementById('trailing-lock-grid');
    var overallTrailSec = document.getElementById('overall-trail-sl-section');
    var increaseLabel = document.getElementById('trailing-increase-label');
    var increaseWrap = document.getElementById('trailing-increase-wrap');
    var trailLabel = document.getElementById('trailing-trail-label');
    var trailWrap = document.getElementById('trailing-trail-wrap');
    var trailRow = document.getElementById('trailing-lock-trail-row');

    function syncTrailingSections() {
        var val = trailSel.value;
        var isLock = val === 'TrailingOption.Lock';
        var isLockAndTrail = val === 'TrailingOption.LockAndTrail';
        var isOverallTrail = val === 'TrailingOption.OverallTrailSL';

        if (lockGrid) lockGrid.style.display = (isLock || isLockAndTrail) ? '' : 'none';
        if (overallTrailSec) overallTrailSec.style.display = isOverallTrail ? 'flex' : 'none';

        // Extra rows only for Lock and Trail
        if (trailRow) {
            trailRow.style.display = isLockAndTrail ? 'flex' : 'none';
            trailRow.classList.toggle('hidden', !isLockAndTrail);
        }
    }

    trailSel.addEventListener('change', syncTrailingSections);
    syncTrailingSections();
})();

(function () {
    var SEL_CLS = 'w-max cursor-pointer appearance-none rounded py-2 text-xs dark:bg-primaryBlack-350 md:text-xs/4 border border-primaryBlack-500 text-primaryBlack-750';
    var SEL_SM = 'cursor-pointer appearance-none rounded py-1 px-2 pr-6 text-xs dark:bg-primaryBlack-350 border border-primaryBlack-500 text-primaryBlack-750';
    var INP_CLS = 'relative w-20 rounded border border-primaryBlack-350 px-1 py-2 text-xs dark:border-primaryBlack-500 dark:bg-primaryBlack-350 md:text-xs/4';
    var INP_SM = 'w-14 rounded border border-primaryBlack-350 px-1 py-1 text-center text-xs dark:border-primaryBlack-500 dark:bg-primaryBlack-350';
    var CHEVRON = '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" class="z-1 pointer-events-none absolute right-0 top-2.5 mr-3 h-4 w-4 dark:bg-primaryBlack-400 md:top-2 bg-primaryBlack-0"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>';
    var CHEVRON_SM = '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" class="pointer-events-none absolute right-1 top-2 h-3 w-3 bg-primaryBlack-0 dark:bg-primaryBlack-350"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>';

    var STRIKE_OPTS = ['ITM20', 'ITM19', 'ITM18', 'ITM17', 'ITM16', 'ITM15', 'ITM14', 'ITM13', 'ITM12', 'ITM11', 'ITM10', 'ITM9', 'ITM8', 'ITM7', 'ITM6', 'ITM5', 'ITM4', 'ITM3', 'ITM2', 'ITM1', 'ATM', 'OTM1', 'OTM2', 'OTM3', 'OTM4', 'OTM5', 'OTM6', 'OTM7', 'OTM8', 'OTM9', 'OTM10', 'OTM11', 'OTM12', 'OTM13', 'OTM14', 'OTM15', 'OTM16', 'OTM17', 'OTM18', 'OTM19', 'OTM20', 'OTM21', 'OTM22', 'OTM23', 'OTM24', 'OTM25', 'OTM26', 'OTM27', 'OTM28', 'OTM29', 'OTM30'];

    function strikeSelect(currentVal) {
        var opts = STRIKE_OPTS.map(function (v) {
            return '<option value="StrikeType.' + v + '" class="w-max"' + (v === (currentVal || 'ATM') ? ' selected' : '') + '>' + v + '</option>';
        }).join('');
        return '<div class="relative w-max"><select title="" class="' + SEL_CLS + '">' + opts + '</select>' + CHEVRON + '</div>';
    }

    function plusMinusSelect(defaultVal) {
        var sel = defaultVal === '-' ? '-' : '+';
        return '<div class="relative" style="display:inline-block"><select class="' + SEL_SM + '" style="padding-right:20px"><option value="+"' + (sel === '+' ? ' selected' : '') + '>+</option><option value="-"' + (sel === '-' ? ' selected' : '') + '>-</option></select>' + CHEVRON_SM + '</div>';
    }

    function numInput(placeholder, min, step, defaultVal) {
        var val = (defaultVal !== undefined && defaultVal !== null) ? ' value="' + defaultVal + '"' : '';
        return '<div class="relative w-max"><input title="' + placeholder + '" placeholder="' + placeholder + '" class="' + INP_CLS + '" min="' + min + '" step="' + step + '"' + val + ' type="number"></div>';
    }

    function numInputSm(defaultVal, min, step) {
        return '<input class="' + INP_SM + '" type="number" value="' + defaultVal + '" min="' + min + '" step="' + step + '">';
    }

    function rangeInputs(min, step) {
        return '<div style="display:flex;align-items:center;gap:4px">' + numInput('Min', min, step, null) + '<span class="text-xs text-primaryBlack-600">to</span>' + numInput('Max', min, step, null) + '</div>';
    }

    function straddleWidthHtml() {
        return '<div style="display:flex;align-items:center;gap:4px;flex-wrap:nowrap">'
            + '<span class="text-xs font-medium text-primaryBlack-750 whitespace-nowrap">[ ATM Strike</span>'
            + plusMinusSelect('+')
            + '<span class="text-xs text-primaryBlack-600">(</span>'
            + numInputSm('0.5', 0, 0.1)
            + '<span class="text-xs font-medium text-primaryBlack-750 whitespace-nowrap">&times; ATM Straddle Price )]</span>'
            + '</div>';
    }

    function atmMultiplierHtml() {
        var opts = STRIKE_OPTS.map(function (v) {
            return '<option value="' + v + '"' + (v === 'ATM' ? ' selected' : '') + '>' + v + '</option>';
        }).join('');
        var strikeDropdown = '<div class="relative" style="display:inline-block"><select class="' + SEL_SM + '" style="padding-right:20px">' + opts + '</select>' + CHEVRON_SM + '</div>';
        return '<div style="display:flex;align-items:center;gap:4px;flex-wrap:nowrap">'
            + strikeDropdown
            + plusMinusSelect('-')
            + numInputSm('0', 0, 1)
            + '<span class="text-xs font-medium text-primaryBlack-750 whitespace-nowrap">% of ATM</span>'
            + '</div>';
    }

    function getConfig(value) {
        switch (value) {
            case 'EntryType.EntryByStrikeType':
                return { label: 'Strike Type', html: null };
            case 'EntryType.EntryByPremiumRange':
                return { label: 'Premium Range (₹)', html: rangeInputs(1, 0.5) };
            case 'EntryType.EntryByPremium':
                return { label: 'Premium (₹)', html: numInput('Value', 1, 0.5, 50) };
            case 'EntryType.EntryByPremiumGEQ':
                return { label: 'Premium \u2265 (\u20b9)', html: numInput('Value', 1, 0.5, 50) };
            case 'EntryType.EntryByPremiumLEQ':
                return { label: 'Premium \u2264 (\u20b9)', html: numInput('Value', 1, 0.5, 50) };
            case 'EntryType.EntryByStraddlePrice':
                return { label: '\u00a0', html: straddleWidthHtml() };
            case 'EntryType.EntryByAtmMultiplier':
                return { label: '\u00a0', html: atmMultiplierHtml() };
            case 'EntryType.EntryBySyntheticFuture':
                return { label: 'Synthetic Future', html: null, isSynthetic: true };
            case 'EntryType.EntryByPremiumCloseToStraddle':
                return { label: 'Straddle Premium %', html: numInput('% Value', 0.1, 0.1, null) };
            case 'EntryType.EntryByDelta':
                return { label: 'Delta', html: numInput('0.01\u20131.00', 0.01, 0.01, 50) };
            case 'EntryType.EntryByDeltaRange':
                return { label: 'Delta Range', html: rangeInputs(0.01, 0.01) };
            default:
                return { label: 'Strike Type', html: null };
        }
    }

    function syncStrike(criteriaSelect, wrapper) {
        var cfg = getConfig(criteriaSelect.value);
        var labelEl = wrapper.querySelector('[data-strike-value-label]');
        var contentEl = wrapper.querySelector('[data-strike-value-content]');
        if (!labelEl || !contentEl) return;

        labelEl.textContent = cfg.label;

        if (cfg.html === null) {
            // Strike Type / Synthetic Future: preserve existing select value or rebuild with ATM default
            var existingSelect = contentEl.querySelector('select[title=""]');
            var prevVal = existingSelect ? existingSelect.value : null;
            contentEl.innerHTML = strikeSelect(prevVal ? prevVal.replace('StrikeType.', '') : 'ATM');
        } else {
            // Save current values before re-rendering so cloned legs keep user-set values
            var savedVals = [];
            contentEl.querySelectorAll('input, select').forEach(function (el) { savedVals.push(el.value); });
            contentEl.innerHTML = cfg.html;
            var newEls = contentEl.querySelectorAll('input, select');
            if (savedVals.length > 0 && newEls.length === savedVals.length) {
                newEls.forEach(function (el, i) { el.value = savedVals[i]; });
            }
        }
    }

    function initPair(criteriaSelect, wrapper) {
        syncStrike(criteriaSelect, wrapper);
        criteriaSelect.addEventListener('change', function () {
            syncStrike(criteriaSelect, wrapper);
        });
    }

    // Init leg builder pair
    document.querySelectorAll('[data-strike-criteria-select]').forEach(function (sel) {
        var container = sel.closest('.flex.flex-wrap.items-end.gap-3');
        if (!container) return;
        var wrapper = container.querySelector('[data-strike-value-wrapper]');
        if (wrapper) initPair(sel, wrapper);
    });

    // Watch for cloned legs added to DOM
    var legsContainer = document.getElementById('legs-container');
    if (legsContainer) {
        var observer = new MutationObserver(function (mutations) {
            mutations.forEach(function (mutation) {
                mutation.addedNodes.forEach(function (node) {
                    if (node.nodeType !== 1) return;
                    var sel = node.querySelector('[data-strike-criteria-select]');
                    if (!sel) return;
                    var container = sel.closest('.flex.flex-wrap.items-end.gap-3');
                    if (!container) return;
                    var wrapper = container.querySelector('[data-strike-value-wrapper]');
                    if (wrapper) initPair(sel, wrapper);
                });
            });
        });
        observer.observe(legsContainer, { childList: true });
    }

    // Expose for lazy leg modal
    window._initStrikePair = initPair;
})();

(function () {
    /* ── helpers ───────────────────────────────────────────── */
    function switchEnabled(container) {
        var btn = container.querySelector('button[role="switch"]');
        return btn && btn.getAttribute('aria-checked') === 'true';
    }

    function firstSelectVal(container) {
        var sel = container.querySelector('select');
        return sel ? sel.value : null;
    }

    function allSelectVals(container) {
        return Array.from(container.querySelectorAll('select')).map(function (s) { return s.value; });
    }

    function allNumberInputs(container) {
        return Array.from(container.querySelectorAll('input[type="number"]'))
            .map(function (i) { return parseFloat(i.value) || 0; });
    }

    /* ── per-section readers ───────────────────────────────── */
    function readStopLoss(legEl) {
        var c = legEl.querySelector('[id^="StopLoss_"]');
        if (!c || !switchEnabled(c)) return { Type: 'None', Value: 0 };
        var type = firstSelectVal(c) || 'LegTgtSLType.Percentage';
        var nums = allNumberInputs(c);
        return { Type: type, Value: nums[0] || 0 };
    }

    function readTarget(legEl) {
        var c = legEl.querySelector('[id^="TargetProfit_"]');
        if (!c || !switchEnabled(c)) return { Type: 'None', Value: 0 };
        var type = firstSelectVal(c) || 'LegTgtSLType.Points';
        var nums = allNumberInputs(c);
        return { Type: type, Value: nums[0] || 0 };
    }

    function readTrailSL(legEl) {
        var c = legEl.querySelector('[id^="TrailSL_"]');
        if (!c || !switchEnabled(c)) return { Type: 'None', Value: { InstrumentMove: 0, StopLossMove: 0 } };
        var type = firstSelectVal(c) || 'TrailStopLossType.Percentage';
        var nums = allNumberInputs(c);
        return { Type: type, Value: { InstrumentMove: nums[0] || 0, StopLossMove: nums[1] || 0 } };
    }

    function readMomentum(legEl) {
        var c = legEl.querySelector('[id^="simple_momentum_"]');
        if (!c || !switchEnabled(c)) return { Type: 'None', Value: 0 };
        var type = firstSelectVal(c) || 'MomentumType.PointsUp';
        var nums = allNumberInputs(c);
        return { Type: type, Value: nums[0] || 0 };
    }

    function readReentry(legEl, idFragment) {
        var btn = legEl.querySelector('[id*="' + idFragment + '"]');
        if (!btn || btn.getAttribute('aria-checked') !== 'true') return { Type: 'None', Value: 0 };
        var c = btn.closest('.flex.w-fit.flex-col.gap-2');
        if (!c) return { Type: 'None', Value: {} };
        var sels = allSelectVals(c);
        var type = sels[0] || 'ReentryType.Immediate';
        var count = parseInt(sels[1]) || 1;
        if (type === 'ReentryType.NextLeg') {
            var typeSelect = c.querySelector('select');
            var lazyRef = typeSelect ? (typeSelect.getAttribute('data-lazy-ref') || '') : '';
            return { Type: type, Value: { NextLegRef: lazyRef } };
        }
        return { Type: type, Value: { ReentryCount: count } };
    }

    function readStrikeParameter(legEl) {
        var criteriaEl = legEl.querySelector('[data-strike-criteria-select]');
        var contentEl = legEl.querySelector('[data-strike-value-content]');
        if (!criteriaEl || !contentEl) return 'StrikeType.ATM';
        var et = criteriaEl.value;

        if (et === 'EntryType.EntryByStrikeType' || et === 'EntryType.EntryBySyntheticFuture') {
            var sel = contentEl.querySelector('select');
            return sel ? sel.value : 'StrikeType.ATM';
        }
        if (et === 'EntryType.EntryByPremiumRange' || et === 'EntryType.EntryByDeltaRange') {
            var nums = allNumberInputs(contentEl);
            return { LowerRange: nums[0] || 0, UpperRange: nums[1] || 0 };
        }
        if (et === 'EntryType.EntryByPremium' || et === 'EntryType.EntryByPremiumGEQ' ||
            et === 'EntryType.EntryByPremiumLEQ' || et === 'EntryType.EntryByDelta') {
            var nums = allNumberInputs(contentEl);
            return nums[0] || 0;
        }
        if (et === 'EntryType.EntryByStraddlePrice') {
            var sels = allSelectVals(contentEl);
            var nums = allNumberInputs(contentEl);
            var adj = sels[0] === '-' ? 'AdjustmentType.Minus' : 'AdjustmentType.Plus';
            return { Multiplier: nums[0] || 0.5, Adjustment: adj, StrikeKind: 'StrikeType.ATM' };
        }
        if (et === 'EntryType.EntryByAtmMultiplier') {
            var sels = allSelectVals(contentEl);
            var nums = allNumberInputs(contentEl);
            var isPlus = sels[1] !== '-';
            var pct = nums[0] || 0;
            return isPlus ? 1 + (pct / 100) : 1 - (pct / 100);
        }
        if (et === 'EntryType.EntryByPremiumCloseToStraddle') {
            var nums = allNumberInputs(contentEl);
            return { Multiplier: (nums[0] || 0) / 100, StrikeKind: 'StrikeType.ATM' };
        }
        return 'StrikeType.ATM';
    }

    /* ── single leg reader ─────────────────────────────────── */
    function readLegConfig(legEl) {
        var lotEl = legEl.querySelector('[id^="number-input"]');
        var posEl = legEl.querySelector('select[title="Position"]');
        var optEl = legEl.querySelector('select[title="Option Type"]');
        var expEl = legEl.querySelector('select[title="Expiry"]');
        var etEl = legEl.querySelector('[data-strike-criteria-select]');
        return {
            id: Math.random().toString(36).slice(2, 10),
            PositionType: posEl ? posEl.value : 'PositionType.Sell',
            LotConfig: { Type: 'LotType.Quantity', Value: lotEl ? parseInt(lotEl.value) || 1 : 1 },
            LegStopLoss: readStopLoss(legEl),
            LegTarget: readTarget(legEl),
            LegTrailSL: readTrailSL(legEl),
            LegMomentum: readMomentum(legEl),
            ExpiryKind: expEl ? expEl.value : 'ExpiryType.Weekly',
            EntryType: etEl ? etEl.value : 'EntryType.EntryByStrikeType',
            StrikeParameter: readStrikeParameter(legEl),
            InstrumentKind: optEl ? optEl.value : 'LegType.CE',
            LegReentrySL: readReentry(legEl, 'Re-entry on SL'),
            LegReentryTP: readReentry(legEl, 'Re-entry on Tgt')
        };
    }

    /* ── full config builder ───────────────────────────────── */
    function buildConfig() {
        var legsContainer = document.getElementById('legs-container');
        var legs = legsContainer
            ? Array.from(legsContainer.children).map(readLegConfig)
            : [];

        var dateInputs = document.querySelectorAll('[title="date-input"]');
        var startDate = dateInputs[0] ? dateInputs[0].value : '';
        var endDate = dateInputs[1] ? dateInputs[1].value : '';
        var indicatorSettings = document.getElementById('indicator-settings');
        var timeInputs = indicatorSettings ? indicatorSettings.querySelectorAll('input[type="time"]') : [];

        function readIndicatorTime(index, fallbackHour, fallbackMinute) {
            var raw = timeInputs[index] && timeInputs[index].value ? String(timeInputs[index].value) : '';
            var parts = raw.split(':');
            var hour = parseInt(parts[0], 10);
            var minute = parseInt(parts[1], 10);
            return {
                Hour: Number.isFinite(hour) ? hour : fallbackHour,
                Minute: Number.isFinite(minute) ? minute : fallbackMinute
            };
        }

        var entryIndicatorTime = readIndicatorTime(0, 9, 16);
        var exitIndicatorTime = readIndicatorTime(1, 15, 15);

        return {
            strategy_id: null,
            name: null,
            start_date: startDate,
            end_date: endDate,
            strategy: {
                Ticker: 'NIFTY',
                TakeUnderlyingFromCashOrNot: 'True',
                TrailSLtoBreakeven: 'False',
                SquareOffAllLegs: 'False',
                EntryIndicators: {
                    Type: 'IndicatorTreeNodeType.OperandNode',
                    OperandType: 'OperandType.And',
                    Value: [{ Type: 'IndicatorTreeNodeType.DataNode', Value: { IndicatorName: 'IndicatorType.TimeIndicator', Parameters: entryIndicatorTime } }]
                },
                ExitIndicators: {
                    Type: 'IndicatorTreeNodeType.OperandNode',
                    OperandType: 'OperandType.And',
                    Value: [{ Type: 'IndicatorTreeNodeType.DataNode', Value: { IndicatorName: 'IndicatorType.TimeIndicator', Parameters: exitIndicatorTime } }]
                },
                StrategyType: 'StrategyType.IntradaySameDay',
                MaxPositionInADay: 1,
                ReentryTimeRestriction: 'None',
                SkipInitialCandles: 0,
                ListOfLegConfigs: legs,
                IdleLegConfigs: (function () {
                    var idle = {};
                    Object.keys(window._idleLegElements || {}).forEach(function (name) {
                        var el = window._idleLegElements[name];
                        var cfg = readLegConfig(el);
                        cfg.id = name;
                        idle[name] = cfg;
                    });
                    return idle;
                }()),
                OverallMomentum: (function () {
                    var btn = document.querySelector('[data-test-id="toggle-handle_Overall Momentum_OverallMomentum_None"]');
                    if (!btn || btn.getAttribute('aria-checked') !== 'true') return { Type: 'None', Value: 0 };
                    var el = btn.closest('[id^="OverallMomentum"]');
                    if (!el) return { Type: 'None', Value: 0 };
                    var sel = el.querySelector('select');
                    var inp = el.querySelector('input[type="number"]');
                    return { Type: sel ? sel.value : 'None', Value: inp ? parseFloat(inp.value) || 0 : 0 };
                }()),
                OverallSL: (function () {
                    var btn = document.querySelector('[data-test-id="toggle-handle_Overall Stop Loss_overall-stoploss"]');
                    if (!btn || btn.getAttribute('aria-checked') !== 'true') return { Type: 'None', Value: 0 };
                    var el = btn.closest('[id^="OverallStop"]');
                    if (!el) return { Type: 'None', Value: 0 };
                    var sel = el.querySelector('select');
                    var inp = el.querySelector('input[type="number"]');
                    return { Type: sel ? sel.value : 'OverallTgtSLType.MTM', Value: inp ? parseFloat(inp.value) || 0 : 0 };
                }()),
                OverallTgt: (function () {
                    var btns = document.querySelectorAll('[data-test-id="toggle-handle_Overall Stop Loss_overall-stoploss"]');
                    var btn = btns[1];
                    if (!btn || btn.getAttribute('aria-checked') !== 'true') return { Type: 'None', Value: 0 };
                    var el = btn.closest('[id^="OverallStop"]');
                    if (!el) return { Type: 'None', Value: 0 };
                    var sel = el.querySelector('select');
                    var inp = el.querySelector('input[type="number"]');
                    return { Type: sel ? sel.value : 'OverallTgtSLType.MTM', Value: inp ? parseFloat(inp.value) || 0 : 0 };
                }()),
                OverallTrailSL: (function () {
                    var btn = document.querySelector('[data-test-id="toggle-handle_Trailing Options_trailing-options"]');
                    if (!btn || btn.getAttribute('aria-checked') !== 'true') return { Type: 'None', Value: {} };
                    var el = btn.closest('[id^="TrailingOptions"]');
                    var sel = el ? el.querySelector('select') : null;
                    if (!sel || sel.value !== 'TrailingOption.OverallTrailSL') return { Type: 'None', Value: {} };
                    var typeSel = document.getElementById('overall-trail-sl-type');
                    var inp1 = document.getElementById('overall-trail-sl-input1');
                    var inp2 = document.getElementById('overall-trail-sl-input2');
                    return {
                        Type: typeSel ? typeSel.value : 'OverallTrailSLType.MTM',
                        Value: {
                            InstrumentMove: inp1 ? parseFloat(inp1.value) || 0 : 0,
                            StopLossMove: inp2 ? parseFloat(inp2.value) || 0 : 0
                        }
                    };
                }()),
                LockAndTrail: (function () {
                    var btn = document.querySelector('[data-test-id="toggle-handle_Trailing Options_trailing-options"]');
                    if (!btn || btn.getAttribute('aria-checked') !== 'true') return { Type: 'None', Value: {} };
                    var el = btn.closest('[id^="TrailingOptions"]');
                    var sel = el ? el.querySelector('select') : null;
                    var trailType = sel ? sel.value : 'TrailingOption.Lock';
                    if (trailType !== 'TrailingOption.Lock' && trailType !== 'TrailingOption.LockAndTrail') {
                        return { Type: 'None', Value: {} };
                    }
                    var pr = document.getElementById('trailing-profit-reaches');
                    var lp = document.getElementById('trailing-lock-profit');
                    var ib = document.getElementById('trailing-increase-by');
                    var tb = document.getElementById('trailing-trail-by');
                    var baseValue = {
                        ProfitReaches: pr ? parseFloat(pr.value) || 0 : 0,
                        LockProfit: lp ? parseFloat(lp.value) || 0 : 0,
                        IncreaseInProfitBy: 0,
                        TrailProfitBy: 0
                    };
                    if (trailType === 'TrailingOption.LockAndTrail') {
                        baseValue.IncreaseInProfitBy = ib ? parseFloat(ib.value) || 0 : 0;
                        baseValue.TrailProfitBy = tb ? parseFloat(tb.value) || 0 : 0;
                    }
                    return {
                        Type: trailType,
                        Value: baseValue
                    };
                }()),
                OverallReentrySL: (function () {
                    var btn = document.querySelector('[data-test-id="toggle-handle_rentry_undefined_Overall Re-entry on SL"]');
                    if (!btn || btn.getAttribute('aria-checked') !== 'true') return { Type: 'None', Value: {} };
                    var c = btn.closest('.flex.w-fit.flex-col.gap-2');
                    if (!c) return { Type: 'None', Value: {} };
                    var sels = c.querySelectorAll('select');
                    return { Type: sels[0] ? sels[0].value : 'ReentryType.Immediate', Value: { ReentryCount: sels[1] ? parseInt(sels[1].value) || 1 : 1 } };
                }()),
                OverallReentryTgt: (function () {
                    var btns = document.querySelectorAll('[data-test-id="toggle-handle_rentry_undefined_Overall Re-entry on SL"]');
                    var btn = btns[1];
                    if (!btn || btn.getAttribute('aria-checked') !== 'true') return { Type: 'None', Value: {} };
                    var c = btn.closest('.flex.w-fit.flex-col.gap-2');
                    if (!c) return { Type: 'None', Value: {} };
                    var sels = c.querySelectorAll('select');
                    return { Type: sels[0] ? sels[0].value : 'ReentryType.Immediate', Value: { ReentryCount: sels[1] ? parseInt(sels[1].value) || 1 : 1 } };
                }()),
                WeeklyOldRegime: true
            },
            attributes: { template: 'Custom', positional: 'False' },
            source: 'WEB'
        };
    }

    // Expose for lazy leg modal
    window._readLegConfig = readLegConfig;

    /* ── wire button ───────────────────────────────────────── */
    var btn = document.getElementById('start-backtest-btn');
    var backtestResultsEl = document.getElementById('backtest-results');
    var progressCard = document.getElementById('strategy-backtest-progress');
    var progressTitleEl = document.getElementById('strategy-backtest-progress-title');
    var progressPercentEl = document.getElementById('strategy-backtest-progress-percent');
    var progressBarEl = document.getElementById('strategy-backtest-progress-bar');
    var progressMetaEl = document.getElementById('strategy-backtest-progress-meta');
    var activeBacktestPollInterval = null;

    function setStrategyResultSectionVisible(visible) {
        if (!backtestResultsEl) return;
        backtestResultsEl.hidden = !visible;
    }

    function setStrategyProgressVisible(visible) {
        if (!progressCard) return;
        progressCard.hidden = !visible;
    }

    function updateStrategyBacktestProgress(state) {
        var total = Number(state && state.total || 0);
        var completed = Number(state && state.completed || 0);
        var percent = Number(state && state.percent);
        var label = state && state.label ? String(state.label) : 'Running Backtest...';
        var currentDay = state && state.currentDay ? String(state.currentDay) : '';
        var strategyName = state && state.strategyName ? String(state.strategyName) : '';
        var metaText = state && state.meta ? String(state.meta) : '';
        var clampedPercent;

        if (!Number.isFinite(percent)) {
            percent = total > 0 ? (completed / total) * 100 : 0;
        }
        clampedPercent = Math.max(0, Math.min(100, percent));

        if (progressTitleEl) progressTitleEl.textContent = label;
        if (progressPercentEl) progressPercentEl.textContent = clampedPercent.toFixed(1) + '%';
        if (progressBarEl) progressBarEl.style.width = clampedPercent + '%';
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

    function resetStrategyBacktestView() {
        setStrategyResultSectionVisible(false);
        setStrategyProgressVisible(false);
        updateStrategyBacktestProgress({
            percent: 0,
            completed: 0,
            total: 0,
            label: 'Running Backtest...',
            meta: 'Preparing backtest...'
        });
    }

    window._setStrategyResultSectionVisible = setStrategyResultSectionVisible;
    window._setStrategyProgressVisible = setStrategyProgressVisible;
    window._updateStrategyBacktestProgress = updateStrategyBacktestProgress;
    window._resetStrategyBacktestView = resetStrategyBacktestView;

    resetStrategyBacktestView();

    if (btn) {
        btn.addEventListener('click', function (event) {
            event.preventDefault();
            var config = buildConfig();
            var originalText = btn.textContent;
            if (activeBacktestPollInterval) {
                clearInterval(activeBacktestPollInterval);
                activeBacktestPollInterval = null;
            }
            setStrategyResultSectionVisible(false);
            setStrategyProgressVisible(true);
            updateStrategyBacktestProgress({
                percent: 0,
                completed: 0,
                total: 0,
                label: 'Running Backtest...',
                meta: 'Preparing backtest...'
            });
            btn.disabled = true;
            btn.textContent = 'Starting...';

            fetch(getStrategyApiUrl('backtestStart'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            })
                .then(function (res) {
                    if (!res.ok) return res.json().then(function (e) { throw new Error(e.detail || 'Failed to start backtest'); });
                    return res.json();
                })
                .then(function (data) {
                    var jobId = data.job_id;
                    var totalCount = Number(data.total || data.strategy_count || data.days || 0);

                    updateStrategyBacktestProgress({
                        percent: 0,
                        completed: 0,
                        total: totalCount,
                        label: 'Running Backtest...',
                        meta: totalCount > 0 ? ('Processing 0/' + totalCount) : 'Preparing backtest...'
                    });

                    activeBacktestPollInterval = setInterval(function () {
                        fetch(getStrategyApiUrl('backtestStatus', jobId))
                            .then(function (r) { return r.json(); })
                            .then(function (s) {
                                if (s.status === 'running') {
                                    var pct = s.percent || 0;
                                    var completed = s.completed || 0;
                                    var total = s.total || totalCount || 0;
                                    var currentDay = s.current_day || '';
                                    btn.textContent = 'Running... ' + pct + '% (' + completed + '/' + total + ' days)';
                                    updateStrategyBacktestProgress({
                                        percent: pct,
                                        completed: completed,
                                        total: total,
                                        label: 'Running Backtest...',
                                        strategyName: s.strategy_name || '',
                                        currentDay: currentDay,
                                        meta: 'Processing ' + completed + '/' + total + (currentDay ? ': ' + currentDay : (s.strategy_name ? ': ' + s.strategy_name : ''))
                                    });
                                } else if (s.status === 'done') {
                                    clearInterval(activeBacktestPollInterval);
                                    activeBacktestPollInterval = null;
                                    btn.textContent = 'Fetching result...';
                                    updateStrategyBacktestProgress({
                                        percent: 100,
                                        completed: Number(s.total || totalCount || 0),
                                        total: Number(s.total || totalCount || 0),
                                        label: 'Finalizing Backtest...',
                                        meta: 'Backtest completed. Fetching result...'
                                    });
                                    fetch(getStrategyApiUrl('backtestResult', jobId))
                                        .then(function (r) { return r.json(); })
                                        .then(function (result) {
                                            btn.disabled = false;
                                            btn.textContent = originalText;
                                            var trades = result.trades || result.data || result;
                                            if (Array.isArray(trades)) {
                                                // Update raw trades — filters (weekday, DTE) use this as base
                                                window._rawTrades = trades;
                                                if (typeof window._populateDTEDropdown === 'function') window._populateDTEDropdown();
                                            }
                                            if (Array.isArray(trades) && typeof window.flattenToSubTrades === 'function') {
                                                var calcTrades = window.flattenToSubTrades(trades);
                                                if (typeof window._setTrades === 'function') window._setTrades(calcTrades, trades);
                                            } else if (typeof window._setTrades === 'function') {
                                                window._setTrades(trades, trades);
                                            }
                                            var resultsEl = document.getElementById('backtest-results');
                                            if (resultsEl) resultsEl.scrollIntoView({ behavior: 'smooth' });
                                        })
                                        .catch(function (err) {
                                            btn.disabled = false;
                                            btn.textContent = originalText;
                                            setStrategyProgressVisible(false);
                                            alert('Error fetching result: ' + err.message);
                                        });
                                } else if (s.status === 'error') {
                                    clearInterval(activeBacktestPollInterval);
                                    activeBacktestPollInterval = null;
                                    btn.disabled = false;
                                    btn.textContent = originalText;
                                    setStrategyProgressVisible(false);
                                    alert('Backtest error: ' + (s.error || 'Unknown error'));
                                }
                            })
                            .catch(function (err) {
                                clearInterval(activeBacktestPollInterval);
                                activeBacktestPollInterval = null;
                                btn.disabled = false;
                                btn.textContent = originalText;
                                setStrategyProgressVisible(false);
                                alert('Poll error: ' + err.message);
                            });
                    }, 1000);
                })
                .catch(function (err) {
                    btn.disabled = false;
                    btn.textContent = originalText;
                    setStrategyProgressVisible(false);
                    alert('Error: ' + err.message);
                });
        });
    }

    // expose buildConfig globally for Save Strategy modal
    window.buildConfig = buildConfig;
})();

/* ══════════════════════════════════════════════════════════
   LAZY LEG SYSTEM
══════════════════════════════════════════════════════════ */
(function () {
    'use strict';

    window._idleLegConfigs = window._idleLegConfigs || {};

    /* ── DOM helpers ─────────────────────────────────────── */

    // typeSelect → div.relative.w-max → div[id^="select_"] (type container)
    function getTypeContainer(typeSelect) {
        return typeSelect.parentElement.parentElement;
    }

    /* ── Lazy Picker ─────────────────────────────────────── */

    function showLazyPicker(typeSelect) {
        var tc = getTypeContainer(typeSelect);
        if (!tc) return;

        // Remove existing picker (refresh scenario)
        var next = tc.nextElementSibling;
        if (next && next.classList.contains('lazy-ref-picker')) next.remove();

        // Hide count wrapper
        var countWrapper = tc.nextElementSibling;
        if (countWrapper) countWrapper.style.display = 'none';

        var keys = Object.keys(window._idleLegConfigs);

        if (keys.length === 0) {
            // No lazy legs → open modal immediately
            openModal(function (name) {
                typeSelect.setAttribute('data-lazy-ref', name);
                showLazyPicker(typeSelect);
            });
            return;
        }

        // Has lazy legs → single dropdown with names + "+ Create New" at bottom
        var picker = document.createElement('div');
        picker.className = 'lazy-ref-picker';
        picker.style.cssText = 'display:inline-flex;align-items:center;position:relative';

        var currentRef = typeSelect.getAttribute('data-lazy-ref');

        var sel = document.createElement('select');
        sel.className = 'cursor-pointer appearance-none rounded py-2 pl-2 pr-8 text-xs dark:bg-primaryBlack-350 border border-primaryBlack-500 text-primaryBlack-750';

        keys.forEach(function (k) {
            var o = document.createElement('option');
            o.value = k; o.textContent = k;
            sel.appendChild(o);
        });

        // "+ Create New" as last option inside the dropdown
        var newOpt = document.createElement('option');
        newOpt.value = '__new__';
        newOpt.textContent = '+ Create New';
        sel.appendChild(newOpt);

        // Explicitly set the selected value — more reliable than o.selected = true
        if (currentRef && keys.indexOf(currentRef) !== -1) {
            sel.value = currentRef;
        } else {
            // No valid ref — default to first lazy leg
            typeSelect.setAttribute('data-lazy-ref', keys[0]);
            sel.value = keys[0];
        }

        sel.addEventListener('change', function () {
            if (sel.value === '__new__') {
                sel.value = typeSelect.getAttribute('data-lazy-ref') || keys[0];
                openModal(function (name) {
                    typeSelect.setAttribute('data-lazy-ref', name);
                    showLazyPicker(typeSelect);
                });
            } else {
                typeSelect.setAttribute('data-lazy-ref', sel.value);
            }
        });

        picker.appendChild(sel);
        picker.insertAdjacentHTML('beforeend', '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" style="pointer-events:none;position:absolute;right:4px;top:9px;height:12px;width:12px"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>');
        tc.insertAdjacentElement('afterend', picker);
    }

    function hideLazyPicker(typeSelect) {
        var tc = getTypeContainer(typeSelect);
        if (!tc) return;
        var next = tc.nextElementSibling;
        if (next && next.classList.contains('lazy-ref-picker')) {
            var countWrapper = next.nextElementSibling;
            next.remove();
            if (countWrapper) countWrapper.style.display = '';
        }
    }

    /* ── Modal ───────────────────────────────────────────── */

    function openModal(onConfirm) {
        var idx = Object.keys(window._idleLegConfigs).length + 1;
        var defaultName = 'lazy' + idx;

        var backdrop = document.createElement('div');
        backdrop.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.55);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px';

        var card = document.createElement('div');
        card.style.cssText = 'background:#fff;border-radius:12px;width:100%;max-width:900px;max-height:92vh;overflow-y:auto;padding:24px;box-shadow:0 25px 60px rgba(0,0,0,0.35)';

        /* header */
        var hdr = document.createElement('div');
        hdr.style.cssText = 'display:flex;align-items:center;justify-content:space-between;margin-bottom:18px;padding-bottom:14px;border-bottom:1px solid #e5e7eb';
        hdr.innerHTML = '<span style="font-size:14px;font-weight:600;color:#111827">Create New Lazy Leg</span>'
            + '<button class="lazy-close" type="button" style="background:none;border:none;cursor:pointer;color:#6b7280;padding:2px">'
            + '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" style="height:18px;width:18px"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"></path></svg></button>';
        card.appendChild(hdr);

        /* name input */
        var nameRow = document.createElement('div');
        nameRow.style.cssText = 'display:flex;align-items:center;gap:12px;margin-bottom:18px';
        nameRow.innerHTML = '<label style="font-size:11px;font-weight:500;color:#374151;white-space:nowrap">Enter Leg Name</label>'
            + '<input class="lazy-name-inp" type="text" value="' + defaultName + '" style="border:1px solid #d1d5db;border-radius:6px;padding:6px 10px;font-size:11px;min-width:160px;outline:none">';
        card.appendChild(nameRow);

        /* cloned leg form */
        var tpl = document.getElementById('backtest');
        var formEl = tpl.cloneNode(true);
        formEl.id = 'lazy-form-' + Date.now();
        formEl.style.cssText = 'display:block;position:relative;border:1px solid #e5e7eb;border-radius:8px;padding:16px;margin-bottom:18px';

        // Remove copy/delete buttons & number badge
        var absBtns = formEl.querySelector('.absolute.-right-3.-top-3');
        if (absBtns) absBtns.remove();
        var badge = formEl.querySelector('.absolute.-left-0');
        if (!badge) badge = formEl.querySelector('[class*="-left-0"][class*="absolute"]');
        if (badge) badge.remove();

        // Remove Range Breakout section — not applicable for lazy legs
        var rbSection = formEl.querySelector('[data-range-breakout]');
        if (rbSection) rbSection.remove();

        // Rewire IDs
        var sfx = '__lazy' + Date.now();
        formEl.querySelectorAll('[id]').forEach(function (el) { el.id = el.id + sfx; });
        formEl.querySelectorAll('[for]').forEach(function (el) {
            var f = el.getAttribute('for');
            if (f) el.setAttribute('for', f + sfx);
        });

        // Set Lots default to 1
        var lotInp = formEl.querySelector('[id^="number-input"]');
        if (lotInp) lotInp.value = '1';

        card.appendChild(formEl);

        /* footer */
        var ftr = document.createElement('div');
        ftr.style.cssText = 'display:flex;justify-content:flex-end;gap:10px;padding-top:14px;border-top:1px solid #e5e7eb';
        ftr.innerHTML = '<button class="lazy-cancel" type="button" style="border:1px solid #d1d5db;border-radius:6px;padding:7px 18px;font-size:11px;cursor:pointer;color:#374151;background:#fff">Cancel</button>'
            + '<button class="lazy-confirm" type="button" style="background:#1d4ed8;border:1px solid #1d4ed8;border-radius:6px;padding:7px 18px;font-size:11px;font-weight:600;cursor:pointer;color:#fff">Create and Select</button>';
        card.appendChild(ftr);

        backdrop.appendChild(card);
        document.body.appendChild(backdrop);

        initFormSwitches(formEl);

        var criteriaEl = formEl.querySelector('[data-strike-criteria-select]');
        if (criteriaEl && window._initStrikePair) {
            var cont = criteriaEl.closest('.flex.flex-wrap.items-end.gap-3');
            var wpr = cont && cont.querySelector('[data-strike-value-wrapper]');
            if (wpr) window._initStrikePair(criteriaEl, wpr);
        }

        function close() { backdrop.remove(); }

        card.querySelector('.lazy-close').addEventListener('click', close);
        card.querySelector('.lazy-cancel').addEventListener('click', close);

        card.querySelector('.lazy-confirm').addEventListener('click', function () {
            var nameVal = (card.querySelector('.lazy-name-inp').value || '').trim() || defaultName;
            var finalName = nameVal;
            var n = 1;
            while (window._idleLegConfigs[finalName]) { finalName = nameVal + '_' + (n++); }

            // Detach formEl from modal before closing
            card.removeChild(formEl);
            close();

            // Re-add badge and action buttons to formEl (stripped for modal display)
            var badgeHtml = '<div class="absolute -left-0 top-0 flex flex-col">'
                + '<div class="flex w-fit items-center gap-1 rounded rounded-bl-none rounded-tr-none bg-primaryBlack-400 text-sm text-primaryBlack-700">'
                + '<h3 class="p-1.5 py-1 text-xs font-semibold">' + finalName + '</h3></div></div>';
            var actionsHtml = '<div class="absolute -right-3 -top-3 flex flex-col items-center">'
                + '<button class="rounded-full border-2 border-white bg-red-500 p-1" type="button">'
                + '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="h-4 stroke-2 text-white"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"></path></svg></button>'
                + '<button class="rounded-full border-2 border-white bg-primaryBlack-600 p-1" type="button">'
                + '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="h-4 text-white"><path stroke-linecap="round" stroke-linejoin="round" d="M8.25 7.5V6.108c0-1.135.845-2.098 1.976-2.192.373-.03.748-.057 1.123-.08M15.75 18H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08M15.75 18.75v-1.875a3.375 3.375 0 00-3.375-3.375h-1.5a1.125 1.125 0 01-1.125-1.125v-1.5A3.375 3.375 0 006.375 7.5H5.25m11.9-3.664A2.251 2.251 0 0015 2.25h-1.5a2.251 2.251 0 00-2.15 1.586m5.8 0c.065.21.1.433.1.664v.75h-6V4.5c0-.231.035-.454.1-.664M6.75 7.5H4.875c-.621 0-1.125.504-1.125 1.125v12c0 .621.504 1.125 1.125 1.125h9.75c.621 0 1.125-.504 1.125-1.125V16.5a9 9 0 00-9-9z"></path></svg></button></div>';
            formEl.insertAdjacentHTML('afterbegin', actionsHtml);
            formEl.insertAdjacentHTML('afterbegin', badgeHtml);

            // Restore card styles (remove modal overrides)
            formEl.style.cssText = '';

            // Register in global state
            window._idleLegElements = window._idleLegElements || {};
            window._idleLegElements[finalName] = formEl;
            window._idleLegConfigs[finalName] = finalName;

            renderLazyLegCard(finalName, formEl);
            onConfirm(finalName);
        });
    }

    /* ── Lazy Legs Section ───────────────────────────────── */

    function getOrCreateLazySection() {
        var sec = document.getElementById('lazy-legs-section');
        if (sec) return sec;

        sec = document.createElement('div');
        sec.id = 'lazy-legs-section';
        sec.style.cssText = 'display:flex;flex-direction:column;gap:16px;margin-top:8px';

        var heading = document.createElement('h6');
        heading.id = 'lazy-legs-heading';
        heading.className = 'text-base font-medium leading-7 text-primaryBlack-750';
        heading.textContent = 'Lazy Legs';
        sec.appendChild(heading);

        var container = document.createElement('div');
        container.id = 'lazy-legs-container';
        container.style.cssText = 'display:flex;flex-direction:column;gap:16px';
        sec.appendChild(container);

        // Insert after legs-container (or backtest template)
        var legsContainer = document.getElementById('legs-container');
        var anchor = legsContainer || document.getElementById('backtest');
        if (anchor && anchor.parentNode) {
            anchor.parentNode.insertBefore(sec, anchor.nextSibling);
        }
        return sec;
    }

    function renderLazyLegCard(name, cardEl) {
        var sec = getOrCreateLazySection();
        var container = document.getElementById('lazy-legs-container');
        cardEl.setAttribute('data-lazy-leg-name', name);
        var badgeText = cardEl.querySelector('.absolute.-left-0 h3, [class*="-left-0"][class*="absolute"] h3');
        if (badgeText) badgeText.textContent = name;
        container.appendChild(cardEl);
        sec.style.display = '';
    }

    /* ── Event delegation for lazy leg card buttons ──────── */

    document.addEventListener('click', function (e) {
        var lazyContainer = document.getElementById('lazy-legs-container');
        if (!lazyContainer) return;

        // Delete button
        var delBtn = e.target.closest('button.bg-red-500.rounded-full');
        if (delBtn && lazyContainer.contains(delBtn)) {
            var cardEl = delBtn.closest('[data-lazy-leg-name]');
            if (cardEl) {
                var legName = cardEl.getAttribute('data-lazy-leg-name');
                cardEl.remove();
                delete window._idleLegElements[legName];
                delete window._idleLegConfigs[legName];
                if (!lazyContainer.children.length) {
                    document.getElementById('lazy-legs-section').style.display = 'none';
                }
            }
            return;
        }

        // Copy button — clone the lazy leg card
        var copyBtn = e.target.closest('button.bg-primaryBlack-600.border-2');
        if (copyBtn && lazyContainer.contains(copyBtn)) {
            var srcCard = copyBtn.closest('[data-lazy-leg-name]');
            if (!srcCard) return;
            var srcName = srcCard.getAttribute('data-lazy-leg-name');
            var newIdx = Object.keys(window._idleLegConfigs).length + 1;
            var newName = 'lazy' + newIdx;
            while (window._idleLegConfigs[newName]) newName = 'lazy' + (++newIdx);

            var cloned = srcCard.cloneNode(true);
            var sfx = '__lazycopy' + Date.now();
            cloned.querySelectorAll('[id]').forEach(function (el) { el.id = el.id + sfx; });
            cloned.querySelectorAll('[for]').forEach(function (el) {
                var f = el.getAttribute('for'); if (f) el.setAttribute('for', f + sfx);
            });
            var badge = cloned.querySelector('.absolute.-left-0 h3, [class*="-left-0"][class*="absolute"] h3');
            if (badge) badge.textContent = newName;
            cloned.setAttribute('data-lazy-leg-name', newName);

            // Copy .value/.checked — cloneNode does not copy JS properties
            var srcInputs = Array.from(srcCard.querySelectorAll('input'));
            var clnInputs = Array.from(cloned.querySelectorAll('input'));
            srcInputs.forEach(function (s, i) {
                if (!clnInputs[i]) return;
                if (s.type === 'radio' || s.type === 'checkbox') { clnInputs[i].checked = s.checked; }
                else { clnInputs[i].value = s.value; }
            });
            var srcSelects = Array.from(srcCard.querySelectorAll('select'));
            var clnSelects = Array.from(cloned.querySelectorAll('select'));
            srcSelects.forEach(function (s, i) { if (clnSelects[i]) clnSelects[i].value = s.value; });

            initFormSwitches(cloned);
            var crit = cloned.querySelector('[data-strike-criteria-select]');
            if (crit && window._initStrikePair) {
                var cont2 = crit.closest('.flex.flex-wrap.items-end.gap-3');
                var wpr2 = cont2 && cont2.querySelector('[data-strike-value-wrapper]');
                if (wpr2) window._initStrikePair(crit, wpr2);
            }

            window._idleLegElements[newName] = cloned;
            window._idleLegConfigs[newName] = newName;
            lazyContainer.appendChild(cloned);
        }
    });

    /* ── Form switch initializer ─────────────────────────── */

    function initFormSwitches(formEl) {
        formEl.querySelectorAll('button[role="switch"]').forEach(function (btn) {
            var knob = btn.querySelector('span');
            function sync() {
                var on = btn.getAttribute('aria-checked') === 'true';
                btn.classList.toggle('bg-secondaryBlue-500', on);
                btn.classList.toggle('bg-primaryBlack-600', !on);
                if (knob) {
                    knob.classList.toggle('translate-x-5', on);
                    knob.classList.toggle('translate-x-1', !on);
                }
                var cur = btn.parentElement;
                while (cur && cur !== formEl) {
                    var ns = cur.nextElementSibling;
                    if (ns) { ns.classList.toggle('pointer-events-none', !on); ns.classList.toggle('opacity-40', !on); break; }
                    cur = cur.parentElement;
                }
            }
            sync();
            btn.addEventListener('click', function () {
                btn.setAttribute('aria-checked', String(btn.getAttribute('aria-checked') !== 'true'));
                sync();
            });
        });
    }

    /* ── Programmatic lazy leg creation (for URL load) ─────── */
    function createIdleLeg(name) {
        var finalName = name;
        var n = 1;
        while (window._idleLegConfigs[finalName]) { finalName = name + '_' + (n++); }

        var tpl = document.getElementById('backtest');
        if (!tpl) return null;
        var formEl = tpl.cloneNode(true);
        formEl.id = 'lazy-form-' + Date.now();

        // Remove original main-leg badge and action buttons
        var absBtns = formEl.querySelector('.absolute.-right-3.-top-3');
        if (absBtns) absBtns.remove();
        var badge = formEl.querySelector('.absolute.-left-0');
        if (!badge) badge = formEl.querySelector('[class*="-left-0"][class*="absolute"]');
        if (badge) badge.remove();

        // Remove range breakout section
        var rbSection = formEl.querySelector('[data-range-breakout]');
        if (rbSection) rbSection.remove();

        // Rewire IDs
        var sfx = '__lazy' + Date.now();
        formEl.querySelectorAll('[id]').forEach(function (el) { el.id = el.id + sfx; });
        formEl.querySelectorAll('[for]').forEach(function (el) {
            var f = el.getAttribute('for');
            if (f) el.setAttribute('for', f + sfx);
        });

        // Set lots default
        var lotInp = formEl.querySelector('[id^="number-input"]');
        if (lotInp) lotInp.value = '1';

        // Add badge
        formEl.insertAdjacentHTML('afterbegin',
            '<div class="absolute -left-0 top-0 flex flex-col">'
            + '<div class="flex w-fit items-center gap-1 rounded rounded-bl-none rounded-tr-none bg-primaryBlack-400 text-sm text-primaryBlack-700">'
            + '<h3 class="p-1.5 py-1 text-xs font-semibold">' + finalName + '</h3></div></div>');

        // Add action buttons
        formEl.insertAdjacentHTML('afterbegin',
            '<div class="absolute -right-3 -top-3 flex flex-col items-center">'
            + '<button class="rounded-full border-2 border-white bg-red-500 p-1" type="button">'
            + '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="h-4 stroke-2 text-white"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"></path></svg></button>'
            + '<button class="rounded-full border-2 border-white bg-primaryBlack-600 p-1" type="button">'
            + '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="h-4 text-white"><path stroke-linecap="round" stroke-linejoin="round" d="M8.25 7.5V6.108c0-1.135.845-2.098 1.976-2.192.373-.03.748-.057 1.123-.08M15.75 18H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08M15.75 18.75v-1.875a3.375 3.375 0 00-3.375-3.375h-1.5a1.125 1.125 0 01-1.125-1.125v-1.5A3.375 3.375 0 006.375 7.5H5.25m11.9-3.664A2.251 2.251 0 0015 2.25h-1.5a2.251 2.251 0 00-2.15 1.586m5.8 0c.065.21.1.433.1.664v.75h-6V4.5c0-.231.035-.454.1-.664M6.75 7.5H4.875c-.621 0-1.125.504-1.125 1.125v12c0 .621.504 1.125 1.125 1.125h9.75c.621 0 1.125-.504 1.125-1.125V16.5a9 9 0 00-9-9z"></path></svg></button></div>');

        formEl.style.cssText = '';

        // Initialize switches
        initFormSwitches(formEl);

        // Initialize strike pair
        var criteriaEl = formEl.querySelector('[data-strike-criteria-select]');
        if (criteriaEl && window._initStrikePair) {
            var cont = criteriaEl.closest('.flex.flex-wrap.items-end.gap-3');
            var wpr = cont && cont.querySelector('[data-strike-value-wrapper]');
            if (wpr) window._initStrikePair(criteriaEl, wpr);
        }

        // Register in global state
        window._idleLegElements = window._idleLegElements || {};
        window._idleLegElements[finalName] = formEl;
        window._idleLegConfigs[finalName] = finalName;

        renderLazyLegCard(finalName, formEl);
        return formEl;
    }

    function clearAllIdleLegs() {
        var container = document.getElementById('lazy-legs-container');
        if (container) {
            while (container.firstChild) container.removeChild(container.firstChild);
        }
        var sec = document.getElementById('lazy-legs-section');
        if (sec) sec.style.display = 'none';
        window._idleLegElements = {};
        window._idleLegConfigs = {};
    }

    window._createIdleLeg = createIdleLeg;
    window._clearAllIdleLegs = clearAllIdleLegs;
    window._showLazyPicker = showLazyPicker;

    /* ── Event delegation ────────────────────────────────── */

    document.addEventListener('change', function (e) {
        if (e.target.tagName !== 'SELECT') return;
        var val = e.target.value;
        if (val === 'ReentryType.NextLeg') {
            showLazyPicker(e.target);
        } else if (val.startsWith('ReentryType.')) {
            hideLazyPicker(e.target);
        }
    });

})();


(function () {
    const JSON_PATH = getStrategyApiUrl('backtestSampleResult');
    const TD = 'border-r border-r-primaryBlack-350 bg-primaryBlack-50 px-2 py-1 text-left text-xs font-light md:border-none md:px-4 md:py-3 !p-0 border-r border-r-primaryBlack-350';

    function fmt(n) {
        if (!Number.isFinite(n) || n === 0) return '0';
        return Math.abs(Math.round(n)).toLocaleString('en-IN');
    }

    function dateLabel(dateString) {
        if (!dateString) return '—';
        const parts = dateString.split('-');
        return parts[2] + '/' + parts[1] + '/' + parts[0];
    }

    function setStatus(message, tone) {
        const tbody = document.getElementById('yearwise-tbody');
        if (!tbody) return;

        const colorClass = tone === 'error' ? 'text-red-500' : 'text-primaryBlack-600';
        tbody.innerHTML = '<tr class="rounded text-primaryBlack-750 !rounded-lg" style="box-shadow: rgba(0, 0, 0, 0.04) 0px 4px 20px 0px;">'
            + '<td colspan="17" class="bg-primaryBlack-0 px-4 py-6 text-center text-sm ' + colorClass + '">'
            + message
            + '</td></tr>';
    }

    function pnlCell(val, extraCls) {
        const col = val < 0 ? 'text-red-500' : 'text-green-500';
        const sign = val < 0 ? '-' : '';
        return '<td class="' + TD + ' ' + (extraCls || 'bg-primaryBlack-0') + '">'
            + '<div class="flex flex-col">'
            + '<p class="whitespace-nowrap px-2.5 font-medium ' + col + '">' + sign + fmt(val) + '</p>'
            + '</div></td>';
    }

    function getTradePnl(trade) {
        if (Number.isFinite(Number(trade.total_pnl))) {
            return Number(trade.total_pnl);
        }

        return (trade.legs || []).reduce(function (sum, leg) {
            return sum + Number(leg.pnl || 0);
        }, 0);
    }

    function getNetTradePnl(trade) {
        var brokerage = typeof window._getTradeBrokerage === 'function' ? Number(window._getTradeBrokerage(trade) || 0) : 0;
        var taxes = typeof window._getTradeTexes === 'function' ? Number(window._getTradeTexes(trade) || 0) : 0;
        var slippage = typeof window._getTradeSlippage === 'function' ? Number(window._getTradeSlippage(trade) || 0) : 0;
        return getTradePnl(trade) - brokerage - taxes - slippage;
    }

    window._getNetTradePnl = getNetTradePnl;

    function computeYearWise(trades) {
        const byYear = {};
        const pnlByDay = {};

        trades.forEach(function (trade) {
            if (!trade.date) return;

            const parts = trade.date.split('-');
            const year = parseInt(parts[0], 10);
            const month = parseInt(parts[1], 10);
            const tradePnl = getNetTradePnl(trade);

            if (!byYear[year]) {
                byYear[year] = { months: {} };
                for (let i = 1; i <= 12; i += 1) {
                    byYear[year].months[i] = 0;
                }
            }

            byYear[year].months[month] += tradePnl;
            pnlByDay[trade.date] = (pnlByDay[trade.date] || 0) + tradePnl;
        });

        const allDays = Object.keys(pnlByDay).sort();
        const results = {};

        Object.keys(byYear).sort().forEach(function (year) {
            const yearDays = allDays.filter(function (day) {
                return day.startsWith(year + '-');
            });

            if (!yearDays.length) return;

            let cumulative = 0;
            let peak = 0;
            let drawdownStartCandidate = yearDays[0];
            let maxDrawdown = 0;
            let mddStart = yearDays[0];
            let mddEnd = yearDays[0];

            yearDays.forEach(function (day) {
                cumulative += pnlByDay[day];

                if (cumulative >= peak) {
                    peak = cumulative;
                    drawdownStartCandidate = day;
                }

                const drawdown = cumulative - peak;
                if (drawdown < maxDrawdown) {
                    maxDrawdown = drawdown;
                    mddStart = drawdownStartCandidate;
                    mddEnd = day;
                }
            });

            const total = Object.values(byYear[year].months).reduce(function (sum, value) {
                return sum + value;
            }, 0);

            const mddDays = mddStart && mddEnd
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

    function renderRows(data) {
        const tbody = document.getElementById('yearwise-tbody');
        if (!tbody) return;

        const years = Object.keys(data).sort();
        if (!years.length) {
            setStatus('No year-wise data available.');
            return;
        }

        tbody.innerHTML = '';

        years.forEach(function (year) {
            const row = data[year];
            const tr = document.createElement('tr');
            tr.className = 'rounded text-primaryBlack-750 !rounded-lg';
            tr.style.boxShadow = 'rgba(0,0,0,0.04) 0px 4px 20px 0px';

            let html = '<td class="' + TD + ' sticky left-0 z-[5] bg-primaryBlack-50"><p class="px-1 font-bold">' + year + '</p></td>';

            for (let month = 1; month <= 12; month += 1) {
                html += pnlCell(row.months[month] || 0);
            }

            html += pnlCell(row.total);
            html += pnlCell(row.maxDD);

            html += '<td class="' + TD + ' z-[5] md:sticky md:right-[120px] bg-primaryBlack-50">'
                + '<p class="px-4">' + row.mddDays + '</p>'
                + '<p class="px-4 text-xs text-primaryBlack-600">' + row.mddRange + '</p>'
                + '</td>';

            html += '<td class="' + TD + ' right-0 z-[5] md:sticky bg-primaryBlack-50">'
                + '<p class="px-2.5 font-medium text-primaryBlack-750">' + row.roMDD + '</p>'
                + '</td>';

            tr.innerHTML = html;
            tbody.appendChild(tr);
        });
    }

    function formatCurrency(value, decimals) {
        var digits = typeof decimals === 'number' ? decimals : 2;
        var absValue = Math.abs(Number(value) || 0);
        var formatted = absValue.toLocaleString('en-IN', {
            minimumFractionDigits: digits,
            maximumFractionDigits: digits
        });
        return (value < 0 ? '₹ -' : '₹ ') + formatted;
    }

    function formatNumber(value, decimals) {
        var digits = typeof decimals === 'number' ? decimals : 2;
        return Number(value || 0).toLocaleString('en-IN', {
            minimumFractionDigits: digits,
            maximumFractionDigits: digits
        });
    }

    function computeSummaryMetrics(trades) {
        var pnls = trades.map(function (trade) {
            return getNetTradePnl(trade);
        });

        var totalProfit = pnls.reduce(function (sum, pnl) {
            return sum + pnl;
        }, 0);
        var winningTrades = pnls.filter(function (pnl) { return pnl > 0; });
        var losingTrades = pnls.filter(function (pnl) { return pnl < 0; });
        var tradeCount = pnls.length;

        var avgProfit = tradeCount ? totalProfit / tradeCount : 0;
        var avgWin = winningTrades.length
            ? winningTrades.reduce(function (sum, pnl) { return sum + pnl; }, 0) / winningTrades.length
            : 0;
        var avgLoss = losingTrades.length
            ? losingTrades.reduce(function (sum, pnl) { return sum + pnl; }, 0) / losingTrades.length
            : 0;

        var maxProfit = tradeCount ? Math.max.apply(null, pnls) : 0;
        var maxLoss = tradeCount ? Math.min.apply(null, pnls) : 0;
        var winPct = tradeCount ? (winningTrades.length / tradeCount) * 100 : 0;
        var lossPct = tradeCount ? (losingTrades.length / tradeCount) * 100 : 0;

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
            var pnl = getNetTradePnl(trade);

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

        var expectancy = tradeCount
            ? ((winPct / 100) * avgWin) + ((lossPct / 100) * avgLoss)
            : 0;

        var rewardToRisk = avgLoss !== 0 ? Math.abs(avgWin / avgLoss) : 0;
        var expectancyRatio = avgLoss !== 0 ? Math.abs(expectancy / avgLoss) : 0;
        var returnToMaxDD = maxDrawdown !== 0 ? Math.abs(totalProfit / maxDrawdown) : 0;
        var durationDays = maxDrawdownStart && maxDrawdownEnd
            ? Math.round((new Date(maxDrawdownEnd) - new Date(maxDrawdownStart)) / 86400000) + 1
            : 0;

        var maxProfitTrade = null;
        var maxLossTrade = null;

        trades.forEach(function (trade) {
            var pnl = getNetTradePnl(trade);
            if (!maxProfitTrade || pnl > getNetTradePnl(maxProfitTrade)) {
                maxProfitTrade = trade;
            }
            if (!maxLossTrade || pnl < getNetTradePnl(maxLossTrade)) {
                maxLossTrade = trade;
            }
        });

        return {
            'Overall Profit': { value: formatCurrency(totalProfit, 2), tone: totalProfit < 0 ? 'negative' : 'positive' },
            'No. of Trades': { value: String(tradeCount), tone: 'neutral' },
            'Average Profit per Trade': { value: formatCurrency(avgProfit, 2), tone: avgProfit < 0 ? 'negative' : 'positive' },
            'Win %': { value: formatNumber(winPct, 2), tone: 'neutral' },
            'Loss %': { value: formatNumber(lossPct, 2), tone: 'neutral' },
            'Average Profit on Winning Trades': { value: formatCurrency(avgWin, 2), tone: avgWin < 0 ? 'negative' : 'positive' },
            'Average Loss on Losing Trades': { value: formatCurrency(avgLoss, 2), tone: 'negative' },
            'Max Profit in Single Trade': {
                value: formatCurrency(maxProfit, 2),
                tone: maxProfit < 0 ? 'negative' : 'positive',
                subtext: maxProfitTrade && maxProfitTrade.date ? 'Date: ' + dateLabel(maxProfitTrade.date) : ''
            },
            'Max Loss in Single Trade': {
                value: formatCurrency(maxLoss, 2),
                tone: 'negative',
                subtext: maxLossTrade && maxLossTrade.date ? 'Date: ' + dateLabel(maxLossTrade.date) : ''
            },
            'Max Drawdown': { value: formatCurrency(maxDrawdown, 2), tone: 'negative' },
            'Duration of Max Drawdown': { value: durationDays + ' [' + dateLabel(maxDrawdownStart) + ' to ' + dateLabel(maxDrawdownEnd) + ']', tone: 'neutral' },
            'Return/MaxDD': { value: formatNumber(returnToMaxDD, 2), tone: 'neutral' },
            'Reward to Risk Ratio': { value: formatNumber(rewardToRisk, 2), tone: 'neutral' },
            'Expectancy Ratio': { value: formatNumber(expectancyRatio, 2), tone: 'neutral' },
            'Max Win Streak (trades)': { value: String(maxWinStreak), tone: 'neutral' },
            'Max Losing Streak (trades)': { value: String(maxLossStreak), tone: 'neutral' },
            'Max trades in any drawdown': { value: String(maxTradesInDrawdown), tone: 'neutral' }
        };
    }

    function renderSummaryMetrics(trades) {
        var metrics = computeSummaryMetrics(trades);
        var tables = document.querySelectorAll('#backtest-results table.box-border');

        tables.forEach(function (table) {
            table.querySelectorAll('tbody tr').forEach(function (row) {
                var labelCell = row.children[0];
                var valueCell = row.children[1];
                if (!labelCell || !valueCell) return;

                var label = labelCell.textContent.replace(/AlgoTest\.in/g, '').replace(/\s+/g, ' ').trim();
                var metric = metrics[label];
                if (!metric) return;

                valueCell.innerHTML = '<div class="flex flex-col items-center gap-1"><span>' + metric.value + '</span>'
                    + (metric.subtext ? '<span class="text-[11px] font-medium text-primaryBlack-700">' + metric.subtext + '</span>' : '')
                    + '</div>';
                valueCell.classList.remove('text-green-500', 'text-red-500', 'text-primaryBlack-600');

                var wrapper = valueCell.firstElementChild;
                if (metric.tone === 'positive') {
                    valueCell.classList.add('text-green-500');
                } else if (metric.tone === 'negative') {
                    valueCell.classList.add('text-red-500');
                } else {
                    valueCell.classList.add('text-primaryBlack-600');
                }

                if (wrapper && metric.subtext) {
                    wrapper.lastElementChild.classList.remove('text-green-500', 'text-red-500', 'text-primaryBlack-600');
                    wrapper.lastElementChild.classList.add('text-primaryBlack-700');
                }
            });
        });
    }

    function formatPrice(value) {
        if (value === null || value === undefined || value === '') return '';
        return Number(value).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function formatPnl(value) {
        if (value === null || value === undefined || value === '') return '';
        return Number(value).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function flattenFullReportRows(trades) {
        var rows = [];

        trades.forEach(function (trade, tradeIndex) {
            var tradeNumber = String(tradeIndex + 1);
            rows.push({
                index: tradeNumber,
                entryDate: trade.date || '',
                entryTime: trade.entry_time || '',
                exitDate: trade.date || '',
                exitTime: trade.exit_time || '',
                type: '',
                strike: '',
                bs: '',
                qty: '',
                entryType: '',
                entryPrice: '',
                exitType: '',
                exitPrice: '',
                legType: '',
                reentry: '',
                reentryReason: '',
                vix: '',
                pnl: getTradePnl(trade),
                isSummary: true
            });

            var detailIndex = 1;
            (trade.legs || []).forEach(function (leg) {
                var qty = Number(leg.lots || 0) * Number(leg.lot_size || 0);
                var subTrades = leg.sub_trades && leg.sub_trades.length ? leg.sub_trades : [null];

                subTrades.forEach(function (subTrade) {
                    rows.push({
                        index: tradeNumber + '.' + detailIndex,
                        entryDate: subTrade && subTrade.entry_date ? subTrade.entry_date : (trade.date || ''),
                        entryTime: subTrade && subTrade.entry_time ? subTrade.entry_time : (leg.entry_time || trade.entry_time || ''),
                        exitDate: subTrade && subTrade.exit_date ? subTrade.exit_date : (trade.date || ''),
                        exitTime: subTrade && subTrade.exit_time ? subTrade.exit_time : (leg.exit_time || trade.exit_time || ''),
                        type: subTrade && subTrade.option_type ? subTrade.option_type : (leg.type || ''),
                        strike: subTrade && subTrade.strike ? subTrade.strike : (leg.strike || ''),
                        bs: leg.position || (subTrade && subTrade.entry_action ? subTrade.entry_action : ''),
                        qty: qty || '',
                        entryType: subTrade && subTrade.entry_action ? subTrade.entry_action : '',
                        entryPrice: subTrade && subTrade.entry_price !== undefined ? subTrade.entry_price : leg.entry_price,
                        exitType: subTrade && subTrade.exit_action ? subTrade.exit_action : '',
                        exitPrice: subTrade && subTrade.exit_price !== undefined ? subTrade.exit_price : leg.exit_price,
                        legType: subTrade && subTrade.reentry_type ? subTrade.reentry_type : '',
                        reentry: subTrade && subTrade.reentry_number !== undefined && subTrade.reentry_number !== null ? subTrade.reentry_number : '',
                        reentryReason: subTrade && subTrade.exit_reason ? subTrade.exit_reason : (leg.exit_reason || ''),
                        vix: '',
                        pnl: subTrade && subTrade.pnl !== undefined ? subTrade.pnl : leg.pnl,
                        isSummary: false
                    });
                    detailIndex += 1;
                });
            });
        });

        return rows;
    }

    // ── Full Report state ──────────────────────────────────────────────
    var FR_PAGE_SIZE = 10;
    var frAllTrades = [];
    window.frAllTrades = frAllTrades;
    var frSortBy = 'EntryDate';
    var frSortDir = 'Asc';
    var frCurrentPage = 1;
    var tradeExplainModal = document.getElementById('trade-explain-modal');
    var tradeExplainBody = document.getElementById('trade-explain-body');
    var tradeExplainSubtitle = document.getElementById('trade-explain-subtitle');
    var tradeExplainCloseBtn = document.getElementById('trade-explain-close');

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function isExplanationNumeric(value) {
        if (typeof value === 'number') return Number.isFinite(value);
        if (typeof value !== 'string') return false;
        return /^-?\d+(\.\d+)?$/.test(value.trim());
    }

    function formatExplanationValue(value, options) {
        var opts = options || {};
        if (value === null || value === undefined || value === '') return '—';
        if (opts.currency && isExplanationNumeric(value)) {
            var amount = Number(value);
            var prefix = amount < 0 ? '-₹' : '₹';
            return prefix + Math.abs(amount).toLocaleString('en-IN', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            });
        }
        return String(value);
    }

    function makeExplanationMetaItem(label, value, extraClass) {
        return '<div class="trade-explain-step__box' + (extraClass ? ' ' + extraClass : '') + '">'
            + '<div class="trade-explain-step__box-title">' + escapeHtml(label) + '</div>'
            + '<div class="trade-explain-step__box-value">' + escapeHtml(value) + '</div>'
            + '</div>';
    }

    function renderExplanationEntries(entries, type) {
        if (!Array.isArray(entries) || !entries.length) {
            return '<div class="trade-explain-step__empty">No ' + type + ' legs at this step.</div>';
        }

        return entries.map(function (entry) {
            var title = formatExplanationValue(entry && entry.leg);
            if (type === 'realized') {
                return '<div class="trade-explain-step__entry">'
                    + '<div class="trade-explain-step__entry-title">' + escapeHtml(title) + '</div>'
                    + '<div class="trade-explain-step__entry-text">Exit '
                    + escapeHtml(formatExplanationValue(entry && entry.exit_time))
                    + ' | Reason ' + escapeHtml(formatExplanationValue(entry && entry.exit_reason))
                    + ' | P/L ' + escapeHtml(formatExplanationValue(entry && entry.pnl, { currency: true }))
                    + '</div></div>';
            }

            return '<div class="trade-explain-step__entry">'
                + '<div class="trade-explain-step__entry-title">' + escapeHtml(title) + '</div>'
                + '<div class="trade-explain-step__entry-text">Entry '
                + escapeHtml(formatExplanationValue(entry && entry.entry_time))
                + ' @ ' + escapeHtml(formatExplanationValue(entry && entry.entry_price, { currency: true }))
                + ' | Candle ' + escapeHtml(formatExplanationValue(entry && entry.candle_time))
                + ' @ ' + escapeHtml(formatExplanationValue(entry && entry.cur_price, { currency: true }))
                + '</div>'
                + '<div class="trade-explain-step__entry-text">Qty '
                + escapeHtml(formatExplanationValue(entry && entry.qty))
                + ' | Unrealized P/L '
                + escapeHtml(formatExplanationValue(entry && entry.unrealized_pnl, { currency: true }))
                + '</div></div>';
        }).join('');
    }

    function renderTradeExplanationSteps(steps) {
        if (!Array.isArray(steps) || !steps.length) {
            return '<div class="trade-explain-modal__empty">Trade explanation is not available for this trade.</div>';
        }

        return steps.map(function (step, stepIndex) {
            var breakdown = step && step.combined_mtm_breakdown ? step.combined_mtm_breakdown : null;
            var html = '<div class="trade-explain-step">'
                + '<div class="trade-explain-step__header">'
                + '<div class="trade-explain-step__badge">Step ' + escapeHtml(formatExplanationValue(step && (step.step != null ? step.step : (stepIndex + 1)))) + '</div>'
                + '<div class="trade-explain-step__time">' + escapeHtml(formatExplanationValue(step && step.time)) + '</div>'
                + '</div>'
                + '<div class="trade-explain-step__grid">'
                + makeExplanationMetaItem('Event Type', formatExplanationValue(step && step.event_type))
                + makeExplanationMetaItem('Parent Leg', formatExplanationValue(step && step.parent_leg))
                + makeExplanationMetaItem('Leg', formatExplanationValue(step && step.leg))
                + makeExplanationMetaItem('Kind', formatExplanationValue(step && step.kind))
                + makeExplanationMetaItem('Combined MTM', formatExplanationValue(step && step.combined_mtm, { currency: true }), 'is-wide')
                + makeExplanationMetaItem('Overall SL Limit', formatExplanationValue(step && step.overall_sl_limit), 'is-wide')
                + '</div>'
                + '<div class="trade-explain-step__section">'
                + '<div class="trade-explain-step__section-title">What happened</div>'
                + '<div class="trade-explain-step__section-text">' + escapeHtml(formatExplanationValue(step && step.description)) + '</div>'
                + '</div>';

            if (breakdown) {
                html += '<div class="trade-explain-step__section">'
                    + '<div class="trade-explain-step__section-title">Combined MTM Breakdown</div>'
                    + '<div class="trade-explain-step__summary" style="margin-top:12px;">'
                    + makeExplanationMetaItem('Candle Time', formatExplanationValue(breakdown.candle_time))
                    + makeExplanationMetaItem('Realized Total', formatExplanationValue(breakdown.realized_total, { currency: true }))
                    + makeExplanationMetaItem('Unrealized Total', formatExplanationValue(breakdown.unrealized_total, { currency: true }))
                    + makeExplanationMetaItem('Combined MTM', formatExplanationValue(breakdown.combined_mtm, { currency: true }))
                    + '</div>'
                    + '<div class="trade-explain-step__subheading">Realized</div>'
                    + renderExplanationEntries(breakdown.realized, 'realized')
                    + '<div class="trade-explain-step__subheading">Unrealized</div>'
                    + renderExplanationEntries(breakdown.unrealized, 'unrealized')
                    + '</div>';
            }

            html += '</div>';
            return html;
        }).join('');
    }

    function closeTradeExplanationModal() {
        if (!tradeExplainModal) return;
        tradeExplainModal.hidden = true;
        document.body.style.overflow = '';
    }

    function openTradeExplanationModal(trade, indexLabel) {
        if (!tradeExplainModal || !tradeExplainBody || !tradeExplainSubtitle) return;
        var tradePnl = getTradePnlForSort(trade);
        var steps = Array.isArray(trade && trade.trade_explanation_content && trade.trade_explanation_content.steps)
            ? trade.trade_explanation_content.steps
            : [];

        tradeExplainSubtitle.textContent = String(indexLabel || '—')
            + ' | '
            + String(trade && trade.date ? trade.date : '—')
            + ' | P/L '
            + formatExplanationValue(tradePnl, { currency: true });
        tradeExplainBody.innerHTML = renderTradeExplanationSteps(steps);
        tradeExplainBody.scrollTop = 0;
        tradeExplainModal.hidden = false;
        document.body.style.overflow = 'hidden';
    }

    if (tradeExplainCloseBtn && !tradeExplainCloseBtn.dataset.boundTradeExplainClose) {
        tradeExplainCloseBtn.dataset.boundTradeExplainClose = 'true';
        tradeExplainCloseBtn.addEventListener('click', closeTradeExplanationModal);
    }

    if (tradeExplainModal && !tradeExplainModal.dataset.boundTradeExplainOverlay) {
        tradeExplainModal.dataset.boundTradeExplainOverlay = 'true';
        tradeExplainModal.addEventListener('click', function (event) {
            if (event.target && event.target.hasAttribute('data-trade-explain-close')) {
                closeTradeExplanationModal();
            }
        });
        document.addEventListener('keydown', function (event) {
            if (event.key === 'Escape' && tradeExplainModal && !tradeExplainModal.hidden) {
                closeTradeExplanationModal();
            }
        });
    }

    function getTradePnlForSort(trade) {
        return getNetTradePnl(trade);
    }

    function sortedTrades() {
        var copy = frAllTrades.slice();
        copy.sort(function (a, b) {
            var cmp = 0;
            if (frSortBy === 'ProfitLoss') {
                cmp = getTradePnlForSort(a) - getTradePnlForSort(b);
            } else {
                // EntryDate: compare date + time string
                var ka = (a.date || '') + ' ' + (a.entry_time || '');
                var kb = (b.date || '') + ' ' + (b.entry_time || '');
                cmp = ka < kb ? -1 : ka > kb ? 1 : 0;
            }
            return frSortDir === 'Desc' ? -cmp : cmp;
        });
        return copy;
    }

    function renderFullReport() {
        var tbody = document.getElementById('full-report-tbody');
        var totalEl = document.getElementById('full-report-total');
        var rangeEl = document.getElementById('full-report-range');
        var pagingEl = document.getElementById('fr-pagination');
        if (!tbody) return;

        var trades = sortedTrades();
        var totalTrades = trades.length;
        var totalPages = Math.ceil(totalTrades / FR_PAGE_SIZE);
        if (frCurrentPage > totalPages) frCurrentPage = 1;

        var startIdx = (frCurrentPage - 1) * FR_PAGE_SIZE;          // trade index
        var pageTrades = trades.slice(startIdx, startIdx + FR_PAGE_SIZE);

        // flatten only the page trades; keep global index numbers
        var cellCls = 'border-2 border-zinc-300 p-1 text-xs font-medium dark:border-primaryBlack-500';
        var html = '';
        pageTrades.forEach(function (trade, i) {
            var globalIdx = startIdx + i + 1;
            var tradePnl = getTradePnlForSort(trade);
            var pnlCls = tradePnl < 0 ? 'text-red-500' : 'text-green-600';
            var parentRowCls = 'border-b border-b-white';
            var parentCellCls = cellCls;
            var parentBgStyle = ' style="background-color: rgb(208, 225, 255);"';

            // summary row
            html += '<tr class="' + parentRowCls + '"' + parentBgStyle + '>'
                + '<td class="' + parentCellCls + ' font-bold"' + parentBgStyle + '>' + globalIdx + '</td>'
                + '<td class="' + parentCellCls + '"' + parentBgStyle + '>' + (trade.date || '') + '</td>'
                + '<td class="' + parentCellCls + '"' + parentBgStyle + '>' + (trade.entry_time || '') + '</td>'
                + '<td class="' + parentCellCls + '"' + parentBgStyle + '>' + (trade.date || '') + '</td>'
                + '<td class="' + parentCellCls + '"' + parentBgStyle + '>' + (trade.exit_time || '') + '</td>'
                + '<td colspan="10" class="' + parentCellCls + '"' + parentBgStyle + '></td>'
                + '<td class="' + parentCellCls + '"' + parentBgStyle + '>'
                + '<button type="button" data-fr-view-trade="' + globalIdx + '"'
                + ' class="inline-flex min-w-[72px] items-center justify-center rounded-full border px-4 text-xs font-semibold"'
                + ' style="background-color: rgb(238, 246, 255); height: 28px; border-color: rgb(15, 98, 165); color: rgb(15, 98, 165);"'
                + '>View</button>'
                + '</td>'
                + '<td class="' + parentCellCls + '"' + parentBgStyle + '></td>'
                + '<td class="' + parentCellCls + ' ' + pnlCls + ' font-bold"' + parentBgStyle + '>' + formatPnl(tradePnl) + '</td>'
                + '</tr>';

            // leg sub-rows
            var detailIdx = 1;
            (trade.legs || []).forEach(function (leg) {
                var qty = Number(leg.lots || 0) * Number(leg.lot_size || 0);
                var subTrades = leg.sub_trades && leg.sub_trades.length ? leg.sub_trades : [null];
                subTrades.forEach(function (st) {
                    var rawStPnl = st && st.pnl !== undefined ? st.pnl : leg.pnl;
                    var detailBrokerage = typeof window._getDetailBrokerage === 'function' ? window._getDetailBrokerage(leg, st) : 0;
                    var stPnl = Number(rawStPnl || 0) - Number(detailBrokerage || 0);
                    var stPnlCls = Number(stPnl || 0) < 0 ? 'text-red-500' : 'text-green-600';
                    html += '<tr class="border-b border-b-white">'
                        + '<td class="' + cellCls + ' bg-primaryBlack-350">' + globalIdx + '.' + detailIdx + '</td>'
                        + '<td class="' + cellCls + '">' + (st && st.entry_date ? st.entry_date : (trade.date || '')) + '</td>'
                        + '<td class="' + cellCls + '">' + (st && st.entry_time ? st.entry_time : (leg.entry_time || '')) + '</td>'
                        + '<td class="' + cellCls + '">' + (st && st.exit_date ? st.exit_date : (trade.date || '')) + '</td>'
                        + '<td class="' + cellCls + '">' + (st && st.exit_time ? st.exit_time : (leg.exit_time || '')) + '</td>'
                        + '<td class="' + cellCls + '">' + (st && st.option_type ? st.option_type : (leg.type || '')) + '</td>'
                        + '<td class="' + cellCls + '">' + (st && st.strike ? st.strike : (leg.strike || '')) + '</td>'
                        + '<td class="' + cellCls + '">' + (leg.position || '') + '</td>'
                        + '<td class="' + cellCls + '">' + (qty || '') + '</td>'
                        + '<td class="' + cellCls + '">' + (st && st.entry_action ? st.entry_action : '') + '</td>'
                        + '<td class="' + cellCls + '">' + formatPrice(st && st.entry_price !== undefined ? st.entry_price : leg.entry_price) + '</td>'
                        + '<td class="' + cellCls + '">' + (st && st.exit_action ? st.exit_action : '') + '</td>'
                        + '<td class="' + cellCls + '">' + formatPrice(st && st.exit_price !== undefined ? st.exit_price : leg.exit_price) + '</td>'
                        + '<td class="' + cellCls + '">' + (st && st.reentry_type ? st.reentry_type : '') + '</td>'
                        + '<td class="' + cellCls + '">' + (st && st.reentry_number !== undefined && st.reentry_number !== null ? st.reentry_number : '') + '</td>'
                        + '<td class="' + cellCls + '">' + (st && st.exit_reason ? st.exit_reason : (leg.exit_reason || '')) + '</td>'
                        + '<td class="' + cellCls + '"></td>'
                        + '<td class="' + cellCls + ' ' + stPnlCls + '">' + formatPnl(stPnl) + '</td>'
                        + '</tr>';
                    detailIdx++;
                });
            });
        });

        tbody.innerHTML = html || '<tr><td colspan="18" class="' + cellCls + ' p-3 text-center text-primaryBlack-600">No data.</td></tr>';

        // showing range (in trades)
        var from = totalTrades ? startIdx + 1 : 0;
        var to = Math.min(startIdx + FR_PAGE_SIZE, totalTrades);
        if (totalEl) totalEl.textContent = totalTrades;
        if (rangeEl) rangeEl.textContent = from + ' - ' + to;

        tbody.querySelectorAll('[data-fr-view-trade]').forEach(function (button) {
            button.addEventListener('click', function () {
                var rowNumber = parseInt(button.getAttribute('data-fr-view-trade') || '', 10);
                if (!Number.isFinite(rowNumber) || rowNumber < 1) return;
                var trade = pageTrades[rowNumber - startIdx - 1];
                if (!trade) return;
                openTradeExplanationModal(trade, 'Index ' + rowNumber);
            });
        });

        // ── Pagination ─────────────────────────────────────────────────
        if (!pagingEl) return;
        var MAX_VISIBLE = 5;
        var btnBase = 'border border-primaryBlue-500 min-w-[28px] px-2 py-0.5 text-sm rounded cursor-pointer';
        var btnActive = btnBase + ' bg-primaryBlue-500 text-white font-semibold';
        var btnNav = 'text-primaryBlue-500 px-1 py-0.5 cursor-pointer disabled:opacity-30';

        var half = Math.floor(MAX_VISIBLE / 2);
        var start = Math.max(1, frCurrentPage - half);
        var end = Math.min(totalPages, start + MAX_VISIBLE - 1);
        if (end - start < MAX_VISIBLE - 1) start = Math.max(1, end - MAX_VISIBLE + 1);

        var pHtml = '';
        // first + prev
        pHtml += '<button class="' + btnNav + '" data-pg="1" title="First">«</button>';
        pHtml += '<button class="' + btnNav + '" data-pg="' + Math.max(1, frCurrentPage - 1) + '" title="Prev">‹</button>';
        // page numbers
        for (var p = start; p <= end; p++) {
            pHtml += '<button class="' + (p === frCurrentPage ? btnActive : btnBase) + '" data-pg="' + p + '">' + p + '</button>';
        }
        // next + last
        pHtml += '<button class="' + btnNav + '" data-pg="' + Math.min(totalPages, frCurrentPage + 1) + '" title="Next">›</button>';
        pHtml += '<button class="' + btnNav + '" data-pg="' + totalPages + '" title="Last">»</button>';

        pagingEl.innerHTML = pHtml;
        pagingEl.querySelectorAll('button[data-pg]').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var pg = parseInt(this.getAttribute('data-pg'));
                if (pg < 1 || pg > totalPages || pg === frCurrentPage) return;
                frCurrentPage = pg;
                renderFullReport();
            });
        });
    }

    function initFullReportControls() {
        // Sort select
        var sel = document.getElementById('fr-sort-select');
        if (sel) {
            sel.value = frSortBy;
            sel.addEventListener('change', function () {
                frSortBy = this.value;
                frCurrentPage = 1;
                renderFullReport();
            });
        }

        // ASC / DESC labels
        var ascLbl = document.getElementById('fr-asc-label');
        var descLbl = document.getElementById('fr-desc-label');

        function applyDir(dir) {
            frSortDir = dir;
            frCurrentPage = 1;
            if (ascLbl) ascLbl.className = ascLbl.className.replace(/bg-tertiaryBlue-500\s*text-white|text-primaryBlack-750/g, '').trim()
                + (dir === 'Asc' ? ' bg-tertiaryBlue-500 text-white' : ' text-primaryBlack-750');
            if (descLbl) descLbl.className = descLbl.className.replace(/bg-tertiaryBlue-500\s*text-white|text-primaryBlack-750/g, '').trim()
                + (dir === 'Desc' ? ' bg-tertiaryBlue-500 text-white' : ' text-primaryBlack-750');
            renderFullReport();
        }

        if (ascLbl) ascLbl.addEventListener('click', function () { applyDir('Asc'); });
        if (descLbl) descLbl.addEventListener('click', function () { applyDir('Desc'); });
        applyDir('Asc'); // set initial visual state without re-rendering (data not loaded yet)
    }

    function loadBacktestData() {
        return fetch(JSON_PATH).then(function (response) {
            if (!response.ok) {
                throw new Error('HTTP ' + response.status);
            }
            return response.json();
        });
    }

    // ── Weekday filter ────────────────────────────────────────────────────────
    var selectedDays = new Set([0, 1, 2, 3, 4, 5, 6]); // all days active by default
    var isWeekdaysExpanded = false;
    var isDTEModeExpanded = false;

    function getDayOfWeek(dateStr) {
        // Returns 0=Sun, 1=Mon, ..., 6=Sat
        var parts = dateStr.split('-');
        return new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2])).getDay();
    }

    function filterByWeekday(trades) {
        if (selectedDays.size === 7) return trades;
        return trades.filter(function (t) {
            return t.date && selectedDays.has(getDayOfWeek(t.date));
        });
    }

    function filterByDTE(trades) {
        if (!dteSelectedSet) return trades;
        return trades.filter(function (t) {
            var d = getTradeDTE(t);
            return d !== null && dteSelectedSet.has(d);
        });
    }

    // Expand each main trade into per-cycle sub-trade objects.
    // A "cycle" = same index across all legs' sub_trades.
    // Cycle legs have entry_price/exit_price/lots for that cycle only,
    // so brokerage / taxes / slippage all calculate correctly per cycle.
    function flattenToSubTrades(trades) {
        var result = [];
        trades.forEach(function (trade) {
            var legs = trade.legs || [];

            // Max cycles across all legs
            var maxCycles = 0;
            legs.forEach(function (leg) {
                var n = leg.sub_trades && leg.sub_trades.length ? leg.sub_trades.length : 1;
                if (n > maxCycles) maxCycles = n;
            });

            if (maxCycles <= 1) {
                result.push(trade);
                return;
            }

            for (var ci = 0; ci < maxCycles; ci++) {
                var cyclePnl = 0;
                var cycleEntryTime = null;
                var cycleExitTime = null;
                var cycleLegs = [];

                legs.forEach(function (leg) {
                    var st = leg.sub_trades && leg.sub_trades.length > ci ? leg.sub_trades[ci] : null;
                    if (!st && ci > 0) return; // leg has fewer cycles — skip

                    var stPnl = st ? Number(st.pnl || 0) : Number(leg.pnl || 0);
                    var entryPx = st ? st.entry_price : leg.entry_price;
                    var exitPx = st ? st.exit_price : leg.exit_price;
                    var entryTm = st ? (st.entry_time || st.entry_date) : leg.entry_time;
                    var exitTm = st ? (st.exit_time || st.exit_date) : leg.exit_time;
                    var stLots = (st && st.lots) ? Number(st.lots) : Number(leg.lots || 1);

                    cyclePnl += stPnl;
                    if (!cycleEntryTime || entryTm < cycleEntryTime) cycleEntryTime = entryTm;
                    if (!cycleExitTime || exitTm > cycleExitTime) cycleExitTime = exitTm;

                    cycleLegs.push({
                        lots: stLots,
                        lot_size: Number(leg.lot_size || 1),
                        position: leg.position,
                        entry_price: entryPx,
                        exit_price: exitPx,
                        pnl: stPnl,
                        sub_trades: []   // already flattened
                    });
                });

                result.push({
                    date: trade.date,
                    entry_time: cycleEntryTime || trade.entry_time,
                    exit_time: cycleExitTime || trade.exit_time,
                    total_pnl: cyclePnl,
                    legs: cycleLegs,
                    _cycle: ci + 1
                });
            }
        });
        return result;
    }

    function refreshAllSections() {
        if (!window._rawTrades) return;
        var filtered = filterByWeekday(window._rawTrades);
        filtered = filterByDTE(filtered);
        var flat = flattenToSubTrades(filtered);  // for metrics / year-wise / charts
        if (window._setTrades) window._setTrades(flat, filtered); // flat=calc, filtered=report
        if (window._rebuildCharts) window._rebuildCharts(flat);
    }
    window._refreshAllSections = refreshAllSections;

    function updateDayBtnStyles() {
        document.querySelectorAll('.wd-day-btn').forEach(function (btn) {
            var day = parseInt(btn.dataset.day);
            // active: blue outline circle  |  inactive: muted gray
            btn.classList.remove(
                'border-blue-500', 'text-blue-500', 'bg-transparent',
                'border-gray-300', 'text-gray-400', 'bg-gray-50',
                'dark:border-gray-600', 'dark:text-gray-500'
            );
            if (selectedDays.has(day)) {
                btn.classList.add('border-blue-500', 'text-blue-500', 'bg-transparent');
            } else {
                btn.classList.add('border-gray-300', 'text-gray-400', 'bg-gray-50',
                    'dark:border-gray-600', 'dark:text-gray-500');
            }
        });
    }

    // ── shared show / hide helpers ────────────────────────────────────────────
    function showEl(id, displayVal) {
        var el = document.getElementById(id);
        if (!el) return;
        el.classList.remove('hidden');
        el.style.display = displayVal || '';
    }
    function hideEl(id) {
        var el = document.getElementById(id);
        if (!el) return;
        el.classList.add('hidden');
        el.style.display = 'none';
    }

    function showNormalFilterRow() {
        isWeekdaysExpanded = false;
        isDTEModeExpanded = false;
        showEl('wd-collapsed-btn');
        hideEl('wd-close-btn');
        var wdPanel = document.getElementById('wd-day-btns');
        if (wdPanel) {
            wdPanel.classList.add('hidden');
            wdPanel.style.display = 'none';
        }
        var dteExpanded = document.getElementById('dte-expanded-wrapper');
        if (dteExpanded) {
            dteExpanded.classList.add('hidden');
            dteExpanded.style.display = 'none';
        }
        var dteCollapsed = document.getElementById('dte-collapsed-btn');
        if (dteCollapsed) dteCollapsed.style.display = '';
        showEl('dte-filter-wrapper');
        showEl('budget-filter-wrapper');
    }

    // ── reset ALL filters → back to default view ──────────────────────────────
    function resetAllFilters(event) {
        if (event) event.preventDefault();
        // reset weekdays
        selectedDays = new Set([0, 1, 2, 3, 4, 5, 6]);
        updateDayBtnStyles();
        // reset DTE
        dteSelectedSet = null;
        var label = document.getElementById('dte-btn-label');
        if (label) label.textContent = 'DTE';
        var dd = document.getElementById('dte-dropdown');
        if (dd) dd.style.display = 'none';
        // restore normal row
        showNormalFilterRow();
        // refresh all sections with no filter
        refreshAllSections();
    }

    function expandWeekdays(event) {
        if (event) event.preventDefault();
        isWeekdaysExpanded = true;
        hideEl('wd-collapsed-btn');
        showEl('wd-close-btn', 'inline-flex');
        var wdPanel = document.getElementById('wd-day-btns');
        if (wdPanel) {
            wdPanel.classList.remove('hidden');
            wdPanel.style.display = 'flex';
        }
        updateDayBtnStyles();
        hideEl('dte-filter-wrapper');
        hideEl('budget-filter-wrapper');
    }
    window._expandWeekdays = expandWeekdays;
    window._resetAllFilters = resetAllFilters;
    window._populateDTEDropdown = populateDTEDropdown;

    var wdCollapsedBtn = document.getElementById('wd-collapsed-btn');
    if (wdCollapsedBtn) {
        wdCollapsedBtn.addEventListener('click', expandWeekdays);
    }

    var wdCloseBtn = document.getElementById('wd-close-btn');
    if (wdCloseBtn) {
        wdCloseBtn.addEventListener('click', resetAllFilters);
    }

    document.querySelectorAll('.wd-day-btn').forEach(function (btn) {
        btn.addEventListener('click', function (event) {
            event.preventDefault();
            var day = parseInt(btn.dataset.day);
            if (selectedDays.has(day)) {
                if (selectedDays.size > 1) selectedDays.delete(day);
            } else {
                selectedDays.add(day);
            }
            updateDayBtnStyles();
            refreshAllSections();
        });
    });

    // ── DTE filter ────────────────────────────────────────────────────────────
    function getTradeDTE(trade) {
        var legs = trade.legs || [];
        for (var i = 0; i < legs.length; i++) {
            if (legs[i].expiry) {
                var p = trade.date.split('-');
                var e = legs[i].expiry.substring(0, 10).split('-');
                var tradeDate = new Date(parseInt(p[0]), parseInt(p[1]) - 1, parseInt(p[2]));
                var expiryDate = new Date(parseInt(e[0]), parseInt(e[1]) - 1, parseInt(e[2]));
                return Math.max(0, Math.round((expiryDate - tradeDate) / 86400000));
            }
        }
        return null;
    }

    function getUniqueDTEs(trades) {
        var seen = {};
        trades.forEach(function (t) {
            var d = getTradeDTE(t);
            if (d !== null) seen[d] = true;
        });
        return Object.keys(seen).map(Number).sort(function (a, b) { return a - b; });
    }

    var dteSelectedSet = null; // null = all selected (no filter)

    function populateDTEDropdown() {
        var options = getUniqueDTEs(window._rawTrades || []);
        var list = document.getElementById('dte-options-list');
        if (!list) return;
        list.innerHTML = '';
        options.forEach(function (dte) {
            var isChecked = dteSelectedSet === null || dteSelectedSet.has(dte);
            var label = document.createElement('label');
            label.className = 'flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-sm hover:bg-primaryBlack-100';
            label.innerHTML = '<input type="checkbox" class="dte-ck accent-primaryBlue-500" data-dte="' + dte + '"'
                + (isChecked ? ' checked' : '') + '> <span>' + dte + '</span>';
            list.appendChild(label);
        });
    }

    function updateDTEBtnLabel() {
        var cks = Array.from(document.querySelectorAll('.dte-ck'));
        var checked = cks.filter(function (ck) { return ck.checked; });
        var label = document.getElementById('dte-btn-label');
        if (!label) return;
        if (!cks.length || checked.length === cks.length) {
            label.textContent = 'All DTE';
        } else if (checked.length === 1) {
            label.textContent = checked[0].dataset.dte + ' DTE';
        } else {
            label.textContent = checked.length + ' Selected';
        }
    }

    function enterDTEMode() {
        isWeekdaysExpanded = false;
        isDTEModeExpanded = true;
        hideEl('wd-collapsed-btn');
        hideEl('wd-day-btns');
        showEl('wd-close-btn', 'inline-flex');
        var collapsed = document.getElementById('dte-collapsed-btn');
        if (collapsed) collapsed.style.display = 'none';
        var expanded = document.getElementById('dte-expanded-wrapper');
        if (expanded) {
            expanded.classList.remove('hidden');
            expanded.style.display = 'flex';
        }
        hideEl('budget-filter-wrapper');
    }

    function exitDTEMode() {
        isDTEModeExpanded = false;
        var expanded = document.getElementById('dte-expanded-wrapper');
        if (expanded) {
            expanded.classList.add('hidden');
            expanded.style.display = 'none';
        }
        var collapsed = document.getElementById('dte-collapsed-btn');
        if (collapsed) collapsed.style.display = '';
        if (!isWeekdaysExpanded) {
            showNormalFilterRow();
        }
    }

    function syncDTESelectAll() {
        var cks = document.querySelectorAll('.dte-ck');
        var allChecked = Array.from(cks).every(function (ck) { return ck.checked; });
        var sa = document.getElementById('dte-select-all');
        if (sa) sa.checked = allChecked;
    }

    function applyDTEFilter() {
        var cks = Array.from(document.querySelectorAll('.dte-ck'));
        if (!cks.length) { dteSelectedSet = null; return; }
        var allChecked = cks.every(function (ck) { return ck.checked; });
        if (allChecked) {
            dteSelectedSet = null;
        } else {
            dteSelectedSet = new Set();
            cks.forEach(function (ck) {
                if (ck.checked) dteSelectedSet.add(parseInt(ck.dataset.dte));
            });
        }
        updateDTEBtnLabel();
        // keep the expanded DTE design even when Select All is chosen; close only via X
        enterDTEMode();
        refreshAllSections();
    }

    function toggleDTE(e) {
        if (e) { e.preventDefault(); e.stopPropagation(); }
        var dd = document.getElementById('dte-dropdown');
        if (!dd) return;
        if (dd.style.display === 'none' || !dd.style.display) {
            populateDTEDropdown();
            dd.style.display = 'block';
            enterDTEMode();
        } else {
            dd.style.display = 'none';
        }
    }
    window._toggleDTE = toggleDTE;

    var dteCollapsedBtn = document.getElementById('dte-collapsed-btn');
    if (dteCollapsedBtn) {
        dteCollapsedBtn.addEventListener('click', toggleDTE);
    }

    var dteFilterBtn = document.getElementById('dte-filter-btn');
    if (dteFilterBtn) {
        dteFilterBtn.addEventListener('click', toggleDTE);
    }

    // close dropdown on outside click
    document.addEventListener('click', function (e) {
        var wrapper = document.getElementById('dte-filter-wrapper');
        var dd = document.getElementById('dte-dropdown');
        if (dd && wrapper && !wrapper.contains(e.target)) {
            dd.style.display = 'none';
        }
    });

    // Select All checkbox
    document.getElementById('dte-select-all').addEventListener('change', function () {
        var checked = this.checked;
        document.querySelectorAll('.dte-ck').forEach(function (ck) { ck.checked = checked; });
        applyDTEFilter();
    });

    // individual DTE checkboxes (delegated)
    document.getElementById('dte-options-list').addEventListener('change', function (e) {
        if (!e.target.classList.contains('dte-ck')) return;
        syncDTESelectAll();
        applyDTEFilter();
    });
    // ── End DTE filter ────────────────────────────────────────────────────────

    // expose flattenToSubTrades for backtest result handler
    window.flattenToSubTrades = flattenToSubTrades;

    // calcTrades  = flat sub-trade cycles  (for metrics / year-wise / charts)
    // reportTrades = original main trades  (for full report display)
    window._setTrades = function (calcTrades, reportTrades) {
        var displayTrades = reportTrades || calcTrades;
        frAllTrades = displayTrades;
        window.frAllTrades = displayTrades;
        frCurrentPage = 1;
        renderSummaryMetrics(calcTrades);
        renderRows(computeYearWise(calcTrades));
        renderFullReport();
        if (typeof window._syncBrokerageCost === 'function') window._syncBrokerageCost();
        if (typeof window._syncTaxesCost === 'function') window._syncTaxesCost();
        if (typeof window._rebuildCharts === 'function') window._rebuildCharts(calcTrades);
        if (typeof window._setStrategyProgressVisible === 'function') window._setStrategyProgressVisible(false);
        if (typeof window._setStrategyResultSectionVisible === 'function') window._setStrategyResultSectionVisible(true);
    };

    initFullReportControls();
    setStatus('Loading year-wise returns...');
    if (typeof window._resetStrategyBacktestView === 'function') {
        window._resetStrategyBacktestView();
    }

    loadBacktestData()
        .then(function (data) {
            var trades = data.trades || [];
            window._rawTrades = trades;                   // originals — for filter resets
            var flat = flattenToSubTrades(trades);         // sub-trade cycles — for calc
            renderSummaryMetrics(flat);                    // metrics based on sub-trades
            renderRows(computeYearWise(flat));             // year-wise based on sub-trades
            frAllTrades = trades;                   // original trades — for full report
            window.frAllTrades = trades;
            frCurrentPage = 1;
            renderFullReport();                            // shows original main trades
            if (typeof window._syncBrokerageCost === 'function') window._syncBrokerageCost();
            if (typeof window._syncTaxesCost === 'function') window._syncTaxesCost();
        })
        .catch(function (error) {
            console.error('Year-wise load error:', error);
            setStatus('Unable to load year-wise returns.', 'error');
        });
})();


(function () {
    var API_URL = getStrategyApiUrl('backtestSampleResult');
    var pnlChart = null;
    var ddChart = null;

    function buildChartData(trades) {
        // daily cumulative pnl
        var pnlByDay = {};
        trades.forEach(function (t) {
            var tradePnl = typeof window._getNetTradePnl === 'function' ? window._getNetTradePnl(t) : 0;
            pnlByDay[t.date] = (pnlByDay[t.date] || 0) + tradePnl;
        });

        var days = Object.keys(pnlByDay).sort();
        var labels = [];
        var values = [];
        var cum = 0;
        days.forEach(function (d) {
            cum += pnlByDay[d];
            labels.push(d);
            values.push(parseFloat(cum.toFixed(2)));
        });
        return { labels: labels, values: values };
    }

    function fmtDate(dateStr) {
        var p = dateStr.split('-');
        var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
            'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        return p[2] + ' ' + months[parseInt(p[1]) - 1] + ' ' + p[0];
    }

    function renderChart(labels, values) {
        var canvas = document.getElementById('pnl-chart-canvas');
        if (!canvas) return;
        var ctx = canvas.getContext('2d');

        if (pnlChart) { pnlChart.destroy(); }

        pnlChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Cumulative PnL',
                    data: values,
                    borderColor: '#22c55e',
                    backgroundColor: 'rgba(34,197,94,0.08)',
                    borderWidth: 2,
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
                            title: function (items) { return fmtDate(items[0].label); },
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
                            callback: function (val, idx) {
                                return fmtDate(this.getLabelForValue(val));
                            }
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
        if (resetBtn) {
            resetBtn.onclick = function () { pnlChart.resetZoom(); };
        }
    }

    function buildDrawdownData(trades) {
        var pnlByDay = {};
        trades.forEach(function (t) {
            var tradePnl = typeof window._getNetTradePnl === 'function' ? window._getNetTradePnl(t) : 0;
            pnlByDay[t.date] = (pnlByDay[t.date] || 0) + tradePnl;
        });

        var days = Object.keys(pnlByDay).sort();
        var labels = [];
        var values = [];
        var cum = 0;
        var peak = 0;
        days.forEach(function (d) {
            cum += pnlByDay[d];
            if (cum > peak) peak = cum;
            labels.push(d);
            values.push(parseFloat((cum - peak).toFixed(2)));
        });
        return { labels: labels, values: values };
    }

    function renderDrawdownChart(labels, values) {
        var canvas = document.getElementById('dd-chart-canvas');
        if (!canvas) return;
        var ctx = canvas.getContext('2d');

        if (ddChart) { ddChart.destroy(); ddChart = null; }
        ddChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Drawdown',
                    data: values,
                    borderColor: '#ef4444',
                    backgroundColor: 'rgba(239,68,68,0.08)',
                    borderWidth: 2,
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
                            title: function (items) { return fmtDate(items[0].label); },
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
                            callback: function (val) { return fmtDate(this.getLabelForValue(val)); }
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
        if (resetBtn) {
            resetBtn.onclick = function () { ddChart.resetZoom(); };
        }
    }

    // Expose chart rebuild so weekday filter can refresh charts with filtered trades
    window._rebuildCharts = function (trades) {
        var cd = buildChartData(trades);
        renderChart(cd.labels, cd.values);
        var dd = buildDrawdownData(trades);
        renderDrawdownChart(dd.labels, dd.values);
    };

    fetch(API_URL)
        .then(function (r) {
            if (!r.ok) throw new Error('HTTP ' + r.status);
            return r.json();
        })
        .then(function (data) {
            var trades = data.trades || [];
            var cd = buildChartData(trades);
            renderChart(cd.labels, cd.values);
            var dd = buildDrawdownData(trades);
            renderDrawdownChart(dd.labels, dd.values);
        })
        .catch(function (err) {
            console.error('Chart load error:', err);
        });
})();

/* ── Load Strategy from URL (?strategy_id=...) ─────────────────────────── */

(function () {

    /* ── helpers ─────────────────────────────────────────────────── */
    function setSwitchEnabled(btn, enabled) {
        if (!btn) return;
        var current = btn.getAttribute('aria-checked') === 'true';
        if (current !== enabled) btn.click();
    }

    function applyStrikeParameter(legEl, entryType, strikeParam) {
        var contentEl = legEl.querySelector('[data-strike-value-content]');
        if (!contentEl) return;

        if (entryType === 'EntryType.EntryByStrikeType' ||
            entryType === 'EntryType.EntryBySyntheticFuture') {
            var sel = contentEl.querySelector('select');
            if (sel && strikeParam) sel.value = strikeParam;

        } else if (entryType === 'EntryType.EntryByPremiumRange' ||
            entryType === 'EntryType.EntryByDeltaRange') {
            var inps = contentEl.querySelectorAll('input[type="number"]');
            if (inps[0]) inps[0].value = (strikeParam && strikeParam.LowerRange) || 0;
            if (inps[1]) inps[1].value = (strikeParam && strikeParam.UpperRange) || 0;

        } else if (entryType === 'EntryType.EntryByPremium' ||
            entryType === 'EntryType.EntryByPremiumGEQ' ||
            entryType === 'EntryType.EntryByPremiumLEQ' ||
            entryType === 'EntryType.EntryByDelta') {
            var inp = contentEl.querySelector('input[type="number"]');
            if (inp) inp.value = strikeParam || 0;

        } else if (entryType === 'EntryType.EntryByStraddlePrice') {
            var sels = contentEl.querySelectorAll('select');
            var inps = contentEl.querySelectorAll('input[type="number"]');
            if (sels[0] && strikeParam)
                sels[0].value = strikeParam.Adjustment === 'AdjustmentType.Minus' ? '-' : '+';
            if (inps[0] && strikeParam) inps[0].value = strikeParam.Multiplier || 0.5;

        } else if (entryType === 'EntryType.EntryByAtmMultiplier') {
            var sels = contentEl.querySelectorAll('select');
            var inps = contentEl.querySelectorAll('input[type="number"]');
            if (strikeParam !== undefined && strikeParam !== null) {
                var isPlus = strikeParam >= 1;
                var pct = Math.round(Math.abs(strikeParam - 1) * 100);
                if (sels[1]) sels[1].value = isPlus ? '+' : '-';
                if (inps[0]) inps[0].value = pct;
            }

        } else if (entryType === 'EntryType.EntryByPremiumCloseToStraddle') {
            var inp = contentEl.querySelector('input[type="number"]');
            if (inp && strikeParam)
                inp.value = Math.round((strikeParam.Multiplier || 0) * 100);
        }
    }

    function applyLegConfig(legEl, cfg) {
        if (!legEl || !cfg) return;

        // Position
        var posEl = legEl.querySelector('select[title="Position"]');
        if (posEl && cfg.PositionType) posEl.value = cfg.PositionType;

        // Option Type
        var optEl = legEl.querySelector('select[title="Option Type"]');
        if (optEl && cfg.InstrumentKind) optEl.value = cfg.InstrumentKind;

        // Expiry
        var expEl = legEl.querySelector('select[title="Expiry"]');
        if (expEl && cfg.ExpiryKind) expEl.value = cfg.ExpiryKind;

        // Lots
        var lotEl = legEl.querySelector('[id^="number-input"]');
        if (lotEl && cfg.LotConfig) lotEl.value = cfg.LotConfig.Value || 1;

        // Strike criteria: set value, call _initStrikePair directly (no MutationObserver dependency)
        var criteriaEl = legEl.querySelector('[data-strike-criteria-select]');
        if (criteriaEl && cfg.EntryType) {
            criteriaEl.value = cfg.EntryType;
            var strikeRow = criteriaEl.closest('.flex.flex-wrap.items-end.gap-3');
            var strikeWrapper = strikeRow ? strikeRow.querySelector('[data-strike-value-wrapper]') : null;
            if (strikeWrapper && typeof window._initStrikePair === 'function') {
                // _initStrikePair calls syncStrike (rebuilds content) then adds change listener
                window._initStrikePair(criteriaEl, strikeWrapper);
            } else {
                criteriaEl.dispatchEvent(new Event('change'));
            }
            // content is rebuilt synchronously — set strike parameter now
            applyStrikeParameter(legEl, cfg.EntryType, cfg.StrikeParameter);
        }

        // Stop Loss
        var slC = legEl.querySelector('[id^="StopLoss_"]');
        if (slC && cfg.LegStopLoss && cfg.LegStopLoss.Type !== 'None') {
            setSwitchEnabled(slC.querySelector('button[role="switch"]'), true);
            var slSel = slC.querySelector('select');
            if (slSel) slSel.value = cfg.LegStopLoss.Type;
            var slInp = slC.querySelector('input[type="number"]');
            if (slInp) slInp.value = cfg.LegStopLoss.Value || 0;
        }

        // Target Profit
        var tgtC = legEl.querySelector('[id^="TargetProfit_"]');
        if (tgtC && cfg.LegTarget && cfg.LegTarget.Type !== 'None') {
            setSwitchEnabled(tgtC.querySelector('button[role="switch"]'), true);
            var tgtSel = tgtC.querySelector('select');
            if (tgtSel) tgtSel.value = cfg.LegTarget.Type;
            var tgtInp = tgtC.querySelector('input[type="number"]');
            if (tgtInp) tgtInp.value = cfg.LegTarget.Value || 0;
        }

        // Trail SL
        var trailC = legEl.querySelector('[id^="TrailSL_"]');
        if (trailC && cfg.LegTrailSL && cfg.LegTrailSL.Type !== 'None') {
            setSwitchEnabled(trailC.querySelector('button[role="switch"]'), true);
            var trailSel = trailC.querySelector('select');
            if (trailSel) trailSel.value = cfg.LegTrailSL.Type;
            var trailInps = trailC.querySelectorAll('input[type="number"]');
            var tv = cfg.LegTrailSL.Value || {};
            if (trailInps[0]) trailInps[0].value = tv.InstrumentMove || 0;
            if (trailInps[1]) trailInps[1].value = tv.StopLossMove || 0;
        }

        // Momentum
        var momC = legEl.querySelector('[id^="simple_momentum_"]');
        if (momC && cfg.LegMomentum && cfg.LegMomentum.Type !== 'None') {
            setSwitchEnabled(momC.querySelector('button[role="switch"]'), true);
            var momSel = momC.querySelector('select');
            if (momSel) momSel.value = cfg.LegMomentum.Type;
            var momInp = momC.querySelector('input[type="number"]');
            if (momInp) momInp.value = cfg.LegMomentum.Value || 0;
        }

        // Re-entry on SL
        applyReentry(legEl, 'Re-entry on SL', cfg.LegReentrySL);

        // Re-entry on TGT
        applyReentry(legEl, 'Re-entry on Tgt', cfg.LegReentryTP);
    }

    function applyReentry(legEl, idFragment, reentry) {
        if (!reentry || reentry.Type === 'None') return;
        var btn = legEl.querySelector('[id*="' + idFragment + '"]');
        if (!btn) return;
        setSwitchEnabled(btn, true);
        var container = btn.closest('.flex.w-fit.flex-col.gap-2');
        if (!container) return;
        var typeSelect = container.querySelector('select');
        if (!typeSelect) return;

        typeSelect.value = reentry.Type;

        if (reentry.Type === 'ReentryType.NextLeg') {
            // Set the lazy leg ref BEFORE calling showLazyPicker so it pre-selects correctly
            var ref = (reentry.Value && reentry.Value.NextLegRef) || '';
            if (ref) typeSelect.setAttribute('data-lazy-ref', ref);
            // Render the picker dropdown (reads data-lazy-ref to pre-select the right lazy leg)
            if (typeof window._showLazyPicker === 'function') {
                window._showLazyPicker(typeSelect);
            }
        } else {
            var sels = container.querySelectorAll('select');
            if (sels[1] && reentry.Value && reentry.Value.ReentryCount) {
                var reentryCount = String(reentry.Value.ReentryCount);
                for (var count = 1; count <= Math.max(parseInt(reentryCount, 10) || 1, 10); count += 1) {
                    var countValue = String(count);
                    if (!Array.from(sels[1].options || []).some(function (opt) { return opt.value === countValue; })) {
                        var option = document.createElement('option');
                        option.value = countValue;
                        option.textContent = countValue;
                        sels[1].appendChild(option);
                    }
                }
                sels[1].value = reentryCount;
            }
        }
    }

    /* ── overall settings apply ──────────────────────────────────── */
    function applyOverallSwitchBlock(btn, cfg) {
        // btn = the role="switch" button for this section
        if (!btn || !cfg || cfg.Type === 'None') return;
        setSwitchEnabled(btn, true);
        var container = btn.closest('[id^="OverallStop"], [id^="OverallMomentum"], [id^="TrailingOptions"]')
            || btn.closest('.flex.w-fit.flex-col.gap-2')
            || btn.parentElement.parentElement;
        var sel = container ? container.querySelector('select') : null;
        if (sel && cfg.Type) sel.value = cfg.Type;
        var inp1 = container ? container.querySelector('[id$="_input1"]') : null;
        if (!inp1 && container) inp1 = container.querySelector('input[type="number"]');
        if (inp1 && cfg.Value !== undefined && cfg.Value !== null && typeof cfg.Value !== 'object')
            inp1.value = cfg.Value;
    }

    function applyOverallSettings(s) {
        // helper: find button by data-test-id, get its closest container, apply type+value
        function applyOverallBlock(testId, cfg, getContainer) {
            if (!cfg || cfg.Type === 'None') return;
            var btn = document.querySelector('[data-test-id="' + testId + '"]');
            if (!btn) return;
            setSwitchEnabled(btn, true);
            var container = getContainer ? getContainer(btn) : btn.closest('.flex.w-fit.flex-col.gap-2');
            if (!container) return;
            var sel = container.querySelector('select');
            if (sel) sel.value = cfg.Type;
            var inp = container.querySelector('input[type="number"]');
            if (inp && cfg.Value !== undefined && typeof cfg.Value !== 'object') inp.value = cfg.Value;
        }

        // Overall Stop Loss
        applyOverallBlock(
            'toggle-handle_Overall Stop Loss_overall-stoploss',
            s.OverallSL,
            function (btn) { return btn.closest('[id^="OverallStop"]'); }
        );

        // Overall Target (second button with same test-id)
        if (s.OverallTgt && s.OverallTgt.Type !== 'None') {
            var tgtBtns = document.querySelectorAll('[data-test-id="toggle-handle_Overall Stop Loss_overall-stoploss"]');
            var tgtBtn = tgtBtns[1];
            if (tgtBtn) {
                setSwitchEnabled(tgtBtn, true);
                var tgtEl = tgtBtn.closest('[id^="OverallStop"]');
                if (tgtEl) {
                    var tgtSel = tgtEl.querySelector('select');
                    if (tgtSel) tgtSel.value = s.OverallTgt.Type;
                    var tgtInp = tgtEl.querySelector('input[type="number"]');
                    if (tgtInp) tgtInp.value = s.OverallTgt.Value || 0;
                }
            }
        }

        // Trailing Options (Lock / LockAndTrail)
        if (s.LockAndTrail && s.LockAndTrail.Type !== 'None') {
            var trailBtn = document.querySelector('[data-test-id="toggle-handle_Trailing Options_trailing-options"]');
            if (trailBtn) {
                setSwitchEnabled(trailBtn, true);
                var trailEl = trailBtn.closest('[id^="TrailingOptions"]');
                if (trailEl) {
                    var trailSel = trailEl.querySelector('select');
                    if (trailSel) trailSel.value = s.LockAndTrail.Type;
                }
                var tv = s.LockAndTrail.Value || {};
                var pr = document.getElementById('trailing-profit-reaches');
                var lp = document.getElementById('trailing-lock-profit');
                var ib = document.getElementById('trailing-increase-by');
                var tb = document.getElementById('trailing-trail-by');
                if (pr) pr.value = tv.ProfitReaches || 0;
                if (lp) lp.value = tv.LockProfit || 0;
                if (ib) ib.value = s.LockAndTrail.Type === 'TrailingOption.LockAndTrail' ? (tv.IncreaseInProfitBy || 0) : 0;
                if (tb) tb.value = s.LockAndTrail.Type === 'TrailingOption.LockAndTrail' ? (tv.TrailProfitBy || 0) : 0;
                if (trailEl) { var ts = trailEl.querySelector('select'); if (ts) ts.dispatchEvent(new Event('change')); }
            }
        }

        // Trailing Options — OverallTrailSL
        if (s.OverallTrailSL && s.OverallTrailSL.Type !== 'None') {
            var trailBtn2 = document.querySelector('[data-test-id="toggle-handle_Trailing Options_trailing-options"]');
            if (trailBtn2) {
                setSwitchEnabled(trailBtn2, true);
                var trailEl2 = trailBtn2.closest('[id^="TrailingOptions"]');
                if (trailEl2) {
                    var trailSel2 = trailEl2.querySelector('select');
                    if (trailSel2) {
                        trailSel2.value = 'TrailingOption.OverallTrailSL';
                        trailSel2.dispatchEvent(new Event('change')); // show overall-trail-sl-section
                    }
                }
                var typeSel = document.getElementById('overall-trail-sl-type');
                var inp1 = document.getElementById('overall-trail-sl-input1');
                var inp2 = document.getElementById('overall-trail-sl-input2');
                var tv2 = s.OverallTrailSL.Value || {};
                if (typeSel) typeSel.value = s.OverallTrailSL.Type;
                if (inp1) inp1.value = tv2.InstrumentMove || 0;
                if (inp2) inp2.value = tv2.StopLossMove || 0;
            }
        }

        // Overall Re-entry on SL
        if (s.OverallReentrySL && s.OverallReentrySL.Type !== 'None') {
            var reSLBtn = document.querySelector('[data-test-id="toggle-handle_rentry_undefined_Overall Re-entry on SL"]');
            if (reSLBtn) {
                setSwitchEnabled(reSLBtn, true);
                var c = reSLBtn.closest('.flex.w-fit.flex-col.gap-2');
                if (c) {
                    var sels = c.querySelectorAll('select');
                    if (sels[0]) sels[0].value = s.OverallReentrySL.Type;
                    if (sels[1] && s.OverallReentrySL.Value && s.OverallReentrySL.Value.ReentryCount) {
                        var overallSlReentryCount = String(s.OverallReentrySL.Value.ReentryCount);
                        for (var slCount = 1; slCount <= Math.max(parseInt(overallSlReentryCount, 10) || 1, 10); slCount += 1) {
                            var slCountValue = String(slCount);
                            if (!Array.from(sels[1].options || []).some(function (opt) { return opt.value === slCountValue; })) {
                                var slOption = document.createElement('option');
                                slOption.value = slCountValue;
                                slOption.textContent = slCountValue;
                                sels[1].appendChild(slOption);
                            }
                        }
                        sels[1].value = overallSlReentryCount;
                    }
                }
            }
        }

        // Overall Re-entry on Tgt (second button)
        if (s.OverallReentryTgt && s.OverallReentryTgt.Type !== 'None') {
            var reTgtBtns = document.querySelectorAll('[data-test-id="toggle-handle_rentry_undefined_Overall Re-entry on SL"]');
            var reTgtBtn = reTgtBtns[1];
            if (reTgtBtn) {
                setSwitchEnabled(reTgtBtn, true);
                var c2 = reTgtBtn.closest('.flex.w-fit.flex-col.gap-2');
                if (c2) {
                    var sels2 = c2.querySelectorAll('select');
                    if (sels2[0]) sels2[0].value = s.OverallReentryTgt.Type;
                    if (sels2[1] && s.OverallReentryTgt.Value && s.OverallReentryTgt.Value.ReentryCount) {
                        var overallTgtReentryCount = String(s.OverallReentryTgt.Value.ReentryCount);
                        for (var tgtCount = 1; tgtCount <= Math.max(parseInt(overallTgtReentryCount, 10) || 1, 10); tgtCount += 1) {
                            var tgtCountValue = String(tgtCount);
                            if (!Array.from(sels2[1].options || []).some(function (opt) { return opt.value === tgtCountValue; })) {
                                var tgtOption = document.createElement('option');
                                tgtOption.value = tgtCountValue;
                                tgtOption.textContent = tgtCountValue;
                                sels2[1].appendChild(tgtOption);
                            }
                        }
                        sels2[1].value = overallTgtReentryCount;
                    }
                }
            }
        }

        // Overall Momentum
        if (s.OverallMomentum && s.OverallMomentum.Type !== 'None') {
            var momBtn = document.querySelector('[data-test-id="toggle-handle_Overall Momentum_OverallMomentum_None"]');
            if (momBtn) {
                setSwitchEnabled(momBtn, true);
                var momEl = momBtn.closest('[id^="OverallMomentum"]');
                if (momEl) {
                    var momSel = momEl.querySelector('select');
                    if (momSel) momSel.value = s.OverallMomentum.Type;
                    var momInp = momEl.querySelector('input[type="number"]');
                    if (momInp) momInp.value = s.OverallMomentum.Value || 0;
                }
            }
        }
    }

    /* ── main apply function ─────────────────────────────────────── */
    function normalizeLazyStrategyConfig(cfg) {
        if (!cfg || !cfg.strategy) return cfg;
        var s = cfg.strategy;
        var idleConfigs = s.IdleLegConfigs || {};
        var oldKeys = Object.keys(idleConfigs);
        if (!oldKeys.length) return cfg;

        var normalizedIdle = {};
        oldKeys.forEach(function (oldName) {
            var legCfg = idleConfigs[oldName] || {};
            if (typeof structuredClone === 'function') {
                legCfg = structuredClone(legCfg);
            } else {
                legCfg = JSON.parse(JSON.stringify(legCfg));
            }
            legCfg.id = oldName;
            normalizedIdle[oldName] = legCfg;
        });

        s.IdleLegConfigs = normalizedIdle;
        return cfg;
    }

    function applyConfig(cfg) {
        if (!cfg) { console.warn('[applyConfig] cfg is null'); return; }
        cfg = normalizeLazyStrategyConfig(cfg);
        var s = cfg.strategy || {};
        console.log('[applyConfig] start_date:', cfg.start_date,
            '| legs:', (s.ListOfLegConfigs || []).length);

        // 1. Date inputs
        var dateInputs = document.querySelectorAll('[title="date-input"]');
        if (dateInputs[0] && cfg.start_date) dateInputs[0].value = cfg.start_date;
        if (dateInputs[1] && cfg.end_date) dateInputs[1].value = cfg.end_date;

        // 2. Entry / Exit time
        var indSettings = document.getElementById('indicator-settings');
        if (indSettings) {
            var timeInputs = indSettings.querySelectorAll('input[type="time"]');
            var entryParams = s.EntryIndicators && s.EntryIndicators.Value &&
                s.EntryIndicators.Value[0] &&
                s.EntryIndicators.Value[0].Value &&
                s.EntryIndicators.Value[0].Value.Parameters;
            var exitParams = s.ExitIndicators && s.ExitIndicators.Value &&
                s.ExitIndicators.Value[0] &&
                s.ExitIndicators.Value[0].Value &&
                s.ExitIndicators.Value[0].Value.Parameters;
            if (timeInputs[0] && entryParams) {
                timeInputs[0].value =
                    String(entryParams.Hour || 9).padStart(2, '0') + ':' +
                    String(entryParams.Minute || 16).padStart(2, '0');
            }
            if (timeInputs[1] && exitParams) {
                timeInputs[1].value =
                    String(exitParams.Hour || 15).padStart(2, '0') + ':' +
                    String(exitParams.Minute || 15).padStart(2, '0');
            }
        }

        // 3. Clear existing legs and lazy legs
        var legsContainer = document.getElementById('legs-container');
        var addLegBtn = document.getElementById('add-leg');
        var legsHeading = document.getElementById('legs-heading');

        if (legsContainer) {
            while (legsContainer.firstChild)
                legsContainer.removeChild(legsContainer.firstChild);
            if (legsHeading) legsHeading.style.display = 'none';
        }
        if (typeof window._clearAllIdleLegs === 'function') window._clearAllIdleLegs();

        // 4. Create ALL lazy leg DOM elements first (no config yet) so all names are
        //    registered in _idleLegConfigs before any re-entry picker is rendered.
        //    This handles cross-references like lazy1 → lazy4 where lazy4 must exist first.
        var idleConfigs = s.IdleLegConfigs || {};
        var idleNames = Object.keys(idleConfigs);
        var idleElements = {};
        idleNames.forEach(function (name) {
            if (typeof window._createIdleLeg !== 'function') return;
            idleElements[name] = window._createIdleLeg(name);
        });

        // 4b. Now apply configs to lazy legs — all names exist in _idleLegConfigs so
        //     NextLegRef pickers resolve correctly
        idleNames.forEach(function (name) {
            var el = idleElements[name];
            if (el) applyLegConfig(el, idleConfigs[name]);
        });

        // 5. Add regular legs — all lazy legs exist so NextLegRef re-entries work
        var legs = s.ListOfLegConfigs || [];
        legs.forEach(function (legCfg) {
            if (!addLegBtn) return;
            addLegBtn.click();
            var legEl = legsContainer && legsContainer.lastElementChild;
            if (legEl) applyLegConfig(legEl, legCfg);
        });

        // 6. Overall strategy settings
        applyOverallSettings(s);
    }

    /* ── auto-load from ?strategy_id= ───────────────────────────── */
    function loadFromUrl() {
        var params = new URLSearchParams(window.location.search);
        var strategyId = params.get('strategy_id');
        if (!strategyId) return;

        console.log('[loadFromUrl] fetching strategy:', strategyId);
        fetch(getStrategyApiUrl('strategyById', encodeURIComponent(strategyId)))
            .then(function (res) {
                if (!res.ok) throw new Error('Strategy not found (status ' + res.status + ')');
                return res.json();
            })
            .then(function (doc) {
                var fullConfig = doc.full_config;
                if (!fullConfig) { console.warn('[loadFromUrl] Strategy has no full_config'); return; }
                applyConfig(fullConfig);
                // Swap buttons: hide "Save Strategy", show "Update" + "Save As"
                var saveBtn = document.getElementById('save-strategy-btn');
                var updateBtn = document.getElementById('update-strategy-btn');
                var saveAsBtn = document.getElementById('save-as-strategy-btn');
                if (saveBtn) saveBtn.style.display = 'none';
                if (updateBtn) updateBtn.style.display = '';
                if (saveAsBtn) saveAsBtn.style.display = '';
            })
            .catch(function (err) {
                console.error('[loadFromUrl] Failed to load strategy:', err);
            });
    }

    /* ── Update Strategy (no popup) ────────────────────────────── */
    var updateBtn = document.getElementById('update-strategy-btn');
    if (updateBtn) {
        updateBtn.addEventListener('click', function () {
            var params = new URLSearchParams(window.location.search);
            var strategyId = params.get('strategy_id');
            if (!strategyId) return;

            var config = typeof buildConfig === 'function' ? buildConfig() : {};
            var origText = updateBtn.querySelector('span') ? updateBtn.querySelector('span').textContent : '';
            updateBtn.disabled = true;
            if (updateBtn.querySelector('span')) updateBtn.querySelector('span').textContent = 'Updating…';

            fetch(getStrategyApiUrl('strategyById', encodeURIComponent(strategyId)), {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            })
                .then(function (res) {
                    if (!res.ok) throw new Error('Update failed (status ' + res.status + ')');
                    return res.json();
                })
                .then(function () {
                    if (updateBtn.querySelector('span')) updateBtn.querySelector('span').textContent = 'Updated ✓';
                    setTimeout(function () {
                        updateBtn.disabled = false;
                        if (updateBtn.querySelector('span')) updateBtn.querySelector('span').textContent = origText || 'Update Strategy';
                    }, 1500);
                })
                .catch(function (err) {
                    console.error('[updateStrategy]', err);
                    if (updateBtn.querySelector('span')) updateBtn.querySelector('span').textContent = 'Failed ✗';
                    setTimeout(function () {
                        updateBtn.disabled = false;
                        if (updateBtn.querySelector('span')) updateBtn.querySelector('span').textContent = origText || 'Update Strategy';
                    }, 1500);
                });
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', loadFromUrl);
    } else {
        loadFromUrl();
    }
})();
// IMPORT STRATEGY
(function () {
    function initImportStrategy() {
        var importBtn = document.getElementById('import-algtst-btn');
        var modal = document.getElementById('import-algtst-modal');
        var closeBtn = document.getElementById('import-algtst-close');
        var cancelBtn = document.getElementById('import-algtst-cancel');
        var dropzone = document.getElementById('import-algtst-dropzone');
        var fileInput = document.getElementById('import-algtst-file');
        var statusEl = document.getElementById('import-algtst-status');

        if (!importBtn || !modal || !dropzone || !fileInput || !statusEl) return;

        function resetStatus() {
            statusEl.className = 'mt-3 hidden text-sm';
            statusEl.textContent = '';
        }

        function showStatus(message, tone) {
            statusEl.textContent = message;
            statusEl.className = 'mt-3 text-sm ' + (tone === 'error' ? 'text-red-500' : tone === 'success' ? 'text-green-600' : 'text-primaryBlack-700');
        }

        function openModal() {
            modal.classList.remove('hidden');
            fileInput.value = '';
            resetStatus();
        }

        function closeModal() {
            modal.classList.add('hidden');
            dropzone.classList.remove('border-primaryBlue-500', 'bg-primaryBlue-100');
        }

        function randomSuffix() {
            return String(Math.floor(10000 + Math.random() * 90000));
        }

        function strategyNameExists(name) {
            return fetch(getStrategyApiUrl('strategyCheckName') + '?name=' + encodeURIComponent(name))
                .then(function (res) {
                    if (!res.ok) throw new Error('check failed');
                    return res.json();
                })
                .then(function (data) {
                    return !!(data && data.exists);
                });
        }

        function resolveUniqueName(baseName) {
            var cleanBase = (baseName || 'Imported Strategy').trim() || 'Imported Strategy';
            var attempts = 0;

            function tryName(candidate) {
                return strategyNameExists(candidate).then(function (exists) {
                    if (!exists) return candidate;
                    attempts += 1;
                    if (attempts > 20) throw new Error('Unable to resolve a unique strategy name');
                    return tryName(cleanBase + ' - ' + randomSuffix());
                });
            }

            return tryName(cleanBase);
        }

        function saveImportedStrategy(payload) {
            return fetch(getStrategyApiUrl('strategySave'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            }).then(function (res) {
                return res.json().catch(function () { return {}; }).then(function (data) {
                    if (!res.ok) {
                        throw new Error(data.detail || 'Failed to save strategy');
                    }
                    return data;
                });
            });
        }

        function processFile(file) {
            if (!file) return;
            if (!/\.algtst$/i.test(file.name || '')) {
                showStatus('Please upload a valid .algtst file.', 'error');
                return;
            }

            showStatus('Importing strategy...', 'info');

            file.text()
                .then(function (raw) {
                    var parsed;
                    try {
                        parsed = JSON.parse(raw);
                    } catch (error) {
                        throw new Error('Invalid .algtst file content');
                    }

                    if (!parsed || !parsed.strategy) {
                        throw new Error('Uploaded file does not contain a valid strategy');
                    }

                    return resolveUniqueName(parsed.name).then(function (uniqueName) {
                        parsed.name = uniqueName;
                        return saveImportedStrategy(parsed).then(function () {
                            return { payload: parsed, savedName: uniqueName };
                        });
                    });
                })
                .then(function (result) {
                    if (typeof applyConfig === 'function') {
                        applyConfig(result.payload);
                    }
                    showStatus('Strategy imported successfully as "' + result.savedName + '".', 'success');
                    setTimeout(closeModal, 1200);
                })
                .catch(function (error) {
                    showStatus(error.message || 'Failed to import strategy.', 'error');
                });
        }

        importBtn.addEventListener('click', openModal);
        if (closeBtn) closeBtn.addEventListener('click', closeModal);
        if (cancelBtn) cancelBtn.addEventListener('click', closeModal);

        modal.addEventListener('click', function (event) {
            if (event.target === modal) closeModal();
        });

        dropzone.addEventListener('click', function () {
            fileInput.click();
        });

        ['dragenter', 'dragover'].forEach(function (eventName) {
            dropzone.addEventListener(eventName, function (event) {
                event.preventDefault();
                event.stopPropagation();
                dropzone.classList.add('border-primaryBlue-500', 'bg-primaryBlue-100');
            });
        });

        ['dragleave', 'dragend', 'drop'].forEach(function (eventName) {
            dropzone.addEventListener(eventName, function (event) {
                event.preventDefault();
                event.stopPropagation();
                dropzone.classList.remove('border-primaryBlue-500', 'bg-primaryBlue-100');
            });
        });

        dropzone.addEventListener('drop', function (event) {
            var file = event.dataTransfer && event.dataTransfer.files ? event.dataTransfer.files[0] : null;
            processFile(file);
        });

        fileInput.addEventListener('change', function () {
            processFile(fileInput.files && fileInput.files[0]);
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initImportStrategy);
    } else {
        initImportStrategy();
    }
})();
