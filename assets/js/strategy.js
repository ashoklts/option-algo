/**
 * strategy.js — Option Chain Strategy Execution
 *
 * Manages all strategy logic for the position simulator.
 * Currently implements: Mini Strangle
 *
 * Reads globals from index.html:
 *   option_chain_data, optionLegs, calcCurrentPnL,
 *   renderLegs, renderPositionTable, updateSummaryStats, updateChart,
 *   window.reloadOptionChain, window._ocGetTimestamp
 */

(function (global) {
    'use strict';

    function isReentryCountSelect(selectEl) {
        if (!selectEl || selectEl.tagName !== 'SELECT') return false;

        var options = Array.from(selectEl.options || []).map(function (opt) {
            return String(opt.value || opt.textContent || '').trim();
        });

        if (options.length !== 3 || options.join(',') !== '1,2,3') return false;

        var container = selectEl.closest('div,section,form,td') || selectEl.parentElement;
        var contextText = '';
        var hops = 0;

        while (container && hops < 4) {
            contextText += ' ' + (container.textContent || '');
            container = container.parentElement;
            hops += 1;
        }

        return /re-entry|reentry/i.test(contextText);
    }

    function extendReentryCountSelect(selectEl) {
        if (!isReentryCountSelect(selectEl) || selectEl.dataset.reentryCountExtended === 'true') {
            return;
        }

        for (var value = 4; value <= 10; value += 1) {
            var option = document.createElement('option');
            option.value = String(value);
            option.textContent = String(value);
            selectEl.appendChild(option);
        }

        selectEl.dataset.reentryCountExtended = 'true';
    }

    function patchReentryCountDropdowns(root) {
        var scope = root && root.querySelectorAll ? root : document;
        Array.from(scope.querySelectorAll('select')).forEach(extendReentryCountSelect);
    }

    function installReentryCountDropdownPatch() {
        patchReentryCountDropdowns(document);

        var observer = new MutationObserver(function (mutations) {
            mutations.forEach(function (mutation) {
                Array.from(mutation.addedNodes || []).forEach(function (node) {
                    if (!node || node.nodeType !== 1) return;
                    if (node.tagName === 'SELECT') extendReentryCountSelect(node);
                    patchReentryCountDropdowns(node);
                });
            });
        });

        observer.observe(document.documentElement || document.body, {
            childList: true,
            subtree: true
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', installReentryCountDropdownPatch, { once: true });
    } else {
        installReentryCountDropdownPatch();
    }

    // ─── helpers ───────────────────────────────────────────────────────────

    function pad(n) { return String(n).padStart(2, '0'); }

    /** Returns the month/year of the currently-selected option-chain timestamp. */
    function getCurrentYearMonth() {
        var ts = typeof global._ocGetTimestamp === 'function'
            ? global._ocGetTimestamp()
            : new Date().toISOString();
        var p = ts.match(/^(\d{4})-(\d{2})/);
        return p ? { year: +p[1], month: +p[2] - 1 } : null; // month is 0-indexed
    }

    // ─── Step 1 · expiry tab selection ────────────────────────────────────

    /**
     * Finds and clicks the correct expiry tab, then returns its date string
     * (e.g. "2025-10-07") so downstream logic can filter option_chain_data.
     *
     * @param {'CURRENT_WEEK'|'NEXT_WEEK'|'MONTHLY_EXPIRY'} expiryType
     * @returns {string|null} expiry date string or null on failure
     */
    function selectExpiryTab(expiryType) {
        // After renderCarousel(), buttons have data-oc-expiry attributes.
        var buttons = Array.from(
            document.querySelectorAll('.expiry_button[data-oc-expiry]')
        );

        if (!buttons.length) {
            console.warn('[Strategy] No .expiry_button[data-oc-expiry] elements found.');
            return null;
        }

        var target = null;

        if (expiryType === 'CURRENT_WEEK') {
            target = buttons[0] || null;

        } else if (expiryType === 'NEXT_WEEK') {
            target = buttons[1] || null;

        } else if (expiryType === 'MONTHLY_EXPIRY') {
            var ym = getCurrentYearMonth();
            if (ym) {
                // collect all expiry buttons whose date falls in the current month
                var inMonth = buttons.filter(function (btn) {
                    var d = new Date(btn.dataset.ocExpiry);
                    return d.getFullYear() === ym.year && d.getMonth() === ym.month;
                });
                // pick the last one (latest expiry of the month)
                target = inMonth.length ? inMonth[inMonth.length - 1] : null;
            }
        }

        if (!target) {
            console.warn('[Strategy] No expiry tab found for type:', expiryType);
            return null;
        }

        // Clicking the button activates the tab and calls renderTable() synchronously.
        target.click();
        return target.dataset.ocExpiry; // "YYYY-MM-DD"
    }

    // ─── Step 2 · strike selection (Mini Strangle) ────────────────────────

    /**
     * Maps the 5th OTM CE premium to the OTM strike index to sell.
     *
     * Premium range  → OTM index to sell
     * < 100          → 4th (or 5th if current PnL > 0)
     * 100 – 160      → 5th
     * 160 – 270      → 6th
     * 270 – 370      → 7th
     * 370 – 470      → 8th
     * > 470          → 9th
     */
    function resolveTargetOtmIndex(fifthOtmPremium) {
        if (fifthOtmPremium < 100) {
            var pnl = typeof calcCurrentPnL === 'function' ? calcCurrentPnL() : 0;
            return pnl > 0 ? 5 : 4;
        }
        if (fifthOtmPremium <= 160) return 5;
        if (fifthOtmPremium <= 270) return 6;
        if (fifthOtmPremium <= 370) return 7;
        if (fifthOtmPremium <= 470) return 8;
        return 9;
    }

    // ─── Step 3 · position creation ───────────────────────────────────────

    function alreadySold(expiry, strike, optionType) {
        if (typeof optionLegs === 'undefined') return false;
        return optionLegs.some(function (leg) {
            return leg.expiry     === expiry  &&
                   leg.strike     === strike  &&
                   leg.optionType === optionType &&
                   leg.type       === 'Sell';
        });
    }

    function refreshUI() {
        if (typeof renderLegs          === 'function') renderLegs();
        if (typeof renderPositionTable === 'function') renderPositionTable();
        if (typeof updateSummaryStats  === 'function') updateSummaryStats();
        if (typeof updateChart         === 'function') updateChart();
        // re-render option chain to reflect active Sell button states
        if (typeof global.reloadOptionChain === 'function') global.reloadOptionChain();
    }

    // ─── Mini Strangle ────────────────────────────────────────────────────

    function executeMiniStrangle(expiryType) {

        // ── 1. activate expiry tab ───────────────────────────────────────
        var expiry = selectExpiryTab(expiryType);
        if (!expiry) return;

        // ── 2. get option chain rows for this expiry ─────────────────────
        if (typeof option_chain_data === 'undefined' || !option_chain_data.length) {
            console.warn('[Strategy] option_chain_data is empty.');
            return;
        }

        var rows = option_chain_data.filter(function (r) { return r.expiry === expiry; });
        if (!rows.length) {
            console.warn('[Strategy] No option chain rows for expiry:', expiry);
            return;
        }

        // ── 3. build strike map (ascending) ─────────────────────────────
        var map = {};
        rows.forEach(function (r) {
            if (!map[r.strike]) map[r.strike] = { call: null, put: null };
            if      (r.type === 'CE') map[r.strike].call = r;
            else if (r.type === 'PE') map[r.strike].put  = r;
        });
        var strikes = Object.keys(map).map(Number).sort(function (a, b) { return a - b; });

        // ── 4. ATM strike ────────────────────────────────────────────────
        var spotPrice = rows[0].spot_price;
        var atmStrike = strikes.reduce(function (best, cur) {
            return Math.abs(cur - spotPrice) < Math.abs(best - spotPrice) ? cur : best;
        }, strikes[0]);
        var atmIdx = strikes.indexOf(atmStrike);

        // ── 5. 5th OTM CE premium ────────────────────────────────────────
        var idx5th = atmIdx + 5;
        if (idx5th >= strikes.length) {
            console.warn('[Strategy] Not enough OTM CE strikes (need at least 5 above ATM).');
            return;
        }
        var row5th       = map[strikes[idx5th]];
        var fifthPremium = (row5th && row5th.call) ? row5th.call.close : 0;

        // ── 6. resolve which OTM index to sell ──────────────────────────
        var targetN = resolveTargetOtmIndex(fifthPremium);
        var ceIdx   = atmIdx + targetN;
        var peIdx   = atmIdx - targetN;

        if (ceIdx >= strikes.length) {
            console.warn('[Strategy] CE target strike out of range (index ' + ceIdx + ').');
            return;
        }
        if (peIdx < 0) {
            console.warn('[Strategy] PE target strike out of range (index ' + peIdx + ').');
            return;
        }

        var ceStrike  = strikes[ceIdx];
        var peStrike  = strikes[peIdx];
        var cePremium = (map[ceStrike].call) ? map[ceStrike].call.close : 0;
        var pePremium = (map[peStrike].put)  ? map[peStrike].put.close  : 0;

        console.info(
            '[Strategy] Mini Strangle → expiry:', expiry,
            '| ATM:', atmStrike,
            '| 5th OTM premium: ₹' + fifthPremium.toFixed(2),
            '| target OTM idx:', targetN,
            '| SELL CE:', ceStrike, '@ ₹' + cePremium,
            '| SELL PE:', peStrike, '@ ₹' + pePremium
        );

        // ── 7. push SELL legs (skip if already open) ────────────────────
        if (typeof optionLegs === 'undefined') {
            console.warn('[Strategy] optionLegs global not found.');
            return;
        }

        var chainTs = option_chain_data[0].timestamp;
        var added   = false;

        if (!alreadySold(expiry, ceStrike, 'Call')) {
            optionLegs.push({
                type:       'Sell',
                optionType: 'Call',
                strike:     ceStrike,
                premium:    cePremium,
                quantity:   65,
                expiry:     expiry,
                entryDate:  chainTs
            });
            added = true;
        } else {
            console.info('[Strategy] CE Sell already open for strike', ceStrike, '— skipped.');
        }

        if (!alreadySold(expiry, peStrike, 'Put')) {
            optionLegs.push({
                type:       'Sell',
                optionType: 'Put',
                strike:     peStrike,
                premium:    pePremium,
                quantity:   65,
                expiry:     expiry,
                entryDate:  chainTs
            });
            added = true;
        } else {
            console.info('[Strategy] PE Sell already open for strike', peStrike, '— skipped.');
        }

        // ── 8. refresh all UI panels ─────────────────────────────────────
        if (added) {
            refreshUI();
            // Freeze adjustment levels after chart has rendered (new Sell positions opened)
            setTimeout(function () {
                if (typeof global.freezeAdjustmentLevels === 'function') {
                    global.freezeAdjustmentLevels();
                }
            }, 300);
        }
    }

    // ─── strategy dispatcher ──────────────────────────────────────────────

    function runStrategy() {
        var stratEl  = document.getElementById('strategySelector');
        var expiryEl = document.getElementById('strategyExpirySelector');
        var strategy = stratEl  ? stratEl.value  : 'mini_strangle';
        var expiry   = expiryEl ? expiryEl.value : 'CURRENT_WEEK';

        switch (strategy) {
            case 'mini_strangle':
                executeMiniStrangle(expiry);
                break;
            default:
                console.warn('[Strategy] Unknown strategy:', strategy);
        }
    }

    // ─── Next Adjustment Deduct ───────────────────────────────────────────

    /** Maps TCP unit string to API numeric type (1 = points, 2 = percentage). */
    function unitToType(unit) {
        return unit === 'pct' ? 2 : 1;
    }

    /** Maps expiry selector value to lowercase snake_case for API. */
    function mapExpiry(v) {
        return (v || 'CURRENT_WEEK').toLowerCase();   // e.g. "current_week"
    }

    /**
     * Renders the API response inline next to the NAD button.
     * Handles both the event-triggered case and the no-event case.
     */
    var _nadValidateTs = null; // stores validation_start_timestamp for Proceed

    function showAdjDeductResult(data) {
        var container = document.getElementById('nadInlineResult');
        if (!container) return;

        _nadValidateTs = (data && data.validation_start_timestamp) || null;

        container.innerHTML = '';
        container.style.display = 'none';

        if (!data) return;

        var badge, info = '', showProceed = false;

        if (data.error) {
            badge = inlineBadge(data.error, '#ef4444', '#fef2f2');

        } else if (data.event_triggered) {
            var evtColors = {
                upper_adjustment_hit: ['#1d4ed8','#eff6ff'],
                lower_adjustment_hit: ['#7c3aed','#f5f3ff'],
                stoploss_hit:         ['#dc2626','#fef2f2'],
                target_hit:           ['#16a34a','#f0fdf4'],
                trailing_sl_hit:      ['#d97706','#fffbeb'],
            };
            var ec = evtColors[data.event_type] || ['#374151','#f9fafb'];
            badge = inlineBadge(data.event_type || 'event', ec[0], ec[1]);
            if (data.trigger_timestamp) {
                info += inlineLabel('Triggered', data.trigger_timestamp);
            }
            if (_nadValidateTs) {
                info += inlineLabel('Validate from', _nadValidateTs);
            }
            showProceed = !!_nadValidateTs;

        } else {
            badge = inlineBadge(data.message || 'No event triggered', '#16a34a', '#f0fdf4');
            if (_nadValidateTs) {
                info += inlineLabel('Validate from', _nadValidateTs);
            }
            showProceed = !!_nadValidateTs;
        }

        container.innerHTML = badge + info
            + (showProceed
                ? '<button id="nadProceedBtn" style="flex-shrink:0; padding:3px 10px; font-size:12px; '
                + 'font-weight:600; background:#0f172a; color:#fff; border:none; border-radius:4px; '
                + 'cursor:pointer; white-space:nowrap;">Proceed →</button>'
                : '');

        container.style.display = 'flex';

        var pb = document.getElementById('nadProceedBtn');
        if (pb) {
            pb.addEventListener('click', function () {
                if (!_nadValidateTs) return;
                // Navigate calendar + load option chain → PnL auto-recalculates on existing positions
                if (typeof global._ocSetAndFetch === 'function') {
                    global._ocSetAndFetch(_nadValidateTs);
                }
                // Keep inline result visible — do not clear
            });
        }
    }

    /** Inline colored badge chip. */
    function inlineBadge(text, color, bg) {
        return '<span style="flex-shrink:0; padding:3px 8px; border-radius:4px; font-size:11px; '
             + 'font-weight:700; background:' + bg + '; color:' + color + '; border:1px solid '
             + color + '33; white-space:nowrap;">' + text + '</span>';
    }

    /** Inline label + value pair. */
    function inlineLabel(label, value) {
        return '<span style="flex-shrink:0; font-size:11px; color:#64748b; white-space:nowrap;">'
             + '<span style="font-weight:700; text-transform:uppercase; letter-spacing:.4px;">'
             + label + '</span>'
             + '<span style="margin-left:4px; color:#1e293b; font-weight:600;">' + value + '</span>'
             + '</span>';
    }

    /**
     * Reads all TCP settings + frozen adjustment levels, builds the payload,
     * and fires POST /next-adjustment-deduct.
     */
    function runNextAdjDeduct() {
        var settings   = typeof global.tcpGetSettings   === 'function' ? global.tcpGetSettings()   : {};
        var slAdjPts   = typeof global._getSlAdjPoints  === 'function' ? global._getSlAdjPoints()  : null;
        var ts         = typeof global._ocGetTimestamp  === 'function' ? global._ocGetTimestamp()  : new Date().toISOString();

        var sl  = settings.stopLoss   || {};
        var tgt = settings.target     || {};
        var tsl = settings.trailSL    || {};
        var tc  = settings.timeControl || {};

        var stratEl  = document.getElementById('strategySelector');
        var expiryEl = document.getElementById('strategyExpirySelector');

        // Serialise open positions (convert 'Call'/'Put' → 'CE'/'PE' for the backend)
        var openPositions = (typeof optionLegs !== 'undefined' ? optionLegs : [])
            .filter(function (leg) { return !leg.exited; })
            .map(function (leg) {
                return {
                    leg_type:      leg.type,
                    option_type:   leg.optionType === 'Call' ? 'CE' : 'PE',
                    strike:        leg.strike,
                    expiry:        leg.expiry,
                    entry_premium: leg.premium,
                    quantity:      leg.quantity
                };
            });

        var closedPnl = typeof global._getClosedPositionsPnl === 'function'
            ? global._getClosedPositionsPnl()
            : 0;

        var payload = {
            timestamp:              ts,
            upper_adjustment_point: slAdjPts ? slAdjPts.upper : null,
            lower_adjustment_point: slAdjPts ? slAdjPts.lower : null,
            lot:                    settings.lots || 2,
            lot_size:               65,
            timeframe:              settings.timeframe || '1m',
            strategy_type:          stratEl  ? stratEl.value  : 'mini_strangle',
            expiry_type:            mapExpiry(expiryEl ? expiryEl.value : 'CURRENT_WEEK'),
            stoploss_status:        sl.enabled  ? 1 : 0,
            stoploss_type:          unitToType(sl.unit),
            stoploss_value:         sl.value  || 0,
            target_status:          tgt.enabled ? 1 : 0,
            target_type:            unitToType(tgt.unit),
            target_value:           tgt.value || 0,
            trailing_sl_status:     tsl.enabled ? 1 : 0,
            trailing_sl_type:       unitToType(tsl.unit),
            trailing_sl_x:          tsl.x || 0,
            trailing_sl_y:          tsl.y || 0,
            position_end_time:      tc.exitTime || '15:26',
            open_positions:         openPositions,
            closed_positions_pnl:   closedPnl,
            lookback_minutes:       settings.lookbackMinutes || 0
        };

        console.info('[NextAdjDeduct] Payload:', payload);

        var btn = document.getElementById('nextAdjDeductBtn');
        if (btn) { btn.disabled = true; btn.textContent = 'Loading…'; }

        fetch('http://0.0.0.0:8000/next-adjustment-deduct', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify(payload)
        })
        .then(function (res) {
            if (!res.ok) throw new Error('HTTP ' + res.status);
            return res.json();
        })
        .then(function (data) {
            showAdjDeductResult(data);
        })
        .catch(function (err) {
            console.error('[NextAdjDeduct] Request failed:', err);
            showAdjDeductResult({ error: err.message });
        })
        .then(function () {
            // always re-enable button (acts as .finally for older engines)
            if (btn) { btn.disabled = false; btn.textContent = 'Next Adjustment Deduct'; }
        });
    }

    // ─── DOM wiring ───────────────────────────────────────────────────────

    function init() {
        // Wire the main strategy_button
        var btn = document.querySelector('button.strategy_button');
        if (btn) {
            btn.addEventListener('click', function (e) {
                e.preventDefault();
                e.stopPropagation();
                runStrategy();
            });
        }

        // Wire Next Adjustment Deduct button
        var nadBtn = document.getElementById('nextAdjDeductBtn');
        if (nadBtn) {
            nadBtn.addEventListener('click', function (e) {
                e.preventDefault();
                e.stopPropagation();
                runNextAdjDeduct();
            });
        }

    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

})(window);
