(function () {

    /* ── helpers ──────────────────────────────────────────────── */
    function fmtExpiry(dateStr) {
        var d = new Date(dateStr);
        var M = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
            'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
        return String(d.getUTCDate()).padStart(2, '0') + ' ' +
            M[d.getUTCMonth()] + " '" +
            String(d.getUTCFullYear()).slice(-2);
    }

    /* format OI → "1.5Cr" / "9.6L" / "24K" */
    function fmtOI(val) {
        if (!val && val !== 0) return '—';
        function trim(n) { return n.slice(-2) === '.0' ? n.slice(0, -2) : n; }
        if (val >= 10000000) return trim((val / 10000000).toFixed(1)) + 'Cr';
        if (val >= 100000) return trim((val / 100000).toFixed(1)) + 'L';
        return Math.round(val / 1000) + 'K';
    }

    function uniqueExpiries(data) {
        var seen = {}, out = [];
        data.forEach(function (r) {
            if (!seen[r.expiry]) { seen[r.expiry] = true; out.push(r.expiry); }
        });
        return out.sort();
    }

    /* ── expiry carousel ──────────────────────────────────────── */
    /** Returns DTE label for an expiry string, e.g. "(CW: 4 DTE)" or "(NW/CM: 10 DTE)" */
    function expiryDteLabel(expStr) {
        // Use the option chain's loaded timestamp as reference (via exposed getter)
        var ts = (typeof window._ocGetTimestamp === 'function')
            ? window._ocGetTimestamp() : '';
        var dateOnly = ts ? ts.replace('T', ' ').slice(0, 10) : null;
        var curDate = dateOnly ? new Date(dateOnly + 'T00:00:00') : new Date();
        curDate.setHours(0, 0, 0, 0);

        // Normalise expiry string to YYYY-MM-DD (handles "2025-10-07 00:00:00" etc.)
        var expOnly = expStr ? expStr.slice(0, 10) : '';
        if (!expOnly.match(/^\d{4}-\d{2}-\d{2}$/)) return '';
        var expDate = new Date(expOnly + 'T00:00:00');
        expDate.setHours(0, 0, 0, 0);

        var dte = Math.round((expDate - curDate) / 86400000);
        if (dte < 0) return '';

        // Current week's Thursday (0 offset if today IS Thursday)
        var dow = curDate.getDay();
        var dToThur = (4 - dow + 7) % 7;
        var thisThur = new Date(curDate);
        thisThur.setDate(curDate.getDate() + dToThur);
        var nextThur = new Date(thisThur);
        nextThur.setDate(thisThur.getDate() + 7);

        function isLastThurOfMonth(d) {
            var nx = new Date(d); nx.setDate(d.getDate() + 7);
            return nx.getMonth() !== d.getMonth();
        }

        var expT = expDate.getTime();
        var labels = [];
        if (expT === thisThur.getTime()) labels.push('CW');
        if (expT === nextThur.getTime()) labels.push('NW');
        if (isLastThurOfMonth(expDate) && expDate.getMonth() === curDate.getMonth())
            labels.push('CM');
        if (isLastThurOfMonth(expDate) && expDate.getMonth() === ((curDate.getMonth() + 1) % 12))
            labels.push('NM');

        var prefix = labels.join('/');
        return prefix ? '(' + prefix + ': ' + dte + ' DTE)' : '(' + dte + ' DTE)';
    }

    /** Updates the option_chain_expiry span with currently selected expiry text */
    function syncExpirySpan(expStr) {
        var span = document.querySelector('.option_chain_expiry');
        if (span) span.textContent = expStr ? '(' + fmtExpiry(expStr) + ')' : '';
    }

    function renderCarousel(expiries, selected) {
        var track = document.querySelector('.expiry-carousel__track');
        if (!track) return;
        track.innerHTML = expiries.map(function (exp) {
            var cls = 'expiry_button item' +
                (exp === selected ? ' selected__oc__expiry' : '');
            var dteLabel = expiryDteLabel(exp);
            return '<div class="' + cls + '" data-oc-expiry="' + exp + '">' +
                '<button>' + fmtExpiry(exp) + '</button>' +
                (dteLabel ? '<div class="expiry_indicator">' + dteLabel + '</div>' : '') +
                '</div>';
        }).join('');

        syncExpirySpan(selected);

        track.querySelectorAll('.expiry_button[data-oc-expiry]').forEach(function (el) {
            el.addEventListener('click', function () {
                track.querySelectorAll('.expiry_button')
                    .forEach(function (b) { b.classList.remove('selected__oc__expiry'); });
                el.classList.add('selected__oc__expiry');
                syncExpirySpan(el.dataset.ocExpiry);
                renderTable(el.dataset.ocExpiry);
                updateStraddlePrem(el.dataset.ocExpiry);
            });
        });
    }

    /* ── option chain table ───────────────────────────────────── */
    function renderTable(expiry, skipATMScroll) {
        var container = document.getElementById('optionChainTable');
        if (!container) return;

        // save scroll position before rebuilding DOM (used when skipATMScroll is true)
        var _existingScrollBox = container.querySelector('.oc-custom-table');
        var _savedScrollTop = (_existingScrollBox && skipATMScroll) ? _existingScrollBox.scrollTop : 0;

        // 1. filter by expiry
        var rows = option_chain_data.filter(function (r) { return r.expiry === expiry; });

        // 2. build strike map
        var map = {};
        rows.forEach(function (r) {
            if (!map[r.strike]) map[r.strike] = { call: null, put: null };
            if (r.type === 'CE') map[r.strike].call = r;
            else if (r.type === 'PE') map[r.strike].put = r;
        });

        var strikes = Object.keys(map).map(Number).sort(function (a, b) { return a - b; });

        // 3. ATM = strike closest to spot_price
        var spotPrice = rows.length ? rows[0].spot_price : 0;
        var atmStrike = strikes.reduce(function (best, cur) {
            return Math.abs(cur - spotPrice) < Math.abs(best - spotPrice) ? cur : best;
        }, strikes[0] || 0);

        // sync payoff chart: update spot price + OI maps for the selected expiry
        if (typeof currentSpotPrice !== 'undefined' && spotPrice) {
            currentSpotPrice = spotPrice;
            // Sync market time from the first row of the newly selected expiry
            if (typeof currentMarketTime !== 'undefined') {
                var tsRow = option_chain_data.find(function (d) { return d.expiry === expiry && d.timestamp; });
                if (tsRow) currentMarketTime = new Date(tsRow.timestamp);
            }
            // Recalculate ±7% zoom range for the new spot price,
            // but only if the user has not manually set a custom zoom.
            if (typeof calcZoomRange === 'function' && !userCustomZoom) {
                var newZoom = calcZoomRange(spotPrice);
                zoomStartPrice = newZoom.start;
                zoomEndPrice = newZoom.end;
                isZoomed = true;
            }
            // Keep slider in sync with new spot price
            var slider = document.getElementById('spotPriceSlider');
            if (slider) {
                slider.min = zoomStartPrice;
                slider.max = zoomEndPrice;
                slider.value = spotPrice;
                var spv = document.getElementById('spotPriceValue');
                if (spv) spv.textContent = spotPrice.toFixed(2);
                var cpd = document.getElementById('currentPriceDisplay');
                if (cpd) cpd.textContent = spotPrice.toFixed(2);
            }
        }
        if (typeof buildOIMap === 'function') {
            buildOIMap(expiry);
            if (typeof updateChart === 'function') updateChart();
        }

        // 4a. compute max OI per side for progress-bar scaling
        var maxCallOI = 0, maxPutOI = 0;
        strikes.forEach(function (s) {
            if (map[s].call && map[s].call.oi) maxCallOI = Math.max(maxCallOI, map[s].call.oi);
            if (map[s].put && map[s].put.oi) maxPutOI = Math.max(maxPutOI, map[s].put.oi);
        });

        // 4b. OI cell builder
        function oiCell(item, side) {
            var oi = (item && item.oi) ? item.oi : 0;
            var maxOI = side === 'call' ? maxCallOI : maxPutOI;
            var pct = (maxOI > 0 && oi > 0) ? ((oi / maxOI) * 100).toFixed(1) : '0';
            var valCls = side === 'call' ? 'oc-oi-call' : 'oc-oi-put';
            var barCls = side === 'call' ? 'oc-oi-bar-call' : 'oc-oi-bar-put';
            var tdCls = side === 'call' ? 'oc-call-oi' : 'oc-put-oi';
            return '<td class="' + tdCls + '">' +
                '<div class="oc-oi-value ' + valCls + '">' + fmtOI(oi) + '</div>' +
                '<div class="oc-oi-bar-wrap">' +
                '<div class="oc-oi-bar ' + barCls + '" style="width:' + pct + '%"></div>' +
                '</div>' +
                '</td>';
        }

        // 4. B/S button markup — data-attributes drive click handler; active class from optionLegs
        function hasPosition(expiry, strike, optionType, type) {
            if (typeof optionLegs === 'undefined') return false;
            return optionLegs.some(function (leg) {
                return leg.expiry === expiry &&
                    leg.strike === strike &&
                    leg.optionType === optionType &&
                    leg.type === type &&
                    !leg.exited;
            });
        }

        function makeBtns(strike, optionType, premium, exp) {
            var buyActive = hasPosition(exp, strike, optionType, 'Buy') ? ' oc-btn-active' : '';
            var sellActive = hasPosition(exp, strike, optionType, 'Sell') ? ' oc-btn-active' : '';
            var wrapperActive = (buyActive || sellActive) ? ' has-active' : '';
            return '<div class="oc-actions action_button' + wrapperActive + '"' +
                ' data-strike="' + strike + '"' +
                ' data-optiontype="' + optionType + '"' +
                ' data-premium="' + premium + '"' +
                ' data-expiry="' + exp + '">' +
                '<button class="buy_button mr-1' + buyActive + '">B</button>' +
                '<button class="sell_button' + sellActive + '">S</button>' +
                '</div>';
        }

        // 5. cell content builder
        function cellContent(item) {
            if (!item) return '<span class="oc-empty">—</span>';
            var ltp = item.close.toFixed(2);
            var delta = item.delta != null ? item.delta.toFixed(2) : '—';
            return '<span class="oc-ltp">' + ltp + '</span>' +
                '<span class="oc-delta">(' + delta + ')</span>';
        }

        // 6. build rows
        var tbody = strikes.map(function (strike) {
            var d = map[strike];

            // row class flags
            var rowCls = 'oc-row';
            if (strike === atmStrike) rowCls += ' oc-atm';
            if (strike < atmStrike) rowCls += ' oc-call-itm'; // call ITM: strike < ATM
            if (strike > atmStrike) rowCls += ' oc-put-itm';  // put  ITM: strike > ATM

            // call cell  →  [B][S]  ltp(delta)
            var callLTP = (d.call && d.call.close) ? d.call.close : 0;
            var putLTP = (d.put && d.put.close) ? d.put.close : 0;

            var callInner =
                '<div class="oc-cell-inner">' +
                makeBtns(strike, 'Call', callLTP, expiry) +
                cellContent(d.call) +
                '</div>';

            // put cell  →  ltp(delta)  [B][S]
            var putInner =
                '<div class="oc-cell-inner">' +
                cellContent(d.put) +
                makeBtns(strike, 'Put', putLTP, expiry) +
                '</div>';

            // Combined IV = (Call IV + Put IV) / 2; fall back to whichever side exists
            var callIV = (d.call && d.call.iv) ? d.call.iv : 0;
            var putIV = (d.put && d.put.iv) ? d.put.iv : 0;
            var combinedIV = (callIV && putIV) ? (callIV + putIV) / 2 : (callIV || putIV);
            var ivDisplay = combinedIV
                ? '<span class="oc-iv-value">' + (combinedIV * 100).toFixed(2) + '%</span>'
                : '<span class="oc-empty">—</span>';

            return '<tr class="' + rowCls + '">' +
                '<td class="oc-call">' + callInner + '</td>' +
                oiCell(d.call, 'call') +
                '<td class="oc-iv">' + ivDisplay + '</td>' +
                '<td class="oc-strike">' + strike + '</td>' +
                oiCell(d.put, 'put') +
                '<td class="oc-put">' + putInner + '</td>' +
                '</tr>';
        }).join('');

        // 7. inject HTML
        container.innerHTML =
            '<div class="oc-custom-table">' +
            '<table>' +
            '<thead><tr>' +
            '<th class="oc-call-head">Call LTP <span class="oc-head-delta">(Δ)</span></th>' +
            '<th class="oc-call-oi-head">Call OI</th>' +
            '<th class="oc-iv-head">IV</th>' +
            '<th class="oc-strike-head">Strike</th>' +
            '<th class="oc-put-oi-head">Put OI</th>' +
            '<th class="oc-put-head">Put LTP <span class="oc-head-delta">(Δ)</span></th>' +
            '</tr></thead>' +
            '<tbody>' + tbody + '</tbody>' +
            '</table>' +
            '</div>';

        // 8. update total OI display
        var totalCallOI = 0, totalPutOI = 0;
        strikes.forEach(function (s) {
            if (map[s].call && map[s].call.oi) totalCallOI += map[s].call.oi;
            if (map[s].put && map[s].put.oi) totalPutOI += map[s].put.oi;
        });
        var elCallOI = document.querySelector('.total_call_oi');
        var elPutOI = document.querySelector('.total_put_oi');
        if (elCallOI) elCallOI.textContent = fmtOI(totalCallOI);
        if (elPutOI) elPutOI.textContent = fmtOI(totalPutOI);

        // 9. wire click handlers on B/S buttons
        container.querySelectorAll('.oc-actions').forEach(function (wrapper) {
            var strike = parseFloat(wrapper.getAttribute('data-strike'));
            var optionType = wrapper.getAttribute('data-optiontype');
            var premium = parseFloat(wrapper.getAttribute('data-premium'));
            var exp = wrapper.getAttribute('data-expiry');

            function syncButtonStates() {
                var buyBtn = wrapper.querySelector('.buy_button');
                var sellBtn = wrapper.querySelector('.sell_button');
                var buyActive = hasPosition(exp, strike, optionType, 'Buy');
                var sellActive = hasPosition(exp, strike, optionType, 'Sell');
                buyBtn.classList.toggle('oc-btn-active', buyActive);
                sellBtn.classList.toggle('oc-btn-active', sellActive);
                wrapper.classList.toggle('has-active', buyActive || sellActive);
                // keep wrapper visible if it now has an active position
                if (buyActive || sellActive) {
                    wrapper.style.display = 'inline-flex';
                }
            }

            function toggleLeg(type) {
                if (typeof optionLegs === 'undefined') return;
                var idx = optionLegs.findIndex(function (leg) {
                    return leg.expiry === exp &&
                        leg.strike === strike &&
                        leg.optionType === optionType &&
                        leg.type === type;
                });
                var isNewSell = false;
                if (idx !== -1) {
                    _recordClosedLegPnl(optionLegs[idx]);
                    optionLegs.splice(idx, 1);
                } else {
                    // Use market snapshot timestamp as entry time (not browser clock)
                    var chainTs = (typeof option_chain_data !== 'undefined' && option_chain_data.length)
                        ? option_chain_data[0].timestamp
                        : (typeof currentMarketTime !== 'undefined' ? currentMarketTime.toISOString() : new Date().toISOString());
                    optionLegs.push({
                        type: type,
                        optionType: optionType,
                        strike: strike,
                        premium: premium,
                        quantity: 65,
                        expiry: exp,
                        entryDate: chainTs
                    });
                    if (type === 'Sell') isNewSell = true;
                }
                // update only this wrapper's button states — no table re-render
                syncButtonStates();
                // refresh legs panel, position table, summary stats, and payoff chart
                if (typeof renderLegs === 'function') renderLegs();
                if (typeof renderPositionTable === 'function') renderPositionTable();
                if (typeof updateSummaryStats === 'function') updateSummaryStats();
                if (typeof updateChart === 'function') updateChart();
                // Auto-adjustment markers removed — user configures stoploss manually
            }

            wrapper.querySelector('.buy_button').addEventListener('click', function (e) {
                e.stopPropagation();
                toggleLeg('Buy');
            });
            wrapper.querySelector('.sell_button').addEventListener('click', function (e) {
                e.stopPropagation();
                toggleLeg('Sell');
            });
        });

        // 9b. wire hover: show B/S on hovered row; keep visible if has-active
        container.querySelectorAll('.oc-row').forEach(function (row) {
            row.addEventListener('mouseenter', function () {
                row.querySelectorAll('.oc-actions').forEach(function (a) {
                    a.style.display = 'inline-flex';
                });
            });
            row.addEventListener('mouseleave', function () {
                row.querySelectorAll('.oc-actions').forEach(function (a) {
                    // keep visible if an active position exists
                    if (!a.classList.contains('has-active')) {
                        a.style.display = 'none';
                    }
                });
            });
        });

        // 10. scroll ATM row to the vertical center of the table viewport
        var scrollBox = container.querySelector('.oc-custom-table');
        if (skipATMScroll) {
            // API refresh: restore the scroll position the user was at
            if (scrollBox) scrollBox.scrollTop = _savedScrollTop;
        } else {
            // Initial load only: auto-scroll to ATM
            var atmRow = container.querySelector('.oc-row.oc-atm');
            if (scrollBox && atmRow) {
                var targetTop = atmRow.offsetTop
                    - (scrollBox.clientHeight / 2)
                    + (atmRow.offsetHeight / 2);
                scrollBox.scrollTo({ top: Math.max(0, targetTop), behavior: 'smooth' });
            }
        }
    }

    /* ── init ─────────────────────────────────────────────────── */
    function init(skipATMScroll) {
        if (typeof option_chain_data === 'undefined' || !option_chain_data.length) return;
        var expiries = uniqueExpiries(option_chain_data);
        // preserve the user's currently selected expiry tab across API refreshes
        var selEl = document.querySelector('.expiry_button.selected__oc__expiry');
        var posExpiry = (typeof optionLegs !== 'undefined' && optionLegs.length > 0) ? optionLegs[0].expiry : null;
        var selExp;
        if (selEl && selEl.dataset.ocExpiry && expiries.indexOf(selEl.dataset.ocExpiry) !== -1) {
            selExp = selEl.dataset.ocExpiry;
        } else if (posExpiry && expiries.indexOf(posExpiry) !== -1) {
            selExp = posExpiry;
        } else {
            selExp = expiries[0];
        }
        renderCarousel(expiries, selExp);
        renderTable(selExp, skipATMScroll);
        updateStraddlePrem(selExp);
    }

    // API refreshes call reloadOptionChain() — skip ATM scroll, preserve position
    window.reloadOptionChain = function () {
        init(true);
        if (typeof window.resetZoomIfAuto === 'function') window.resetZoomIfAuto();
    };

    /* ── ATM Straddle Premium ──────────────────────────────────────────── */
    function updateStraddlePrem(expiry) {
        var el = document.getElementById('straddlePremValue');
        if (!el) return;
        if (typeof option_chain_data === 'undefined' || !option_chain_data.length) { el.textContent = '—'; return; }
        var spot = (typeof currentSpotPrice !== 'undefined' && currentSpotPrice)
            ? currentSpotPrice : option_chain_data[0].spot_price || 0;
        var rows = option_chain_data.filter(function (r) { return r.expiry === expiry; });
        if (!rows.length) { el.textContent = '—'; return; }
        var strikes = [...new Set(rows.map(function (r) { return r.strike; }))].sort(function (a, b) { return a - b; });
        var atm = strikes.reduce(function (best, cur) {
            return Math.abs(cur - spot) < Math.abs(best - spot) ? cur : best;
        }, strikes[0]);
        var ce = rows.find(function (r) { return r.strike === atm && r.type === 'CE'; });
        var pe = rows.find(function (r) { return r.strike === atm && r.type === 'PE'; });
        var ceLtp = ce ? (ce.close || 0) : 0;
        var peLtp = pe ? (pe.close || 0) : 0;
        if (!ceLtp && !peLtp) { el.textContent = '—'; return; }
        el.textContent = (ceLtp + peLtp).toFixed(2);
    }
    window.updateStraddlePrem = updateStraddlePrem;

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

})();
