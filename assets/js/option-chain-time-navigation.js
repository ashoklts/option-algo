/* ── Option Chain Time Navigation & API Fetch ─────────────────────── */
(function () {
    var currentTimestamp = '2025-10-01T09:16:00';

    function formatTimestamp(date) {
        var pad = function (n) { return String(n).padStart(2, '0'); };
        return date.getFullYear() + '-'
            + pad(date.getMonth() + 1) + '-'
            + pad(date.getDate()) + 'T'
            + pad(date.getHours()) + ':'
            + pad(date.getMinutes()) + ':'
            + pad(date.getSeconds());
    }

    function parseTimestamp(ts) {
        // parse as local time: "YYYY-MM-DDTHH:mm:ss"
        var parts = ts.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})$/);
        if (!parts) return new Date();
        return new Date(
            +parts[1], +parts[2] - 1, +parts[3],
            +parts[4], +parts[5], +parts[6]
        );
    }

    var TRADE_START_MINS = 9 * 60 + 16;   // 09:16
    var TRADE_END_MINS = 15 * 60 + 29;  // 15:29

    // ── Holiday / trading-day helpers ─────────────────────────────────────
    var _marketHolidays = new Set(); // populated from API on boot

    function _dateStr(d) {
        var p = function (n) { return String(n).padStart(2, '0'); };
        return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate());
    }

    function _isTradingDay(d) {
        var dow = d.getDay(); // 0=Sun, 6=Sat
        if (dow === 0 || dow === 6) return false;
        return !_marketHolidays.has(_dateStr(d));
    }

    /** Advance date by +1 or -1 days until a trading day is found. */
    function _skipToTradingDay(d, forward) {
        var limit = 0;
        d.setDate(d.getDate() + (forward ? 1 : -1));
        while (!_isTradingDay(d) && limit++ < 14) {
            d.setDate(d.getDate() + (forward ? 1 : -1));
        }
    }

    function adjustTimestamp(deltaMinutes) {
        var d = parseTimestamp(currentTimestamp);
        var cur = d.getHours() * 60 + d.getMinutes();
        var next = cur + deltaMinutes;

        if (next > TRADE_END_MINS) {
            // carry overflow minutes to next trading day from 09:16
            // subtract 1 so that 15:29 + 1m → 09:16 (not 09:17)
            var overflow = next - TRADE_END_MINS - 1;
            _skipToTradingDay(d, true);
            next = TRADE_START_MINS + overflow;
            if (next > TRADE_END_MINS) next = TRADE_END_MINS;
        } else if (next < TRADE_START_MINS) {
            // borrow underflow minutes from previous trading day ending at 15:29
            // subtract 1 so that 09:16 - 1m → 15:29 (not 15:28)
            var underflow = TRADE_START_MINS - next - 1;
            _skipToTradingDay(d, false);
            next = TRADE_END_MINS - underflow;
            if (next < TRADE_START_MINS) next = TRADE_START_MINS;
        }

        d.setHours(Math.floor(next / 60), next % 60, 0, 0);
        currentTimestamp = formatTimestamp(d);
    }

    function setSOD() {
        var d = parseTimestamp(currentTimestamp);
        d.setHours(9, 16, 0, 0);
        currentTimestamp = formatTimestamp(d);
    }

    function setEOD() {
        var d = parseTimestamp(currentTimestamp);
        d.setHours(15, 29, 0, 0);
        currentTimestamp = formatTimestamp(d);
    }

    function nextDay() {
        var d = parseTimestamp(currentTimestamp);
        var h = d.getHours(), m = d.getMinutes();
        _skipToTradingDay(d, true);
        d.setHours(h, m, 0, 0);
        currentTimestamp = formatTimestamp(d);
    }

    function prevDay() {
        var d = parseTimestamp(currentTimestamp);
        var h = d.getHours(), m = d.getMinutes();
        _skipToTradingDay(d, false);
        d.setHours(h, m, 0, 0);
        currentTimestamp = formatTimestamp(d);
    }

    function loadMarketHolidays() {
        fetch('http://0.0.0.0:8000/simulator/get-market-holidays')
            .then(function (r) { return r.json(); })
            .then(function (json) {
                if (json && Array.isArray(json.holidays)) {
                    _marketHolidays = new Set(json.holidays);
                    console.info('[MarketCalendar] Loaded', _marketHolidays.size, 'holidays');
                }
            })
            .catch(function (e) {
                console.warn('[MarketCalendar] Could not load holidays:', e);
            });
    }

    function fetchAndUpdateOptionChain(ts) {
        fetch('http://0.0.0.0:8000/simulator/get-option-chain?timestamp=' + encodeURIComponent(ts))
            .then(function (res) {
                if (!res.ok) throw new Error('HTTP ' + res.status);
                return res.json();
            })
            .then(function (json) {
                if (json && Array.isArray(json.data) && json.data.length) {
                    option_chain_data = json.data;
                    if (typeof window.reloadOptionChain === 'function') {
                        window.reloadOptionChain();
                    }
                    // Explicitly rebuild OI map + redraw payoff chart after data update
                    // (reloadOptionChain may not re-trigger buildOIMap if expiry hasn't changed)
                    if (typeof buildOIMap === 'function') {
                        var activeExpBtn = document.querySelector('.expiry_button.active[data-oc-expiry]');
                        var activeExp = activeExpBtn ? activeExpBtn.dataset.ocExpiry : null;
                        if (!activeExp && option_chain_data.length) {
                            activeExp = option_chain_data[0].expiry || null;
                        }
                        if (activeExp) {
                            buildOIMap(activeExp);
                            if (typeof updateChart === 'function') updateChart();
                        }
                    }
                    // refresh position table: updates exit_input with the new close price
                    // for each open position and recalculates per-row PnL
                    if (typeof renderPositionTable === 'function') renderPositionTable();
                    if (typeof updateSummaryStats === 'function') updateSummaryStats();
                    // sync date/time display to the option chain timestamp
                    if (typeof window.syncDateTimeDisplay === 'function') {
                        window.syncDateTimeDisplay(ts);
                    }
                }
            })
            .catch(function (err) {
                console.warn('Option chain fetch failed:', err);
            });
    }

    /* ── expose for calendar ── */
    window._ocGetTimestamp = function () { return currentTimestamp; };
    window._ocSetAndFetch = function (ts) {
        currentTimestamp = ts;
        if (typeof window.syncDateTimeDisplay === 'function') window.syncDateTimeDisplay(ts);
        fetchAndUpdateOptionChain(ts);
    };

    function getTimestampFromDropdowns() {
        var d = parseTimestamp(currentTimestamp);
        var hEl = document.getElementById('currentHourSelect');
        var mEl = document.getElementById('currentMinuteSelect');
        var h = hEl ? parseInt(hEl.value, 10) : d.getHours();
        var m = mEl ? parseInt(mEl.value, 10) : d.getMinutes();
        d.setHours(h, m, 0, 0);
        return formatTimestamp(d);
    }

    function wireDropdowns() {
        var hEl = document.getElementById('currentHourSelect');
        var mEl = document.getElementById('currentMinuteSelect');

        function onHourChange() {
            // market opens at 09:16 — force minute to 16 when hour 9 is selected
            if (hEl && parseInt(hEl.value, 10) === 9 && mEl) {
                mEl.value = '16';
            }
            currentTimestamp = getTimestampFromDropdowns();
            if (typeof window.syncDateTimeDisplay === 'function') window.syncDateTimeDisplay(currentTimestamp);
            fetchAndUpdateOptionChain(currentTimestamp);
        }

        function onMinuteChange() {
            currentTimestamp = getTimestampFromDropdowns();
            if (typeof window.syncDateTimeDisplay === 'function') window.syncDateTimeDisplay(currentTimestamp);
            fetchAndUpdateOptionChain(currentTimestamp);
        }

        if (hEl) hEl.addEventListener('change', onHourChange);
        if (mEl) mEl.addEventListener('change', onMinuteChange);
    }

    function wireButtons() {
        document.querySelectorAll('.arrow_button').forEach(function (btn) {
            var label = btn.textContent.trim();
            btn.addEventListener('click', function () {
                switch (label) {
                    case '<< Day': prevDay(); break;
                    case 'SOD': setSOD(); break;
                    case '-2h': adjustTimestamp(-120); break;
                    case '-30m': adjustTimestamp(-30); break;
                    case '-15m': adjustTimestamp(-15); break;
                    case '-5m': adjustTimestamp(-5); break;
                    case '-1m': adjustTimestamp(-1); break;
                    case '1m+': adjustTimestamp(1); break;
                    case '5m+': adjustTimestamp(5); break;
                    case '15m+': adjustTimestamp(15); break;
                    case '30m+': adjustTimestamp(30); break;
                    case '2h+': adjustTimestamp(120); break;
                    case 'EOD': setEOD(); break;
                    case 'Day >>': nextDay(); break;
                    default: return;
                }
                if (typeof window.syncDateTimeDisplay === 'function') window.syncDateTimeDisplay(currentTimestamp);
                fetchAndUpdateOptionChain(currentTimestamp);
            });
        });
    }

    function bootstrap() {
        loadMarketHolidays();
        wireButtons();
        wireDropdowns();
        if (typeof window.syncDateTimeDisplay === 'function') window.syncDateTimeDisplay(currentTimestamp);
        fetchAndUpdateOptionChain(currentTimestamp);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bootstrap);
    } else {
        bootstrap();
    }
})();
