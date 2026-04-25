
(function () {
    'use strict';

    /* ── 1. Lot selector 1–50 ─────────────────────────── */
    var lotSel = document.getElementById('tcpLotSelector');
    if (lotSel) {
        for (var i = 1; i <= 50; i++) {
            var o = document.createElement('option');
            o.value = i;
            o.textContent = i + (i === 1 ? ' Lot' : ' Lots');
            lotSel.appendChild(o);
        }
    }

    /* ── 2a. Toggle helper → enable / disable its inputs ── */
    function wireToggle(cbId, fieldIds) {
        var cb = document.getElementById(cbId);
        if (!cb) return;
        function sync() {
            fieldIds.forEach(function (id) {
                var el = document.getElementById(id);
                if (el) el.disabled = !cb.checked;
            });
        }
        cb.addEventListener('change', sync);
        sync(); // run once on load
    }
    wireToggle('tcpSlEnabled', ['tcpSlUnit', 'tcpSlValue']);
    wireToggle('tcpTgtEnabled', ['tcpTgtUnit', 'tcpTgtValue']);
    wireToggle('tcpTslEnabled', ['tcpTslUnit', 'tcpTslX', 'tcpTslY']);

    /* ── 2b. Toggle helper → show / hide a container div ── */
    function wireToggleShow(cbId, containerId) {
        var cb = document.getElementById(cbId);
        var container = document.getElementById(containerId);
        if (!cb || !container) return;
        function sync() {
            container.style.display = cb.checked ? 'flex' : 'none';
        }
        cb.addEventListener('change', sync);
        sync(); // run once on load
    }
    /* Time Control: enable/disable inputs (never hide) */
    (function () {
        var cb = document.getElementById('tcpTimeEnabled');
        var entry = document.getElementById('tcpEntryTime');
        var exit = document.getElementById('tcpExitTime');
        if (!cb) return;
        function syncTime() {
            var on = cb.checked;
            if (entry) entry.disabled = !on;
            if (exit) exit.disabled = !on;
        }
        cb.addEventListener('change', syncTime);
        syncTime();
    })();

    /* ── 2c. Strike Type: populate tcpStrikeDrop + enable/disable + swap ── */
    (function () {
        var cb = document.getElementById('tcpStrikeEnabled');
        var modeSel = document.getElementById('tcpStrikeMode');
        var valInput = document.getElementById('tcpStrikeInput');
        var drop = document.getElementById('tcpStrikeDrop');

        /* ── populate OTM 30 → OTM 1 → ATM → ITM 1 → ITM 30 ── */
        if (drop) {
            var frag = document.createDocumentFragment();
            for (var i = 30; i >= 1; i--) {
                var o = document.createElement('option');
                o.value = 'OTM' + i; o.dataset.dir = 'otm';
                o.textContent = 'OTM ' + i; frag.appendChild(o);
            }
            var atm = document.createElement('option');
            atm.value = 'ATM'; atm.textContent = 'ATM'; atm.selected = true;
            frag.appendChild(atm);
            for (var j = 1; j <= 30; j++) {
                var p = document.createElement('option');
                p.value = 'ITM' + j; p.dataset.dir = 'itm';
                p.textContent = 'ITM ' + j; frag.appendChild(p);
            }
            drop.appendChild(frag);
        }

        /* ── swap textbox ↔ strike dropdown based on mode ── */
        function syncStrikeMode() {
            if (!modeSel || !valInput || !drop) return;
            var isStrike = modeSel.value === 'strike';
            valInput.style.display = isStrike ? 'none' : 'inline-block';
            drop.style.display = isStrike ? 'inline-block' : 'none';
            valInput.placeholder = modeSel.value === 'delta' ? 'Delta' : 'Premium';
        }
        if (modeSel) modeSel.addEventListener('change', syncStrikeMode);

        /* ── enable/disable all strike controls with toggle ── */
        function syncStrikeEnabled() {
            var on = cb && cb.checked;
            if (modeSel) modeSel.disabled = !on;
            if (valInput) valInput.disabled = !on;
            if (drop) drop.disabled = !on;
        }
        if (cb) cb.addEventListener('change', syncStrikeEnabled);

        /* run both on load */
        syncStrikeMode();
        syncStrikeEnabled();
    })();

    /* ── 3. Trail SL: Y < X validation ───────────────── */
    function validateTsl() {
        var xEl = document.getElementById('tcpTslX');
        var yEl = document.getElementById('tcpTslY');
        var errEl = document.getElementById('tcpTslErr');
        var cb = document.getElementById('tcpTslEnabled');
        if (!xEl || !yEl || !errEl) return true;
        var x = parseFloat(xEl.value) || 0;
        var y = parseFloat(yEl.value) || 0;
        var invalid = !!(cb && cb.checked && x > 0 && y >= x);
        yEl.classList.toggle('tcp-err', invalid);
        errEl.classList.toggle('show', invalid);
        return !invalid;
    }
    var tslX = document.getElementById('tcpTslX');
    var tslY = document.getElementById('tcpTslY');
    if (tslX) tslX.addEventListener('input', validateTsl);
    if (tslY) tslY.addEventListener('input', validateTsl);

    /* ── 4. Trade mode → show / hide Pause button ─────── */
    var modeEl = document.getElementById('tcpTradeMode');
    var pauseEl = document.getElementById('tcpPauseBtn');
    function syncMode() {
        if (!pauseEl) return;
        pauseEl.style.display =
            (modeEl && modeEl.value === 'semi_auto') ? 'inline-block' : 'none';
    }
    if (modeEl) modeEl.addEventListener('change', syncMode);
    syncMode();

    /* ── 5. Pause ↔ Continue ──────────────────────────── */
    if (pauseEl) {
        pauseEl.addEventListener('click', function () {
            var paused = pauseEl.classList.toggle('is-paused');
            pauseEl.textContent = paused ? 'Continue' : 'Pause';
        });
    }

    /* ── 6. Expose settings to strategy / adj engine ─── */
    window.tcpGetSettings = function () {
        var x = parseFloat(document.getElementById('tcpTslX')?.value || 0);
        var y = parseFloat(document.getElementById('tcpTslY')?.value || 0);
        return {
            lots: parseInt(document.getElementById('tcpLotSelector')?.value || 1, 10),
            timeframe: document.getElementById('tcpTimeframe')?.value || '5m',
            lookbackMinutes: parseInt(document.getElementById('tcpLookbackMinutes')?.value || 0, 10),
            tradeMode: document.getElementById('tcpTradeMode')?.value || 'manual',
            isRunning: pauseEl ? !pauseEl.classList.contains('is-paused') : true,
            stopLoss: {
                enabled: document.getElementById('tcpSlEnabled')?.checked || false,
                unit: document.getElementById('tcpSlUnit')?.value || 'points',
                value: parseFloat(document.getElementById('tcpSlValue')?.value || 0)
            },
            target: {
                enabled: document.getElementById('tcpTgtEnabled')?.checked || false,
                unit: document.getElementById('tcpTgtUnit')?.value || 'points',
                value: parseFloat(document.getElementById('tcpTgtValue')?.value || 0)
            },
            trailSL: {
                enabled: document.getElementById('tcpTslEnabled')?.checked || false,
                unit: document.getElementById('tcpTslUnit')?.value || 'points',
                x: x,
                y: y,
                valid: !(x > 0 && y >= x)
            },
            timeControl: {
                enabled: document.getElementById('tcpTimeEnabled')?.checked || false,
                entryTime: document.getElementById('tcpEntryTime')?.value || '09:15',
                exitTime: document.getElementById('tcpExitTime')?.value || '15:30'
            },
            strikeType: {
                enabled: document.getElementById('tcpStrikeEnabled')?.checked || false,
                mode: document.getElementById('tcpStrikeMode')?.value || 'delta',
                value: parseFloat(document.getElementById('tcpStrikeInput')?.value || 0),
                strike: document.getElementById('tcpStrikeDrop')?.value || 'ATM'
            }
        };
    };

})();
