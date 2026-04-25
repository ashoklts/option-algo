
(function () {
    /* ── state ─────────────────────────────────────────────────── */
    var slMode = false;   // interactive mode on/off
    var slPlaced = false;   // user has clicked a price in this session
    var slHoverPrice = null;    // price under cursor
    var slMarkerPrice = null;    // price placed in current session
    var slSavedUpper = null;    // { price, condition:'>=' } — persisted
    var slSavedLower = null;    // { price, condition:'<=' } — persisted
    // legacy alias (kept for hook refresh)
    Object.defineProperty(window, '_slSaved', { get: function () { return slSavedUpper || slSavedLower; } });

    /* ── DOM refs ───────────────────────────────────────────────── */
    function $(id) { return document.getElementById(id); }
    var configBtn = $('slConfigBtn');
    var panel = $('slConfigPanel');
    var closeBtn = $('slPanelClose');
    var cancelBtn = $('slCancelBtn');
    var saveBtn = $('slSaveBtn');
    var instruction = $('slInstruction');
    var condSel = $('slCondition');
    var priceInput = $('slPriceInput');
    var spotDisp = $('slSpotDisplay');
    var ptsDiff = $('slPointsDiff');
    var pctDiff = $('slPctDiff');
    var pnlBox = $('slPnlBox');
    var pnlVal = $('slPnlValue');
    var hoverLine = $('slHoverLine');
    var markerLine = $('slMarkerLine');   // editing marker
    var shadeReg = $('slShadeRegion');  // editing shade
    var markerUpper = $('slMarkerUpper');  // saved >= marker
    var markerLower = $('slMarkerLower');  // saved <= marker
    var shadeUpper = $('slShadeUpper');   // saved >= shade
    var shadeLower = $('slShadeLower');   // saved <= shade
    var savedBar = $('slSavedBar');
    var savedText = $('slSavedText');
    var savedEdit = $('slSavedEdit');
    var canvas = $('payoffChart');
    var chartWrapper = canvas ? canvas.parentElement : null;

    /* ── helpers ────────────────────────────────────────────────── */
    function getChartArea() {
        return (typeof chart !== 'undefined' && chart && chart.chartArea)
            ? chart.chartArea : null;
    }

    function priceToX(price) {
        var ca = getChartArea();
        if (!ca || !pnlPriceRange || !pnlPriceRange.length) return null;
        var best = -1, bestD = Infinity;
        for (var i = 0; i < pnlPriceRange.length; i++) {
            var d = Math.abs(pnlPriceRange[i] - price);
            if (d < bestD) { bestD = d; best = i; }
        }
        if (best < 0) return null;
        return ca.left + (best / (pnlPriceRange.length - 1)) * (ca.right - ca.left);
    }

    function xToPrice(x) {
        var ca = getChartArea();
        if (!ca || !pnlPriceRange || !pnlPriceRange.length) return null;
        var ratio = (x - ca.left) / (ca.right - ca.left);
        ratio = Math.max(0, Math.min(1, ratio));
        var idx = Math.round(ratio * (pnlPriceRange.length - 1));
        return pnlPriceRange[idx] || null;
    }

    function calcPnL(price) {
        return (typeof calculatePnL === 'function') ? calculatePnL(price) : null;
    }

    function fmtPnL(v) {
        if (v === null) return '—';
        var sign = v >= 0 ? '+' : '-';
        return sign + '₹' + Math.abs(Math.round(v)).toLocaleString('en-IN');
    }

    function updatePanelValues(price) {
        var spot = (typeof currentSpotPrice !== 'undefined') ? currentSpotPrice : null;
        spotDisp.textContent = spot ? '₹' + spot.toLocaleString('en-IN', { maximumFractionDigits: 2 }) : '—';
        if (!price || !spot) {
            ptsDiff.textContent = '— pts';
            pctDiff.textContent = '—%';
            pnlVal.textContent = '—';
            pnlVal.className = 'sl-pnl-value';
            pnlBox.className = 'sl-pnl-box';
            return;
        }
        var pts = Math.round(price - spot);
        var pct = ((price - spot) / spot * 100).toFixed(2);
        ptsDiff.textContent = (pts >= 0 ? '+' : '') + pts + ' pts';
        pctDiff.textContent = (pct >= 0 ? '+' : '') + pct + '%';
        var pnl = calcPnL(price);
        var txt = fmtPnL(pnl);
        pnlVal.textContent = txt;
        var isProfit = pnl !== null && pnl >= 0;
        pnlVal.className = 'sl-pnl-value' + (isProfit ? ' profit' : '');
        pnlBox.className = 'sl-pnl-box' + (isProfit ? ' profit' : '');
    }

    function positionLine(el, price, height, top) {
        var x = priceToX(price);
        var ca = getChartArea();
        if (x === null || !ca) { el.style.display = 'none'; return; }
        el.style.display = 'block';
        el.style.left = x + 'px';
        el.style.top = (top !== undefined ? top : ca.top) + 'px';
        el.style.height = (height !== undefined ? height : (ca.bottom - ca.top)) + 'px';
    }

    function updateShading(price, condition) {
        var ca = getChartArea();
        if (!ca || !price) { shadeReg.style.display = 'none'; return; }
        var x = priceToX(price);
        if (x === null) { shadeReg.style.display = 'none'; return; }
        shadeReg.style.display = 'block';
        shadeReg.style.top = ca.top + 'px';
        shadeReg.style.height = (ca.bottom - ca.top) + 'px';
        if (condition === '<=') {
            // shade left of marker
            shadeReg.style.left = ca.left + 'px';
            shadeReg.style.width = (x - ca.left) + 'px';
        } else {
            // shade right of marker
            shadeReg.style.left = x + 'px';
            shadeReg.style.width = (ca.right - x) + 'px';
        }
    }

    function clearHoverLine() {
        hoverLine.style.display = 'none';
        slHoverPrice = null;
    }

    function setMarker(price) {
        slMarkerPrice = price;
        slPlaced = true;
        var ca = getChartArea();
        if (ca) positionLine(markerLine, price, ca.bottom - ca.top, ca.top);
        updateShading(price, condSel.value);
        priceInput.value = price;
        updatePanelValues(price);
        instruction.textContent = 'Price set. Adjust or save.';
        saveBtn.disabled = false;
    }

    /* ── render persistent saved markers ────────────────────────── */
    function buildLabelHTML(price, spot) {
        var pts = Math.round(price - spot);
        var pct = ((price - spot) / spot * 100).toFixed(2);
        var ptsStr = (pts >= 0 ? '+' : '') + pts + ' pts';
        var pctStr = (pct >= 0 ? '+' : '') + pct + '%';
        return '<span class="sl-lbl-price">\u20B9' + price.toLocaleString('en-IN') + '</span>'
            + '<span class="sl-lbl-dist">' + ptsStr + ' (' + pctStr + ')</span>';
    }

    function renderSavedMarkers() {
        var ca = getChartArea();
        var spot = (typeof currentSpotPrice !== 'undefined') ? currentSpotPrice : 0;
        // Upper (>=)
        if (slSavedUpper && ca) {
            positionLine(markerUpper, slSavedUpper.price, ca.bottom - ca.top, ca.top);
            var lblU = $('slLabelUpper');
            if (lblU) lblU.innerHTML = buildLabelHTML(slSavedUpper.price, spot);
            // shade right
            var x = priceToX(slSavedUpper.price);
            if (x !== null) {
                shadeUpper.style.display = 'block';
                shadeUpper.style.top = ca.top + 'px';
                shadeUpper.style.height = (ca.bottom - ca.top) + 'px';
                shadeUpper.style.left = x + 'px';
                shadeUpper.style.width = (ca.right - x) + 'px';
                shadeUpper.style.background = 'rgba(22,163,74,.10)';
            }
        } else {
            markerUpper.style.display = 'none';
            shadeUpper.style.display = 'none';
        }
        // Lower (<=)
        if (slSavedLower && ca) {
            positionLine(markerLower, slSavedLower.price, ca.bottom - ca.top, ca.top);
            var lblL = $('slLabelLower');
            if (lblL) lblL.innerHTML = buildLabelHTML(slSavedLower.price, spot);
            // shade left
            var xL = priceToX(slSavedLower.price);
            if (xL !== null) {
                shadeLower.style.display = 'block';
                shadeLower.style.top = ca.top + 'px';
                shadeLower.style.height = (ca.bottom - ca.top) + 'px';
                shadeLower.style.left = ca.left + 'px';
                shadeLower.style.width = (xL - ca.left) + 'px';
                shadeLower.style.background = 'rgba(220,38,38,.10)';
            }
        } else {
            markerLower.style.display = 'none';
            shadeLower.style.display = 'none';
        }
        // Saved bar
        if (slSavedUpper || slSavedLower) {
            savedBar.style.display = 'flex';
            var html = '<span class="sl-saved-label">SL:</span>';
            if (slSavedUpper) {
                html += '<span class="sl-saved-item">'
                    + 'Above \u2265 \u20B9' + slSavedUpper.price.toLocaleString('en-IN')
                    + ' <span class="sl-saved-pnl">(' + fmtPnL(calcPnL(slSavedUpper.price)) + ')</span>'
                    + ' <a class="sl-remove-link" href="#" onclick="window._slRemoveSide(\'upper\');return false;">remove</a>'
                    + '</span>';
            }
            if (slSavedUpper && slSavedLower) {
                html += '<span class="sl-saved-sep">|</span>';
            }
            if (slSavedLower) {
                html += '<span class="sl-saved-item">'
                    + 'Below \u2264 \u20B9' + slSavedLower.price.toLocaleString('en-IN')
                    + ' <span class="sl-saved-pnl">(' + fmtPnL(calcPnL(slSavedLower.price)) + ')</span>'
                    + ' <a class="sl-remove-link" href="#" onclick="window._slRemoveSide(\'lower\');return false;">remove</a>'
                    + '</span>';
            }
            savedText.innerHTML = html;
        } else {
            savedBar.style.display = 'none';
        }
    }

    /* ── enter / exit config mode ───────────────────────────────── */
    function enterMode() {
        slMode = true;
        slPlaced = false;
        configBtn.classList.add('active');
        configBtn.innerHTML = '&#10005; Cancel';
        panel.style.display = 'block';

        // Default condition based on which is not yet set
        if (!slSavedUpper && slSavedLower) condSel.value = '>=';
        else condSel.value = '<=';

        // Load matching saved config if any
        var existing = condSel.value === '>=' ? slSavedUpper : slSavedLower;
        if (existing) {
            priceInput.value = existing.price;
            setMarker(existing.price);
            instruction.textContent = 'Modify or click chart to update.';
        } else {
            priceInput.value = '';
            markerLine.style.display = 'none';
            shadeReg.style.display = 'none';
            saveBtn.disabled = true;
            instruction.textContent = 'Click on the chart to place stoploss level';
        }
        canvas.style.cursor = 'crosshair';
    }

    function exitMode() {
        slMode = false;
        slPlaced = false;
        clearHoverLine();
        configBtn.classList.remove('active');
        configBtn.innerHTML = '&#9889; Stoploss';
        panel.style.display = 'none';
        canvas.style.cursor = '';
        // Hide editing overlays
        markerLine.style.display = 'none';
        shadeReg.style.display = 'none';
        // Re-render persisted saved markers
        renderSavedMarkers();
    }

    /* ── canvas events ──────────────────────────────────────────── */
    if (canvas) {
        canvas.addEventListener('mousemove', function (e) {
            if (!slMode) return;
            var ca = getChartArea();
            if (!ca) return;
            var rect = canvas.getBoundingClientRect();
            var x = e.clientX - rect.left;
            if (x < ca.left || x > ca.right) { clearHoverLine(); return; }
            var price = xToPrice(x);
            if (!price) return;
            slHoverPrice = price;
            positionLine(hoverLine, price, ca.bottom - ca.top, ca.top);
            // Update panel values live on hover (don't update priceInput while hover)
            updatePanelValues(slPlaced ? slMarkerPrice : price);
            if (!slPlaced) updateShading(price, condSel.value);
        });

        canvas.addEventListener('mouseleave', function () {
            if (!slMode) return;
            clearHoverLine();
            if (!slPlaced) {
                shadeReg.style.display = 'none';
                updatePanelValues(null);
            }
        });

        canvas.addEventListener('click', function (e) {
            if (!slMode) return;
            var ca = getChartArea();
            if (!ca) return;
            var rect = canvas.getBoundingClientRect();
            var x = e.clientX - rect.left;
            if (x < ca.left || x > ca.right) return;
            var price = xToPrice(x);
            if (!price) return;
            setMarker(price);
        });
    }

    /* ── price input two-way binding ────────────────────────────── */
    priceInput.addEventListener('input', function () {
        var v = parseFloat(this.value);
        if (isNaN(v) || v <= 0) return;
        setMarker(v);
    });

    /* ── condition change → load matching saved config + update shading ── */
    condSel.addEventListener('change', function () {
        var cond = this.value;
        var existing = cond === '>=' ? slSavedUpper : slSavedLower;
        if (existing) {
            priceInput.value = existing.price;
            setMarker(existing.price);
        } else {
            // clear editing overlays for a fresh placement
            markerLine.style.display = 'none';
            shadeReg.style.display = 'none';
            priceInput.value = '';
            slPlaced = false;
            slMarkerPrice = null;
            saveBtn.disabled = true;
            instruction.textContent = 'Click on the chart to place stoploss level';
        }
        if (slPlaced && slMarkerPrice) updateShading(slMarkerPrice, cond);
    });

    /* ── buttons ─────────────────────────────────────────────────── */
    configBtn.addEventListener('click', function () {
        if (slMode) exitMode();
        else enterMode();
    });

    [closeBtn, cancelBtn].forEach(function (btn) {
        btn.addEventListener('click', function () { exitMode(); });
    });

    saveBtn.addEventListener('click', function () {
        if (!slMarkerPrice) return;
        var config = { price: slMarkerPrice, condition: condSel.value };
        if (condSel.value === '>=') slSavedUpper = config;
        else slSavedLower = config;
        exitMode();
    });

    savedEdit.addEventListener('click', function () { enterMode(); });

    /* ── draggable panel ─────────────────────────────────────────── */
    (function () {
        var header = panel.querySelector('.sl-panel-header');
        var dragging = false;
        var startX, startY, origLeft, origTop;
        // Remember last position across open/close
        var savedLeft = null, savedTop = null;

        function applyPosition(left, top) {
            // Clamp within viewport
            left = Math.max(0, Math.min(left, window.innerWidth - panel.offsetWidth));
            top = Math.max(0, Math.min(top, window.innerHeight - panel.offsetHeight));
            panel.style.left = left + 'px';
            panel.style.top = top + 'px';
            panel.style.right = 'auto';
            savedLeft = left;
            savedTop = top;
        }

        // When panel becomes visible, restore last position or use default
        var _origEnterMode = enterMode;
        enterMode = function () {
            _origEnterMode();
            if (savedLeft !== null && savedTop !== null) {
                panel.style.left = savedLeft + 'px';
                panel.style.top = savedTop + 'px';
                panel.style.right = 'auto';
            }
        };

        header.addEventListener('mousedown', function (e) {
            if (e.target === $('slPanelClose')) return;
            dragging = true;
            startX = e.clientX;
            startY = e.clientY;
            var rect = panel.getBoundingClientRect();
            // rect is already in viewport coords since panel is position:fixed
            origLeft = rect.left;
            origTop = rect.top;
            header.style.cursor = 'grabbing';
            e.preventDefault();
        });

        document.addEventListener('mousemove', function (e) {
            if (!dragging) return;
            var dx = e.clientX - startX;
            var dy = e.clientY - startY;
            applyPosition(origLeft + dx, origTop + dy);
        });

        document.addEventListener('mouseup', function () {
            if (!dragging) return;
            dragging = false;
            header.style.cursor = 'grab';
        });
    })();

    /* ── keep lines in sync when chart redraws ───────────────────── */
    // Hook into updateChart — call our refresh after every chart update
    var _origUpdateChart = null;
    function hookUpdateChart() {
        if (typeof updateChart === 'function' && !_origUpdateChart) {
            _origUpdateChart = updateChart;
            window.updateChart = function () {
                _origUpdateChart.apply(this, arguments);
                // After chart renders, reposition all SL DOM overlays
                setTimeout(slRefreshOverlays, 220);
            };
        }
    }

    function slRefreshOverlays() {
        var ca = getChartArea();
        if (!ca) return;
        if (slMode && slPlaced && slMarkerPrice) {
            positionLine(markerLine, slMarkerPrice, ca.bottom - ca.top, ca.top);
            updateShading(slMarkerPrice, condSel.value);
        } else if (!slMode) {
            markerLine.style.display = 'none';
            shadeReg.style.display = 'none';
        }
        renderSavedMarkers();
    }

    // Try to hook immediately, retry if updateChart not ready yet
    hookUpdateChart();
    if (!_origUpdateChart) {
        var hookInterval = setInterval(function () {
            hookUpdateChart();
            if (_origUpdateChart) clearInterval(hookInterval);
        }, 300);
    }

    // Expose stoploss adjustment points for strategy.js API call
    window._getSlAdjPoints = function () {
        return {
            upper: slSavedUpper ? slSavedUpper.price : null,
            lower: slSavedLower ? slSavedLower.price : null,
        };
    };

    // Remove one side of the saved stoploss
    window._slRemoveSide = function (side) {
        if (side === 'upper') {
            slSavedUpper = null;
            markerUpper.style.display = 'none';
            shadeUpper.style.display = 'none';
        } else if (side === 'lower') {
            slSavedLower = null;
            markerLower.style.display = 'none';
            shadeLower.style.display = 'none';
        }
        renderSavedMarkers();
    };

    // Clear auto-adjustment markers on load
    if (typeof clearAdjustmentLevels === 'function') clearAdjustmentLevels();
    else setTimeout(function () {
        if (typeof clearAdjustmentLevels === 'function') clearAdjustmentLevels();
    }, 500);

})();