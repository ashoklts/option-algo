/* ── Option Chain Calendar ────────────────────────────────────────── */
(function () {
    var MONTHS = ['January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'];
    var DOW = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'];
    var viewY, viewM;

    function getSelected() {
        var ts = typeof window._ocGetTimestamp === 'function'
            ? window._ocGetTimestamp() : '2025-10-01T09:16:00';
        var p = ts.match(/^(\d{4})-(\d{2})-(\d{2})/);
        return p ? { y: +p[1], m: +p[2] - 1, d: +p[3] } : { y: 2025, m: 9, d: 1 };
    }

    function daysIn(y, m) { return new Date(y, m + 1, 0).getDate(); }

    function render() {
        var popup = document.getElementById('ocCalendarPopup');
        if (!popup) return;
        var sel = getSelected();

        var hdr =
            '<div class="oc-cal-hdr">' +
            '<button class="oc-cal-nav-btn" id="ocCalPrev">&#8249;</button>' +
            '<div class="oc-cal-title">' +
            '<span>' + MONTHS[viewM] + '</span>' +
            '<span>' + viewY + '</span>' +
            '<div class="oc-cal-yr-wrap">' +
            '<button class="oc-cal-yr-btn" id="ocCalYrUp">&#9650;</button>' +
            '<button class="oc-cal-yr-btn" id="ocCalYrDn">&#9660;</button>' +
            '</div>' +
            '</div>' +
            '<button class="oc-cal-nav-btn" id="ocCalNext">&#8250;</button>' +
            '</div>';

        var cells = DOW.map(function (d) {
            return '<div class="oc-cal-dow">' + d + '</div>';
        }).join('');

        var first = new Date(viewY, viewM, 1).getDay();
        var prevTail = daysIn(viewY, viewM - 1);
        for (var i = first - 1; i >= 0; i--) {
            cells += '<div class="oc-cal-day oc-cal-other">' + (prevTail - i) + '</div>';
        }

        var total = daysIn(viewY, viewM);
        for (var d = 1; d <= total; d++) {
            var dow = (first + d - 1) % 7;
            var cls = 'oc-cal-day';
            if (dow === 0) cls += ' oc-cal-sun';
            if (dow === 6) cls += ' oc-cal-sat';
            if (viewY === sel.y && viewM === sel.m && d === sel.d) cls += ' oc-cal-sel';
            cells += '<div class="' + cls + '" data-d="' + d + '">' + d + '</div>';
        }

        var filled = first + total;
        var rem = filled % 7 ? 7 - (filled % 7) : 0;
        for (var n = 1; n <= rem; n++) {
            cells += '<div class="oc-cal-day oc-cal-other">' + n + '</div>';
        }

        popup.innerHTML = hdr + '<div class="oc-cal-grid">' + cells + '</div>';

        popup.querySelector('#ocCalPrev').addEventListener('click', function (e) {
            e.stopPropagation();
            viewM--; if (viewM < 0) { viewM = 11; viewY--; } render();
        });
        popup.querySelector('#ocCalNext').addEventListener('click', function (e) {
            e.stopPropagation();
            viewM++; if (viewM > 11) { viewM = 0; viewY++; } render();
        });
        popup.querySelector('#ocCalYrUp').addEventListener('click', function (e) {
            e.stopPropagation(); viewY++; render();
        });
        popup.querySelector('#ocCalYrDn').addEventListener('click', function (e) {
            e.stopPropagation(); viewY--; render();
        });

        popup.querySelectorAll('.oc-cal-day:not(.oc-cal-other)').forEach(function (el) {
            el.addEventListener('click', function (e) {
                e.stopPropagation();
                pick(viewY, viewM, parseInt(el.getAttribute('data-d'), 10));
            });
        });
    }

    function pick(y, m, d) {
        var p = function (n) { return String(n).padStart(2, '0'); };
        var hEl = document.getElementById('currentHourSelect');
        var mEl = document.getElementById('currentMinuteSelect');
        var h = hEl ? parseInt(hEl.value, 10) : 9;
        var min = mEl ? parseInt(mEl.value, 10) : 16;
        var ts = y + '-' + p(m + 1) + '-' + p(d) + 'T' + p(h) + ':' + p(min) + ':00';
        close();
        if (typeof window._ocSetAndFetch === 'function') window._ocSetAndFetch(ts);
    }

    function open() {
        var s = getSelected();
        viewY = s.y; viewM = s.m;
        var popup = document.getElementById('ocCalendarPopup');
        if (!popup) return;
        render();
        popup.style.display = 'block';
    }

    function close() {
        var popup = document.getElementById('ocCalendarPopup');
        if (popup) popup.style.display = 'none';
    }

    function init() {
        var btn = document.getElementById('currentDateDisplay');
        if (btn) {
            btn.style.cursor = 'pointer';
            btn.addEventListener('click', function (e) {
                e.stopPropagation();
                var popup = document.getElementById('ocCalendarPopup');
                popup && popup.style.display !== 'none' ? close() : open();
            });
        }
        document.addEventListener('click', function (e) {
            var popup = document.getElementById('ocCalendarPopup');
            if (popup && popup.style.display !== 'none' && !popup.contains(e.target)) {
                close();
            }
        });
    }

    document.readyState === 'loading'
        ? document.addEventListener('DOMContentLoaded', init)
        : init();
})();
