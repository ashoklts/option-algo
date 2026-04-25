/* ── Date / Time Display — driven by option chain timestamp ────────── */
/* Call window.syncDateTimeDisplay('2025-10-01T09:16:00') to update.   */
window.syncDateTimeDisplay = (function () {
    var DAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    var MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    function parseLocal(ts) {
        // parse "YYYY-MM-DDTHH:mm:ss" as local time (no UTC offset shift)
        var p = ts.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
        if (!p) return null;
        return new Date(+p[1], +p[2] - 1, +p[3], +p[4], +p[5]);
    }

    function selectClosest(selectEl, value) {
        var opts = selectEl.options;
        var bestIdx = 0, bestDiff = Infinity;
        for (var i = 0; i < opts.length; i++) {
            var diff = Math.abs(parseInt(opts[i].value, 10) - value);
            if (diff < bestDiff) { bestDiff = diff; bestIdx = i; }
        }
        selectEl.selectedIndex = bestIdx;
    }

    return function (ts) {
        var d = parseLocal(ts);
        if (!d) return;

        var dateEl = document.getElementById('currentDateDisplay');
        var hourEl = document.getElementById('currentHourSelect');
        var minEl = document.getElementById('currentMinuteSelect');

        if (dateEl) dateEl.textContent =
            DAYS[d.getDay()] + ', ' + MONTHS[d.getMonth()] + ' ' +
            d.getDate() + ', ' + d.getFullYear();

        if (hourEl) selectClosest(hourEl, d.getHours());
        if (minEl) selectClosest(minEl, d.getMinutes());
    };
})();