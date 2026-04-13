
/* ── Option Chain Hide / Show toggle ─────────────────────────────────── */
(function () {
    function init() {
        var btn = document.querySelector('a.hide_image');
        var img = btn ? btn.querySelector('img') : null;
        var ocBox = document.querySelector('.smulator_option_chain_box');
        var resultBox = document.querySelector('.simulator_result_box');
        var slot = document.getElementById('ocPositionsSlot');
        if (!btn || !ocBox || !resultBox || !slot) return;

        btn.style.cursor = 'pointer';
        var hidden = false;

        // Elements that live in right panel but move to left when OC hides
        function getMovable() {
            var rc = resultBox.querySelector('.simulator_card') || resultBox;
            return {
                stats: rc.querySelector('.simulator__stats'),
                advance: rc.querySelector('.advance__setting'),
                posTabs: rc.querySelectorAll('.table_tabs_container')[1],
                posTable: rc.querySelector('.position_table'),
                rc: rc,
            };
        }

        // OC-specific content to hide (keeps the box but empties its visible content)
        function getOcContent() {
            var sc = ocBox.querySelector('.simulator_card') || ocBox;
            return sc.querySelectorAll(
                '.pcr, .expiry__viewer, #optionChainTable, .oc_filter, .eod__message'
            );
        }

        // Placeholders so we know where to reinsert elements
        var ph = {};

        btn.addEventListener('click', function (e) {
            e.preventDefault();
            e.stopPropagation();
            hidden = !hidden;

            var m = getMovable();

            if (hidden) {
                // ── Insert placeholders then move elements ──────────────
                ['stats', 'advance', 'posTabs', 'posTable'].forEach(function (key) {
                    var el = m[key];
                    if (!el) return;
                    var p = document.createElement('div');
                    p.id = 'ocph_' + key;
                    p.style.display = 'none';
                    el.parentNode.insertBefore(p, el);
                    ph[key] = p;
                    slot.appendChild(el);
                });

                // Hide OC-specific content
                getOcContent().forEach(function (el) { el.style.display = 'none'; });

                slot.style.display = 'block';
                img.src = 'assets/images/show_option_chain.svg';
                img.title = 'Show Option Chain';
                img.style.transform = '';

            } else {
                // ── Move elements back before their placeholders ────────
                ['stats', 'advance', 'posTabs', 'posTable'].forEach(function (key) {
                    var el = slot.querySelector(
                        key === 'stats' ? '.simulator__stats' :
                            key === 'advance' ? '.advance__setting' :
                                key === 'posTabs' ? '.table_tabs_container' :
                                    '.position_table'
                    );
                    var p = document.getElementById('ocph_' + key);
                    if (el && p && p.parentNode) {
                        p.parentNode.insertBefore(el, p);
                        p.parentNode.removeChild(p);
                    }
                    delete ph[key];
                });

                // Restore OC content
                getOcContent().forEach(function (el) { el.style.display = ''; });

                slot.style.display = 'none';
                img.src = 'assets/images/hide_left.svg';
                img.title = 'Hide Option Chain';
            }
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
