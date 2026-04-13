
(function () {
    function init(scope) {
        var root = scope || document;
        var btn = root.querySelector('.mobile_nav_button .toggle_icon');
        var container = root.querySelector('.header-nav-link-container');
        if (!btn || !container || btn.dataset.navBound === 'true') return;

        var overlay = document.querySelector('.pt-nav-overlay');
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.className = 'pt-nav-overlay';
            document.body.appendChild(overlay);
        }

        container.classList.remove('show');
        btn.classList.remove('icon-burger--active');
        btn.dataset.navBound = 'true';

        function openMenu() {
            container.classList.add('show');
            btn.classList.add('icon-burger--active');
            overlay.classList.add('show');
        }
        function closeMenu() {
            container.classList.remove('show');
            btn.classList.remove('icon-burger--active');
            overlay.classList.remove('show');
        }

        btn.addEventListener('click', function () {
            container.classList.contains('show') ? closeMenu() : openMenu();
        });

        overlay.addEventListener('click', closeMenu);
    }

    window.initAppHeaderNav = init;

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
