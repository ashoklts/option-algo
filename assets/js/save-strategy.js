(function () {
    function getApiUrl(routeName, suffix) {
        if (typeof window.buildNamedApiUrl === 'function') {
            return window.buildNamedApiUrl(routeName, suffix);
        }
        var baseUrl = (window.APP_CONFIG && window.APP_CONFIG.algoApiBaseUrl)
            || (typeof window.getBackendUrl === 'function' ? window.getBackendUrl() : '')
            || window.APP_ALGO_API_BASE_URL
            || '';
        var routeMap = window.APP_API_ROUTES || {};
        var routePath = routeMap[routeName] || routeName || '';
        var normalizedRoute = String(routePath).replace(/\/+$/, '');
        var normalizedSuffix = String(suffix || '').replace(/^\/+/, '');
        return baseUrl.replace(/\/+$/, '') + '/' + (normalizedSuffix ? normalizedRoute + '/' + normalizedSuffix : normalizedRoute);
    }

    function initSaveStrategy() {
        var modal = document.getElementById('save-strategy-modal');
        var nameInput = document.getElementById('save-strategy-name');
        var alertEl = document.getElementById('save-strategy-alert');
        var doneBtn = document.getElementById('save-strategy-done');
        var openBtn = document.getElementById('save-strategy-btn');
        var saveAsBtn = document.getElementById('save-as-strategy-btn');
        var closeBtn = document.getElementById('save-strategy-close');
        var cancelBtn = document.getElementById('save-strategy-cancel');

        if (!modal || !nameInput || !alertEl || !doneBtn) return;

        function showAlert(msg, isError) {
            alertEl.textContent = msg;
            alertEl.className = 'text-xs mt-1 mb-1 ' + (isError ? 'text-red-500' : 'text-green-600');
            alertEl.classList.remove('hidden');
        }

        function hideAlert() {
            alertEl.classList.add('hidden');
            alertEl.textContent = '';
        }

        function resetModal() {
            nameInput.value = '';
            hideAlert();
            doneBtn.disabled = false;
            doneBtn.textContent = 'Done';
        }

        function openModal() {
            resetModal();
            modal.classList.remove('hidden');
            setTimeout(function () { nameInput.focus(); }, 50);
        }

        function closeModal() {
            modal.classList.add('hidden');
        }

        if (openBtn) openBtn.addEventListener('click', openModal);
        if (saveAsBtn) saveAsBtn.addEventListener('click', openModal);
        if (closeBtn) closeBtn.addEventListener('click', closeModal);
        if (cancelBtn) cancelBtn.addEventListener('click', closeModal);

        modal.addEventListener('click', function (e) {
            if (e.target === modal) closeModal();
        });

        nameInput.addEventListener('input', hideAlert);

        doneBtn.addEventListener('click', function () {
            var name = (nameInput.value || '').trim();
            if (!name) {
                showAlert('Please enter a strategy name.', true);
                return;
            }

            doneBtn.disabled = true;
            doneBtn.textContent = 'Saving…';
            hideAlert();

            var config = typeof window.buildConfig === 'function' ? window.buildConfig() : {};
            config.name = name;

            fetch(getApiUrl('strategySave'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            })
                .then(function (res) {
                    if (res.status === 409) {
                        return res.json().then(function () {
                            showAlert('Strategy name "' + name + '" already exists. Please use a different name.', true);
                            doneBtn.disabled = false;
                            doneBtn.textContent = 'Done';
                        });
                    }
                    if (!res.ok) throw new Error('Save failed');
                    return res.json().then(function () {
                        showAlert('Strategy saved successfully!', false);
                        doneBtn.disabled = false;
                        doneBtn.textContent = 'Done';
                        setTimeout(closeModal, 1200);
                    });
                })
                .catch(function () {
                    showAlert('Failed to save. Check server connection.', true);
                    doneBtn.disabled = false;
                    doneBtn.textContent = 'Done';
                });
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initSaveStrategy);
    } else {
        initSaveStrategy();
    }
})();
