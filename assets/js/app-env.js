(function () {
    /*
     * Frontend environment switch.
     *
     * Local:
     *   window.APP_ENV_LIVE = false;
     *
     * AWS / finededgealgo.com:
     *   window.APP_ENV_LIVE = true;
     */
    window.APP_ENV_LIVE = false;
    window.APP_LIVE_SITE_ORIGIN = window.APP_LIVE_SITE_ORIGIN || 'https://finedgealgo.com';
    var APP_LIVE_SITE_ORIGIN = String(window.APP_LIVE_SITE_ORIGIN || '').replace(/\/+$/, '');
    window.APP_LOCAL_ALGO_API_BASE_URL = window.APP_LOCAL_ALGO_API_BASE_URL || 'http://localhost:8000/algo';
    window.APP_LIVE_ALGO_API_BASE_URL = window.APP_LIVE_ALGO_API_BASE_URL
        || (APP_LIVE_SITE_ORIGIN ? (APP_LIVE_SITE_ORIGIN + '/algo') : '');

    function parseBooleanFlag(value) {
        if (typeof value === 'boolean') {
            return value;
        }
        if (value === null || typeof value === 'undefined') {
            return null;
        }
        var normalizedValue = String(value).trim().toLowerCase();
        if (['1', 'true', 'yes', 'y', 'live', 'production', 'prod'].indexOf(normalizedValue) !== -1) {
            return true;
        }
        if (['0', 'false', 'no', 'n', 'local', 'development', 'dev'].indexOf(normalizedValue) !== -1) {
            return false;
        }
        return null;
    }

    function isAlgoApiLiveEnvironment() {
        var resolvedLiveFlag = parseBooleanFlag(window.APP_ENV_LIVE);
        if (resolvedLiveFlag !== null) {
            return resolvedLiveFlag;
        }
        return false;
    }

    function resolveAlgoApiBaseUrl() {
        var configuredBaseUrl = isAlgoApiLiveEnvironment()
            ? (window.APP_LIVE_ALGO_API_BASE_URL || window.APP_LOCAL_ALGO_API_BASE_URL || '')
            : (window.APP_LOCAL_ALGO_API_BASE_URL || window.APP_LIVE_ALGO_API_BASE_URL || '');
        var normalizedConfiguredBaseUrl = String(configuredBaseUrl || '').trim().replace(/\/+$/, '');
        if (normalizedConfiguredBaseUrl) {
            return normalizedConfiguredBaseUrl;
        }
        return isAlgoApiLiveEnvironment()
            ? (String(window.APP_LIVE_ALGO_API_BASE_URL || '').replace(/\/+$/, '') || (APP_LIVE_SITE_ORIGIN ? (APP_LIVE_SITE_ORIGIN + '/algo') : ''))
            : 'http://localhost:8000/algo';
    }

    window.parseBooleanFlag = window.parseBooleanFlag || parseBooleanFlag;
    window.isAlgoApiLiveEnvironment = isAlgoApiLiveEnvironment;
    window.resolveAlgoApiBaseUrl = resolveAlgoApiBaseUrl;
    window.APP_ALGO_API_BASE_URL = resolveAlgoApiBaseUrl();
    window.getAlgoApiBaseUrl = function () {
        return resolveAlgoApiBaseUrl();
    };
})();
