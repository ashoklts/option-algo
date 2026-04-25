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
    window.APP_LOCAL_ALGO_API_BASE_URL = window.APP_LOCAL_ALGO_API_BASE_URL || 'http://localhost:8000/algo';
    window.APP_LIVE_ALGO_API_BASE_URL = window.APP_LIVE_ALGO_API_BASE_URL || 'https://finedgealgo.com/algo';

    function normalizeBaseUrl(value) {
        return String(value || '').replace(/\/+$/, '');
    }

    function resolveAppEnvLive() {
        if (typeof window.APP_ENV_LIVE === 'boolean') {
            return window.APP_ENV_LIVE;
        }
        var normalized = String(window.APP_ENV_LIVE == null ? '' : window.APP_ENV_LIVE).trim().toLowerCase();
        if (!normalized) {
            return false;
        }
        return ['1', 'true', 'yes', 'y', 'live', 'production', 'prod'].indexOf(normalized) !== -1;
    }

    function getLocalAlgoApiBaseUrl() {
        return normalizeBaseUrl(window.APP_LOCAL_ALGO_API_BASE_URL);
    }

    function getLiveAlgoApiBaseUrl() {
        return normalizeBaseUrl(window.APP_LIVE_ALGO_API_BASE_URL);
    }

    function getBackendUrl() {
        return resolveAppEnvLive() ? getLiveAlgoApiBaseUrl() : getLocalAlgoApiBaseUrl();
    }

    function buildBackendUrl(path) {
        return getBackendUrl() + '/' + String(path || '').replace(/^\/+/, '');
    }

    function getBackendOriginUrl() {
        return getBackendUrl().replace(/\/algo\/?$/, '').replace(/\/+$/, '');
    }

    window.resolveAppEnvLive = window.resolveAppEnvLive || resolveAppEnvLive;
    window.getLocalAlgoApiBaseUrl = window.getLocalAlgoApiBaseUrl || getLocalAlgoApiBaseUrl;
    window.getLiveAlgoApiBaseUrl = window.getLiveAlgoApiBaseUrl || getLiveAlgoApiBaseUrl;
    window.getBackendUrl = window.getBackendUrl || getBackendUrl;
    window.buildBackendUrl = window.buildBackendUrl || buildBackendUrl;
    window.getBackendOriginUrl = window.getBackendOriginUrl || getBackendOriginUrl;
    window.APP_ALGO_API_BASE_URL = window.APP_ALGO_API_BASE_URL || getBackendUrl();
})();
