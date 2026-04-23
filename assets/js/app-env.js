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
    window.APP_ENV_LIVE = true;
    window.APP_LOCAL_ALGO_API_BASE_URL = window.APP_LOCAL_ALGO_API_BASE_URL || 'http://localhost:8000/algo';
    window.APP_LIVE_ALGO_API_BASE_URL = window.APP_LIVE_ALGO_API_BASE_URL || 'http://finedgealgo.com:8000/algo';
})();
