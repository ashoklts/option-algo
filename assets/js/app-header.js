(function () {
    var APP_ROOT_FOLDER_NAME = window.APP_ROOT_FOLDER_NAME || 'option-algo';

    function detectFileRootPath() {
        var pathname = (window.location && window.location.pathname) || '';
        var normalizedPath = pathname.replace(/\\/g, '/');
        var marker = '/' + APP_ROOT_FOLDER_NAME + '/';
        var markerIndex = normalizedPath.indexOf(marker);

        if (markerIndex !== -1) {
            return normalizedPath.slice(0, markerIndex + marker.length - 1);
        }
        if (normalizedPath.slice(-(APP_ROOT_FOLDER_NAME.length + 1)) === '/' + APP_ROOT_FOLDER_NAME) {
            return normalizedPath;
        }
        return '/' + APP_ROOT_FOLDER_NAME;
    }

    function getDefaultLocalAlgoApiBaseUrl() {
        if (window.location && window.location.protocol === 'file:') {
            return 'http://localhost:8000/algo';
        }
        if (window.location && /^https?:$/i.test(window.location.protocol || '') && window.location.origin) {
            return window.location.origin.replace(/\/+$/, '') + '/algo';
        }
        return 'http://localhost:8000/algo';
    }

    var APP_LOCAL_ALGO_API_BASE_URL = window.APP_LOCAL_ALGO_API_BASE_URL || getDefaultLocalAlgoApiBaseUrl();
    var APP_LIVE_ALGO_API_BASE_URL = window.APP_LIVE_ALGO_API_BASE_URL || 'https://finedgealgo.com/algo';

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

    function detectLiveEnvironment() {
        var configuredLiveFlag = parseBooleanFlag(window.APP_ENV_LIVE);
        if (configuredLiveFlag !== null) {
            return configuredLiveFlag;
        }
        var configuredEnv = parseBooleanFlag(window.APP_ENV);
        if (configuredEnv !== null) {
            return configuredEnv;
        }
        var hostname = (window.location && window.location.hostname || '').toLowerCase();
        return hostname === 'finedgealgo.com' || hostname === 'www.finedgealgo.com';
    }

    var APP_ENV_LIVE = detectLiveEnvironment();
    var APP_HEADER_ROOT_PATH = window.APP_HEADER_ROOT_PATH
        || (window.location && window.location.protocol === 'file:'
            ? detectFileRootPath()
            : (APP_ENV_LIVE ? '' : '/' + APP_ROOT_FOLDER_NAME));
    function normalizeAlgoApiBaseUrl(baseUrl, isLive) {
        var normalized = String(baseUrl || '').trim();
        if (normalized) {
            return normalized.replace(/\/+$/, '');
        }
        return isLive ? 'https://finedgealgo.com/algo' : getDefaultLocalAlgoApiBaseUrl();
    }

    var APP_ALGO_API_BASE_URL = normalizeAlgoApiBaseUrl(
        APP_ENV_LIVE ? APP_LIVE_ALGO_API_BASE_URL : APP_LOCAL_ALGO_API_BASE_URL,
        APP_ENV_LIVE
    );
    var APP_LISTENING_STORAGE_PREFIX = window.APP_LISTENING_STORAGE_PREFIX || 'option_algo_listening';
    var APP_API_ROUTES = Object.assign({
        strategySave: 'strategy/save',
        strategyList: 'strategy/list',
        strategyById: 'strategy',
        strategyCheckName: 'strategy/check-name',
        portfolioList: 'portfolio/list',
        portfolioSave: 'portfolio/save',
        portfolioById: 'portfolio',
        portfolioActivate: 'portfolio/activate',
        portfolioStartGroup: 'portfolio/start',
        portfolioBacktestStart: 'portfolio/backtest/start',
        tradesList: 'trades/list',
        executions: 'executions',
        backtestStart: 'backtest/start',
        backtestStatus: 'backtest/status',
        backtestResult: 'backtest/result',
        backtestSampleResult: 'backtest/sample-result'
    }, window.APP_API_ROUTES || {});
    var APP_PAGE_ROUTES = Object.assign({
        strategyBuilder: 'test.html',
        portfolio: 'portfolio.html',
        portfolioActivation: 'portfolio-activation.html',
        algoBacktestDashboard: 'algo-backtest/dashboard.html',
        forwardTestDashboard: 'fast-forward/dashboard.html',
        liveDashboard: 'fast-forward-clone.html'
    }, window.APP_PAGE_ROUTES || {});

    var APP_HEADER_TEMPLATE = '' +
        '<div class="app_header __no__print">' +
        '    <div class="mobile_nav_button">' +
        '        <button type="button" class="toggle_icon icon-burger--active" style="width: 30px; background: inherit; border: 0px">' +
        '            <div class="icon-burger">' +
        '                <span class="icon-burger__bar"></span><span class="icon-burger__bar"></span><span class="icon-burger__bar"></span>' +
        '            </div>' +
        '        </button>' +
        '        <div class="page_title_header">Simulator</div>' +
        '    </div>' +
        '    <a href="https://www.stockmock.in/#/home" class="navbar-brand nontab_only"><img src="{{base}}/assets/images/StockMock_New.svg" alt="logo" /></a>' +
        '    <div class="header-nav">' +
        '        <div class="header-nav-link-container show">' +
        '            <div>' +
        '                <div class="header_group backtest_group"><div class="group_title">Aglo</div></div>' +
        '                <div class="header_group analytics_group"><div class="group_title">Analytics</div></div>' +
        '                <a href="https://www.stockmock.in/#!/home" class="header_nav_link"><span>Home</span></a>' +
        '                <a href="{{base}}/strategy.html" class="header_nav_link __active"><span>Strategy</span></a>' +
        '                <a href="{{base}}/portfolios.html" class="header_nav_link"><span>Portfolio</span></a>' +
        '                <a href="{{base}}/algo-backtest/dashboard.html" class="header_nav_link"><span>AgloTrade</span></a>' +
        '                <a href="{{base}}/fast-forward/dashboard.html" class="header_nav_link"><span>Forward Test</span></a>' +
        '                <a href="{{base}}/algo-backtest/dashboard.html" class="header_nav_link"><span>Algo Backtest</span></a>' +
        '                <a href="https://www.stockmock.in/#!/simulator" class="header_nav_link"><span>Simulator</span></a>' +
        '                <a href="https://www.stockmock.in/#!/builder" class="header_nav_link"><span>Builder</span></a>' +
        '                <a href="https://www.stockmock.in/#!/portfolio" class="header_nav_link"><span>Portfolio</span></a>' +
        '                <a href="https://www.stockmock.in/#!/option_chain" class="header_nav_link"><span>Option Chain</span></a>' +
        '                <a href="{{base}}/paper_trade.html" class="header_nav_link" style="color:#1a56db;font-weight:700;"><span>⚡ Paper Trade</span></a>' +
        '            </div>' +
        '            <div>' +
        '                <div class="dark_mode_button">' +
        '                    <input type="checkbox" id="darkmode-toggle" /><label for="darkmode-toggle"><svg width="18" height="18" viewBox="0 0 18 18" fill="none" xmlns="http://www.w3.org/2000/svg" class="sun"><g clip-path="url(#clip0_2343_53)"><path d="M9 13.875C11.6924 13.875 13.875 11.6924 13.875 9C13.875 6.30761 11.6924 4.125 9 4.125C6.30761 4.125 4.125 6.30761 4.125 9C4.125 11.6924 6.30761 13.875 9 13.875Z" fill="transparent" stroke="white" stroke-linecap="round" stroke-linejoin="round"></path><path d="M14.355 14.355L14.2575 14.2575M14.2575 3.7425L14.355 3.645L14.2575 3.7425ZM3.645 14.355L3.7425 14.2575L3.645 14.355ZM9 1.56V1.5V1.56ZM9 16.5V16.44V16.5ZM1.56 9H1.5H1.56ZM16.5 9H16.44H16.5ZM3.7425 3.7425L3.645 3.645L3.7425 3.7425Z" stroke="white" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"></path></g><defs><clipPath id="clip0_2343_53"><rect width="18" height="18" fill="white"></rect></clipPath></defs></svg><svg width="14" height="14" viewBox="0 0 14 14" fill="none" xmlns="http://www.w3.org/2000/svg" class="moon"><path d="M1.18381 7.24476C1.39381 10.2489 3.94298 12.6931 6.99382 12.8273C9.14632 12.9206 11.0713 11.9173 12.2263 10.3364C12.7047 9.68892 12.448 9.25726 11.6488 9.40309C11.258 9.47309 10.8555 9.50226 10.4355 9.48476C7.58298 9.36809 5.24965 6.98226 5.23798 4.16473C5.23215 3.4064 5.38965 2.6889 5.67548 2.03557C5.99048 1.31223 5.61131 0.968067 4.88214 1.27723C2.57214 2.2514 0.991312 4.5789 1.18381 7.24476Z" fill="transparent" stroke="#7e7e7e" stroke-linecap="round" stroke-linejoin="round"></path></svg></label>' +
        '                </div>' +
        '                <div class="nontab_only">' +
        '                    <div class="credit__button">' +
        '                        <div><div><img src="{{base}}/assets/images/credit.svg" style="filter: grayscale(1)" /><span class="pl-2 pr-2" style="color: rgb(177, 177, 177)">20</span></div><div class="credit_type" style="color: rgb(177, 177, 177)">Free Credits</div></div>' +
        '                        <a href="https://www.stockmock.in/#!/subscription" class="__button __donate__button"><span>Buy Plans</span></a>' +
        '                    </div>' +
        '                </div>' +
        '                <a class="header_nav_link nav_user_name ml-3"><span class="tool__tip">Hi, Ashok<div class="tool__tip__text">Ashok kumar</div></span></a>' +
        '                <div class="nontab_only dropdown">' +
        '                    <a data-toggle="dropdown" class="header_nav_link d-flex align-items-center pr-0"><i class="fa fa-user" style="font-size: 20px"></i><i class="fa fa-chevron-down" style="font-size: 10px"></i></a>' +
        '                    <div class="dropdown-menu"><a href="https://www.stockmock.in/#!/profile" class="dropdown-item">Profile</a><a class="dropdown-item">Chart Line Colors</a><a class="dropdown-item">Log Out</a></div>' +
        '                </div>' +
        '            </div>' +
        '        </div>' +
        '        <div class="mobile_nav_button">' +
        '            <div class="credit__button">' +
        '                <div><div><img src="{{base}}/assets/images/credit.svg" style="filter: grayscale(1)" /><span class="pl-2 pr-2" style="color: rgb(177, 177, 177)">20</span></div><div class="credit_type" style="color: rgb(177, 177, 177)">Free Credits</div></div>' +
        '                <a href="https://www.stockmock.in/#!/subscription" class="__button __donate__button"><span>Buy Plans</span></a>' +
        '            </div>' +
        '            <div class="dropdown">' +
        '                <a data-toggle="dropdown" class="header_nav_link d-flex align-items-center pr-0"><i class="fa fa-user" style="font-size: 20px"></i><i class="fa fa-chevron-down" style="font-size: 10px"></i></a>' +
        '                <div class="dropdown-menu"><a href="https://www.stockmock.in/#!/profile" class="dropdown-item">Profile</a><a class="dropdown-item">Chart Line Colors</a><a class="dropdown-item">Log Out</a></div>' +
        '            </div>' +
        '        </div>' +
        '    </div>' +
        '</div>';

    function normalizeBasePath(basePath) {
        if (!basePath) {
            return APP_HEADER_ROOT_PATH;
        }
        if (window.location && window.location.protocol === 'file:' && /^[.]{1,2}(\/|$)/.test(basePath)) {
            return APP_HEADER_ROOT_PATH;
        }
        if (/^(https?:)?\/\//.test(basePath) || basePath.charAt(0) === '/') {
            return basePath;
        }
        if (APP_HEADER_ROOT_PATH.slice(-1) === '/') {
            return APP_HEADER_ROOT_PATH.slice(0, -1) + '/' + basePath.replace(/^\/+/, '');
        }
        return APP_HEADER_ROOT_PATH + '/' + basePath.replace(/^\/+/, '');
    }

    function getBasePath(mountNode) {
        var configuredBase = (mountNode && mountNode.getAttribute('data-header-base')) || window.APP_HEADER_BASE_PATH || '';
        return normalizeBasePath(configuredBase);
    }

    function buildAppUrl(relativePath) {
        var rootPath = APP_HEADER_ROOT_PATH.replace(/\/+$/, '');
        var normalizedRelativePath = String(relativePath || '').replace(/^\/+/, '');
        if (window.location && window.location.protocol === 'file:') {
            return 'file://' + rootPath + '/' + normalizedRelativePath;
        }
        return rootPath + '/' + normalizedRelativePath;
    }

    function buildAlgoApiUrl(path) {
        return APP_ALGO_API_BASE_URL.replace(/\/+$/, '') + '/' + String(path || '').replace(/^\/+/, '');
    }

    function getAlgoApiBaseUrl() {
        return String(APP_ALGO_API_BASE_URL || '').replace(/\/+$/, '');
    }

    function getApiOriginUrl() {
        return getAlgoApiBaseUrl().replace(/\/algo\/?$/, '').replace(/\/+$/, '');
    }

    function buildNamedApiUrl(routeName, suffix) {
        var routePath = APP_API_ROUTES[routeName] || routeName || '';
        var normalizedRoute = String(routePath).replace(/\/+$/, '');
        var normalizedSuffix = String(suffix || '').replace(/^\/+/, '');
        return buildAlgoApiUrl(normalizedSuffix ? normalizedRoute + '/' + normalizedSuffix : normalizedRoute);
    }

    function buildNamedPageUrl(routeName, suffix) {
        var routePath = APP_PAGE_ROUTES[routeName] || routeName || '';
        var normalizedRoute = String(routePath).replace(/\/+$/, '');
        var normalizedSuffix = String(suffix || '').replace(/^\/+/, '');
        return buildAppUrl(normalizedSuffix ? normalizedRoute + '/' + normalizedSuffix : normalizedRoute);
    }

    function buildHeaderHtml(basePath) {
        return APP_HEADER_TEMPLATE.replace(/\{\{base\}\}/g, basePath);
    }

    function getListeningStorageKey(mode) {
        return APP_LISTENING_STORAGE_PREFIX + ':' + String(mode || 'default');
    }

    function loadListeningState(mode) {
        try {
            var rawValue = window.localStorage.getItem(getListeningStorageKey(mode));
            return rawValue ? JSON.parse(rawValue) : null;
        } catch (error) {
            return null;
        }
    }

    function saveListeningState(mode, payload) {
        try {
            window.localStorage.setItem(
                getListeningStorageKey(mode),
                JSON.stringify(Object.assign({}, payload || {}, { updated_at: Date.now() }))
            );
            return true;
        } catch (error) {
            return false;
        }
    }

    function clearListeningState(mode) {
        try {
            window.localStorage.removeItem(getListeningStorageKey(mode));
            return true;
        } catch (error) {
            return false;
        }
    }

    function resolveMount(target) {
        if (!target) {
            return document.querySelector('[data-app-header]');
        }

        if (typeof target === 'string') {
            return document.getElementById(target) || document.querySelector(target);
        }

        return target;
    }

    function renderAppHeader(target) {
        var mountNode = resolveMount(target);
        if (!mountNode) {
            return;
        }

        mountNode.innerHTML = buildHeaderHtml(getBasePath(mountNode));

        if (typeof window.initAppHeaderNav === 'function') {
            window.initAppHeaderNav(mountNode);
        }
    }

    window.APP_HEADER_HTML = APP_HEADER_TEMPLATE;
    window.APP_ROOT_FOLDER_NAME = APP_ROOT_FOLDER_NAME;
    window.APP_HEADER_ROOT_PATH = APP_HEADER_ROOT_PATH;
    window.APP_ENV_LIVE = APP_ENV_LIVE;
    window.APP_LOCAL_ALGO_API_BASE_URL = APP_LOCAL_ALGO_API_BASE_URL;
    window.APP_LIVE_ALGO_API_BASE_URL = APP_LIVE_ALGO_API_BASE_URL;
    window.APP_ALGO_API_BASE_URL = APP_ALGO_API_BASE_URL;
    window.APP_LISTENING_STORAGE_PREFIX = APP_LISTENING_STORAGE_PREFIX;
    window.APP_API_ROUTES = APP_API_ROUTES;
    window.APP_PAGE_ROUTES = APP_PAGE_ROUTES;
    window.buildAppUrl = buildAppUrl;
    window.buildAlgoApiUrl = buildAlgoApiUrl;
    window.getAlgoApiBaseUrl = getAlgoApiBaseUrl;
    window.getApiOriginUrl = getApiOriginUrl;
    window.buildNamedApiUrl = buildNamedApiUrl;
    window.buildNamedPageUrl = buildNamedPageUrl;
    window.APP_LISTENING_MANAGER = {
        load: loadListeningState,
        save: saveListeningState,
        clear: clearListeningState
    };
    window.APP_CONFIG = Object.assign({}, window.APP_CONFIG || {}, {
        rootFolderName: APP_ROOT_FOLDER_NAME,
        rootPath: APP_HEADER_ROOT_PATH,
        isLive: APP_ENV_LIVE,
        localAlgoApiBaseUrl: APP_LOCAL_ALGO_API_BASE_URL,
        liveAlgoApiBaseUrl: APP_LIVE_ALGO_API_BASE_URL,
        algoApiBaseUrl: APP_ALGO_API_BASE_URL,
        apiOriginUrl: getApiOriginUrl(),
        listeningStoragePrefix: APP_LISTENING_STORAGE_PREFIX,
        apiRoutes: APP_API_ROUTES,
        pageRoutes: APP_PAGE_ROUTES,
        buildAppUrl: buildAppUrl,
        buildAlgoApiUrl: buildAlgoApiUrl,
        getAlgoApiBaseUrl: getAlgoApiBaseUrl,
        getApiOriginUrl: getApiOriginUrl,
        buildNamedApiUrl: buildNamedApiUrl,
        buildNamedPageUrl: buildNamedPageUrl,
        listeningManager: window.APP_LISTENING_MANAGER
    });
    window.renderAppHeader = renderAppHeader;

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function () {
            renderAppHeader();
        });
    } else {
        renderAppHeader();
    }
})();
