(function (window) {
    'use strict';

    function resolveLiveFlag() {
        if (typeof window.APP_ENV_LIVE === 'boolean') {
            return window.APP_ENV_LIVE;
        }
        var normalized = String(window.APP_ENV_LIVE == null ? '' : window.APP_ENV_LIVE).trim().toLowerCase();
        return ['1', 'true', 'yes', 'y', 'live', 'production', 'prod'].indexOf(normalized) !== -1;
    }

    function buildDefaultSocketUrl() {
        var isLive = resolveLiveFlag();
        var configuredApiBase = isLive
            ? (window.APP_LIVE_ALGO_API_BASE_URL
                || (window.APP_CONFIG && window.APP_CONFIG.liveAlgoApiBaseUrl)
                || (window.APP_CONFIG && window.APP_CONFIG.algoApiBaseUrl)
                || window.APP_ALGO_API_BASE_URL
                || 'https://finedgealgo.com/algo')
            : (window.APP_LOCAL_ALGO_API_BASE_URL
                || (window.APP_CONFIG && window.APP_CONFIG.localAlgoApiBaseUrl)
                || 'http://localhost:8000/algo');
        if (configuredApiBase) {
            var normalizedApiBase = String(configuredApiBase).replace(/\/+$/, '');
            if (/^https?:\/\//i.test(normalizedApiBase)) {
                return normalizedApiBase.replace(/^https?:\/\//i, isLive ? 'wss://' : 'ws://') + '/ws/executions';
            }
        }

        var protocol = window.location.protocol || '';
        if (!isLive || protocol === 'file:') {
            return 'ws://localhost:8000/algo/ws/executions';
        }

        var origin = window.location.origin || 'https://finedgealgo.com';
        var socketOrigin = origin.replace(/^https?:\/\//i, isLive ? 'wss://' : 'ws://');
        if (!/^wss?:\/\//i.test(socketOrigin)) {
            socketOrigin = isLive ? 'wss://finedgealgo.com' : 'ws://localhost:8000';
        }
        return socketOrigin.replace(/\/$/, '') + '/algo/ws/executions';
    }

    function safeParseJson(value) {
        try {
            return JSON.parse(value);
        } catch (error) {
            return value;
        }
    }

    function createExecutionSocket(options) {
        var config = options || {};
        var reconnectDelayMs = typeof config.reconnectDelayMs === 'number' ? config.reconnectDelayMs : 3000;
        var socketUrl = config.url || buildDefaultSocketUrl();
        var socket = null;
        var reconnectTimer = null;
        var manuallyClosed = false;
        var pendingMessages = [];
        var listeners = {
            status: typeof config.onStatusChange === 'function' ? [config.onStatusChange] : [],
            message: typeof config.onMessage === 'function' ? [config.onMessage] : []
        };

        function emitStatus(status, meta) {
            listeners.status.forEach(function (listener) {
                listener(status, meta || {});
            });
        }

        function emitMessage(payload) {
            listeners.message.forEach(function (listener) {
                listener(payload);
            });
        }

        function clearReconnectTimer() {
            if (reconnectTimer) {
                window.clearTimeout(reconnectTimer);
                reconnectTimer = null;
            }
        }

        function flushPendingMessages() {
            if (!socket || socket.readyState !== window.WebSocket.OPEN || !pendingMessages.length) {
                return;
            }
            pendingMessages.splice(0).forEach(function (payload) {
                send(payload, true);
            });
        }

        function connect() {
            if (socket && (socket.readyState === window.WebSocket.OPEN || socket.readyState === window.WebSocket.CONNECTING)) {
                return;
            }
            clearReconnectTimer();
            manuallyClosed = false;
            emitStatus(socket ? 'reconnecting' : 'connecting', { url: socketUrl });
            socket = new window.WebSocket(socketUrl);

            socket.onopen = function () {
                emitStatus('connected', { url: socketUrl });
                send({ action: 'subscribe_executions', scope: 'algo-backtest-dashboard' }, true);
                flushPendingMessages();
            };

            socket.onmessage = function (event) {
                emitMessage(safeParseJson(event.data));
            };

            socket.onerror = function () {
                emitStatus('error', { url: socketUrl });
            };

            socket.onclose = function () {
                emitStatus('disconnected', { url: socketUrl });
                socket = null;
                if (!manuallyClosed) {
                    reconnectTimer = window.setTimeout(connect, reconnectDelayMs);
                }
            };
        }

        function send(payload, skipQueue) {
            if (!socket || socket.readyState !== window.WebSocket.OPEN) {
                if (!skipQueue) {
                    pendingMessages.push(payload);
                }
                return false;
            }
            var message = typeof payload === 'string' ? payload : JSON.stringify(payload);
            socket.send(message);
            return true;
        }

        function close() {
            manuallyClosed = true;
            clearReconnectTimer();
            pendingMessages = [];
            if (socket) {
                socket.close();
                socket = null;
            }
        }

        function on(eventName, listener) {
            if (!listeners[eventName] || typeof listener !== 'function') {
                return function () {};
            }
            listeners[eventName].push(listener);
            return function () {
                listeners[eventName] = listeners[eventName].filter(function (item) {
                    return item !== listener;
                });
            };
        }

        return {
            connect: connect,
            close: close,
            send: send,
            on: on,
            getUrl: function () {
                return socketUrl;
            }
        };
    }

    window.AlgoBacktestExecutionSocket = {
        create: createExecutionSocket,
        buildDefaultSocketUrl: buildDefaultSocketUrl
    };
})(window);
