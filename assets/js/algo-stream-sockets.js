(function (window) {
    'use strict';

    // Cross-tab relay via BroadcastChannel.
    // Any channel listed here will relay its received messages to all other tabs.
    var RELAY_CHANNELS = { 'execute-orders': true };
    var BC_NAME = 'algo_execute_orders';

    var broadcastChannel = null;
    var broadcastListeners = [];  // { channel, fn }

    function getBroadcastChannel() {
        if (broadcastChannel) return broadcastChannel;
        if (typeof BroadcastChannel === 'undefined') return null;
        try {
            broadcastChannel = new BroadcastChannel(BC_NAME);
            broadcastChannel.onmessage = function (event) {
                var msg = event && event.data;
                if (!msg || !msg._bc_channel || msg._bc_relayed) return;
                broadcastListeners.forEach(function (entry) {
                    if (entry.channel === msg._bc_channel) {
                        entry.fn(msg._bc_payload);
                    }
                });
            };
        } catch (e) {
            broadcastChannel = null;
        }
        return broadcastChannel;
    }

    function relayToBroadcast(channel, payload) {
        var bc = getBroadcastChannel();
        if (!bc || !payload || typeof payload !== 'object') return;
        try {
            bc.postMessage({ _bc_channel: channel, _bc_relayed: true, _bc_payload: payload });
        } catch (e) { /* ignore */ }
    }

    function onBroadcast(channel, fn) {
        getBroadcastChannel(); // ensure initialized
        broadcastListeners.push({ channel: channel, fn: fn });
    }

    function buildSocketBaseUrl() {
        var protocol = window.location.protocol || '';
        if (protocol === 'file:') {
            return 'ws://localhost:8000/algo/ws';
        }

        var origin = window.location.origin || 'http://localhost:8000';
        var socketOrigin = origin.replace(/^http/i, 'ws');
        if (!/^wss?:\/\//i.test(socketOrigin)) {
            socketOrigin = 'ws://localhost:8000';
        }
        return socketOrigin.replace(/\/$/, '') + '/algo/ws';
    }

    function safeParseJson(value) {
        try {
            return JSON.parse(value);
        } catch (error) {
            return value;
        }
    }

    function createSocketChannel(options) {
        var config = options || {};
        var reconnectDelayMs = typeof config.reconnectDelayMs === 'number' ? config.reconnectDelayMs : 3000;
        var channel = String(config.channel || '').replace(/^\/+/, '');
        var socketUrl = config.url || (buildSocketBaseUrl() + '/' + channel);
        var subscribePayload = config.subscribePayload || null;
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
            emitStatus(socket ? 'reconnecting' : 'connecting', { url: socketUrl, channel: channel });
            socket = new window.WebSocket(socketUrl);

            socket.onopen = function () {
                emitStatus('connected', { url: socketUrl, channel: channel });
                if (subscribePayload) {
                    send(subscribePayload, true);
                }
                flushPendingMessages();
            };

            socket.onmessage = function (event) {
                var parsed = safeParseJson(event.data);
                emitMessage(parsed);
                // Relay to all other tabs that have this channel open
                if (RELAY_CHANNELS[channel] && parsed && typeof parsed === 'object') {
                    relayToBroadcast(channel, parsed);
                }
            };

            socket.onerror = function () {
                emitStatus('error', { url: socketUrl, channel: channel });
            };

            socket.onclose = function () {
                emitStatus('disconnected', { url: socketUrl, channel: channel });
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
            socket.send(typeof payload === 'string' ? payload : JSON.stringify(payload));
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
            channel: channel,
            connect: connect,
            close: close,
            send: send,
            on: on,
            getUrl: function () {
                return socketUrl;
            }
        };
    }

    window.AlgoStreamSockets = {
        createChannel: createSocketChannel,
        buildSocketBaseUrl: buildSocketBaseUrl,
        onBroadcast: onBroadcast
    };
})(window);
