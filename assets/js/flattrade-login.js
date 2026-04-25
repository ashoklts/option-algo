/**
 * flattrade-login.js
 * FlatTrade Connect login via popup window.
 *
 * Usage:
 *   flattradeLogin({ brokerDocId: "mongo_id" })
 *     .then(result => console.log(result.access_token))
 *     .catch(err  => console.error(err));
 *
 * Or with callbacks:
 *   flattradeLogin({ brokerDocId: "mongo_id", onSuccess, onError });
 */

function resolveFlatTradeAlgoApiBaseUrl() {
  if (typeof window.getBackendUrl === "function") return window.getBackendUrl();
  if (window.APP_ALGO_API_BASE_URL) return window.APP_ALGO_API_BASE_URL;
  if (window.APP_CONFIG && window.APP_CONFIG.algoApiBaseUrl) return window.APP_CONFIG.algoApiBaseUrl;
  var liveFlag = window.APP_ENV_LIVE;
  var hasLiveFlag = typeof liveFlag !== "undefined" && liveFlag !== null && String(liveFlag).trim() !== "";
  var isLive = hasLiveFlag
    ? ["1", "true", "yes", "live", "production", "prod"].indexOf(String(liveFlag).trim().toLowerCase()) !== -1
    : ["finedgealgo.com", "www.finedgealgo.com"].indexOf((window.location && window.location.hostname || "").toLowerCase()) !== -1;
  return isLive
    ? (window.APP_LIVE_ALGO_API_BASE_URL || window.APP_LOCAL_ALGO_API_BASE_URL || "")
    : (window.APP_LOCAL_ALGO_API_BASE_URL || "");
}

const FLATTRADE_BACKEND = (typeof window.getBackendOriginUrl === "function"
  ? window.getBackendOriginUrl()
  : resolveFlatTradeAlgoApiBaseUrl().replace(/\/algo.*$/, ""));

function flattradeLogin({ brokerDocId = "", onSuccess = null, onError = null } = {}) {
  return new Promise(function (resolve, reject) {

    // 1. Build login URL — backend handles redirect to FlatTrade auth page
    var loginUrl = FLATTRADE_BACKEND + "/broker/flattrade/login"
      + (brokerDocId ? "?broker_doc_id=" + encodeURIComponent(brokerDocId) : "");

    // 2. Open popup window
    var popupWidth  = 600;
    var popupHeight = 700;
    var left = Math.round((screen.width  - popupWidth)  / 2);
    var top  = Math.round((screen.height - popupHeight) / 2);

    var popup = window.open(
      loginUrl,
      "flattrade_login_popup",
      "width=" + popupWidth + ",height=" + popupHeight
        + ",left=" + left + ",top=" + top
        + ",resizable=yes,scrollbars=yes"
    );

    if (!popup || popup.closed) {
      var err = "Popup blocked! Please allow popups for this site.";
      if (onError) onError(err);
      return reject(err);
    }

    // 3. Listen for postMessage from redirect page
    function handleMessage(event) {
      if (!event.data || event.data.type !== "FLATTRADE_LOGIN") return;

      window.removeEventListener("message", handleMessage);
      clearInterval(checkClosed);

      var result = event.data;
      if (result.success) {
        if (onSuccess) onSuccess(result);
        resolve(result);
      } else {
        if (onError) onError(result.message);
        reject(result.message);
      }
    }

    window.addEventListener("message", handleMessage);

    // 4. Fallback: popup closed without postMessage
    var checkClosed = setInterval(function () {
      if (popup.closed) {
        clearInterval(checkClosed);
        window.removeEventListener("message", handleMessage);
        var err = "Login window closed by user";
        if (onError) onError(err);
        reject(err);
      }
    }, 500);
  });
}
