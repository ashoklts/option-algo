/**
 * kite-login.js
 * Automatic Kite Connect login via popup window.
 *
 * Usage:
 *   kiteLogin({ brokerDocId: "mongo_id" })
 *     .then(result => console.log(result.access_token))
 *     .catch(err  => console.error(err));
 *
 * Or with callback:
 *   kiteLogin({ brokerDocId: "mongo_id", onSuccess, onError });
 */

function resolveKiteAlgoApiBaseUrl() {
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

const KITE_BACKEND = resolveKiteAlgoApiBaseUrl().replace(/\/algo.*$/, "");

function kiteLogin({ brokerDocId = "", onSuccess = null, onError = null } = {}) {
  return new Promise(async (resolve, reject) => {

    // 1. Get login URL from backend
    let loginUrl;
    try {
      const res = await fetch(`${KITE_BACKEND}/broker/kite/login-url`);
      const data = await res.json();
      loginUrl = data.login_url;
    } catch (e) {
      const err = "Failed to fetch login URL: " + e.message;
      if (onError) onError(err);
      return reject(err);
    }

    // 2. Append broker_doc_id to redirect URL so backend knows which doc to update
    //    Kite redirect_url is set in Kite console as:
    //    http://localhost:8000/broker/kite/redirect
    //    We pass broker_doc_id as extra param (Kite passes it through to redirect)
    if (brokerDocId) {
      // Kite appends request_token to redirect_url — we encode our extra params
      // by storing them temporarily and reading on redirect
      sessionStorage.setItem("kite_broker_doc_id", brokerDocId);
    }

    // 3. Open popup window
    const popupWidth  = 600;
    const popupHeight = 700;
    const left = Math.round((screen.width  - popupWidth)  / 2);
    const top  = Math.round((screen.height - popupHeight) / 2);

    const popup = window.open(
      loginUrl,
      "kite_login_popup",
      `width=${popupWidth},height=${popupHeight},left=${left},top=${top},resizable=yes,scrollbars=yes`
    );

    if (!popup || popup.closed) {
      const err = "Popup blocked! Please allow popups for this site.";
      if (onError) onError(err);
      return reject(err);
    }

    // 4. Listen for postMessage from redirect page
    function handleMessage(event) {
      if (!event.data || event.data.type !== "KITE_LOGIN") return;

      window.removeEventListener("message", handleMessage);

      const result = event.data;

      if (result.success) {
        if (onSuccess) onSuccess(result);
        resolve(result);
      } else {
        if (onError) onError(result.message);
        reject(result.message);
      }
    }

    window.addEventListener("message", handleMessage);

    // 5. Fallback: if popup closes without postMessage (user closed manually)
    const checkClosed = setInterval(() => {
      if (popup.closed) {
        clearInterval(checkClosed);
        window.removeEventListener("message", handleMessage);
        const err = "Login window closed by user";
        if (onError) onError(err);
        reject(err);
      }
    }, 500);
  });
}
