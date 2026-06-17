"""
broker_gateway.py
─────────────────
Single broker abstraction layer — THE ONE FILE that changes when switching brokers.

Architecture:
  • All market-data and trading operations across the system import from here.
  • Never import kite_* / dhan_* directly from business-logic files.
  • Active broker is read from kite_market_config (enabled=true, broker=kite|dhan).
  • To add a new broker: create <broker>_ticker.py + <broker>_broker_ws.py +
    <broker>_broker.py with the same public function signatures, then wire them
    in the routing sections below.

Current supported brokers: kite (Zerodha), dhan (DhanHQ)
"""

from __future__ import annotations

import threading

# ── Active broker detection ───────────────────────────────────────────────────
# Cached after first call. One broker per server session (restart to switch).

_broker_cache: list[str] = []
_broker_lock  = threading.Lock()


def _active_broker() -> str:
    """Returns 'kite' or 'dhan' based on kite_market_config enabled record."""
    if _broker_cache:
        return _broker_cache[0]
    with _broker_lock:
        if _broker_cache:
            return _broker_cache[0]
        try:
            from features.mongo_data import MongoData  # type: ignore
            _db = MongoData()
            try:
                cfg = _db._db['kite_market_config'].find_one({'enabled': True}) or {}
                name = str(cfg.get('broker') or 'kite').strip().lower()
            finally:
                _db.close()
        except Exception:
            name = 'kite'
        _broker_cache.append(name)
        return name


def reset_broker_cache() -> None:
    """Call if broker config changes at runtime (e.g. switching between kite↔dhan)."""
    with _broker_lock:
        _broker_cache.clear()


# ── Ticker manager proxy ──────────────────────────────────────────────────────
# Delegates attribute/method access to the correct broker ticker at runtime.

class _BrokerTickerProxy:
    """Proxy that routes to kite_ticker or dhan_ticker based on active broker."""

    @property
    def _delegate(self):
        if _active_broker() == 'dhan':
            from features.dhan_ticker import dhan_ticker_manager  # type: ignore
            return dhan_ticker_manager
        from features.kite_ticker import ticker_manager  # type: ignore
        return ticker_manager

    @property
    def ltp_map(self):       return self._delegate.ltp_map

    @property
    def spot_map(self):      return self._delegate.spot_map

    @property
    def status(self):        return self._delegate.status

    @property
    def tick_count(self):    return self._delegate.tick_count

    @property
    def started_at(self):    return self._delegate.started_at

    @property
    def error_msg(self):     return self._delegate.error_msg

    @property
    def subscribed_tokens(self): return self._delegate.subscribed_tokens

    @property
    def _ticker(self):       return self._delegate._ticker

    def get_ltp(self, token):       return self._delegate.get_ltp(token)
    def get_spot(self, underlying): return self._delegate.get_spot(underlying)
    def get_status(self):           return self._delegate.get_status()
    def add_tick_listener(self, listener):    return self._delegate.add_tick_listener(listener)
    def remove_tick_listener(self, listener): return self._delegate.remove_tick_listener(listener)
    def subscribe_tokens(self, ids, exchange='NSE_FNO'):
        return self._delegate.subscribe_tokens(ids, exchange)
    def start(self, db):            return self._delegate.start(db)
    def stop(self):                 return self._delegate.stop()
    def restart(self, db):          return self._delegate.restart(db)


broker_ticker_manager = _BrokerTickerProxy()


# ── Credential / WebSocket functions — broker-routed ─────────────────────────
# All functions have the same signature as kite_broker_ws equivalents.

def get_broker_ltp_map() -> dict[str, float]:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import get_ltp_map  # type: ignore
        return get_ltp_map()
    from features.kite_broker_ws import get_ltp_map  # type: ignore
    return get_ltp_map()


_REST_QUOTE_CACHE: dict[str, tuple[float, dict]] = {}  # token → (epoch, result)
_REST_QUOTE_CACHE_TTL = 3.0  # seconds — avoid 429 rate limit


def get_broker_rest_quotes(
    token_ids: list[str],
    db,
    ws_segments: dict[str, str] | None = None,
) -> dict[str, dict]:
    """
    Return {token_str: {"ltp": float, "oi": int}} using:
      - Dhan WS ltp_map / oi_map where available
      - Dhan REST /marketfeed/quote fallback for missing tokens
      - Handles both NSE_FNO and BSE_FNO (SENSEX/BANKEX)
    Kite: returns {} (caller uses kite_quote_map for LTP+OI).
    """
    if _active_broker() != 'dhan':
        return {}
    from features.dhan_ticker import dhan_ticker_manager as _dtm  # type: ignore

    result: dict[str, dict] = {}
    ws_ltp = _dtm.ltp_map
    ws_oi  = _dtm.oi_map
    ws_ts  = _dtm.ltp_ts_map

    import time as _time
    from datetime import datetime as _dt
    _now_epoch = _time.time()

    # Populate from WS first — skip stale ticks older than 5 minutes
    for t in token_ids:
        raw_ltp = float(ws_ltp.get(t) or 0)
        oi      = int(ws_oi.get(t) or 0)
        ltp     = raw_ltp
        ts_str  = ws_ts.get(t)
        tick_age_sec = None
        if raw_ltp > 0:
            if ts_str:
                try:
                    tick_age_sec = _now_epoch - _dt.fromisoformat(ts_str).timestamp()
                    if tick_age_sec > 300:
                        ltp = 0.0  # stale — refresh via REST
                except Exception:
                    ltp = 0.0  # can't verify freshness → treat as stale
            else:
                ltp = 0.0  # no timestamp → initial subscription price, treat as stale
        print(f'[REST_QUOTES_DEBUG] token={t} ws_ltp={raw_ltp} ws_ts={ts_str} tick_age={tick_age_sec} using_ws_ltp={ltp}')
        if ltp > 0 or oi > 0:
            result[t] = {"ltp": ltp, "oi": oi}

    # REST fallback for tokens missing LTP (BSE_FNO never gets WS ticks after hours)
    missing = [t for t in token_ids if t not in result or result[t]["ltp"] == 0]
    _subscribed = set(_dtm.subscribed_tokens or [])
    _not_subscribed = [t for t in token_ids if t not in _subscribed]
    print(
        f'[REST_QUOTES_DEBUG] missing_tokens={missing}  will_call_rest={bool(missing)}'
        f'  |  requested={list(token_ids)}  subscribed_count={len(_subscribed)}'
        f'  |  not_in_subscribed={_not_subscribed}'
    )
    if not missing:
        return result

    _segs = ws_segments or {}

    # Serve from REST cache to avoid 429 rate limit
    import time as _tc
    _cache_now = _tc.time()
    still_missing = []
    for t in missing:
        cached = _REST_QUOTE_CACHE.get(t)
        if cached and (_cache_now - cached[0]) < _REST_QUOTE_CACHE_TTL:
            result[t] = cached[1]
        else:
            still_missing.append(t)
    missing = still_missing

    if not missing:
        return result

    nse_missing = [t for t in missing if _segs.get(t, "NSE_FNO").upper() != "BSE_FNO"]
    bse_missing = [t for t in missing if _segs.get(t, "").upper() == "BSE_FNO"]

    try:
        cfg = db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
        access_token = str(cfg.get("access_token") or "").strip()
        client_id    = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
        if not access_token:
            print(f'[REST_QUOTES_DEBUG] no access_token in db — REST skipped')
            return result

        def _to_int(tok: str) -> int | None:
            """Strip exchange prefix and convert to int: 'NSE_54808' → 54808."""
            n = tok.split("_", 1)[-1] if "_" in tok else tok
            try:
                return int(n)
            except (ValueError, TypeError):
                return None

        import requests as _req
        req_body: dict = {}
        if nse_missing:
            req_body["NSE_FNO"] = [v for t in nse_missing if t and (v := _to_int(t)) is not None]
        if bse_missing:
            req_body["BSE_FNO"] = [v for t in bse_missing if t and (v := _to_int(t)) is not None]
        if not req_body:
            print(f'[REST_QUOTES_DEBUG] req_body empty (token format issue?) — REST skipped')
            return result

        print(f'[REST_QUOTES_DEBUG] calling Dhan REST req_body={req_body}')
        resp = _req.post(
            "https://api.dhan.co/v2/marketfeed/quote",
            headers={
                "access-token": access_token,
                "client-id": client_id,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=req_body,
            timeout=10,
        )
        print(f'[REST_QUOTES_DEBUG] REST status={resp.status_code} response={resp.text[:500]}')
        # Build reverse map: numeric_str → original prefixed token (e.g. "54808" → "NSE_54808")
        _numeric_to_original: dict[str, str] = {}
        for t in missing:
            n = t.split("_", 1)[-1] if "_" in t else t
            _numeric_to_original.setdefault(n, t)

        if resp.status_code == 200:
            raw = resp.json()
            data = raw.get("data") or raw
            _rest_epoch = _tc.time()
            for exch in ("NSE_FNO", "BSE_FNO"):
                for tok, v in (data.get(exch) or {}).items():
                    if not isinstance(v, dict):
                        continue
                    ltp = float(v.get("last_price") or 0)
                    oi  = int(v.get("oi") or 0)
                    tok_str = str(tok)
                    for _key in (tok_str, _numeric_to_original.get(tok_str, tok_str)):
                        existing = result.get(_key, {"ltp": 0, "oi": 0})
                        entry = {
                            "ltp": ltp if ltp > 0 else existing["ltp"],
                            "oi":  oi  if oi  > 0 else existing["oi"],
                        }
                        result[_key] = entry
                        _REST_QUOTE_CACHE[_key] = (_rest_epoch, entry)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("[BROKER REST QUOTES] %s", exc)
        print(f'[REST_QUOTES_DEBUG] REST exception: {exc}')

    print(f'[REST_QUOTES_DEBUG] final_result={result}')
    return result


def get_broker_oi_map(
    token_ids: list[str],
    db,
    ws_segments: dict[str, str] | None = None,
) -> dict[str, int]:
    """Backward-compat wrapper — returns only OI. Use get_broker_rest_quotes for LTP+OI."""
    quotes = get_broker_rest_quotes(token_ids, db, ws_segments)
    return {t: v["oi"] for t, v in quotes.items() if v["oi"] > 0}



def get_broker_credentials() -> tuple[str, str]:
    """(api_key, access_token) for Kite  |  (client_id, access_token) for Dhan."""
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import get_common_credentials  # type: ignore
        return get_common_credentials()
    from features.kite_broker_ws import get_common_credentials  # type: ignore
    return get_common_credentials()


def get_broker_api_key() -> str:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import get_common_api_key  # type: ignore
        return get_common_api_key()
    from features.kite_broker_ws import get_common_api_key  # type: ignore
    return get_common_api_key()


def broker_is_configured() -> bool:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import is_configured  # type: ignore
        return is_configured()
    from features.kite_broker_ws import is_configured  # type: ignore
    return is_configured()


def load_broker_credentials_from_db(db) -> bool:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import load_credentials_from_db  # type: ignore
        return load_credentials_from_db(db)
    from features.kite_broker_ws import load_credentials_from_db  # type: ignore
    return load_credentials_from_db(db)


def set_broker_credentials(key: str, access_token: str) -> None:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import set_common_credentials  # type: ignore
        set_common_credentials(key, access_token)
    else:
        from features.kite_broker_ws import set_common_credentials  # type: ignore
        set_common_credentials(key, access_token)


def save_broker_access_token(db, access_token: str) -> None:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import save_access_token_to_db  # type: ignore
        save_access_token_to_db(db, access_token)
    else:
        from features.kite_broker_ws import save_access_token_to_db  # type: ignore
        save_access_token_to_db(db, access_token)


def save_broker_credentials_to_db(db, key: str, access_token: str) -> None:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import save_credentials_to_db  # type: ignore
        save_credentials_to_db(db, key, access_token)
    else:
        from features.kite_broker_ws import save_credentials_to_db  # type: ignore
        save_credentials_to_db(db, key, access_token)


def get_broker_ws_login_url() -> str:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import get_login_url  # type: ignore
        return get_login_url()
    from features.kite_broker_ws import get_login_url  # type: ignore
    return get_login_url()


def broker_generate_access_token(request_token: str) -> str:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import generate_access_token  # type: ignore
        return generate_access_token(request_token)
    from features.kite_broker_ws import generate_access_token  # type: ignore
    return generate_access_token(request_token)


def broker_validate_access_token(access_token: str = '') -> bool:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import validate_access_token  # type: ignore
        return validate_access_token(access_token)
    from features.kite_broker_ws import validate_access_token  # type: ignore
    return validate_access_token(access_token)


def broker_extract_instrument_tokens(positions: list[dict]) -> list[int]:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import extract_instrument_tokens  # type: ignore
        return extract_instrument_tokens(positions)
    from features.kite_broker_ws import extract_instrument_tokens  # type: ignore
    return extract_instrument_tokens(positions)


def broker_register_user_tokens(user_id: str, tokens: list) -> bool:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import register_user_tokens  # type: ignore
        return register_user_tokens(user_id, tokens)
    from features.kite_broker_ws import register_user_tokens  # type: ignore
    return register_user_tokens(user_id, tokens)


def broker_unregister_user(user_id: str) -> None:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import unregister_user  # type: ignore
        unregister_user(user_id)
    else:
        from features.kite_broker_ws import unregister_user  # type: ignore
        unregister_user(user_id)


def broker_refresh_user_tokens(user_id: str, new_tokens: list) -> bool:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import refresh_user_tokens  # type: ignore
        return refresh_user_tokens(user_id, new_tokens)
    from features.kite_broker_ws import refresh_user_tokens  # type: ignore
    return refresh_user_tokens(user_id, new_tokens)


def broker_add_tick_listener(listener) -> bool:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import add_tick_listener  # type: ignore
        return add_tick_listener(listener)
    from features.kite_broker_ws import add_tick_listener  # type: ignore
    return add_tick_listener(listener)


def broker_remove_tick_listener(listener) -> None:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import remove_tick_listener  # type: ignore
        remove_tick_listener(listener)
    else:
        from features.kite_broker_ws import remove_tick_listener  # type: ignore
        remove_tick_listener(listener)


def broker_wait_for_tokens_ltp(
    tokens: list,
    timeout_seconds: float = 2.0,
) -> dict[str, float]:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import wait_for_tokens_ltp  # type: ignore
        return wait_for_tokens_ltp(tokens, timeout_seconds)
    from features.kite_broker_ws import wait_for_tokens_ltp  # type: ignore
    return wait_for_tokens_ltp(tokens, timeout_seconds)


def broker_stop_all() -> None:
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import stop_all  # type: ignore
        stop_all()
    else:
        from features.kite_broker_ws import stop_all  # type: ignore
        stop_all()


# ── REST client / OAuth ───────────────────────────────────────────────────────

def get_broker_rest_client_with_token(access_token: str):
    """
    Return a broker REST client pre-configured with a specific access_token.
    Kite: KiteConnect(api_key).set_access_token(token)
    Dhan: dhanhq.dhanhq(client_id, token)
    """
    if _active_broker() == 'dhan':
        try:
            from features.dhan_broker_ws import get_common_api_key  # type: ignore
            import dhanhq  # type: ignore
            client_id = get_common_api_key() or ''
            return dhanhq.dhanhq(client_id, str(access_token or '').strip())
        except Exception:
            return None
    from features.kite_broker import get_kite_instance  # type: ignore
    return get_kite_instance(access_token)


def broker_get_login_url() -> str:
    if _active_broker() == 'dhan':
        return ''
    from features.kite_broker import get_login_url  # type: ignore
    return get_login_url()


def broker_generate_session(request_token: str, api_secret: str = ''):
    if _active_broker() == 'dhan':
        return {}
    from features.kite_broker import generate_session  # type: ignore
    return generate_session(request_token, api_secret)


def save_broker_session(db, session: dict) -> None:
    if _active_broker() == 'dhan':
        return
    from features.kite_broker import save_kite_session  # type: ignore
    save_kite_session(db, session)


def get_stored_broker_access_token(db) -> str:
    if _active_broker() == 'dhan':
        try:
            raw = db._db if hasattr(db, '_db') else db
            cfg = raw['kite_market_config'].find_one({'broker': 'dhan', 'enabled': True}) or {}
            return str(cfg.get('access_token') or '').strip()
        except Exception:
            return ''
    from features.kite_broker import get_stored_access_token  # type: ignore
    return get_stored_access_token(db)


# ── Broker-specific constants ─────────────────────────────────────────────────
# Lazy dicts/objects so the correct values are used regardless of when imported.

_KITE_INDEX_TOKENS: dict[str, int] = {
    'NIFTY': 256265, 'BANKNIFTY': 260105, 'SENSEX': 265,
    'FINNIFTY': 257801, 'MIDCPNIFTY': 288009, 'INDIA_VIX': 264969,
}
_DHAN_INDEX_TOKENS: dict[str, int] = {
    'NIFTY': 13, 'BANKNIFTY': 25, 'SENSEX': 51,
    'FINNIFTY': 27, 'MIDCPNIFTY': 11915, 'INDIA_VIX': 20225,
}


class _LazyBrokerTokenDict(dict):
    """Populates with correct broker tokens on first key access."""
    _loaded: bool = False

    def _load(self) -> None:
        if not self._loaded:
            self._loaded = True
            self.update(_DHAN_INDEX_TOKENS if _active_broker() == 'dhan' else _KITE_INDEX_TOKENS)

    def get(self, key, default=None):  # type: ignore[override]
        self._load(); return super().get(key, default)

    def __getitem__(self, key):
        self._load(); return super().__getitem__(key)

    def __contains__(self, key):  # type: ignore[override]
        self._load(); return super().__contains__(key)

    def __iter__(self):
        self._load(); return super().__iter__()

    def items(self):   self._load(); return super().items()    # type: ignore[override]
    def keys(self):    self._load(); return super().keys()     # type: ignore[override]
    def values(self):  self._load(); return super().values()   # type: ignore[override]


class _LazyVIXToken:
    """Scalar proxy for BROKER_VIX_TOKEN — resolves to correct value on first use."""
    def _val(self) -> int:
        return 20225 if _active_broker() == 'dhan' else 264969
    def __int__(self):          return self._val()
    def __index__(self):        return self._val()
    def __str__(self):          return str(self._val())
    def __repr__(self):         return repr(self._val())
    def __eq__(self, other):    return self._val() == other  # type: ignore[override]
    def __hash__(self):         return hash(self._val())
    def __format__(self, spec): return format(self._val(), spec)


BROKER_INDEX_TOKENS: dict = _LazyBrokerTokenDict()
BROKER_VIX_TOKEN         = _LazyVIXToken()
BROKER_CONFIG_COLLECTION: str = 'kite_market_config'


def get_broker_index_token(underlying: str) -> int:
    return BROKER_INDEX_TOKENS.get(str(underlying or '').strip().upper(), 0)


# ── Market data: instruments, expiries, contracts, quotes ─────────────────────
# Lazy imports prevent circular dependency with spot_atm_utils
# (spot_atm_utils imports broker_gateway for credentials + LTP).

def load_broker_instruments(force: bool = False) -> dict:
    if _active_broker() == 'dhan':
        from features.spot_atm_utils import _load_dhan_instruments  # type: ignore
        return _load_dhan_instruments(force=force)
    from features.spot_atm_utils import _load_kite_instruments  # type: ignore
    return _load_kite_instruments(force=force)


def get_broker_expiries(underlying: str, from_date: str, *, force_refresh: bool = False) -> list[str]:
    if _active_broker() == 'dhan':
        from features.spot_atm_utils import get_dhan_expiries  # type: ignore
        return get_dhan_expiries(underlying, from_date, force_refresh=force_refresh)
    from features.spot_atm_utils import get_kite_expiries  # type: ignore
    return get_kite_expiries(underlying, from_date, force_refresh=force_refresh)


def list_broker_option_contracts(underlying: str, expiry: str, *, force_refresh: bool = False) -> list[dict]:
    if _active_broker() == 'dhan':
        from features.spot_atm_utils import list_dhan_option_contracts  # type: ignore
        return list_dhan_option_contracts(underlying, expiry, force_refresh=force_refresh)
    from features.spot_atm_utils import list_kite_option_contracts  # type: ignore
    return list_kite_option_contracts(underlying, expiry, force_refresh=force_refresh)


def get_broker_chain_doc(underlying: str, expiry: str, strike: float, option_type: str) -> dict:
    if _active_broker() == 'dhan':
        from features.spot_atm_utils import get_dhan_chain_doc  # type: ignore
        return get_dhan_chain_doc(underlying, expiry, strike, option_type)
    from features.spot_atm_utils import get_kite_chain_doc  # type: ignore
    return get_kite_chain_doc(underlying, expiry, strike, option_type)


def fetch_broker_quotes_for_expiry(underlying: str, expiry: str) -> dict[str, float]:
    if _active_broker() == 'dhan':
        from features.spot_atm_utils import fetch_dhan_quotes_for_expiry  # type: ignore
        return fetch_dhan_quotes_for_expiry(underlying, expiry)
    from features.spot_atm_utils import fetch_kite_quotes_for_expiry  # type: ignore
    return fetch_kite_quotes_for_expiry(underlying, expiry)


def get_broker_rest_client(db=None):
    """
    Return a broker REST client configured from credentials.
    Kite: KiteConnect  |  Dhan: dhanhq instance
    """
    try:
        broker = _active_broker()
        if broker == 'dhan':
            from features.dhan_broker_ws import (  # type: ignore
                get_common_credentials, load_credentials_from_db,
            )
            if db is not None:
                load_credentials_from_db(db)
            client_id, access_token = get_common_credentials()
            if not client_id or not access_token:
                return None
            import dhanhq  # type: ignore
            return dhanhq.dhanhq(client_id, access_token)
        else:
            if db is not None:
                from features.kite_delta_chain import _get_kite_credentials  # type: ignore
                api_key, access_token = _get_kite_credentials(db)
            else:
                api_key, access_token = get_broker_credentials()
            if not api_key or not access_token:
                return None
            from kiteconnect import KiteConnect  # type: ignore
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            return kite
    except Exception:
        return None


# ── Black-Scholes helpers (broker-agnostic math) ──────────────────────────────

def get_bs_helpers():
    """
    Returns (_calc_iv, _calc_greeks, _time_to_expiry,
             _RISK_FREE_RATE, _DIVIDEND_YIELDS, _DEFAULT_DIVIDEND_YIELD).
    Math is broker-agnostic — only data inputs differ per broker.
    """
    from features.kite_delta_chain import (  # type: ignore
        _calc_iv, _calc_greeks, _time_to_expiry,
        _RISK_FREE_RATE, _DIVIDEND_YIELDS, _DEFAULT_DIVIDEND_YIELD,
    )
    return (
        _calc_iv, _calc_greeks, _time_to_expiry,
        _RISK_FREE_RATE, _DIVIDEND_YIELDS, _DEFAULT_DIVIDEND_YIELD,
    )


def get_broker_credentials_from_db(db=None) -> tuple[str, str]:
    """
    (api_key/client_id, access_token) from in-memory cache or DB.
    """
    if _active_broker() == 'dhan':
        from features.dhan_broker_ws import (  # type: ignore
            get_common_credentials, load_credentials_from_db,
        )
        if db is not None:
            load_credentials_from_db(db)
        return get_common_credentials()
    from features.kite_delta_chain import _get_kite_credentials  # type: ignore
    return _get_kite_credentials(db)
