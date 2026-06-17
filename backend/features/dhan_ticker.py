"""
dhan_ticker.py
──────────────
Dhan HQ Live Market Feed WebSocket integration.

Auth: URL query params (NOT headers, NOT message)
  wss://api-feed.dhan.co?version=2&token={access_token}&clientId={user_id}&authType=2

Subscribe message (JSON, sent after connect):
  {"RequestCode": 15, "InstrumentCount": N, "InstrumentList": [...]}
  RequestCode 15 = Ticker (LTP only)  ← we use this
  RequestCode 17 = Quote (OHLC + LTP)
  RequestCode 21 = Full (depth + OI)

Binary response — LITTLE ENDIAN:
  Header (8 bytes):
    Byte 0      : feed_response_code  (uint8)
    Bytes 1-2   : message_length      (int16, LE)
    Byte 3      : exchange_segment    (int8)
    Bytes 4-7   : security_id         (int32, LE)
  Ticker payload (8 bytes, ResponseCode=2):
    Bytes 8-11  : LTP                 (float32, LE)
    Bytes 12-15 : last_trade_time     (int32,   LE)

Exchange segments:
  IDX_I = "IDX_I"   (0)   ← Indices
  NSE_FNO = "NSE_FNO" (2) ← F&O options
"""

from __future__ import annotations

import json
import struct
import threading
import logging
from datetime import datetime
from urllib.parse import urlencode

from features.live_tick_dispatcher import live_tick_dispatcher

logger = logging.getLogger(__name__)

ACTIVATION_MODE = "live"

DHAN_WS_BASE = "wss://api-feed.dhan.co"

# Subscribe request codes
REQ_TICKER_SUB   = 15   # LTP only
REQ_FULL_SUB     = 21   # Full: LTP + OI + depth (used for FNO options)
REQ_TICKER_UNSUB = 16

# Feed response codes (Dhan v2)
RESP_TICKER     = 2   # LTP + last_trade_time
RESP_QUOTE      = 4   # OHLC + LTP + volume etc.
RESP_OI         = 5   # OI Data — sent alongside subscriptions as separate packet
RESP_PREV_CLOSE = 6   # Previous close + previous day OI
RESP_FULL       = 8   # Full: LTP + LTQ + LTT + ATP + vol + OI + depth
RESP_DISCONNECT = 50

# Binary structs — LITTLE ENDIAN, all offsets from packet byte 0
# Header (8 bytes): uint8 + int16 + int8 + int32
_HDR = struct.Struct('<BhbI')

# LTP: offset 8, float32 — same position in Ticker, Quote, Full packets
_TICKER_LTP = struct.Struct('<f')

# Ticker only: last_trade_time at offset 12, int32 (uint)
_TICKER_LTT = struct.Struct('<I')

# OI Data Packet (RESP_OI=5): OI at offset 8, int32 (right after header)
_OI_PKT = struct.Struct('<I')       # offset 8, size 4

# Full Packet (RESP_FULL=8) — Dhan v2 official layout:
#   offset 8:  LTP           float32  (4)
#   offset 12: LTQ           int16    (2)  ← 2 bytes, not 4!
#   offset 14: LTT           int32    (4)
#   offset 18: ATP           float32  (4)
#   offset 22: Volume        int32    (4)
#   offset 26: Sell Qty      int32    (4)
#   offset 30: Buy Qty       int32    (4)
#   offset 34: OI            int32    (4)  ← bytes 35-38 (1-indexed)
#   offset 38: OI Day High   int32    (4)
#   offset 42: OI Day Low    int32    (4)
#   offset 46: Open          float32  (4)
#   offset 50: Close         float32  (4)
#   offset 54: High          float32  (4)
#   offset 58: Low           float32  (4)
#   offset 62: Market Depth  100 bytes
_FULL_OI = struct.Struct('<I')       # offset 34, size 4 (OI in Full packet, uint32)

# ── Fallback hardcoded tokens (used only if DB is unreachable) ────────────────
_DHAN_SPOT_FALLBACK: dict[str, str] = {
    "13":    "NIFTY",
    "25":    "BANKNIFTY",
    "27":    "FINNIFTY",
    "51":    "SENSEX",
    "11915": "MIDCPNIFTY",
}
_DHAN_VIX_FALLBACK = "20225"

# Backward compat module-level names
DHAN_SPOT_TOKENS           = _DHAN_SPOT_FALLBACK
DHAN_INDIA_VIX_SECURITY_ID = _DHAN_VIX_FALLBACK
DHAN_SPOT_EXCHANGE          = "IDX_I"
DHAN_FO_EXCHANGE            = "NSE_FNO"
DHAN_MAX_TOKENS             = 5000


def _load_dhan_spot_tokens(db) -> tuple[dict[str, str], str]:
    """Load Dhan spot tokens from market_feed_tokens collection (broker=dhan)."""
    try:
        from features.market_feed_tokens import ensure_seeded, get_spot_tokens, get_vix_token
        ensure_seeded(db)
        spot    = get_spot_tokens(db, "dhan")
        vix_tok = get_vix_token(db, "dhan")
        return spot or _DHAN_SPOT_FALLBACK, vix_tok or _DHAN_VIX_FALLBACK
    except Exception as exc:
        logger.warning("[dhan_ticker] token load fallback: %s", exc)
        return _DHAN_SPOT_FALLBACK, _DHAN_VIX_FALLBACK


class _DhanCompatTicker:
    """
    KiteTicker-compatible shim so existing code that calls
    ticker_manager._ticker.subscribe() / set_mode() works unchanged.
    Translates Kite-style integer tokens → Dhan string security IDs via
    active_option_tokens (broker=dhan), then delegates to dhan_ticker_manager.
    """
    MODE_LTP = "ltp"

    def __init__(self, manager: "_DhanTickerManager") -> None:
        self._mgr = manager

    def subscribe(self, tokens: list) -> None:
        str_ids = [str(t) for t in tokens if t]
        if str_ids:
            self._mgr.subscribe_tokens(str_ids, exchange=DHAN_FO_EXCHANGE)

    def set_mode(self, mode, tokens: list) -> None:
        pass   # Dhan has no mode concept — all subscriptions are LTP

    def unsubscribe(self, tokens: list) -> None:
        pass   # not implemented for Dhan yet


class _DhanTickerManager:
    def __init__(self):
        self._ws              = None
        self._lock            = threading.Lock()
        self._stopped         = False
        self._listeners: list = []

        self.ltp_map:         dict[str, float] = {}
        self.ltp_ts_map:      dict[str, str]   = {}   # token → ISO timestamp of last tick received
        self.ltp_trade_ts_map: dict[str, int]  = {}   # token → Unix epoch of last actual trade
        self.oi_map:          dict[str, int]   = {}   # token → open interest (from Full packets)
        self.spot_map:        dict[str, float] = {}
        self.status:          str  = "stopped"
        self.error_msg:       str  = ""
        self.started_at:      str  = ""
        self.tick_count:      int  = 0
        self.subscribed_tokens: set[str] = set()
        self.token_labels: dict[str, str] = {}
        self._active_spot_tokens: dict[str, str] = dict(_DHAN_SPOT_FALLBACK)
        self._active_vix_token: str = _DHAN_VIX_FALLBACK

        # Compatibility shim so code that does ticker_manager._ticker.subscribe() works
        self._ticker = _DhanCompatTicker(self)

    # ── public ────────────────────────────────────────────────────────────────

    def _validate_token(self, access_token: str) -> dict:
        """Call Dhan profile API to validate token and get user info."""
        try:
            import requests as _req
            resp = _req.get(
                "https://api.dhan.co/v2/profile",
                headers={"access-token": access_token, "Content-Type": "application/json"},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                print(
                    f"[DHAN TICKER] Token valid ✓ "
                    f"client_id={data.get('dhanClientId')} "
                    f"token_validity={data.get('tokenValidity')} "
                    f"active_segment={data.get('activeSegment')} "
                    f"data_plan={data.get('dataPlan')} "
                    f"data_validity={data.get('dataValidity')}"
                )
                data_plan = str(data.get("dataPlan") or "").strip()
                if data_plan.lower() in ("deactive", "inactive", "na", ""):
                    print(
                        "[DHAN TICKER] WARNING: dataPlan is inactive — "
                        "Live Market Feed WebSocket requires an active Dhan Data Plan. "
                        "Subscribe at web.dhan.co → Data APIs"
                    )
                return {"ok": True, "profile": data}
            else:
                return {"ok": False, "message": f"Profile API {resp.status_code}: {resp.text[:200]}"}
        except Exception as exc:
            return {"ok": False, "message": f"Profile API error: {exc}"}

    def start(self, db) -> dict:
        with self._lock:
            if self.status == "running":
                return {"ok": False, "message": "Already running"}

            cfg = db["kite_market_config"].find_one({"broker": "dhan", "enabled": True})
            if not cfg:
                self.status    = "error"
                self.error_msg = "No enabled dhan config in kite_market_config"
                print("[DHAN TICKER START FAIL] reason=no_enabled_dhan_config")
                return {"ok": False, "message": self.error_msg}

            user_id      = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
            access_token = str(cfg.get("access_token") or "").strip()

            if not user_id or not access_token:
                self.status    = "error"
                self.error_msg = "user_id or access_token missing in kite_market_config (broker=dhan)"
                print("[DHAN TICKER START FAIL] reason=missing_credentials "
                      f"has_user_id={bool(user_id)} has_token={bool(access_token)}")
                return {"ok": False, "message": self.error_msg}

            # Validate token + print user info
            validation = self._validate_token(access_token)
            if not validation["ok"]:
                self.status    = "error"
                self.error_msg = validation["message"]
                print(f"[DHAN TICKER START FAIL] token_invalid: {self.error_msg}")
                return {"ok": False, "message": self.error_msg}

            # Load spot tokens from DB
            active_spot_tokens, active_vix_token = _load_dhan_spot_tokens(db)

            self.ltp_map            = {}
            self.ltp_ts_map         = {}
            self.ltp_trade_ts_map   = {}
            self.oi_map             = {}
            self.spot_map           = {}
            self.tick_count    = 0
            self._active_spot_tokens = dict(active_spot_tokens)
            self._active_vix_token   = active_vix_token
            # Start with spot + VIX only. Open-position sync after WS connect
            # adds active strategy option tokens on demand.
            self.subscribed_tokens = set(active_spot_tokens.keys()) | {active_vix_token}
            self.error_msg     = ""
            self._stopped      = False
            self.status        = "connecting"
            self.started_at    = datetime.now().isoformat()

            print(
                f"[DHAN TICKER START] user_id={user_id} "
                f"spot={len(active_spot_tokens)} options=0"
            )

            threading.Thread(
                target=self._run_ws,
                args=(user_id, access_token, active_spot_tokens, active_vix_token, [], []),
                daemon=True,
                name="dhan_ticker_ws",
            ).start()

        return {"ok": True, "message": "Dhan ticker starting"}

    def stop(self) -> dict:
        with self._lock:
            self._stopped = True
            if self._ws:
                try:
                    self._ws.close()
                except Exception:
                    pass
                self._ws = None
            self.status = "stopped"
        print("[DHAN TICKER STOP]")
        return {"ok": True, "message": "stopped"}

    def restart(self, db) -> dict:
        self.stop()
        return self.start(db)

    def get_ltp(self, token: str) -> float | None:
        t = str(token or "")
        val = self.ltp_map.get(t)
        if val is not None:
            return val
        # Normalize exchange-prefixed token: "NSE_54808" → "54808"
        if "_" in t:
            numeric_t = t.split("_", 1)[-1]
            val = self.ltp_map.get(numeric_t)
            if val is not None:
                return val
        return None

    def get_ltp_with_ts(self, token: str) -> tuple[float | None, str | None]:
        """Return (ltp, received_at_ts) so callers can detect stale data."""
        t = str(token or "")
        return self.ltp_map.get(t), self.ltp_ts_map.get(t)

    def is_ltp_stale(self, token: str, max_age_seconds: int = 300) -> bool:
        """Return True if the last actual trade for this token is older than max_age_seconds."""
        import time as _time
        t = str(token or "")
        ltt = self.ltp_trade_ts_map.get(t)
        if not ltt:
            return True  # no trade time info = assume stale
        return (_time.time() - ltt) > max_age_seconds

    def get_spot(self, underlying: str) -> float | None:
        return self.spot_map.get(str(underlying or "").upper())

    def register_option_token(self, token: str, label: str = "") -> None:
        normalized = str(token or "").strip()
        if not normalized:
            return
        self.subscribed_tokens.add(normalized)
        if str(label or "").strip():
            self.token_labels[normalized] = str(label).strip()

    def add_tick_listener(self, listener) -> None:
        with self._lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def remove_tick_listener(self, listener) -> None:
        with self._lock:
            self._listeners = [l for l in self._listeners if l is not listener]

    def get_status(self) -> dict:
        return {
            "status":     self.status,
            "tick_count": self.tick_count,
            "ltp_count":  len(self.ltp_map),
            "spot_map":   dict(self.spot_map),
            "started_at": self.started_at,
            "error":      self.error_msg,
            "broker":     "dhan",
        }

    def add_listener(self, listener) -> None:
        with self._lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def subscribe_tokens(self, security_ids: list[str], exchange: str = "NSE_FNO") -> None:
        new_ids = [sid for sid in security_ids if sid not in self.subscribed_tokens]
        if not new_ids or not self._ws:
            return
        self._send_subscribe(new_ids, exchange)
        self.subscribed_tokens.update(new_ids)

    # ── internal ──────────────────────────────────────────────────────────────

    def _send_subscribe(self, security_ids: list[str], exchange: str = "NSE_FNO", request_code: int = REQ_FULL_SUB) -> None:
        if not self._ws:
            return
        # Max 100 per message
        for i in range(0, len(security_ids), 100):
            batch = security_ids[i:i + 100]
            msg = json.dumps({
                "RequestCode":     request_code,
                "InstrumentCount": len(batch),
                "InstrumentList":  [
                    {"ExchangeSegment": exchange, "SecurityId": sid}
                    for sid in batch
                ],
            })
            try:
                self._ws.send(msg)
            except Exception as exc:
                logger.warning("[DHAN TICKER] subscribe send error: %s", exc)

    def _run_ws(
        self,
        user_id: str,
        access_token: str,
        active_spot_tokens: dict[str, str],
        active_vix_token: str,
        nse_option_ids: list[str],
        bse_option_ids: list[str],
    ) -> None:
        try:
            import websocket
            import ssl
            import time
        except ImportError:
            self.status    = "error"
            self.error_msg = "websocket-client not installed — pip install websocket-client"
            logger.error("[DHAN TICKER] %s", self.error_msg)
            return

        params = urlencode({
            "version":  "2",
            "token":    access_token,
            "clientId": user_id,
            "authType": "2",
        })
        ws_url = f"{DHAN_WS_BASE}?{params}"

        def _subscribe_all(ws):
            spot_ids = list(active_spot_tokens.keys()) + [active_vix_token]
            spot_msg = json.dumps({
                "RequestCode":     REQ_TICKER_SUB,
                "InstrumentCount": len(spot_ids),
                "InstrumentList":  [
                    {"ExchangeSegment": DHAN_SPOT_EXCHANGE, "SecurityId": sid}
                    for sid in spot_ids
                ],
            })
            ws.send(spot_msg)
            print(f"[DHAN TICKER] subscribed {len(spot_ids)} spot+vix indices from DB")

            if nse_option_ids:
                self._send_subscribe(nse_option_ids, "NSE_FNO")
                print(f"[DHAN TICKER] subscribed {len(nse_option_ids)} NSE_FNO option tokens")
            if bse_option_ids:
                self._send_subscribe(bse_option_ids, "BSE_FNO")
                print(f"[DHAN TICKER] subscribed {len(bse_option_ids)} BSE_FNO option tokens (SENSEX/BANKEX)")

        def _on_open(ws):
            self._ws    = ws
            self.status = "running"
            print(f"[DHAN TICKER] connected user_id={user_id}")
            _subscribe_all(ws)
            try:
                from features.live_event import sync_live_open_position_subscriptions
                from features.fast_forward_event import sync_fast_forward_open_position_subscriptions
                trade_date = datetime.now().strftime("%Y-%m-%d")
                sync_live_open_position_subscriptions(trade_date)
                sync_fast_forward_open_position_subscriptions(trade_date)
            except Exception as exc:
                logger.warning("[DHAN TICKER] open position sync error: %s", exc)

        def _on_message(ws, message):
            if self._stopped:
                return
            if isinstance(message, bytes):
                self._handle_binary(message)
            elif isinstance(message, str):
                try:
                    data = json.loads(message)
                    print(f"[DHAN TICKER] text msg: {data}")
                except Exception:
                    pass

        def _on_error(ws, error):
            self.error_msg = str(error)
            logger.error("[DHAN TICKER] ws error: %s", error)

        def _on_close(ws, close_status_code, close_msg):
            print(f"[DHAN TICKER] closed status={close_status_code} msg={close_msg}")
            if not self._stopped:
                self.status = "reconnecting"

        def _on_ping(ws, message):
            # Explicitly respond to server ping with pong
            try:
                ws.send(message, websocket.ABNF.OPCODE_PONG)
            except Exception:
                pass

        # ── Auto-reconnect loop ────────────────────────────────────────────
        retry_delay = 5
        while not self._stopped:
            try:
                ws_app = websocket.WebSocketApp(
                    ws_url,
                    on_open=_on_open,
                    on_message=_on_message,
                    on_error=_on_error,
                    on_close=_on_close,
                    on_ping=_on_ping,
                )
                ws_app.run_forever(
                    ping_interval=0,           # disable client-side pings; Dhan server pings us
                    sslopt={"cert_reqs": ssl.CERT_NONE},
                    reconnect=0,               # we handle reconnect ourselves
                )
            except Exception as exc:
                logger.error("[DHAN TICKER] run_forever error: %s", exc)

            if self._stopped:
                break

            print(f"[DHAN TICKER] disconnected — reconnecting in {retry_delay}s...")
            self.status = "reconnecting"
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)   # exponential backoff, max 60s

        self.status = "stopped"
        print("[DHAN TICKER] loop exited")

    def _handle_binary(self, data: bytes) -> None:
        """
        Parse Dhan binary feed packets (LITTLE ENDIAN).

        Header (8 bytes):
          byte 0   : feed_response_code (uint8)
          bytes 1-2: message_length     (int16, LE)
          byte 3   : exchange_segment   (int8)
          bytes 4-7: security_id        (int32, LE)

        Ticker payload (ResponseCode=2, bytes 8-15):
          bytes 8-11 : LTP              (float32, LE)
          bytes 12-15: last_trade_time  (int32, LE)
        """
        now_dt      = datetime.now()
        now_ts      = now_dt.strftime("%Y-%m-%dT%H:%M:%S")
        trade_date  = now_ts[:10]
        now_minute  = now_ts[:16]
        listen_time = now_ts[11:16]

        spot_ticks_received: list[tuple[str, float, str]] = []
        changed: dict[str, float] = {}

        _fno_count_in_msg = 0
        _idx_count_in_msg = 0
        _full_count_in_msg = 0

        # Exchange segment constants (from Dhan binary protocol)
        # 0 = IDX_I (indices), 2 = NSE_FNO, 3 = BSE_FNO
        _EXCH_IDX     = 0
        _EXCH_NSE_FNO = 2
        _EXCH_BSE_FNO = 3

        offset = 0
        while offset + _HDR.size <= len(data):
            try:
                feed_code, msg_len, _exch_seg, security_id = _HDR.unpack_from(data, offset)
            except struct.error:
                break

            # Advance by packet size (header + payload)
            pkt_size = _HDR.size + max(0, msg_len)
            if pkt_size < _HDR.size:
                pkt_size = _HDR.size
            offset += pkt_size

            if feed_code == RESP_DISCONNECT:
                print("[DHAN TICKER] feed disconnect packet received")
                continue

            # ── OI Data Packet (Code 5) — sent by Dhan alongside subscriptions ──
            # Header + int32 OI at offset 8. No LTP in this packet.
            if feed_code == RESP_OI:
                pkt_base = offset - pkt_size
                oi_byte = pkt_base + _HDR.size
                if oi_byte + 4 <= len(data):
                    try:
                        oi_val = _OI_PKT.unpack_from(data, oi_byte)[0]
                        if oi_val > 0:
                            self.oi_map[str(security_id)] = oi_val
                    except Exception:
                        pass
                continue

            if feed_code not in (RESP_TICKER, RESP_QUOTE, RESP_FULL, RESP_PREV_CLOSE):
                continue

            # Count by exchange segment and feed type for diagnostics
            if _exch_seg == _EXCH_IDX:
                _idx_count_in_msg += 1
            elif _exch_seg == _EXCH_NSE_FNO:
                _fno_count_in_msg += 1
            if feed_code == RESP_FULL:
                _full_count_in_msg += 1

            # LTP is at bytes 8-11 (first 4 bytes after header)
            ltp_offset = offset - pkt_size + _HDR.size
            if ltp_offset + 4 > len(data):
                continue

            try:
                ltp_val = _TICKER_LTP.unpack_from(data, ltp_offset)[0]
            except struct.error:
                continue

            if not (ltp_val > 0):
                continue

            sid_str = str(security_id)
            self.ltp_map[sid_str]    = ltp_val
            self.ltp_ts_map[sid_str] = now_ts
            changed[sid_str]         = ltp_val

            # Also store exchange-prefixed key ("NSE_54808", "BSE_12345") so code
            # that stores tokens with the exchange prefix can still find the LTP.
            if _exch_seg == _EXCH_NSE_FNO:
                _pfx = "NSE_" + sid_str
                self.ltp_map[_pfx] = ltp_val
                changed[_pfx]      = ltp_val
            elif _exch_seg == _EXCH_BSE_FNO:
                _pfx = "BSE_" + sid_str
                self.ltp_map[_pfx] = ltp_val
                changed[_pfx]      = ltp_val

            # Ticker: last_trade_time at ltp_offset+4 (int32)
            ltt_offset = ltp_offset + 4
            if feed_code == RESP_TICKER and ltt_offset + 4 <= len(data):
                try:
                    ltt_val = _TICKER_LTT.unpack_from(data, ltt_offset)[0]
                    if ltt_val > 0:
                        self.ltp_trade_ts_map[sid_str] = ltt_val
                except struct.error:
                    pass

            # Full packet: OI at offset 34 (ltp_offset+26), int32
            # Layout: LTP(4) + LTQ(2) + LTT(4) + ATP(4) + Vol(4) + Sell(4) + Buy(4) + OI(4)
            #         = 4+2+4+4+4+4+4 = 26 bytes before OI
            if feed_code == RESP_FULL:
                oi_offset = ltp_offset + 26
                if oi_offset + 4 <= len(data):
                    try:
                        oi_val = _FULL_OI.unpack_from(data, oi_offset)[0]
                        if oi_val > 0:
                            self.oi_map[sid_str] = oi_val
                    except Exception:
                        pass

            if sid_str in self._active_spot_tokens:
                underlying = self._active_spot_tokens[sid_str]
                self.spot_map[underlying] = ltp_val
                spot_ticks_received.append((underlying, ltp_val, now_ts))

        # Log segment + feed-type breakdown to file
        if self.tick_count < 100:
            logger.info('[DHAN BINARY SEG] tick_count=%d idx=%d fno=%d full=%d changed=%d oi_map=%d',
                        self.tick_count, _idx_count_in_msg, _fno_count_in_msg,
                        _full_count_in_msg, len(changed), len(self.oi_map))

        if not changed:
            return

        self.tick_count += len(changed)

        if self.tick_count <= len(changed):
            logger.info('[DHAN FIRST TICKS] count=%d spot_map=%s sample=%s oi_map=%d',
                        len(changed), dict(self.spot_map),
                        list(changed.items())[:5], len(self.oi_map))

        try:
            live_tick_dispatcher.dispatch_tick(
                trade_date=trade_date,
                now_ts=now_ts,
                now_minute=now_minute,
                listen_time=listen_time,
                broker_ltp_map=dict(changed),
                spot_ticks_received=spot_ticks_received,
            )
        except Exception as exc:
            logger.error("[DHAN TICKER] dispatch error: %s", exc)

        listeners = list(self._listeners)
        if listeners:
            payload = {
                "timestamp":       now_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
                "ltp_map":         dict(self.ltp_map),
                "spot_map":        dict(self.spot_map),
                "changed_ltp_map": changed,
                "tick_count":      self.tick_count,
                "status":          self.status,
            }
            for listener in listeners:
                try:
                    listener(payload)
                except Exception as exc:
                    logger.warning("[DHAN TICKER] listener error: %s", exc)


dhan_ticker_manager = _DhanTickerManager()
