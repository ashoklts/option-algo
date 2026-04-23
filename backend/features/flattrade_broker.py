"""
flattrade_broker.py
───────────────────
FlatTrade REST API adapter.

Provides a KiteConnect-compatible interface so live_order_manager.py
can use either Kite or FlatTrade without any extra branching.

broker_configuration document for FlatTrade:
  {
    "name":         "Broker.FlatTrade",
    "broker_icon":  "flattrade.svg",
    "broker_type":  "live",
    "user_id":      "<FlatTrade client ID>",
    "access_token": "<jKey session token>",
  }

Login flow:
  1. GET  /broker/flattrade/login?broker_doc_id=<id>
         → redirect to https://auth.flattrade.in/?app_key=<API_KEY>&state=<session_id>
  2. FlatTrade redirects back with ?code=<request_code>&state=<session_id>
  3. GET  /broker/flattrade/redirect?code=<code>&state=<session_id>
         → exchange request_code for jKey → save to broker_configuration
"""

from __future__ import annotations

import hashlib
import json
import logging
import os

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

FLATTRADE_API_KEY    = os.getenv("FLATTRADE_API_KEY", "")
FLATTRADE_API_SECRET = os.getenv("FLATTRADE_API_SECRET", "")

_AUTH_URL    = "https://auth.flattrade.in/"
_TOKEN_URL   = "https://authapi.flattrade.in/trade/apitoken"
_BASE_URL    = "https://piconnect.flattrade.in/PiConnectTP"


# ── Auth helpers ──────────────────────────────────────────────────────────────

def get_login_url(state: str = "") -> str:
    url = f"{_AUTH_URL}?app_key={FLATTRADE_API_KEY}"
    if state:
        url += f"&state={state}"
    return url


def generate_session(request_code: str) -> dict:
    """Exchange request_code for jKey. Returns {"token": jKey, "clientid": uid, ...}."""
    checksum = hashlib.sha256(
        f"{FLATTRADE_API_KEY}{request_code}{FLATTRADE_API_SECRET}".encode()
    ).hexdigest()
    resp = requests.post(
        _TOKEN_URL,
        json={
            "api_key":      FLATTRADE_API_KEY,
            "request_code": request_code,
            "api_secret":   checksum,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("stat") == "Not_Ok" or not data.get("token"):
        raise ValueError(f"FlatTrade session error: {data.get('emsg', data)}")
    return data


def save_flattrade_session(db, broker_doc_id: str, session: dict) -> None:
    """Persist jKey and login time into broker_configuration."""
    from bson import ObjectId
    from datetime import datetime, timezone
    db["broker_configuration"].update_one(
        {"_id": ObjectId(broker_doc_id)},
        {"$set": {
            "access_token": session.get("token"),
            "user_id":      session.get("clientid"),
            "user_name":    session.get("clientid"),
            "login_time":   datetime.now(timezone.utc).isoformat(),
        }},
    )


def get_stored_access_token(db, broker_doc_id: str) -> str | None:
    from bson import ObjectId
    doc = db["broker_configuration"].find_one(
        {"_id": ObjectId(broker_doc_id)},
        {"access_token": 1},
    )
    return (doc or {}).get("access_token")


# ── FlatTrade adapter ─────────────────────────────────────────────────────────

class FlatTradeAdapter:
    """
    Wraps FlatTrade Noren REST API with a KiteConnect-compatible surface:
      place_order(**params) → order_id str
      orders()              → list[dict] in Kite field names
      cancel_order(variety, order_id)
      quote(symbols)        → dict in Kite depth format
    """

    def __init__(self, user_id: str, jkey: str):
        self.user_id = user_id
        self.jkey    = jkey

    def _post(self, endpoint: str, data: dict) -> object:
        payload = f"jData={json.dumps(data)}&jKey={self.jkey}"
        resp = requests.post(
            f"{_BASE_URL}/{endpoint}",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    # ── place_order ──────────────────────────────────────────────────────────

    def place_order(
        self,
        tradingsymbol: str,
        exchange: str,
        transaction_type: str,      # 'BUY' / 'SELL'
        quantity: int,
        order_type: str,             # 'LIMIT' / 'MARKET' / 'SL' / 'SL-M'
        product: str,                # 'NRML' / 'MIS'
        variety: str = "regular",    # ignored — FlatTrade has no variety concept
        price: float = 0.0,
        trigger_price: float = 0.0,
    ) -> str:
        _prctyp = {
            "LIMIT":   "LMT",
            "MARKET":  "MKT",
            "SL":      "SL-LMT",
            "SL-M":    "SL-MKT",
        }.get(order_type, "LMT")

        body: dict = {
            "uid":         self.user_id,
            "actid":       self.user_id,
            "exch":        exchange,
            "tsym":        tradingsymbol,
            "qty":         str(int(quantity)),
            "prc":         str(round(float(price or 0), 2)),
            "dscqty":      "0",
            "prd":         "I" if product == "MIS" else "M",
            "trantype":    "B" if transaction_type == "BUY" else "S",
            "prctyp":      _prctyp,
            "ret":         "DAY",
            "ordersource": "API",
        }
        if _prctyp in ("SL-LMT", "SL-MKT") and trigger_price:
            body["trgprc"] = str(round(float(trigger_price), 2))

        result = self._post("PlaceOrder", body)
        if not isinstance(result, dict) or result.get("stat") != "Ok":
            raise Exception(
                f"FlatTrade PlaceOrder failed: {(result or {}).get('emsg', result)}"
            )
        return str(result.get("norenordno") or "")

    # ── orders ───────────────────────────────────────────────────────────────

    def orders(self) -> list:
        """Return order book as list of Kite-shaped dicts."""
        result = self._post("OrderBook", {"uid": self.user_id})
        if not isinstance(result, list):
            return []

        _status_map = {
            "COMPLETE":        "COMPLETE",
            "OPEN":            "OPEN",
            "TRIGGER_PENDING": "OPEN",
            "REJECTED":        "REJECTED",
            "CANCELLED":       "CANCELLED",
        }
        out = []
        for o in result:
            raw_status = str(o.get("status") or "").upper()
            out.append({
                "order_id":           str(o.get("norenordno") or ""),
                "status":             _status_map.get(raw_status, raw_status),
                "average_price":      float(o.get("avgprc") or o.get("flprc") or 0),
                "price":              float(o.get("prc") or 0),
                "trigger_price":      float(o.get("trgprc") or 0),
                "filled_quantity":    int(o.get("fillshares") or 0),
                "quantity":           int(o.get("qty") or 0),
                "tradingsymbol":      str(o.get("tsym") or ""),
                "transaction_type":   "BUY" if o.get("trantype") == "B" else "SELL",
                "product":            "MIS" if o.get("prd") == "I" else "NRML",
                "last_price":         float(o.get("lp") or 0),
                "status_message":     str(o.get("rejreason") or ""),
                "status_message_raw": str(o.get("rejreason") or ""),
            })
        return out

    # ── cancel_order ─────────────────────────────────────────────────────────

    def cancel_order(self, variety: str = "regular", order_id: str = "") -> str:
        result = self._post("CancelOrder", {
            "norenordno": order_id,
            "uid":        self.user_id,
        })
        if not isinstance(result, dict) or result.get("stat") != "Ok":
            raise Exception(
                f"FlatTrade CancelOrder failed: {(result or {}).get('emsg', result)}"
            )
        return str(result.get("result") or order_id)

    # ── quote ────────────────────────────────────────────────────────────────

    def quote(self, symbols: list) -> dict:
        """
        Get depth/bid-ask for a list of 'EXCHANGE:SYMBOL' strings.
        Returns Kite-compatible dict.

        FlatTrade GetQuotes works by tsym (tradingsymbol). Returns bp1/sp1
        as best bid/ask prices.
        """
        result = {}
        for sym_key in (symbols or []):
            parts = sym_key.split(":", 1)
            exch  = parts[0] if len(parts) == 2 else "NFO"
            tsym  = parts[1] if len(parts) == 2 else parts[0]
            try:
                q = self._post("GetQuotes", {
                    "uid":  self.user_id,
                    "exch": exch,
                    "tsym": tsym,
                })
                if isinstance(q, dict) and q.get("stat") == "Ok":
                    bp = float(q.get("bp1") or 0)
                    sp = float(q.get("sp1") or 0)
                    lp = float(q.get("lp")  or 0)
                    result[sym_key] = {
                        "last_price": lp,
                        "depth": {
                            "buy":  [{"price": bp, "quantity": int(q.get("bq1") or 0)}],
                            "sell": [{"price": sp, "quantity": int(q.get("sq1") or 0)}],
                        },
                    }
            except Exception as exc:
                log.debug("FlatTrade quote error sym=%s: %s", sym_key, exc)
        return result


# ── Factory ───────────────────────────────────────────────────────────────────

def get_flattrade_instance(user_id: str, access_token: str) -> FlatTradeAdapter | None:
    if not user_id or not access_token:
        return None
    return FlatTradeAdapter(user_id=user_id, jkey=access_token)


def _is_flattrade_doc(doc: dict) -> bool:
    """Return True if broker_configuration doc belongs to FlatTrade.
    Uses name only — icon field can be wrong in DB (both may share same icon).
    """
    name = str(doc.get("name") or "").lower()
    return "flattrade" in name
