"""
kite_broker.py
──────────────
Kite Connect broker integration.

Login flow:
  1. GET  /broker/kite/login-url  → redirect user to Zerodha login page
  2. Zerodha redirects back with ?request_token=xxx
  3. POST /broker/kite/callback   → exchange request_token for access_token
  4. access_token stored in MongoDB broker_configuration collection
"""

import os
import hashlib
from datetime import datetime, timezone

from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()

KITE_API_KEY    = os.getenv("KITE_API_KEY", "")
KITE_API_SECRET = os.getenv("KITE_API_SECRET", "")


def get_kite_instance(access_token: str = None) -> KiteConnect:
    kite = KiteConnect(api_key=KITE_API_KEY)
    if access_token:
        kite.set_access_token(access_token)
    return kite


def get_login_url() -> str:
    kite = get_kite_instance()
    return kite.login_url()


def generate_session(request_token: str) -> dict:
    """Exchange request_token for access_token. Returns session dict."""
    kite = get_kite_instance()
    session = kite.generate_session(request_token, api_secret=KITE_API_SECRET)
    return session


def save_kite_session(db, broker_doc_id: str, session: dict):
    """Persist access_token and login time into broker_configuration."""
    from bson import ObjectId
    db["broker_configuration"].update_one(
        {"_id": ObjectId(broker_doc_id)},
        {"$set": {
            "access_token":  session.get("access_token"),
            "login_time":    datetime.now(timezone.utc).isoformat(),
            "user_id":       session.get("user_id"),
            "user_name":     session.get("user_name"),
        }},
    )


def get_stored_access_token(db, broker_doc_id: str) -> str | None:
    """Load access_token from broker_configuration for a given broker doc."""
    from bson import ObjectId
    doc = db["broker_configuration"].find_one(
        {"_id": ObjectId(broker_doc_id)},
        {"access_token": 1},
    )
    return (doc or {}).get("access_token")


def get_option_instrument_token(
    kite: KiteConnect,
    name: str,           # e.g. "NIFTY", "BANKNIFTY"
    expiry,              # datetime.date object
    strike: float,
    option_type: str,    # "CE" or "PE"
    exchange: str = "NFO",
) -> int | None:
    """Return instrument_token for a specific option contract."""
    instruments = kite.instruments(exchange)
    for inst in instruments:
        if (
            inst["name"] == name
            and inst["instrument_type"] == option_type.upper()
            and inst["strike"] == strike
            and inst["expiry"] == expiry
        ):
            return inst["instrument_token"]
    return None


def get_option_historical_data(
    access_token: str,
    name: str,           # e.g. "NIFTY", "BANKNIFTY"
    expiry,              # datetime.date object
    strike: float,
    option_type: str,    # "CE" or "PE"
    from_date=None,      # datetime.date, defaults to today
    to_date=None,        # datetime.date, defaults to today
    interval: str = "minute",
    exchange: str = "NFO",
) -> list:
    """
    Fetch OHLCV candles for an option contract.

    Returns list of dicts:
      [{"date": datetime, "open": float, "high": float,
        "low": float, "close": float, "volume": int}, ...]
    """
    from datetime import date

    kite = get_kite_instance(access_token)

    if from_date is None:
        from_date = date.today()
    if to_date is None:
        to_date = date.today()

    token = get_option_instrument_token(kite, name, expiry, strike, option_type, exchange)
    if token is None:
        raise ValueError(
            f"Instrument not found: {name} {strike}{option_type} expiry={expiry} on {exchange}"
        )

    data = kite.historical_data(
        instrument_token=token,
        from_date=from_date,
        to_date=to_date,
        interval=interval,
    )
    return data
