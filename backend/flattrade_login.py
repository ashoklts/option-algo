"""
flattrade_login.py
──────────────────
Standalone script to generate FlatTrade access token (jKey) and save
it to MongoDB broker_configuration.

Usage:
  python3 flattrade_login.py
  python3 flattrade_login.py --broker_doc_id <mongo_object_id>

Steps:
  1. Script prints the FlatTrade login URL
  2. Open URL in browser → login with FlatTrade credentials
  3. After login, FlatTrade redirects to your configured redirect URL
     The redirect URL will contain  ?code=<REQUEST_CODE>
  4. Copy that REQUEST_CODE and paste it here when prompted
  5. Script generates jKey and saves to MongoDB (or prints it)
"""

import argparse
import hashlib
import json
import os
import sys
import webbrowser
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── Load .env from project root ───────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env")
load_dotenv(Path(__file__).parent / ".env")   # also try backend/.env

API_KEY    = os.getenv("FLATTRADE_API_KEY", "").strip()
API_SECRET = os.getenv("FLATTRADE_API_SECRET", "").strip()

_AUTH_URL  = "https://auth.flattrade.in/"
_TOKEN_URL = "https://authapi.flattrade.in/trade/apitoken"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _login_url() -> str:
    return f"{_AUTH_URL}?app_key={API_KEY}"


def _generate_token(request_code: str) -> dict:
    """Exchange request_code for jKey session token."""
    checksum = hashlib.sha256(
        f"{API_KEY}{request_code}{API_SECRET}".encode()
    ).hexdigest()

    resp = requests.post(
        _TOKEN_URL,
        json={
            "api_key":      API_KEY,
            "request_code": request_code,
            "api_secret":   checksum,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("stat") == "Not_Ok" or not data.get("token"):
        raise ValueError(f"FlatTrade token error: {data.get('emsg', data)}")

    return data   # {"token": "<jKey>", "clientid": "<uid>", ...}


def _save_to_mongo(broker_doc_id: str, token: str, client_id: str) -> bool:
    """Save jKey to broker_configuration in MongoDB."""
    try:
        sys.path.insert(0, str(Path(__file__).parent))
        from features.mongo_data import MongoData
        from bson import ObjectId

        db = MongoData()
        db._db["broker_configuration"].update_one(
            {"_id": ObjectId(broker_doc_id)},
            {"$set": {
                "access_token": token,
                "user_id":      client_id,
                "user_name":    client_id,
                "login_time":   datetime.now(timezone.utc).isoformat(),
            }},
        )
        db.close()
        return True
    except Exception as exc:
        print(f"  [WARN] MongoDB save failed: {exc}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="FlatTrade login — get access token")
    parser.add_argument(
        "--broker_doc_id", "-b",
        default="",
        help="MongoDB broker_configuration _id to save token into",
    )
    parser.add_argument(
        "--open-browser", action="store_true", default=True,
        help="Auto-open login URL in browser (default: True)",
    )
    args = parser.parse_args()

    if not API_KEY or not API_SECRET:
        print("ERROR: FLATTRADE_API_KEY / FLATTRADE_API_SECRET not set in .env")
        sys.exit(1)

    login_url = _login_url()

    print()
    print("=" * 60)
    print("  FlatTrade Login")
    print("=" * 60)
    print()
    print("  Step 1: Open this URL in your browser:")
    print(f"\n  {login_url}\n")

    try:
        webbrowser.open(login_url)
        print("  (Browser opened automatically)")
    except Exception:
        print("  (Could not open browser — copy-paste the URL manually)")

    print()
    print("  Step 2: Login with your FlatTrade credentials.")
    print()
    print("  Step 3: After login, you will be redirected to a URL like:")
    print("    http://your-redirect-url?code=XXXXXXXXX")
    print()
    print("  Step 4: Copy the value after  ?code=  (the request code)")
    print()

    request_code = input("  Paste the request code here: ").strip()
    if not request_code:
        print("ERROR: No request code entered.")
        sys.exit(1)

    print()
    print("  Generating access token ...")

    try:
        session = _generate_token(request_code)
    except Exception as exc:
        print(f"  ERROR generating token: {exc}")
        sys.exit(1)

    jkey      = session.get("token", "")
    client_id = session.get("clientid", "")

    print()
    print("  ✓ Token generated successfully!")
    print(f"  Client ID   : {client_id}")
    print(f"  Access Token: {jkey}")
    print()

    if args.broker_doc_id:
        print(f"  Saving to MongoDB broker_doc_id={args.broker_doc_id} ...")
        ok = _save_to_mongo(args.broker_doc_id, jkey, client_id)
        if ok:
            print("  ✓ Saved to MongoDB successfully!")
        else:
            print("  Token NOT saved to MongoDB (see warning above).")
    else:
        print("  Tip: To save automatically to MongoDB, run with:")
        print(f"    python3 flattrade_login.py --broker_doc_id <mongo_id>")

    print()
    print("  Add this to your broker_configuration document if needed:")
    print(json.dumps({
        "access_token": jkey,
        "user_id":      client_id,
        "login_time":   datetime.now(timezone.utc).isoformat(),
    }, indent=4))
    print()


if __name__ == "__main__":
    main()
