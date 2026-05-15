"""
flattrade_login.py
──────────────────
FlatTrade OAuth login — Python CLI (no browser HTML needed).

Two modes:
  1. --local-server  (recommended for local dev)
     Starts a local HTTP server on port 8765 to capture the OAuth callback.
     Requires: FlatTrade developer console → Redirect URL = http://localhost:8765/callback

  2. Manual code paste (default — works with any redirect URL)
     Opens browser, you login, then paste just the 'code' value shown
     in the final redirect URL.

Usage:
  python3 flattrade_login.py --broker_doc_id 69e8f91d0e59052e21916992
  python3 flattrade_login.py --broker_doc_id 69e8f91d0e59052e21916992 --local-server
  python3 flattrade_login.py --broker_doc_id 69e8f91d0e59052e21916992 --live-db
"""

import argparse
import hashlib
import http.server
import json
import os
import sys
import threading
import time
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv

# ── Load .env ─────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env")
load_dotenv(_ROOT.parent / ".env")

API_KEY    = os.getenv("FLATTRADE_API_KEY", "").strip()
API_SECRET = os.getenv("FLATTRADE_API_SECRET", "").strip()

_AUTH_URL  = "https://auth.flattrade.in/"
_TOKEN_URL = "https://authapi.flattrade.in/trade/apitoken"

DEFAULT_BROKER_DOC_ID = "69e8f91d0e59052e21916992"

# ── Local callback server ─────────────────────────────────────────────────────

_captured: dict = {}


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        code  = (params.get("code")  or [""])[0].strip()
        error = (params.get("error") or [""])[0].strip()

        if error:
            _captured["error"] = error
            msg = f"<h2 style='color:#ef4444'>Login Failed</h2><p>{error}</p>"
        elif code:
            _captured["code"] = code
            msg = "<h2 style='color:#22c55e'>Login Successful!</h2><p>You can close this window.</p>"
        else:
            msg = "<h2>Waiting...</h2>"

        html = (
            "<!DOCTYPE html><html><head><meta charset='utf-8'><title>FlatTrade</title>"
            "<style>body{font-family:sans-serif;display:flex;align-items:center;"
            "justify-content:center;height:100vh;margin:0;background:#0f172a;color:#f1f5f9}"
            ".box{text-align:center;padding:2rem;background:#1e293b;border-radius:12px}</style>"
            f"</head><body><div class='box'>{msg}<p style='color:#64748b;font-size:.85rem'>"
            "This window will close automatically...</p></div>"
            "<script>setTimeout(()=>window.close(),1500)</script></body></html>"
        )
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode())

    def log_message(self, *args):
        pass


def _wait_for_local_code(port: int) -> str:
    server = http.server.HTTPServer(("localhost", port), _CallbackHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print(f"\n  Local server listening on http://localhost:{port}/callback ...")
    print("  Waiting for FlatTrade redirect... (Ctrl+C to cancel)\n")
    timeout, start = 180, time.time()
    while not _captured and (time.time() - start) < timeout:
        time.sleep(0.4)
    server.shutdown()
    if _captured.get("error"):
        raise ValueError(f"FlatTrade error: {_captured['error']}")
    return _captured.get("code", "")


# ── Token exchange ────────────────────────────────────────────────────────────

def _exchange_code(request_code: str) -> dict:
    checksum = hashlib.sha256(
        f"{API_KEY}{request_code}{API_SECRET}".encode()
    ).hexdigest()
    resp = requests.post(
        _TOKEN_URL,
        json={"api_key": API_KEY, "request_code": request_code, "api_secret": checksum},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    print(f"  FlatTrade response: {data}")
    if data.get("stat") == "Not_Ok":
        raise ValueError(f"FlatTrade token error: {data.get('emsg', data)}")
    return data


def _get_token(session: dict) -> str:
    return (
        str(session.get("token") or session.get("susertoken")
            or session.get("jKey") or session.get("jkey") or "")
    ).strip()


def _get_user_id(session: dict) -> str:
    return (
        str(session.get("clientid") or session.get("uid")
            or session.get("uname") or session.get("actid") or "")
    ).strip()


# ── MongoDB save ──────────────────────────────────────────────────────────────

def _save_to_mongo(broker_doc_id: str, token: str, user_id: str) -> bool:
    try:
        sys.path.insert(0, str(_ROOT))
        from features.mongo_data import MongoData
        from bson import ObjectId
        db = MongoData()
        result = db._db["broker_configuration"].update_one(
            {"_id": ObjectId(broker_doc_id)},
            {"$set": {
                "access_token": token,
                "user_id":      user_id,
                "user_name":    user_id,
                "login_time":   datetime.now(timezone.utc).isoformat(),
            }},
        )
        db.close()
        return result.matched_count > 0
    except Exception as exc:
        print(f"  [ERROR] MongoDB save failed: {exc}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="FlatTrade Python login")
    parser.add_argument("--broker_doc_id", "-b", default=DEFAULT_BROKER_DOC_ID)
    parser.add_argument("--local-server", action="store_true",
                        help="Use local HTTP server to capture redirect (port 8765)")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    if not API_KEY or not API_SECRET:
        print("ERROR: FLATTRADE_API_KEY / FLATTRADE_API_SECRET not set in backend/.env")
        sys.exit(1)

    login_url = f"{_AUTH_URL}?app_key={API_KEY}"

    print()
    print("=" * 60)
    print("  FlatTrade Python Login")
    print("=" * 60)
    print(f"  API Key      : {API_KEY[:12]}...")
    print(f"  Broker Doc   : {args.broker_doc_id}")
    print(f"  Mode         : {'Local server (port ' + str(args.port) + ')' if args.local_server else 'Manual code paste'}")

    if args.local_server:
        print()
        print(f"  *** FlatTrade developer console → Redirect URL must be: ***")
        print(f"      http://localhost:{args.port}/callback")
        print()
        input("  Set redirect URL, then press Enter to open browser...")
    else:
        print()
        print("  After login, FlatTrade will redirect to the configured URL.")
        print("  Copy ONLY the 'code' value from the redirect URL.")
        print("  Example redirect URL:")
        print("    https://finedgealgo.com/broker/flattrade/redirect?code=XXXXX&state=YYY")
        print("                                                              ^^^^^")
        print("                                                    copy only this part")
        print()
        input("  Press Enter to open browser...")

    webbrowser.open(login_url)
    print(f"\n  Browser opened: {login_url}")

    # Get code
    try:
        if args.local_server:
            request_code = _wait_for_local_code(args.port)
        else:
            print()
            request_code = input("  Paste the 'code' value from redirect URL: ").strip()
    except KeyboardInterrupt:
        print("\n  Cancelled.")
        sys.exit(0)

    if not request_code:
        print("  ERROR: No code received.")
        sys.exit(1)

    print(f"\n  Code: {request_code[:12]}...")
    print("  Exchanging code for token...")

    try:
        session = _exchange_code(request_code)
    except Exception as exc:
        print(f"\n  ERROR: {exc}")
        sys.exit(1)

    token   = _get_token(session)
    user_id = _get_user_id(session)

    if not token:
        print(f"\n  ERROR: No token in response. Full response: {session}")
        sys.exit(1)

    print(f"\n  Token   : {token[:25]}...")
    print(f"  User ID : {user_id}")

    # Save to MongoDB
    print(f"\n  Saving to MongoDB (broker_doc_id={args.broker_doc_id})...")
    saved = _save_to_mongo(args.broker_doc_id, token, user_id)

    if saved:
        print("  ✓ Token saved to MongoDB successfully!")
    else:
        print("  [WARN] broker_doc_id not found in DB. Token generated but not saved.")
        print("  Copy manually:")
        print(json.dumps({"access_token": token, "user_id": user_id}, indent=4))

    print()


if __name__ == "__main__":
    main()
