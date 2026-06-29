"""
live_quote_socket.py
─────────────────────
Frontend-facing WebSocket that streams live LTP for an arbitrary, client-declared
set of instrument tokens — independent of any algo_trades record.

Why this exists instead of reusing /ws/update (execution_socket.py):
/ws/update's `subscribe_tokens` is built entirely from running trade records
(_build_subscribe_tokens), so it only ever covers tokens belonging to an active
algo trade. A manually-built basket (e.g. the paper-trade builder's "New Position"
legs, picked straight from the option chain) is pure client-side state that's
never persisted as a trade record until the user actually places the order, so it
has no token coverage there. This module reads broker_gateway.broker_ticker_manager
(the same kite/dhan-routed ltp_map every other live feature reads) and lets any
client subscribe to whichever tokens it currently cares about.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

log = logging.getLogger(__name__)
IST = timezone(timedelta(hours=5, minutes=30))
EMIT_INTERVAL_SECONDS = 0.5
REST_REFRESH_INTERVAL_SECONDS = 1.5
# Always included in _refresh_underlying_quotes' instrument set, regardless
# of whether any open strategy/subscribed token currently references them —
# standalone surfaces with no strategy of their own (e.g. the simulator's
# bare chart page) still need a live NIFTY spot tick to show something.
ALWAYS_TRACKED_UNDERLYINGS = {"NIFTY"}

live_quote_socket_router = APIRouter()


def _now_iso() -> str:
    return datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")


@dataclass
class _LiveQuoteSession:
    websocket: WebSocket
    session_id: str
    subscribed_tokens: set[str] = field(default_factory=set)
    last_sent: dict[str, float] = field(default_factory=dict)
    # Underlying (index/stock) spot broadcast is global, not opt-in per
    # session — see _collect_changed_underlyings — so this tracks "last
    # spot_price this session was sent" the same way last_sent does for
    # option tokens, just keyed by instrument name instead.
    last_sent_underlying: dict[str, float] = field(default_factory=dict)
    closed: bool = False
    task: asyncio.Task | None = None


class _LiveQuoteHub:
    def __init__(self) -> None:
        self._sessions: dict[str, _LiveQuoteSession] = {}
        self._lock = asyncio.Lock()
        # Fallback for tokens broker_ticker_manager.ltp_map has nothing for
        # (just-subscribed, illiquid, or off-peak — the WS tick simply never
        # arrives) — see _refresh_missing_via_rest. Keyed by token, shared
        # across every session so two clients watching the same token only
        # cost one REST call, not one each.
        self._rest_ltp_cache: dict[str, float] = {}
        self._rest_refresh_task: asyncio.Task | None = None
        # instrument (e.g. "NIFTY", "BSE") → {spot_price, change_pct, change_points, ...}
        # for every underlying with at least one open paper-trade strategy,
        # across *every* portfolio — refreshed on the same cadence as
        # _rest_ltp_cache above (_rest_refresh_loop), broadcast to every
        # connected session regardless of that session's own option-token
        # subscriptions (see _collect_changed_underlyings). This is what
        # makes "watch every open strategy's underlying move, system-wide"
        # possible without each client having to know in advance which
        # instruments to ask for.
        self._underlying_quote_cache: dict[str, dict] = {}

    def _ensure_rest_refresh_started(self) -> None:
        if self._rest_refresh_task is None or self._rest_refresh_task.done():
            self._rest_refresh_task = asyncio.create_task(self._rest_refresh_loop())

    async def register(self, websocket: WebSocket) -> _LiveQuoteSession:
        await websocket.accept()
        session = _LiveQuoteSession(websocket=websocket, session_id=uuid.uuid4().hex)
        async with self._lock:
            self._sessions[session.session_id] = session
        session.task = asyncio.create_task(self._emit_loop(session))
        self._ensure_rest_refresh_started()
        await self._send_message(session, "message", {
            "message": "live quote socket connected",
            "session_id": session.session_id,
        })
        return session

    async def unregister(self, session: _LiveQuoteSession) -> None:
        session.closed = True
        if session.task and not session.task.done():
            session.task.cancel()
            try:
                await session.task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                log.debug("live quote task close error session=%s: %s", session.session_id, exc)
        async with self._lock:
            self._sessions.pop(session.session_id, None)

    async def handle_client_message(self, session: _LiveQuoteSession, raw_message: str) -> None:
        try:
            payload = json.loads(raw_message or "{}")
        except Exception:
            return
        action = str(payload.get("action") or "").strip().lower()
        tokens = [str(t or "").strip() for t in (payload.get("tokens") or []) if str(t or "").strip()]

        if action == "resolve":
            await self._handle_resolve(session, payload)
            return

        if action == "unsubscribe":
            for token in tokens:
                session.subscribed_tokens.discard(token)
                session.last_sent.pop(token, None)
            return

        if action == "subscribe":
            new_tokens = [t for t in tokens if t not in session.subscribed_tokens]
            session.subscribed_tokens.update(tokens)
        elif action == "replace":
            # The basket's leg set changes as a whole on every add/remove/expiry-change, so the
            # client just resends its current full token list rather than diffing client-side.
            new_tokens = [t for t in tokens if t not in session.subscribed_tokens]
            removed_tokens = session.subscribed_tokens - set(tokens)
            session.subscribed_tokens = set(tokens)
            for token in removed_tokens:
                session.last_sent.pop(token, None)
        else:
            return

        if new_tokens:
            await asyncio.to_thread(self._ensure_broker_subscribed, new_tokens)

    async def _handle_resolve(self, session: _LiveQuoteSession, payload: dict) -> None:
        """
        Resolve {instrument, expiry, strike, option_type} → token via active_option_tokens,
        subscribe it, and return its current ltp (if already live) — all in one round trip.

        Deliberately not /live-greeks-chain: that endpoint builds the *entire* chain's
        Greeks plus a Dhan REST quote call (~1.5s) just to hand back one row. A contract lookup
        is a single indexed Mongo query (idx_active_option_contract_v2) — sub-5ms — since all
        this needs is the token; the socket's own ltp_map already carries the live price once
        subscribed.
        """
        request_id = str(payload.get("request_id") or "").strip()
        instrument = str(payload.get("instrument") or "").strip().upper()
        expiry = str(payload.get("expiry") or "").strip()[:10]
        option_type = str(payload.get("option_type") or "").strip().upper()
        # A futures contract has no strike — always stored as 0.0 (see
        # _sync_dhan_index_future_tokens), so a FUT resolve request doesn't need
        # one supplied at all; CE/PE still require a real strike.
        is_future = option_type == "FUT"
        try:
            strike = float(payload.get("strike"))
        except (TypeError, ValueError):
            strike = 0.0 if is_future else None

        if not instrument or not expiry or option_type not in ("CE", "PE", "FUT") or strike is None:
            await self._send_message(session, "resolve_error", {
                "request_id": request_id,
                "message": "instrument, expiry and option_type are required (strike too, unless option_type is FUT)",
            })
            return

        contract = await asyncio.to_thread(self._lookup_contract, instrument, expiry, strike, option_type)
        if not contract:
            await self._send_message(session, "resolve_error", {
                "request_id": request_id,
                "instrument": instrument,
                "expiry": expiry,
                "strike": strike,
                "option_type": option_type,
                "message": "contract not found",
            })
            return

        token = contract["token"]
        session.subscribed_tokens.add(token)
        await asyncio.to_thread(self._ensure_broker_subscribed, [token])

        from features.broker_gateway import broker_ticker_manager
        ltp = broker_ticker_manager.get_ltp(token)

        await self._send_message(session, "resolved", {
            "request_id": request_id,
            "instrument": instrument,
            "expiry": expiry,
            "strike": strike,
            "option_type": option_type,
            "token": token,
            "symbol": contract.get("symbol") or "",
            "ltp": float(ltp) if ltp else None,
        })

    def _lookup_contract(self, instrument: str, expiry: str, strike: float, option_type: str) -> dict | None:
        from features.mongo_data import MongoData
        from features.broker_gateway import _active_broker
        db = MongoData()
        try:
            doc = db._db["active_option_tokens"].find_one(
                {
                    "instrument": instrument,
                    "expiry": expiry,
                    "strike": strike,
                    "option_type": option_type,
                    "broker": _active_broker(),
                },
                {"_id": 0, "token": 1, "tokens": 1, "symbol": 1},
            )
        finally:
            db.close()
        if not doc:
            return None
        token = str(doc.get("token") or doc.get("tokens") or "").strip()
        if not token:
            return None
        return {"token": token, "symbol": str(doc.get("symbol") or "")}

    def _ensure_broker_subscribed(self, tokens: list[str]) -> None:
        try:
            from features.live_event import _subscribe_live_option_token
        except Exception as exc:
            log.debug("live quote subscribe import error: %s", exc)
            return
        for token in tokens:
            try:
                _subscribe_live_option_token(token)
            except Exception as exc:
                log.debug("live quote subscribe error token=%s: %s", token, exc)

    async def _emit_loop(self, session: _LiveQuoteSession) -> None:
        try:
            while not session.closed:
                if session.subscribed_tokens:
                    changed = self._collect_changed_ltp(session)
                    if changed:
                        await session.websocket.send_text(json.dumps({
                            "type": "ltp_update",
                            "data": changed,
                            "server_time": _now_iso(),
                        }))
                # Underlying broadcast is unconditional — every connected
                # session gets every open strategy's instrument move, not
                # just the legs it explicitly subscribed to (see
                # _collect_changed_underlyings).
                changed_underlyings = self._collect_changed_underlyings(session)
                if changed_underlyings:
                    await session.websocket.send_text(json.dumps({
                        "type": "underlying_update",
                        "data": changed_underlyings,
                        "server_time": _now_iso(),
                    }))
                await asyncio.sleep(EMIT_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("live quote emit loop error session=%s: %s", session.session_id, exc)

    def _collect_changed_ltp(self, session: _LiveQuoteSession) -> list[dict]:
        from features.broker_gateway import broker_ticker_manager
        ltp_map = broker_ticker_manager.ltp_map or {}
        changed: list[dict] = []
        for token in session.subscribed_tokens:
            ltp = ltp_map.get(token)
            ltp_float = float(ltp) if ltp else 0.0
            if ltp_float <= 0:
                # No WS tick has ever landed for this token (just subscribed,
                # illiquid, or off-peak) — _rest_refresh_loop's periodic
                # REST fallback below is the only other source; this hot
                # 0.5s loop stays a pure dict lookup either way, no I/O here.
                ltp_float = self._rest_ltp_cache.get(token, 0.0)
            if ltp_float <= 0 or session.last_sent.get(token) == ltp_float:
                continue
            session.last_sent[token] = ltp_float
            changed.append({"token": token, "ltp": ltp_float})
        return changed

    def _collect_changed_underlyings(self, session: _LiveQuoteSession) -> list[dict]:
        """Pure dict lookup, no I/O — same shape as _collect_changed_ltp, just
        reading the hub-wide cache _refresh_underlying_quotes keeps warm."""
        changed: list[dict] = []
        for instrument, quote in self._underlying_quote_cache.items():
            spot_price = float(quote.get("spot_price") or 0)
            if spot_price <= 0 or session.last_sent_underlying.get(instrument) == spot_price:
                continue
            session.last_sent_underlying[instrument] = spot_price
            changed.append({
                "instrument": instrument,
                "spot_price": spot_price,
                "change_pct": quote.get("change_pct"),
                "change_points": quote.get("change_points"),
            })
        return changed

    async def _rest_refresh_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(REST_REFRESH_INTERVAL_SECONDS)
                try:
                    await self._refresh_missing_via_rest()
                except Exception as exc:
                    log.warning("live quote REST refresh error: %s", exc)
                try:
                    await self._refresh_underlying_quotes()
                except Exception as exc:
                    log.warning("live quote underlying refresh error: %s", exc)
        except asyncio.CancelledError:
            pass

    async def _refresh_underlying_quotes(self) -> None:
        """
        Spot price (+ change%) for every instrument currently relevant to
        *any* connected client — not scoped to one session's subscriptions,
        since this is meant to be a shared, always-on feed for system-wide
        monitoring (see _collect_changed_underlyings). Three sources, unioned:

          1. ALWAYS_TRACKED_UNDERLYINGS — kept warm unconditionally so a
             standalone surface with no strategy/token of its own (the
             simulator's bare chart page, say) still gets a live tick
             instead of silently getting nothing until something else in
             the system happens to be watching the same instrument.
          2. Every open paper-trade strategy's instrument, across *every*
             portfolio (covers PortfolioNew.tsx even before any of its legs
             have been subscribed as option tokens).
          3. Whichever instruments the option tokens *currently pooled
             across every session* (the same union _refresh_missing_via_rest
             already builds) belong to — covers real broker positions
             (Positions.tsx) and PaperTradeNew.tsx's legs without needing a
             separate Dhan /positions REST call: the tokens are already
             flowing through this socket, so resolving token → instrument is
             a single indexed Mongo lookup, no extra broker API hit at all.

        Reuses features.execution_socket._fetch_dhan_index_quotes, which
        already prices both indices (IDX_I) and individual F&O stocks
        (NSE_EQ, see its stock-equity branch) in one batched call with its
        own persistent last-good fallback — same function
        /simulator/paper-trade's underlying-quotes endpoint and
        /live-greeks-chain both already rely on, so this stays
        consistent with every other surface instead of inventing a fourth
        way to price an underlying.
        """
        from features.mongo_data import MongoData
        from features.execution_socket import _fetch_dhan_index_quotes

        async with self._lock:
            subscribed_tokens = set()
            for s in self._sessions.values():
                subscribed_tokens |= s.subscribed_tokens

        db = MongoData()
        try:
            instruments = set(ALWAYS_TRACKED_UNDERLYINGS)
            instruments |= {
                str(doc.get("instrument") or "").strip().upper()
                for doc in db._db["simulator_strategy"].find(
                    {"all_exited": {"$ne": True}},
                    {"_id": 0, "instrument": 1},
                )
                if str(doc.get("instrument") or "").strip()
            }
            if subscribed_tokens:
                instruments |= {
                    str(doc.get("instrument") or "").strip().upper()
                    for doc in db._db["active_option_tokens"].find(
                        {"token": {"$in": list(subscribed_tokens)}},
                        {"_id": 0, "instrument": 1},
                    )
                    if str(doc.get("instrument") or "").strip()
                }
            quotes = await asyncio.to_thread(_fetch_dhan_index_quotes, db, instruments)
        finally:
            db.close()
        if quotes:
            self._underlying_quote_cache.update(quotes)

    async def _refresh_missing_via_rest(self) -> None:
        """
        broker_ticker_manager.ltp_map is purely passive — it only ever has a
        value for a token once the broker's own WS feed has sent at least one
        tick for it. A just-subscribed or thinly-traded contract can sit with
        nothing in ltp_map indefinitely, and _collect_changed_ltp on its own
        has no way to notice or do anything about that (by design — it's a
        0.5s hot loop, no I/O allowed in it). This is the active counterpart:
        every few seconds, find subscribed tokens still missing a live tick
        and ask the broker directly via the same get_broker_rest_quotes
        every other simulator surface already uses this session (WS-first,
        REST fallback, proper NSE_FNO/BSE_FNO segment routing).
        """
        from features.broker_gateway import broker_ticker_manager, get_broker_rest_quotes, _active_broker
        if _active_broker() != "dhan":
            return  # Kite path: caller-side kite_quote_map covers this elsewhere, untouched here.

        async with self._lock:
            all_tokens = set()
            for s in self._sessions.values():
                all_tokens |= s.subscribed_tokens
        if not all_tokens:
            return

        ltp_map = broker_ticker_manager.ltp_map or {}
        missing = [t for t in all_tokens if not ltp_map.get(t)]
        if not missing:
            return

        from features.mongo_data import MongoData
        db = MongoData()
        try:
            segment_by_token = {
                str(row.get("token") or row.get("tokens") or "").strip(): str(row.get("ws_segment") or "NSE_FNO").strip().upper()
                for row in db._db["active_option_tokens"].find(
                    {"broker": "dhan", "token": {"$in": missing}},
                    {"_id": 0, "token": 1, "tokens": 1, "ws_segment": 1},
                )
            }
            quotes = await asyncio.to_thread(get_broker_rest_quotes, missing, db._db, segment_by_token)
        finally:
            db.close()

        for token, info in quotes.items():
            ltp = float((info or {}).get("ltp") or 0)
            if ltp > 0:
                self._rest_ltp_cache[token] = ltp

    async def _send_message(self, session: _LiveQuoteSession, message_type: str, data: Any) -> None:
        await session.websocket.send_text(json.dumps({
            "type": message_type,
            "data": data,
            "server_time": _now_iso(),
        }))

    def get_status(self) -> dict:
        return {"connections": len(self._sessions)}


live_quote_hub = _LiveQuoteHub()


@live_quote_socket_router.get("/live-quotes/status")
async def live_quote_status():
    return live_quote_hub.get_status()


@live_quote_socket_router.websocket("/ws/live-quotes")
async def live_quote_socket(websocket: WebSocket):
    session = await live_quote_hub.register(websocket)
    try:
        while True:
            raw_message = await websocket.receive_text()
            await live_quote_hub.handle_client_message(session, raw_message)
    except WebSocketDisconnect:
        pass
    finally:
        await live_quote_hub.unregister(session)
