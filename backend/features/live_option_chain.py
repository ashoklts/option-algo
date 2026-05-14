"""
live_option_chain.py
────────────────────
Fetch the full live option chain (CE + PE) from Kite before taking entry.
Used for ALL entry types in forward/live activation mode.

Before any leg entry the caller fetches both sides, prints the combined table,
then calls select_strike_live() to pick the correct strike for any entry type.

Public API
──────────
  fetch_full_chain(db, underlying, expiry, spot_price, leg_id='')
    → {'CE': [rows], 'PE': [rows]}
    Each row: {strike, ltp, iv, delta, gamma, theta, vega, oi, token, symbol}

  select_strike_live(chain, entry_kind, strike_param_raw, option_type,
                     position, spot_price, underlying, leg_id='')
    → {'strike', 'ltp', 'token', 'symbol', 'meta'} | None
"""

from __future__ import annotations

import ast
import logging
import math
import threading
import time
from typing import Any

from features.debug_flags import entry_print

log = logging.getLogger(__name__)

# ── module-level TTL cache shared across all callers ──────────────────────────
# Prevents duplicate Kite REST calls when _handle_entry_leg and
# process_momentum_pending_legs both run for the same leg in the same tick.
_CHAIN_CACHE: dict[tuple, tuple[dict, float]] = {}
_CHAIN_TTL_SECONDS = 2.0

# ── in-flight deduplication ───────────────────────────────────────────────────
# When two threads request the same (underlying, expiry) simultaneously, only the
# first thread fetches from Kite; the second waits for the first's result.
_CHAIN_FETCHING: dict[tuple, threading.Event] = {}
_CHAIN_MUTEX = threading.Lock()


def _cache_get(key: tuple) -> dict | None:
    entry = _CHAIN_CACHE.get(key)
    if entry and (time.perf_counter() - entry[1]) < _CHAIN_TTL_SECONDS:
        return entry[0]
    return None


def _cache_set(key: tuple, chain: dict) -> None:
    _CHAIN_CACHE[key] = (chain, time.perf_counter())


# ── re-use BS helpers from kite_delta_chain ───────────────────────────────────
def _bs():
    from features.kite_delta_chain import (  # type: ignore
        _calc_iv, _calc_greeks, _time_to_expiry,
        _get_kite_credentials, _RISK_FREE_RATE,
        _DIVIDEND_YIELDS, _DEFAULT_DIVIDEND_YIELD,
    )
    return (
        _calc_iv,
        _calc_greeks,
        _time_to_expiry,
        _get_kite_credentials,
        _RISK_FREE_RATE,
        _DIVIDEND_YIELDS,
        _DEFAULT_DIVIDEND_YIELD,
    )


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _parse_sp(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and '{' in raw:
        try:
            return ast.literal_eval(raw)
        except Exception:
            pass
    return {}


def _is_sell(position: str) -> bool:
    return 'sell' in str(position or '').lower()


_STRIKE_STEP_MAP = {
    'NIFTY': 50, 'BANKNIFTY': 100, 'FINNIFTY': 50,
    'MIDCPNIFTY': 25, 'SENSEX': 100, 'BANKEX': 100,
}

def _atm_step(underlying: str) -> int:
    return _STRIKE_STEP_MAP.get(str(underlying or '').strip().upper(), 100)


def _atm_from_spot(spot: float, underlying: str) -> int:
    step = _atm_step(underlying)
    return int(round(spot / step) * step)


def _find_atm_row(rows: list[dict], atm_strike: float) -> dict:
    """Return the row whose strike is closest to atm_strike."""
    if not rows:
        return {}
    return min(rows, key=lambda r: abs(_safe_float(r.get('strike')) - atm_strike))


def _get_kite_rest_client(db) -> Any | None:
    """Return a configured KiteConnect instance using the shared credential path."""
    try:
        _, _, _, _get_kite_credentials, _, _, _ = _bs()
        api_key, access_token = _get_kite_credentials(db)
        if not api_key or not access_token:
            return None
        from kiteconnect import KiteConnect  # type: ignore
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        return kite
    except Exception:
        return None


def _resolve_chain_reference_spot(
    rows_by_side: dict[str, dict[float, dict]],
    spot_price: float,
    T: float,
    r: float,
    q: float,
) -> float:
    """Mirror the live API reference-spot logic for identical chain pricing."""
    if spot_price <= 0:
        return 0.0

    ce_by_strike = rows_by_side.get('CE') or {}
    pe_by_strike = rows_by_side.get('PE') or {}
    common_strikes = [
        strike
        for strike in set(ce_by_strike) & set(pe_by_strike)
        if _safe_float((ce_by_strike.get(strike) or {}).get('ltp')) > 0
        and _safe_float((pe_by_strike.get(strike) or {}).get('ltp')) > 0
    ]
    if not common_strikes:
        return spot_price

    atm_strike = min(common_strikes, key=lambda strike: abs(strike - spot_price))
    ce_ltp = _safe_float((ce_by_strike.get(atm_strike) or {}).get('ltp'))
    pe_ltp = _safe_float((pe_by_strike.get(atm_strike) or {}).get('ltp'))
    synthetic_future = atm_strike + ce_ltp - pe_ltp
    if synthetic_future <= 0:
        return spot_price
    return synthetic_future * math.exp(-(r - q) * max(T, 0.0))


# ── fetch full chain from Kite ────────────────────────────────────────────────

def fetch_full_chain(
    db,
    underlying: str,
    expiry: str,
    spot_price: float,
    leg_id: str = '',
) -> dict[str, list[dict]]:
    """
    Fetch ALL strikes for the expiry from Kite (both CE and PE).
    Computes Black-Scholes IV + Greeks for each row.
    Prints the combined CE/PE chain table.
    Returns {'CE': [rows], 'PE': [rows]}.

    Results are cached for 2 seconds so multiple code paths in the same tick
    share one Kite REST call instead of each fetching independently.
    """
    _cache_key = (underlying, expiry)

    # Fast path: valid cache hit (no lock needed for read)
    _cached = _cache_get(_cache_key)
    if _cached is not None:
        entry_print(f'[LIVE CHAIN] leg={leg_id} reusing cached chain {underlying} {expiry}')
        return _cached

    # Serialize concurrent fetches for the same (underlying, expiry) key.
    with _CHAIN_MUTEX:
        # Re-check under lock — another thread may have just populated the cache.
        _cached = _cache_get(_cache_key)
        if _cached is not None:
            entry_print(f'[LIVE CHAIN] leg={leg_id} reusing cached chain {underlying} {expiry}')
            return _cached
        if _cache_key in _CHAIN_FETCHING:
            _wait_ev = _CHAIN_FETCHING[_cache_key]
            _is_fetcher = False
        else:
            _wait_ev = threading.Event()
            _CHAIN_FETCHING[_cache_key] = _wait_ev
            _is_fetcher = True

    if not _is_fetcher:
        entry_print(f'[LIVE CHAIN] leg={leg_id} waiting for in-flight chain {underlying} {expiry}')
        _wait_ev.wait(timeout=10.0)
        _cached = _cache_get(_cache_key)
        if _cached is not None:
            return _cached
        return {'CE': [], 'PE': []}

    # We are the fetcher — proceed with Kite API call.
    try:
        return _fetch_full_chain_from_kite(
            db, underlying, expiry, spot_price, leg_id,
        )
    finally:
        with _CHAIN_MUTEX:
            _CHAIN_FETCHING.pop(_cache_key, None)
        _wait_ev.set()


def _fetch_full_chain_from_kite(
    db,
    underlying: str,
    expiry: str,
    spot_price: float,
    leg_id: str,
) -> dict[str, list[dict]]:
    _cache_key = (underlying, expiry)
    (
        _calc_iv,
        _calc_greeks,
        _time_to_expiry,
        _get_kite_credentials,
        _RISK_FREE_RATE,
        _DIVIDEND_YIELDS,
        _DEFAULT_DIVIDEND_YIELD,
    ) = _bs()

    api_key, access_token = _get_kite_credentials(db)
    if not api_key or not access_token:
        log.warning('[LIVE CHAIN] leg=%s no Kite credentials', leg_id)
        return {'CE': [], 'PE': []}

    T = _time_to_expiry(expiry)
    r = _RISK_FREE_RATE
    q_yield = _DIVIDEND_YIELDS.get(str(underlying or '').strip().upper(), _DEFAULT_DIVIDEND_YIELD)

    # ── load contracts from the same source used by the live-greeks API ─────
    contracts: list[dict] = []
    try:
        tok_col = db._db['active_option_tokens']
        contracts = list(tok_col.find(
            {'instrument': str(underlying or '').strip().upper(), 'expiry': {'$regex': f'^{str(expiry or "").strip()[:10]}'}},
            {'_id': 0, 'strike': 1, 'option_type': 1, 'token': 1, 'tokens': 1, 'symbol': 1},
        ))
    except Exception as exc:
        log.warning('[LIVE CHAIN] leg=%s active_option_tokens read error: %s', leg_id, exc)

    if not contracts:
        log.warning('[LIVE CHAIN] leg=%s no contracts found underlying=%s expiry=%s',
                    leg_id, underlying, expiry)
        return {'CE': [], 'PE': []}

    ce_count = sum(1 for c in contracts if str(c.get('option_type') or '').strip().upper() == 'CE')
    pe_count = sum(1 for c in contracts if str(c.get('option_type') or '').strip().upper() == 'PE')
    total    = ce_count + pe_count
    entry_print(
        f'[LIVE CHAIN] leg={leg_id} ⟶  Getting option chain ...'
        f'  underlying={underlying}  expiry={expiry}  spot={spot_price}'
        f'  CE_strikes={ce_count}  PE_strikes={pe_count}  total_tokens={total}'
    )

    import time as _time
    _t0 = _time.perf_counter()

    kite = _get_kite_rest_client(db)
    token_to_quote: dict[str, dict] = {}
    if kite:
        try:
            index_symbols = {
                'NIFTY': 'NSE:NIFTY 50',
                'BANKNIFTY': 'NSE:NIFTY BANK',
                'FINNIFTY': 'NSE:NIFTY FIN SERVICE',
                'SENSEX': 'BSE:SENSEX',
                'MIDCPNIFTY': 'NSE:NIFTY MID SELECT',
            }
            index_sym = index_symbols.get(str(underlying or '').strip().upper(), '')
            if index_sym:
                idx_q = kite.quote([index_sym]) or {}
                rest_spot_price = _safe_float((idx_q.get(index_sym) or {}).get('last_price'))
                if rest_spot_price > 0:
                    spot_price = rest_spot_price
        except Exception as exc:
            log.warning('[LIVE CHAIN] leg=%s spot quote error: %s', leg_id, exc)

        all_tokens = [
            int(str(c.get('token') or c.get('tokens') or 0))
            for c in contracts
            if c.get('token') or c.get('tokens')
        ]
        all_tokens = [tok for tok in all_tokens if tok]
        for i in range(0, len(all_tokens), 500):
            try:
                quotes = kite.quote(all_tokens[i:i + 500]) or {}
                for _sym, quote in quotes.items():
                    token = str(quote.get('instrument_token') or '').strip()
                    if token:
                        token_to_quote[token] = quote
            except Exception as exc:
                log.warning('[LIVE CHAIN] leg=%s quote batch[%d] error: %s', leg_id, i, exc)

    _elapsed_ms = round((_time.perf_counter() - _t0) * 1000, 1)
    entry_print(
        f'[LIVE CHAIN] leg={leg_id} ✓  Got option chain'
        f'  quotes_received={len(token_to_quote)}  elapsed={_elapsed_ms}ms'
    )

    rows_by_side: dict[str, dict[float, dict]] = {'CE': {}, 'PE': {}}
    raw_rows: list[dict] = []
    for contract in contracts:
        opt = str(contract.get('option_type') or '').strip().upper()
        if opt not in ('CE', 'PE'):
            continue
        stk = _safe_float(contract.get('strike'))
        tok = str(contract.get('token') or contract.get('tokens') or '').strip()
        sym = str(contract.get('symbol') or '').strip()
        if not stk or not tok:
            continue
        quote = token_to_quote.get(tok) or {}
        ltp = _safe_float(quote.get('last_price'))
        if ltp == 0:
            ltp = _safe_float((quote.get('ohlc') or {}).get('close'))
        oi = int(quote.get('oi') or 0)
        vol = int(quote.get('volume') or 0)
        row = {
            'opt': opt,
            'strike': stk,
            'ltp': ltp,
            'oi': oi,
            'volume': vol,
            'token': tok,
            'symbol': sym,
        }
        raw_rows.append(row)
        rows_by_side[opt][stk] = row

    pricing_spot = _resolve_chain_reference_spot(rows_by_side, spot_price, T, r, q_yield) or spot_price

    # ── compute Greeks per side ───────────────────────────────────────────────
    chain: dict[str, list[dict]] = {'CE': [], 'PE': []}
    for row in raw_rows:
        opt = str(row.get('opt') or '')
        stk = _safe_float(row.get('strike'))
        ltp = _safe_float(row.get('ltp'))
        if pricing_spot > 0 and ltp > 0:
            iv = _calc_iv(ltp, pricing_spot, stk, T, r, opt, q_yield)
            greeks = _calc_greeks(pricing_spot, stk, T, r, iv, opt, q_yield)
        else:
            iv, greeks = 0.0, {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}
        chain[opt].append({
            'strike': stk,
            'ltp':    ltp,
            'iv':     round(iv * 100, 2),
            'delta':  greeks['delta'],
            'gamma':  greeks['gamma'],
            'theta':  greeks['theta'],
            'vega':   greeks['vega'],
            'oi':     int(row.get('oi') or 0),
            'volume': int(row.get('volume') or 0),
            'token':  str(row.get('token') or ''),
            'symbol': str(row.get('symbol') or ''),
        })

    chain['CE'].sort(key=lambda item: _safe_float(item.get('strike')))
    chain['PE'].sort(key=lambda item: _safe_float(item.get('strike')))

    _print_combined_table(chain, underlying, expiry, pricing_spot, leg_id)
    _cache_set(_cache_key, chain)
    return chain


# ── per-strike delta fetch for pre-entry check ───────────────────────────────

def get_live_delta_for_strike(
    db,
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,
) -> dict:
    """
    Fetch live delta for a single strike using WS ltp_map (primary) + Kite REST
    (fallback) — same pricing source as the /algo/live-greeks-chain API.

    Returns {'delta': float, 'ltp': float, 'iv': float} or {} on failure.
    """
    (
        _calc_iv, _calc_greeks, _time_to_expiry,
        _get_kite_credentials, _RISK_FREE_RATE,
        _DIVIDEND_YIELDS, _DEFAULT_DIVIDEND_YIELD,
    ) = _bs()

    opt = str(option_type or '').strip().upper()
    und = str(underlying or '').strip().upper()
    exp = str(expiry or '').strip()[:10]
    stk = _safe_float(strike)
    if not opt or not und or not exp or not stk:
        return {}

    # ── Step 1: spot price from WS ltp_map ────────────────────────────────────
    try:
        from features.kite_broker_ws import get_ltp_map  # type: ignore
        from features.spot_atm_utils import KITE_INDEX_TOKENS  # type: ignore
        ltp_map: dict = get_ltp_map() or {}
        idx_token = str(KITE_INDEX_TOKENS.get(und, 0))
        spot_price = _safe_float(ltp_map.get(idx_token))
    except Exception:
        ltp_map = {}
        spot_price = 0.0

    # ── Step 2: load token for this strike from active_option_tokens ──────────
    token = ''
    symbol = ''
    try:
        tok_col = db._db['active_option_tokens']
        doc = tok_col.find_one(
            {
                'instrument': und,
                'expiry': {'$regex': f'^{exp}'},
                'option_type': opt,
                'strike': {'$in': [stk, int(stk), str(int(stk)), str(stk)]},
            },
            {'_id': 0, 'token': 1, 'tokens': 1, 'symbol': 1},
        )
        if doc:
            token = str(doc.get('token') or doc.get('tokens') or '').strip()
            symbol = str(doc.get('symbol') or '').strip()
    except Exception as exc:
        log.warning('[DELTA CHECK] active_option_tokens error: %s', exc)

    if not token:
        return {}

    # ── Step 3: ltp — WS first, Kite REST fallback ────────────────────────────
    ltp = _safe_float(ltp_map.get(token)) if ltp_map else 0.0
    if ltp <= 0:
        kite = _get_kite_rest_client(db)
        if kite:
            try:
                q = (kite.quote([int(token)]) or {})
                for _, qdata in q.items():
                    ltp = _safe_float(qdata.get('last_price'))
                    if ltp <= 0:
                        ltp = _safe_float((qdata.get('ohlc') or {}).get('close'))
                    if spot_price <= 0:
                        pass  # spot already from WS; don't override
            except Exception as exc:
                log.warning('[DELTA CHECK] kite quote error: %s', exc)

    if ltp <= 0 or spot_price <= 0:
        return {}

    # ── Step 4: Black-Scholes Greeks ──────────────────────────────────────────
    T = _time_to_expiry(exp)
    r = _RISK_FREE_RATE
    q_yield = _DIVIDEND_YIELDS.get(und, _DEFAULT_DIVIDEND_YIELD)
    try:
        iv = _calc_iv(ltp, spot_price, stk, T, r, opt, q_yield)
        greeks = _calc_greeks(spot_price, stk, T, r, iv, opt, q_yield)
    except Exception as exc:
        log.warning('[DELTA CHECK] greeks calc error: %s', exc)
        return {}

    return {
        'delta': greeks['delta'],
        'ltp': ltp,
        'iv': round(iv * 100, 2),
        'symbol': symbol,
    }


# ── print combined CE / PE table ──────────────────────────────────────────────

def _print_combined_table(
    chain: dict[str, list[dict]],
    underlying: str,
    expiry: str,
    spot_price: float,
    leg_id: str,
) -> None:
    ce_by_strike = {r['strike']: r for r in chain.get('CE', [])}
    pe_by_strike = {r['strike']: r for r in chain.get('PE', [])}
    all_strikes  = sorted(set(ce_by_strike) | set(pe_by_strike))

    sep = '[LIVE CHAIN] ' + '─' * 110
    entry_print(
        f'\n[LIVE CHAIN] leg={leg_id}  {underlying}  expiry={expiry}  '
        f'spot={spot_price}  strikes={len(all_strikes)}'
    )
    entry_print(sep)
    entry_print(
        f'[LIVE CHAIN] {"CE_LTP":>9}  {"CE_IV%":>7}  {"CE_Delta":>9}  │'
        f'  {"STRIKE":>7}  │  {"PE_Delta":>9}  {"PE_IV%":>7}  {"PE_LTP":>9}'
    )
    entry_print(sep)
    atm = _atm_from_spot(spot_price, underlying)
    for s in all_strikes:
        ce = ce_by_strike.get(s, {})
        pe = pe_by_strike.get(s, {})
        atm_marker = ' ←ATM' if int(s) == atm else ''
        entry_print(
            f'[LIVE CHAIN] {_safe_float(ce.get("ltp")):>9.2f}  '
            f'{_safe_float(ce.get("iv")):>7.2f}  '
            f'{_safe_float(ce.get("delta")):>9.4f}  │'
            f'  {int(s):>7}  │  '
            f'{_safe_float(pe.get("delta")):>9.4f}  '
            f'{_safe_float(pe.get("iv")):>7.2f}  '
            f'{_safe_float(pe.get("ltp")):>9.2f}'
            f'{atm_marker}'
        )
    entry_print(sep + '\n')


# ── strike selection from live chain ─────────────────────────────────────────

def select_strike_live(
    chain: dict[str, list[dict]],
    entry_kind: str,
    strike_param_raw: Any,
    option_type: str,
    position: str,
    spot_price: float,
    underlying: str,
    leg_id: str = '',
) -> dict | None:
    """
    Select a strike from the live chain based on entry_kind.
    Returns {'strike', 'ltp', 'token', 'symbol', 'meta'} or None.
    """
    opt      = option_type.upper()
    rows     = chain.get(opt, [])
    ce_rows  = chain.get('CE', [])
    pe_rows  = chain.get('PE', [])
    step     = _atm_step(underlying)
    atm      = _atm_from_spot(spot_price, underlying)
    sp       = _parse_sp(strike_param_raw)

    if not rows:
        log.warning('[LIVE SELECT] leg=%s no rows for opt=%s', leg_id, opt)
        return None

    # ── helpers ───────────────────────────────────────────────────────────────
    def _row_for_strike(target_strike: float) -> dict | None:
        if not rows:
            return None
        valid = [r for r in rows if _safe_float(r.get('ltp')) > 0]
        pool  = valid if valid else rows
        return min(pool, key=lambda r: abs(_safe_float(r.get('strike')) - target_strike))

    def _result(row: dict, meta: dict | None = None) -> dict:
        return {
            'strike': _safe_float(row.get('strike')),
            'ltp':    _safe_float(row.get('ltp')),
            'token':  str(row.get('token') or ''),
            'symbol': str(row.get('symbol') or ''),
            'iv':     _safe_float(row.get('iv')),
            'delta':  _safe_float(row.get('delta')),
            'meta':   meta or {},
        }

    # ── PremiumCloseToStraddle ────────────────────────────────────────────────
    if 'PremiumCloseToStraddle' in entry_kind:
        multiplier = _safe_float(sp.get('Multiplier') or 0.5)
        atm_ce = _find_atm_row(ce_rows, atm)
        atm_pe = _find_atm_row(pe_rows, atm)
        ce_ltp = _safe_float(atm_ce.get('ltp'))
        pe_ltp = _safe_float(atm_pe.get('ltp'))
        straddle = ce_ltp + pe_ltp
        if straddle <= 0:
            log.warning('[LIVE SELECT] leg=%s straddle=0 — skipping', leg_id)
            return None
        target = straddle * multiplier
        row = min(rows, key=lambda r: abs(_safe_float(r.get('ltp')) - target))
        entry_print(
            f'[LIVE SELECT] leg={leg_id} method=StraddlePct '
            f'atm={atm} ce={ce_ltp} pe={pe_ltp} straddle={round(straddle,2)} '
            f'target={round(target,2)} → strike={row.get("strike")} ltp={row.get("ltp")}'
        )
        return _result(row, {
            'atm_strike': atm, 'ce_atm_price': ce_ltp, 'pe_atm_price': pe_ltp,
            'straddle': round(straddle, 2), 'target': round(target, 2), 'multiplier': multiplier,
        })

    # ── StraddlePrice ─────────────────────────────────────────────────────────
    if 'StraddlePrice' in entry_kind:
        multiplier = _safe_float(sp.get('Multiplier') or 0.5)
        adjustment = str(sp.get('Adjustment') or 'AdjustmentType.Plus')
        is_plus    = 'Minus' not in adjustment
        atm_ce = _find_atm_row(ce_rows, atm)
        atm_pe = _find_atm_row(pe_rows, atm)
        straddle = _safe_float(atm_ce.get('ltp')) + _safe_float(atm_pe.get('ltp'))
        offset     = multiplier * straddle
        raw_strike = atm + offset if is_plus else atm - offset
        final_str  = int(round(raw_strike / step) * step)
        row = _row_for_strike(final_str)
        if not row:
            return None
        entry_print(
            f'[LIVE SELECT] leg={leg_id} method=StraddlePrice '
            f'straddle={round(straddle,2)} offset={round(offset,2)} '
            f'{"+" if is_plus else "-"} → strike={final_str}'
        )
        return _result(row, {
            'atm_strike':    atm,
            'ce_atm_price':  round(_safe_float(atm_ce.get('ltp')), 2),
            'pe_atm_price':  round(_safe_float(atm_pe.get('ltp')), 2),
            'straddle':      round(straddle, 2),
            'multiplier':    multiplier,
            'offset':        round(offset, 2),
            'adjustment':    '+' if is_plus else '-',
        })

    # ── SyntheticFuture ───────────────────────────────────────────────────────
    if 'SyntheticFuture' in entry_kind:
        import re as _re
        atm_ce = _find_atm_row(ce_rows, atm)
        atm_pe = _find_atm_row(pe_rows, atm)
        ce_ltp = _safe_float(atm_ce.get('ltp'))
        pe_ltp = _safe_float(atm_pe.get('ltp'))
        syn_future = atm - pe_ltp + ce_ltp
        syn_atm    = int(round(syn_future / step) * step)
        _sp_str  = str(strike_param_raw or '')
        m  = _re.search(r'OTM(\d+)', _sp_str)
        m2 = _re.search(r'ITM(\d+)', _sp_str)
        _raw_offset = int(m.group(1)) if m else (-int(m2.group(1)) if m2 else 0)
        # PE: OTM is below syn_atm, ITM is above syn_atm
        offset_n  = -_raw_offset if opt == 'PE' else _raw_offset
        final_str = syn_atm + offset_n * step
        row = _row_for_strike(final_str)
        if not row:
            return None
        _syn_label = f'OTM{abs(_raw_offset)}' if _raw_offset > 0 else (f'ITM{abs(_raw_offset)}' if _raw_offset < 0 else 'ATM')
        entry_print(
            f'[LIVE SELECT] leg={leg_id} method=SyntheticFuture+{_syn_label} '
            f'ce={ce_ltp} pe={pe_ltp} syn={round(syn_future,2)} '
            f'syn_atm={syn_atm} offset={offset_n} → strike={final_str}'
        )
        return _result(row, {
            'atm_strike': atm, 'ce_atm_price': ce_ltp, 'pe_atm_price': pe_ltp,
            'synthetic_future': round(syn_future, 2),
        })

    # ── AtmMultiplier ─────────────────────────────────────────────────────────
    if 'AtmMultiplier' in entry_kind:
        multiplier  = _safe_float(strike_param_raw) if not isinstance(strike_param_raw, dict) else 1.0
        raw_strike  = atm * multiplier
        final_str   = int(round(raw_strike / step) * step)
        row = _row_for_strike(final_str)
        if not row:
            entry_print(f'[LIVE SELECT] leg={leg_id} method=AtmMultiplier no chain row for strike={final_str} — skipped')
            return None
        actual_strike = _safe_float(row.get('strike'))
        if abs(actual_strike - final_str) > step:
            entry_print(
                f'[LIVE SELECT] leg={leg_id} method=AtmMultiplier '
                f'target={final_str} nearest={actual_strike} gap={abs(actual_strike - final_str)} > step={step} — skipped'
            )
            return None
        pct = round((multiplier - 1) * 100, 4)
        entry_print(f'[LIVE SELECT] leg={leg_id} method=AtmMultiplier atm={atm} mult={multiplier} pct={pct:+.4g}% → strike={final_str}')
        return _result(row, {'atm_strike': atm, 'multiplier': multiplier})

    # ── PremiumRange ──────────────────────────────────────────────────────────
    if 'PremiumRange' in entry_kind:
        lower = _safe_float(sp.get('LowerRange') or 0)
        upper = _safe_float(sp.get('UpperRange') or 0)
        mid   = (lower + upper) / 2
        valid = [r for r in rows if lower <= _safe_float(r.get('ltp')) <= upper]
        if not valid:
            entry_print(f'[LIVE SELECT] leg={leg_id} method=PremiumRange no strikes in [{lower},{upper}] — skipped')
            return None
        row = min(valid, key=lambda r: abs(_safe_float(r.get('ltp')) - mid))
        entry_print(f'[LIVE SELECT] leg={leg_id} method=PremiumRange [{lower},{upper}] mid={mid} → strike={row.get("strike")} ltp={row.get("ltp")}')
        return _result(row, {'lower_range': lower, 'upper_range': upper})

    # ── PremiumGEQ / PremiumLTE / Premium ─────────────────────────────────────
    if 'Premium' in entry_kind:
        target_val = _safe_float(strike_param_raw) if not isinstance(strike_param_raw, dict) else 0.0
        ek_lower   = entry_kind.lower()
        is_geq     = 'geq' in ek_lower
        is_lte     = 'lte' in ek_lower or 'leq' in ek_lower
        is_closest = not is_geq and not is_lte  # plain EntryByPremium → Closest Premium

        if is_closest:
            # pick the strike whose ltp is absolutely nearest to target (above OR below)
            below = sorted(
                [r for r in rows if _safe_float(r.get('ltp')) <= target_val],
                key=lambda r: _safe_float(r.get('ltp')), reverse=True
            )
            above = sorted(
                [r for r in rows if _safe_float(r.get('ltp')) >= target_val],
                key=lambda r: _safe_float(r.get('ltp'))
            )
            b = below[0] if below else None
            a = above[0] if above else None
            if not b and not a:
                entry_print(f'[LIVE SELECT] leg={leg_id} method=ClosestPremium target={target_val} — no strikes found, skipped')
                return None
            if not b:
                row = a
            elif not a:
                row = b
            else:
                diff_b = abs(_safe_float(b.get('ltp')) - target_val)
                diff_a = abs(_safe_float(a.get('ltp')) - target_val)
                row = b if diff_b <= diff_a else a  # on tie pick below (conservative)
            entry_print(f'[LIVE SELECT] leg={leg_id} method=ClosestPremium target={target_val} → strike={row.get("strike")} ltp={row.get("ltp")} diff={round(abs(_safe_float(row.get("ltp")) - target_val), 2)}')
        elif is_geq:
            candidates = sorted(
                [r for r in rows if _safe_float(r.get('ltp')) >= target_val],
                key=lambda r: _safe_float(r.get('ltp'))
            )
            if not candidates:
                entry_print(f'[LIVE SELECT] leg={leg_id} method=Premium no strike ≥ {target_val} — skipped')
                return None
            row = candidates[0]
            entry_print(f'[LIVE SELECT] leg={leg_id} method=Premium ≥{target_val} → strike={row.get("strike")} ltp={row.get("ltp")}')
        else:
            candidates = sorted(
                [r for r in rows if _safe_float(r.get('ltp')) <= target_val],
                key=lambda r: _safe_float(r.get('ltp')), reverse=True
            )
            if not candidates:
                entry_print(f'[LIVE SELECT] leg={leg_id} method=Premium no strike ≤ {target_val} — skipped')
                return None
            row = candidates[0]
            entry_print(f'[LIVE SELECT] leg={leg_id} method=Premium ≤{target_val} → strike={row.get("strike")} ltp={row.get("ltp")}')
        return _result(row)

    # ── DeltaRange ────────────────────────────────────────────────────────────
    if 'DeltaRange' in entry_kind:
        from features.delta_selector import select_delta_range, print_delta_chain_table  # type: ignore
        lower_pct = _safe_float(sp.get('LowerRange') or 0)
        upper_pct = _safe_float(sp.get('UpperRange') or 0)
        print_delta_chain_table(rows, underlying, '', opt, 'EntryByDeltaRange', leg_id, spot_price)
        chosen = select_delta_range(rows, lower_pct, upper_pct, opt, position, leg_id, spot_price)
        if not chosen:
            return None
        return _result(chosen, {'lower_pct': lower_pct, 'upper_pct': upper_pct,
                                 'selected_delta': _safe_float(chosen.get('delta'))})

    # ── Delta (closest) ───────────────────────────────────────────────────────
    if 'Delta' in entry_kind:
        from features.delta_selector import select_closest_delta, print_delta_chain_table  # type: ignore
        target_pct = _safe_float(strike_param_raw) if not isinstance(strike_param_raw, dict) else 50.0
        print_delta_chain_table(rows, underlying, '', opt, 'EntryByDelta', leg_id, spot_price)
        chosen = select_closest_delta(rows, target_pct, opt, leg_id)
        if not chosen:
            return None
        return _result(chosen, {'target_delta_pct': target_pct,
                                 'selected_delta': _safe_float(chosen.get('delta'))})

    # ── ATM / ATM offset (StrikeType.OTMn / ITMn) ────────────────────────────
    import re as _re
    sp_str   = str(strike_param_raw or '')
    m_otm    = _re.search(r'OTM(\d+)', sp_str)
    m_itm    = _re.search(r'ITM(\d+)', sp_str)
    _raw_offset = int(m_otm.group(1)) if m_otm else (-int(m_itm.group(1)) if m_itm else 0)
    # PE direction is reversed: OTM is below ATM, ITM is above ATM
    offset_n  = -_raw_offset if opt == 'PE' else _raw_offset
    final_str = atm + offset_n * step
    label = f'OTM{abs(_raw_offset)}' if _raw_offset > 0 else (f'ITM{abs(_raw_offset)}' if _raw_offset < 0 else 'ATM')

    # print available strikes near target so mismatch is easy to diagnose
    near = sorted(
        [_safe_float(r.get('strike')) for r in rows
         if abs(_safe_float(r.get('strike')) - final_str) <= step * 2],
    )
    entry_print(f'[LIVE SELECT] leg={leg_id} method={label} spot={spot_price} atm={atm} target={final_str} strikes_near_target={near}')

    row = _row_for_strike(final_str)
    if not row:
        return None
    actual_strike = _safe_float(row.get('strike'))
    if actual_strike != final_str:
        entry_print(
            f'[LIVE SELECT] leg={leg_id} method={label} WARNING: '
            f'target={final_str} NOT in chain → nearest={actual_strike} selected instead'
        )
    entry_print(f'[LIVE SELECT] leg={leg_id} method={label} → strike={actual_strike} ltp={_safe_float(row.get("ltp"))}')
    return _result(row, {'atm_strike': atm, 'offset': offset_n})
