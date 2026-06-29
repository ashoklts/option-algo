"""
Fire-and-forget Telegram notifications for live-trading errors.

Two fixed destinations for now (one global admin chat, one global user chat —
per-user chat-ID lookup is deferred until real multi-user auth exists):
  TELEGRAM_BOT_TOKEN          – bot token from @BotFather
  TELEGRAM_ADMIN_CHAT_ID      – chat_id that receives backend/infra-class errors
  TELEGRAM_USER_CHAT_ID       – chat_id that receives order/trade-class errors
  TELEGRAM_NOTIFICATIONS_ENABLED – kill-switch, same on/off convention as LIVE_ORDER_STATUS

This module never raises and never blocks the caller — every send happens on a
daemon thread with a short timeout, so a slow/unreachable Telegram API can't
stall order placement or the monitor/poll loops that call into this.
"""

import logging
import os
import threading
import time

import requests

log = logging.getLogger(__name__)

_TELEGRAM_API_BASE = 'https://api.telegram.org'
_SEND_TIMEOUT_SECONDS = 5
_DEDUP_WINDOW_SECONDS = 45

_dedup_lock = threading.Lock()
_last_sent_at: dict[str, float] = {}


def _env_flag_enabled(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, '')).strip().lower()
    if not raw:
        return default
    return raw in {'1', 'true', 'yes', 'on'}


def _notifications_enabled() -> bool:
    return _env_flag_enabled('TELEGRAM_NOTIFICATIONS_ENABLED', default=False)


def _format_message(event_type: str, message: str, context: dict | None) -> str:
    lines = [f'[{event_type}]', message.strip()]
    if context:
        context_lines = ', '.join(f'{k}={v}' for k, v in context.items() if v not in (None, ''))
        if context_lines:
            lines.append(context_lines)
    return '\n'.join(lines)


def _is_duplicate(dedup_key: str) -> bool:
    now = time.monotonic()
    with _dedup_lock:
        last = _last_sent_at.get(dedup_key)
        if last is not None and (now - last) < _DEDUP_WINDOW_SECONDS:
            return True
        _last_sent_at[dedup_key] = now
        return False


def _send_sync(chat_id: str, text: str) -> None:
    token = str(os.getenv('TELEGRAM_BOT_TOKEN') or '').strip()
    if not token or not chat_id:
        log.warning('[TELEGRAM] skipped — bot token or chat_id not configured. text=%s', text)
        return
    try:
        resp = requests.post(
            f'{_TELEGRAM_API_BASE}/bot{token}/sendMessage',
            json={'chat_id': chat_id, 'text': text},
            timeout=_SEND_TIMEOUT_SECONDS,
        )
        if resp.status_code != 200:
            log.error('[TELEGRAM] send failed status=%s body=%s', resp.status_code, resp.text[:300])
    except Exception as exc:
        log.error('[TELEGRAM] send error: %s', exc)


def _dispatch(chat_env_var: str, event_type: str, message: str, context: dict | None) -> None:
    text = _format_message(event_type, message, context)
    log.info('[TELEGRAM %s] %s', chat_env_var, text.replace('\n', ' | '))
    if not _notifications_enabled():
        return
    dedup_key = f'{chat_env_var}:{event_type}:{(context or {}).get("trade_id", "")}:{(context or {}).get("leg_id", "")}'
    if _is_duplicate(dedup_key):
        return
    chat_id = str(os.getenv(chat_env_var) or '').strip()
    threading.Thread(target=_send_sync, args=(chat_id, text), daemon=True).start()


def notify_admin(event_type: str, message: str, context: dict | None = None) -> None:
    """Backend/infra-class errors — LTP fetch, leg-resolution logic, broker-unreachable polling."""
    _dispatch('TELEGRAM_ADMIN_CHAT_ID', event_type, message, context)


def notify_user(event_type: str, message: str, context: dict | None = None) -> None:
    """Order/trade-class errors — broker rejected an order, strategy paused, etc."""
    _dispatch('TELEGRAM_USER_CHAT_ID', event_type, message, context)


def notify_both(event_type: str, message: str, context: dict | None = None) -> None:
    """Genuinely unclassifiable failures — sent to both admin and user."""
    notify_admin(event_type, message, context)
    notify_user(event_type, message, context)
