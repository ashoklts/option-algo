"""
Shared debug print toggle for backtest runtime logs.
Set DEBUG_PRINTS = True to enable verbose console output.
Set SHOW_ENTRY_LOGS = True to enable option chain / entry prints.
"""

DEBUG_PRINTS = False

# Controls: LIVE CHAIN, LIVE SELECT, STRIKE CALC, ENTRY MISS,
#           MOMENTUM PENDING, PENDING ENTRY, SNAPSHOT PRE-RESOLVE,
#           LIVE OPTION SUBSCRIBE, LIVE ENTRY SNAPSHOT, ENTRY KITE TOKEN
SHOW_ENTRY_LOGS = False

# Controls: BROKER TICK, BROKER GROUP, BROKER TRADE LOOP, MTM TOTAL,
#           KITE TICK STREAM, ENTRY CHECK, ENTRY MONITOR, ENTRY SKIP,
#           LiveEntryMonitor cache/subscribe prints
SHOW_RUNTIME_LOGS = False


def debug_print(*args, **kwargs):
    if DEBUG_PRINTS:
        print(*args, **kwargs)


def entry_print(*args, **kwargs):
    if SHOW_ENTRY_LOGS:
        print(*args, **kwargs)


def runtime_print(*args, **kwargs):
    if SHOW_RUNTIME_LOGS:
        print(*args, **kwargs)
