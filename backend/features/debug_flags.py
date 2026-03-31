"""
Shared debug print toggle for backtest runtime logs.
Set DEBUG_PRINTS = True to enable verbose console output.
"""

DEBUG_PRINTS = False


def debug_print(*args, **kwargs):
    if DEBUG_PRINTS:
        print(*args, **kwargs)
