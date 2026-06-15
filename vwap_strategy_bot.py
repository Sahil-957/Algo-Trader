"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    VWAP STRATEGY — TRADING BOT                             ║
║            Angel One Smart API  ·  NIFTY F&O  ·  Intraday                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

STRATEGY SUMMARY
────────────────
• Market hours  : 10:30 AM – 3:00 PM IST (runs every 60 seconds)
• Direction     : NIFTY Futures 5-min VWAP → Bullish (CE) or Bearish (PE)
• Option select : ATM+100 CE if bullish  /  ATM−100 PE if bearish
• Entry trigger : Bullish VWAP crossover on the selected option's 5-min chart
                  (prev candle CLOSE < VWAP, curr candle CLOSE > VWAP)
• Entry price   : LTP breaks above the CROSSOVER CANDLE'S HIGH (live, no wait)
• Stop loss     : Low of the crossover candle
• Target        : Entry + 2 × Risk  (1:2 R:R)
• Signal expiry : 2 candles (10 min) after the crossover; discarded if stale
• Trade limit   : ONE active trade at a time
• Risk controls : Max daily loss / max daily profit auto-stop

DEPENDENCIES
────────────
pip install smartapi-python pyotp pandas APScheduler python-dotenv pytz requests

USAGE
─────
1. Copy .env.example → .env and fill in your credentials.
2. Set PAPER_TRADE=True in .env to simulate without placing real orders.
3. Run: python vwap_strategy_bot.py

DISCLAIMER
──────────
This software is for educational purposes. Trading in F&O carries substantial
risk. The author is not responsible for financial losses. Always test in paper
mode first and consult a SEBI-registered advisor before live trading.
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────

import os
import sys
import time
import json
import csv
import logging
import threading
import random
from collections import deque
from pathlib import Path
import logzero
import pyotp
import pytz
import requests
import pandas as pd

from datetime import datetime, timedelta
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler

_smartapi_logzero_error = logzero.logger.error
logzero.logger.error = lambda *args, **kwargs: None
from SmartApi import SmartConnect
try:
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
except Exception:
    SmartWebSocketV2 = None
logzero.logger.error = _smartapi_logzero_error

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  (loaded from .env — never hard-code secrets)
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

# Angel One Smart API credentials
API_KEY        = os.getenv("ANGEL_API_KEY", "")
CLIENT_ID      = os.getenv("ANGEL_CLIENT_ID", "")
MPIN           = os.getenv("ANGEL_MPIN", "")
TOTP_SECRET    = os.getenv("ANGEL_TOTP_SECRET", "")

# Strategy parameters
INDEX           = os.getenv("INDEX", "NIFTY")           # NIFTY / BANKNIFTY / FINNIFTY
ATM_OFFSET      = int(os.getenv("ATM_OFFSET", "100"))   # Points away from ATM
CE_ITM_OFFSET   = int(os.getenv("CE_ITM_OFFSET", str(ATM_OFFSET)))
PE_ITM_OFFSET   = int(os.getenv("PE_ITM_OFFSET", str(ATM_OFFSET)))
LOTS            = int(os.getenv("LOTS", "1"))           # Lots per trade
LOT_SIZE        = int(os.getenv("LOT_SIZE", "75"))      # Fallback lot size when instrument data is unavailable
MAX_TRADES_DAY  = int(os.getenv("MAX_TRADES_DAY", "3")) # Max trades per day
SIGNAL_EXPIRY_CANDLES = int(os.getenv("SIGNAL_EXPIRY_CANDLES", "2"))  # 2 × 5min = 10 min

# Trading window (IST)
MARKET_START    = os.getenv("MARKET_START", "10:30")
MARKET_END      = os.getenv("MARKET_END", "15:00")
CHECK_INTERVAL  = int(os.getenv("CHECK_INTERVAL", "60"))  # Seconds between each cycle
LTP_CHECK_INTERVAL = int(os.getenv("LTP_CHECK_INTERVAL", "1"))  # Seconds between LTP checks
USE_WEBSOCKET = os.getenv("USE_WEBSOCKET", "False").lower() == "true"

# Risk controls
MAX_DAILY_LOSS   = float(os.getenv("MAX_DAILY_LOSS", "5000"))    # ₹ — bot stops if hit
MAX_DAILY_PROFIT = float(os.getenv("MAX_DAILY_PROFIT", "15000")) # ₹ — bot stops if hit

# Paper trading mode: no real orders are placed
PAPER_TRADE = os.getenv("PAPER_TRADE", "True").lower() == "true"
USE_MOCK_DATA = os.getenv("USE_MOCK_DATA", "False").lower() == "true"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# NSE exchange string used in Smart API calls
EXCHANGE = "NFO"

# IST timezone object
IST = pytz.timezone("Asia/Kolkata")

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("vwap_strategy.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("VWAPStrategy")
_recent_logs = deque(maxlen=50)
_telegram_error_guard = False


class _RecentLogHandler(logging.Handler):
    def emit(self, record):
        try:
            _recent_logs.append(self.format(record))
        except Exception:
            pass


_recent_log_handler = _RecentLogHandler()
_recent_log_handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%Y-%m-%d %H:%M:%S"))
log.addHandler(_recent_log_handler)


class _TelegramErrorHandler(logging.Handler):
    def emit(self, record):
        global _telegram_error_guard
        if record.levelno < logging.ERROR:
            return
        if _telegram_error_guard:
            return
        message = self.format(record)
        _telegram_error_guard = True
        try:
            send_telegram(f"⚠️ {message}")
        finally:
            _telegram_error_guard = False


_telegram_error_handler = _TelegramErrorHandler()
_telegram_error_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
log.addHandler(_telegram_error_handler)

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL STATE  (single-process, no DB required for intraday)
# ─────────────────────────────────────────────────────────────────────────────

state = {
    # Active trade tracking
    "active_trade": None,       # Dict with trade details when in a position
    "trade_order_id": None,     # Angel One order ID for the entry order
    "sl_order_id": None,        # Order ID for the SL order
    "sl_status": None,          # Pending / Cancelled / Triggered / Filled
    "target_order_id": None,    # Order ID for the target order

    # Current VWAP crossover signal (discarded after SIGNAL_EXPIRY_CANDLES)
    "signal": None,             # Dict: {entry, sl, target, option_token, option_symbol,
                                #         crossover_candle_index, candle_time}

    # Daily statistics
    "daily_pnl": 0.0,           # Running P&L for today in ₹
    "trades_today": 0,          # Count of completed trades
    "trade_log": [],            # List of dicts for each completed trade
    "lot_size": LOT_SIZE,       # Instrument-derived lot size, fallback to .env

    # Bot lifecycle
    "bot_active": True,         # Set to False to gracefully stop
    "angel": None,              # SmartConnect session object
    "auth_token": None,         # JWT returned by Angel One
    "feed_token": None,         # WebSocket feed token

    # Public dashboard fields
    "direction": None,
    "selected_option": None,
    "selected_option_token": None,
    "selected_strike": None,
    "itm_distance": None,
    "atm_strike": None,
    "setup_status": "Idle",
    "last_check_time": None,
    "bot_running": False,
    "recent_logs": [],
    "market_status": "CLOSED",
    "historical_anchor": "--",
    "ltp_monitor_running": False,
    "api_connected": False,
    "last_api_call": "--",
    "total_api_calls": 0,
    "failed_api_calls": 0,
    "rate_limit_hits": 0,
    "entry_in_progress": False,
    "paper_order_seq": 0,
    "bot_start_time": None,
    "last_ltp_update": "--",
    "websocket_connected": False,
    "websocket_status": "DISCONNECTED",
    "data_feed": "REST",
    "live_ltp": {},
    "selected_option_token": None,
    "filled_quantity": None,
    "remaining_quantity": None,
    "average_fill_price": None,
    "candle_cache": {},

    # Symbol data loaded at startup
    "nifty_futures_token": None,    # Instrument token for NIFTY current-month fut
    "nifty_futures_symbol": None,   # Trading symbol, e.g. "NIFTY25JUNFUT"
}

shared_state = state
state_lock = threading.Lock()
_mock_state = {}
STATE_FILE = Path(__file__).resolve().parent / "state.json"
HOLIDAYS_FILE = Path(__file__).resolve().parent / "nse_holidays.json"
SIGNAL_AUDIT_FILE = Path(__file__).resolve().parent / "signal_audit.csv"
SIGNAL_AUDIT_FIELDS = [
    "timestamp",
    "event",
    "symbol",
    "direction",
    "crossover_time",
    "crossover_high",
    "crossover_low",
    "crossover_close",
    "entry",
    "SL",
    "target",
    "entry_trigger_type",
    "trade_id",
    "reason",
]


def attach_shared_state(external_state: dict):
    """Point the strategy at an external shared dict used by the Flask app."""
    global state, shared_state
    if external_state is None:
        return state

    state = external_state
    shared_state = external_state
    state.setdefault("active_trade", None)
    state.setdefault("trade_order_id", None)
    state.setdefault("sl_order_id", None)
    state.setdefault("sl_status", None)
    state.setdefault("target_order_id", None)
    state.setdefault("signal", None)
    state.setdefault("daily_pnl", 0.0)
    state.setdefault("trades_today", 0)
    state.setdefault("trade_log", [])
    state.setdefault("lot_size", LOT_SIZE)
    state.setdefault("bot_active", True)
    state.setdefault("angel", None)
    state.setdefault("auth_token", None)
    state.setdefault("feed_token", None)
    state.setdefault("nifty_futures_token", None)
    state.setdefault("nifty_futures_symbol", None)
    state.setdefault("direction", None)
    state.setdefault("selected_option", None)
    state.setdefault("selected_strike", None)
    state.setdefault("itm_distance", None)
    state.setdefault("atm_strike", None)
    state.setdefault("setup_status", "Idle")
    state.setdefault("last_check_time", None)
    state.setdefault("bot_running", False)
    state.setdefault("recent_logs", [])
    state.setdefault("market_status", "CLOSED")
    state.setdefault("historical_anchor", "--")
    state.setdefault("ltp_monitor_running", False)
    state.setdefault("api_connected", False)
    state.setdefault("last_api_call", "--")
    state.setdefault("total_api_calls", 0)
    state.setdefault("failed_api_calls", 0)
    state.setdefault("rate_limit_hits", 0)
    state.setdefault("entry_in_progress", False)
    state.setdefault("paper_order_seq", 0)
    state.setdefault("bot_start_time", None)
    state.setdefault("last_ltp_update", "--")
    state.setdefault("websocket_connected", False)
    state.setdefault("websocket_status", "DISCONNECTED")
    state.setdefault("data_feed", "REST")
    state.setdefault("live_ltp", {})
    state.setdefault("selected_option_token", None)
    state.setdefault("filled_quantity", None)
    state.setdefault("remaining_quantity", None)
    state.setdefault("average_fill_price", None)
    state.setdefault("candle_cache", {})
    return state


def effective_lot_size() -> int:
    try:
        return int(state.get("lot_size") or LOT_SIZE)
    except (TypeError, ValueError):
        return LOT_SIZE


def validate_lot_size():
    detected = state.get("lot_size")
    if detected is None:
        log.info(f"Instrument master lot size unavailable; using .env fallback ({LOT_SIZE}).")
        return

    detected = int(detected)
    state["lot_size"] = detected
    if detected != LOT_SIZE:
        log.info(f"Instrument master lot size ({detected}) overrides .env value ({LOT_SIZE}).")
    else:
        log.info(f"Instrument master lot size detected: {detected}.")


def send_telegram(message: str) -> bool:
    """Send a Telegram alert if credentials are configured; fail silently otherwise."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
    }
    try:
        response = requests.post(url, data=payload, timeout=10)
        if response.status_code >= 400:
            log.warning(f"Telegram alert failed: HTTP {response.status_code}")
            return False
        data = response.json()
        if not data.get("ok", False):
            log.warning("Telegram alert failed: API returned not ok")
            return False
        return True
    except Exception as exc:
        log.warning(f"Telegram alert failed: {exc}")
        return False


def _load_nse_holidays() -> set[str]:
    if not HOLIDAYS_FILE.exists():
        return set()

    try:
        data = json.loads(HOLIDAYS_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        log.error(f"Failed to read holiday file {HOLIDAYS_FILE.name}: {exc}", exc_info=True)
        return set()

    holidays = set()
    if isinstance(data, list):
        for item in data:
            if isinstance(item, str):
                holidays.add(item[:10])
            elif isinstance(item, dict):
                for key in ("date", "holiday_date"):
                    if key in item and item[key]:
                        holidays.add(str(item[key])[:10])
                        break
    return holidays


def is_market_open() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False

    holidays = _load_nse_holidays()
    if not holidays:
        return True

    today = now.date().isoformat()
    if today in holidays:
        log.info("Market closed due to holiday")
        return False
    return True


def _previous_market_date(ref_date: datetime | None = None):
    """Return the most recent NSE trading date on or before ref_date."""
    current = (ref_date or datetime.now(IST)).date()
    holidays = _load_nse_holidays()
    while current.weekday() >= 5 or current.isoformat() in holidays:
        current -= timedelta(days=1)
    return current


def _candle_bucket_start(dt: datetime | None = None) -> datetime:
    """Return the start time of the current 5-minute candle bucket."""
    current = dt or datetime.now(IST)
    return current.replace(second=0, microsecond=0, minute=(current.minute // 5) * 5)


def needs_candle_refresh(symbol: str) -> bool:
    """Return True only when a new 5-minute candle bucket has started."""
    cache_entry = state.get("candle_cache", {}).get(symbol)
    if not cache_entry:
        return True

    last_fetch_time = cache_entry.get("last_fetch_time")
    if not isinstance(last_fetch_time, datetime):
        return True

    return _candle_bucket_start(datetime.now(IST)) > _candle_bucket_start(last_fetch_time)


def _get_cached_candles(symbol: str) -> pd.DataFrame | None:
    cache_entry = state.get("candle_cache", {}).get(symbol)
    if not cache_entry:
        return None

    data = cache_entry.get("data")
    if isinstance(data, pd.DataFrame) and not data.empty:
        return data.copy(deep=True)
    return None


def _store_candle_cache(symbol: str, df: pd.DataFrame):
    cache = state.setdefault("candle_cache", {})
    cache[symbol] = {
        "last_fetch_time": datetime.now(IST),
        "data": df.copy(deep=True),
    }


def _serialize_for_state(value):
    if isinstance(value, dict):
        return {k: _serialize_for_state(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialize_for_state(v) for v in value]
    if isinstance(value, tuple):
        return [_serialize_for_state(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat") and not isinstance(value, (str, bytes)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return value


def _restore_state_value(value):
    if isinstance(value, dict):
        restored = {k: _restore_state_value(v) for k, v in value.items()}
        for key in ("candle_time", "entry_time", "exit_time"):
            if key in restored and isinstance(restored[key], str):
                try:
                    restored[key] = datetime.fromisoformat(restored[key])
                except ValueError:
                    pass
        return restored
    if isinstance(value, list):
        return [_restore_state_value(v) for v in value]
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def save_state():
    """Persist the important bot state to disk."""
    keys = [
        "active_trade",
        "signal",
        "daily_pnl",
        "trades_today",
        "trade_log",
        "trade_order_id",
        "sl_order_id",
        "sl_status",
        "target_order_id",
        "lot_size",
        "direction",
        "selected_option",
        "selected_strike",
        "itm_distance",
        "atm_strike",
        "setup_status",
        "entry_in_progress",
        "api_connected",
        "last_api_call",
        "total_api_calls",
        "failed_api_calls",
        "rate_limit_hits",
        "paper_order_seq",
        "bot_start_time",
        "last_ltp_update",
    ]
    payload = {key: _serialize_for_state(state.get(key)) for key in keys}
    payload["saved_at"] = datetime.now(IST).isoformat()
    try:
        STATE_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        log.error(f"Failed to save bot state to {STATE_FILE.name}: {exc}", exc_info=True)


def load_state():
    """Load persisted bot state from disk, if available."""
    if not STATE_FILE.exists():
        return False

    try:
        payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        for key in [
            "active_trade",
            "signal",
            "daily_pnl",
            "trades_today",
            "trade_log",
            "trade_order_id",
            "sl_order_id",
            "sl_status",
            "target_order_id",
            "lot_size",
            "direction",
            "selected_option",
            "selected_strike",
            "itm_distance",
            "atm_strike",
            "setup_status",
            "entry_in_progress",
            "api_connected",
            "last_api_call",
            "total_api_calls",
            "failed_api_calls",
            "rate_limit_hits",
            "paper_order_seq",
            "bot_start_time",
            "last_ltp_update",
        ]:
            if key in payload:
                state[key] = _restore_state_value(payload[key])

        state["daily_pnl"] = float(state.get("daily_pnl") or 0.0)
        state["trades_today"] = int(state.get("trades_today") or 0)
        state["trade_log"] = list(state.get("trade_log") or [])
        if state.get("lot_size") is not None:
            state["lot_size"] = int(state["lot_size"])
        state["entry_in_progress"] = bool(state.get("entry_in_progress"))
        state["api_connected"] = bool(state.get("api_connected"))
        state["total_api_calls"] = int(state.get("total_api_calls") or 0)
        state["failed_api_calls"] = int(state.get("failed_api_calls") or 0)
        state["rate_limit_hits"] = int(state.get("rate_limit_hits") or 0)
        state["paper_order_seq"] = int(state.get("paper_order_seq") or 0)
        return True
    except Exception as exc:
        log.error(f"Failed to load bot state from {STATE_FILE.name}: {exc}", exc_info=True)
        return False


def _mock_base_price(symbol: str) -> float:
    symbol = (symbol or "").upper()
    if "NIFTY" in symbol and "BANK" not in symbol:
        return 24600.0
    if "BANKNIFTY" in symbol:
        return 54500.0
    if "FINNIFTY" in symbol:
        return 23200.0
    if symbol.endswith("CE") or symbol.endswith("PE"):
        return 220.0
    return 1000.0


def _mock_direction_bias(symbol: str) -> int:
    now_bucket = int(datetime.now(IST).timestamp() // 300)
    seed = sum(ord(c) for c in (symbol or ""))
    return 1 if (now_bucket + seed) % 2 == 0 else -1


def generate_mock_candles(symbol: str, n: int = 20) -> pd.DataFrame:
    """Generate realistic-looking OHLCV candles for paper trading."""
    end_time = datetime.now(IST).replace(second=0, microsecond=0)
    end_time = end_time - timedelta(minutes=end_time.minute % 5)
    start_time = end_time - timedelta(minutes=5 * (n - 1))
    base = _mock_base_price(symbol)
    bias = _mock_direction_bias(symbol)

    rng = random.Random(f"{symbol}:{end_time:%Y%m%d%H%M}")
    price = base + bias * rng.uniform(-60, 60)
    rows = []

    for i in range(n):
        ts = start_time + timedelta(minutes=5 * i)
        drift = bias * (0.8 + (i / max(n - 1, 1)) * 1.8)
        if i >= n - 4:
            drift += 2.5
        if i < n // 3:
            drift -= 1.5

        open_price = price + rng.uniform(-1.5, 1.5)
        close_price = max(1.0, open_price + drift + rng.uniform(-1.2, 1.2))
        high_price = max(open_price, close_price) + rng.uniform(0.5, 2.2)
        low_price = max(0.5, min(open_price, close_price) - rng.uniform(0.5, 2.0))
        volume = int(rng.uniform(1200, 5000) * (1 + i / max(n, 1)))
        rows.append(
            {
                "timestamp": ts,
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": volume,
            }
        )
        price = close_price

    df = pd.DataFrame(rows)
    if len(df) >= 2 and symbol.upper().endswith(("CE", "PE")):
        df.loc[df.index[-2], "close"] = round(df.loc[df.index[-2], "close"] - 2.5, 2)
        df.loc[df.index[-1], "close"] = round(df.loc[df.index[-1], "close"] + 4.5, 2)
        df.loc[df.index[-2], "high"] = max(df.loc[df.index[-2], "high"], df.loc[df.index[-2], "close"] + 0.8)
        df.loc[df.index[-1], "high"] = max(df.loc[df.index[-1], "high"], df.loc[df.index[-1], "close"] + 1.0)
        df.loc[df.index[-1], "low"] = min(df.loc[df.index[-1], "low"], df.loc[df.index[-1], "close"] - 2.0)

    return df.sort_values("timestamp").reset_index(drop=True)


def get_mock_ltp(symbol: str) -> float:
    df = _mock_state.get(symbol)
    if df is None or df.empty:
        df = generate_mock_candles(symbol)
        _mock_state[symbol] = df
    last_close = float(df["close"].iloc[-1])
    rng = random.Random(f"ltp:{symbol}:{datetime.now(IST):%Y%m%d%H%M}")
    return round(max(0.5, last_close + rng.uniform(-0.8, 1.2)), 2)


def publish_shared_state(persist: bool = True):
    """Update public fields for the Flask dashboard."""
    state["bot_running"] = bool(state.get("bot_active"))
    state["recent_logs"] = list(_recent_logs)
    state["order_mode"] = "PAPER" if PAPER_TRADE else "LIVE"
    state["data_source"] = "MOCK" if (PAPER_TRADE and USE_MOCK_DATA) else "LIVE"
    state["lot_size"] = int(state.get("lot_size") or LOT_SIZE)
    state["lots"] = LOTS
    state["effective_quantity"] = state["lots"] * state["lot_size"]
    state["market_status"] = "OPEN" if is_market_open() else "CLOSED"
    if state["market_status"] == "OPEN":
        state["historical_anchor"] = "Current Trading Day"
    else:
        state["historical_anchor"] = f"Previous Trading Day ({_previous_market_date().isoformat()})"
    state["ltp_monitor_running"] = bool(state.get("ltp_monitor_running"))
    state["api_connected"] = bool(state.get("api_connected"))
    state["last_api_call"] = state.get("last_api_call") or "--"
    state["total_api_calls"] = int(state.get("total_api_calls") or 0)
    state["failed_api_calls"] = int(state.get("failed_api_calls") or 0)
    state["rate_limit_hits"] = int(state.get("rate_limit_hits") or 0)
    state["entry_in_progress"] = bool(state.get("entry_in_progress"))
    state["setup_status"] = state.get("setup_status") or ("In Trade" if state.get("active_trade") else ("Signal Active" if state.get("signal") else ("Scanning" if state.get("bot_running") else "Idle")))
    state["websocket_connected"] = bool(state.get("websocket_connected"))
    state["websocket_status"] = state.get("websocket_status") or ("CONNECTED" if state["websocket_connected"] else "DISCONNECTED")
    state["data_feed"] = "WEBSOCKET" if (USE_WEBSOCKET and state["websocket_connected"]) else "REST"

    bot_started = state.get("bot_start_time")
    if isinstance(bot_started, str):
        try:
            bot_started = datetime.fromisoformat(bot_started)
        except ValueError:
            bot_started = None
    if isinstance(bot_started, datetime):
        state["bot_uptime"] = str(datetime.now(IST) - bot_started).split(".")[0]
    else:
        state["bot_uptime"] = "--"

    signal = state.get("signal")
    if signal:
        signal_time = _signal_time_as_datetime(signal)
        signal_created_at = _signal_created_at_as_datetime(signal)
        signal_expiry_time = _signal_expiry_time_as_datetime(signal)
        confirmation_candle_start = _confirmation_candle_start_as_datetime(signal)
        waiting_for_confirmation = not _is_after_signal_candle(signal)
        crossover_high = signal.get("crossover_high") or signal.get("signal_high")
        crossover_low = signal.get("crossover_low") or signal.get("signal_low")
        crossover_close = signal.get("crossover_close") or signal.get("signal_close")
        signal_public = {
            "option_symbol": signal.get("option_symbol"),
            "option_token": signal.get("option_token"),
            "option_type": signal.get("option_type"),
            "atm_strike": signal.get("atm_strike"),
            "selected_strike": signal.get("selected_strike"),
            "itm_distance": signal.get("itm_distance"),
            "entry": crossover_high,
            "signal_high": crossover_high,
            "signal_low": crossover_low,
            "signal_close": crossover_close,
            "crossover_high": crossover_high,
            "crossover_low": crossover_low,
            "crossover_close": crossover_close,
            "signal_created_at": signal_created_at.strftime("%Y-%m-%d %H:%M:%S") if isinstance(signal_created_at, datetime) else signal.get("signal_created_at"),
            "signal_expiry_time": signal_expiry_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(signal_expiry_time, datetime) else signal.get("signal_expiry_time"),
            "confirmation_candle_start": confirmation_candle_start.strftime("%Y-%m-%d %H:%M:%S") if isinstance(confirmation_candle_start, datetime) else signal.get("confirmation_candle_start"),
            "signal_time": signal_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(signal_time, datetime) else signal.get("signal_time"),
            "signal_candle_time": signal_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(signal_time, datetime) else signal.get("signal_time"),
            "sl": signal.get("sl"),
            "target": signal.get("target"),
            "risk": signal.get("risk"),
            "direction": signal.get("direction"),
            "crossover_candle_index": signal.get("crossover_candle_index"),
            "candle_time": signal.get("candle_time").strftime("%Y-%m-%d %H:%M:%S") if hasattr(signal.get("candle_time"), "strftime") else signal.get("candle_time"),
            "expiry_candles": SIGNAL_EXPIRY_CANDLES,
            "waiting_for_confirmation": waiting_for_confirmation,
        }
        if signal_public.get("signal_expiry_time"):
            signal_public["expiry_time"] = signal_public["signal_expiry_time"]
    else:
        signal_public = None

    active_trade = state.get("active_trade")
    if active_trade:
        entry_time = active_trade.get("entry_time")
        entry_dt = None
        if isinstance(entry_time, datetime):
            entry_dt = entry_time
        elif isinstance(entry_time, str):
            try:
                parsed_time = datetime.strptime(entry_time, "%H:%M:%S").time()
                entry_dt = datetime.combine(datetime.now(IST).date(), parsed_time)
                entry_dt = IST.localize(entry_dt.replace(tzinfo=None))
            except Exception:
                entry_dt = None

        current_ltp = _safe_float(
            active_trade.get("current_ltp")
            or state.get("live_ltp", {}).get(str(active_trade.get("token")))
            or 0.0,
            0.0,
        )
        avg_fill = _safe_float(
            active_trade.get("average_fill_price")
            or state.get("average_fill_price")
            or active_trade.get("entry_price")
            or 0.0,
            0.0,
        )
        filled_qty = _safe_int(active_trade.get("filled_quantity") or state.get("filled_quantity") or 0, 0)
        if filled_qty <= 0:
            filled_qty = int(LOTS * effective_lot_size())
        pnl_mtm = round((current_ltp - avg_fill) * filled_qty, 2) if current_ltp > 0 and avg_fill > 0 else 0.0
        time_in_trade = "--"
        if entry_dt is not None:
            time_in_trade = str(datetime.now(IST) - entry_dt).split(".")[0]
        trade_public = {
            "trade_id": active_trade.get("trade_id"),
            "symbol": active_trade.get("symbol"),
            "token": active_trade.get("token"),
            "direction": active_trade.get("direction"),
            "entry_price": active_trade.get("entry_price"),
            "entry_trigger_price": active_trade.get("entry_trigger_price") or active_trade.get("crossover_high"),
            "current_ltp": current_ltp if current_ltp > 0 else active_trade.get("current_ltp"),
            "filled_quantity": active_trade.get("filled_quantity"),
            "remaining_quantity": active_trade.get("remaining_quantity"),
            "average_fill_price": active_trade.get("average_fill_price"),
            "sl": active_trade.get("sl"),
            "target": active_trade.get("target"),
            "entry_time": active_trade.get("entry_time"),
            "order_id": active_trade.get("order_id"),
            "sl_order_id": state.get("sl_order_id"),
            "time_in_trade": time_in_trade,
            "unrealized_pnl": pnl_mtm,
            "mtm": pnl_mtm,
            "ltp_last_updated": state.get("last_ltp_update") or "--",
        }
    else:
        trade_public = None

    state.update(
        {
            "signal": signal,
            "public_signal": signal_public,
            "active_trade_public": trade_public,
            "trade_log_public": list(state.get("trade_log", [])),
            "last_check_time": state.get("last_check_time"),
            "sl_status": state.get("sl_status"),
            "api_connected": state.get("api_connected"),
            "last_api_call": state.get("last_api_call"),
            "total_api_calls": state.get("total_api_calls"),
            "failed_api_calls": state.get("failed_api_calls"),
            "rate_limit_hits": state.get("rate_limit_hits"),
            "bot_uptime": state.get("bot_uptime"),
            "setup_status": state.get("setup_status"),
            "entry_in_progress": state.get("entry_in_progress"),
            "filled_quantity": state.get("filled_quantity"),
            "remaining_quantity": state.get("remaining_quantity"),
            "average_fill_price": state.get("average_fill_price"),
            "last_ltp_update": state.get("last_ltp_update"),
        }
    )
    if persist:
        save_state()
    return state


def use_mock_market_data() -> bool:
    return PAPER_TRADE and USE_MOCK_DATA


def auth_enabled() -> bool:
    return not use_mock_market_data()


def _record_api_call(success: bool = True, rate_limit: bool = False):
    state["total_api_calls"] = int(state.get("total_api_calls") or 0) + 1
    if not success:
        state["failed_api_calls"] = int(state.get("failed_api_calls") or 0) + 1
    if rate_limit:
        state["rate_limit_hits"] = int(state.get("rate_limit_hits") or 0) + 1
    state["api_connected"] = bool(success)
    state["last_api_call"] = datetime.now(IST).strftime("%H:%M:%S")


def _update_api_health():
    _record_api_call(success=True)


def _mark_api_failure(rate_limit: bool = False):
    _record_api_call(success=False, rate_limit=rate_limit)


def _is_rate_limit_message(message: object) -> bool:
    text = str(message or "").lower()
    return "access rate" in text or "rate limit" in text or "exceeding access" in text


def _trade_id_now() -> str:
    return f"TRADE-{datetime.now(IST).strftime('%Y%m%d-%H%M%S')}"


def _signal_audit_timestamp(value: object) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    if value is None:
        return "--"
    return str(value)


def _append_signal_audit_row(row: dict) -> None:
    try:
        with state_lock:
            file_exists = SIGNAL_AUDIT_FILE.exists()
            with SIGNAL_AUDIT_FILE.open("a", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=SIGNAL_AUDIT_FIELDS)
                if not file_exists:
                    writer.writeheader()
                writer.writerow({field: row.get(field, "--") for field in SIGNAL_AUDIT_FIELDS})
    except Exception as exc:
        log.error(f"Failed to write signal audit row: {exc}", exc_info=True)


def log_signal_event(
    event: str,
    *,
    symbol: str | None = None,
    direction: str | None = None,
    crossover_time: object = None,
    crossover_high: object = None,
    crossover_low: object = None,
    crossover_close: object = None,
    entry: object = None,
    sl: object = None,
    target: object = None,
    entry_trigger_type: str | None = None,
    reason: str | None = None,
    trade_id: str | None = None,
) -> None:
    _append_signal_audit_row(
        {
            "timestamp": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
            "event": event,
            "symbol": symbol,
            "direction": direction,
            "crossover_time": _signal_audit_timestamp(crossover_time),
            "crossover_high": crossover_high,
            "crossover_low": crossover_low,
            "crossover_close": crossover_close,
            "entry": entry,
            "SL": sl,
            "target": target,
            "entry_trigger_type": entry_trigger_type,
            "trade_id": trade_id,
            "reason": reason,
        }
    )


def log_signal_created(sig: dict) -> None:
    log_signal_event(
        "SIGNAL CREATED",
        symbol=sig.get("option_symbol"),
        direction=sig.get("direction"),
        crossover_time=sig.get("crossover_time") or sig.get("signal_time") or sig.get("candle_time"),
        crossover_high=sig.get("crossover_high") or sig.get("signal_high"),
        crossover_low=sig.get("crossover_low") or sig.get("signal_low"),
        crossover_close=sig.get("crossover_close") or sig.get("signal_close"),
        entry=sig.get("entry") or sig.get("crossover_high") or sig.get("signal_high"),
        sl=sig.get("sl"),
        target=sig.get("target"),
    )


def log_signal_cleared(sig: dict | None, reason: str) -> None:
    log_signal_event(
        "SIGNAL CLEARED",
        symbol=(sig or {}).get("option_symbol"),
        direction=(sig or {}).get("direction"),
        crossover_time=(sig or {}).get("crossover_time") or (sig or {}).get("signal_time") or (sig or {}).get("candle_time"),
        crossover_high=(sig or {}).get("crossover_high") or (sig or {}).get("signal_high"),
        crossover_low=(sig or {}).get("crossover_low") or (sig or {}).get("signal_low"),
        crossover_close=(sig or {}).get("crossover_close") or (sig or {}).get("signal_close"),
        entry=(sig or {}).get("entry") or (sig or {}).get("crossover_high") or (sig or {}).get("signal_high"),
        sl=(sig or {}).get("sl"),
        target=(sig or {}).get("target"),
        reason=reason,
    )


def log_signal_expired(sig: dict | None, reason: str) -> None:
    log_signal_event(
        "SIGNAL EXPIRED",
        symbol=(sig or {}).get("option_symbol"),
        direction=(sig or {}).get("direction"),
        crossover_time=(sig or {}).get("crossover_time") or (sig or {}).get("signal_time") or (sig or {}).get("candle_time"),
        crossover_high=(sig or {}).get("crossover_high") or (sig or {}).get("signal_high"),
        crossover_low=(sig or {}).get("crossover_low") or (sig or {}).get("signal_low"),
        crossover_close=(sig or {}).get("crossover_close") or (sig or {}).get("signal_close"),
        entry=(sig or {}).get("entry") or (sig or {}).get("crossover_high") or (sig or {}).get("signal_high"),
        sl=(sig or {}).get("sl"),
        target=(sig or {}).get("target"),
        reason=reason,
    )


def log_signal_executed(sig: dict, trade_id: str, entry_trigger_price: float, current_ltp: float, reason: str) -> None:
    log_signal_event(
        "TRADE EXECUTED",
        symbol=sig.get("option_symbol"),
        direction=sig.get("direction"),
        crossover_time=sig.get("crossover_time") or sig.get("signal_time") or sig.get("candle_time"),
        crossover_high=sig.get("crossover_high") or sig.get("signal_high"),
        crossover_low=sig.get("crossover_low") or sig.get("signal_low"),
        crossover_close=sig.get("crossover_close") or sig.get("signal_close"),
        entry=entry_trigger_price,
        sl=sig.get("sl"),
        target=sig.get("target"),
        entry_trigger_type="HIGH_BREAKOUT",
        reason=reason,
        trade_id=trade_id,
    )


def log_trade_closed(trade: dict, reason: str) -> None:
    log_signal_event(
        "TRADE CLOSED",
        symbol=trade.get("symbol"),
        direction=trade.get("direction"),
        crossover_time=trade.get("crossover_time") or trade.get("signal_time") or trade.get("candle_time"),
        crossover_high=trade.get("crossover_high") or trade.get("signal_high"),
        crossover_low=trade.get("crossover_low") or trade.get("signal_low"),
        crossover_close=trade.get("crossover_close") or trade.get("signal_close"),
        entry=trade.get("entry_price"),
        sl=trade.get("sl"),
        target=trade.get("target"),
        reason=reason,
        trade_id=trade.get("trade_id"),
    )


def _paper_order_id(transaction_type: str, order_type: str) -> str:
    seq = int(state.get("paper_order_seq") or 0) + 1
    state["paper_order_seq"] = seq
    prefix = "PAPER-SELL"
    tx = (transaction_type or "").upper()
    ot = (order_type or "").upper()
    if tx == "BUY":
        prefix = "PAPER-BUY"
    elif tx == "SELL" and ot in {"SL", "SL-M", "SLM"}:
        prefix = "PAPER-SL"
    return f"{prefix}-{seq:05d}"


def _retry_with_backoff(action_label: str, failure_text: str, func, critical: bool = False):
    delays = [1, 2, 4]
    last_exc = None
    for attempt in range(1, 4):
        log.info(f"[RETRY] Attempt {attempt}/3 for {action_label}")
        try:
            result = func()
            if result is not None:
                return result
        except Exception as exc:
            last_exc = exc
            log.warning(f"{failure_text} (attempt {attempt}/3): {exc}", exc_info=True)
        if attempt < 3:
            time.sleep(delays[attempt - 1])

    _mark_api_failure()
    log.error(failure_text)
    if critical:
        send_telegram(f"⚠️ {failure_text}")
    return None

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1: AUTHENTICATION — Smart API login with TOTP
# ─────────────────────────────────────────────────────────────────────────────

def login() -> bool:
    """
    Authenticate with Angel One Smart API using TOTP-based 2FA.
    Stores the session object and tokens in global state.
    Returns True on success, False on failure.
    """
    log.info("Logging in to Angel One Smart API…")
    try:
        angel = SmartConnect(api_key=API_KEY)

        # Generate the current TOTP code from the shared secret
        totp_code = pyotp.TOTP(TOTP_SECRET).now()
        log.info(f"TOTP generated: {totp_code}")

        # Perform the login — this returns a session JSON
        session = angel.generateSession(CLIENT_ID, MPIN, totp_code)

        if session.get("status") is False:
            log.error(f"Login failed: {session.get('message', 'Unknown error')}")
            return False

        state["angel"]      = angel
        state["auth_token"] = session["data"]["jwtToken"]
        state["feed_token"] = session["data"]["feedToken"]
        _update_api_health()

        log.info(f"Login successful. Client: {CLIENT_ID}")
        return True

    except Exception as exc:
        log.error(f"Login exception: {exc}", exc_info=True)
        return False


def refresh_session():
    """
    Re-authenticate to keep the session alive.
    Angel One tokens expire every few hours; call this if API starts returning
    auth errors.
    """
    log.info("Refreshing API session…")
    login()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2: INSTRUMENT LOOKUP
# ─────────────────────────────────────────────────────────────────────────────

def download_instruments() -> pd.DataFrame:
    """
    Download the full NSE F&O instrument master from Angel One.
    Returns a DataFrame with columns: symbol, token, name, expiry, strike,
    lotsize, instrumenttype, exch_seg.
    Caches the file locally for the current day to avoid repeat downloads.
    """
    cache_file = f"instruments_{datetime.now(IST).strftime('%Y%m%d')}.json"

    if os.path.exists(cache_file):
        log.info(f"Loading instruments from cache: {cache_file}")
        with open(cache_file) as f:
            data = json.load(f)
    else:
        log.info("Downloading instrument master from Angel One…")
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        with open(cache_file, "w") as f:
            json.dump(data, f)
        log.info(f"Instruments saved to {cache_file}")
        _update_api_health()

    df = pd.DataFrame(data)
    df = df[df["exch_seg"] == "NFO"]   # Keep only F&O instruments
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce") / 100  # Angel stores strike × 100
    df["expiry"] = pd.to_datetime(df["expiry"], format="%d%b%Y", errors="coerce")
    return df


def get_nearest_expiry(df: pd.DataFrame, instrument_type: str) -> datetime:
    """Return the nearest future expiry date for the given instrument type (FUTIDX / OPTIDX)."""
    today = datetime.now(IST).replace(tzinfo=None).date()
    future_expiries = df[
        (df["instrumenttype"] == instrument_type) &
        (df["name"] == INDEX) &
        (df["expiry"].dt.date >= today)
    ]["expiry"].dropna().unique()
    # Use sorted() — works on both DatetimeArray and ndarray (pandas version safe)
    future_expiries = sorted(future_expiries)
    return pd.Timestamp(future_expiries[0])


def resolve_nifty_futures(df: pd.DataFrame) -> bool:
    """
    Find the current-month NIFTY Futures contract and store its token.
    """
    try:
        expiry = get_nearest_expiry(df, "FUTIDX")
        mask = (
            (df["name"] == INDEX) &
            (df["instrumenttype"] == "FUTIDX") &
            (df["expiry"] == expiry)
        )
        row = df[mask].iloc[0]
        state["nifty_futures_token"]  = row["token"]
        state["nifty_futures_symbol"] = row["symbol"]
        detected_lot_size = int(row.get("lotsize") or LOT_SIZE)
        state["lot_size"] = detected_lot_size
        log.info(f"Detected lot size for {INDEX}: {detected_lot_size}")
        log.info(f"{INDEX} Futures: {row['symbol']} | Token: {row['token']} | Expiry: {expiry.date()}")
        log.info(f"Index: {INDEX}")
        log.info(f"Configured Lots: {LOTS}")
        log.info(f"Detected Lot Size: {detected_lot_size}")
        log.info(f"Effective Quantity: {LOTS * detected_lot_size}")
        return True
    except Exception as exc:
        log.error(f"Could not resolve NIFTY Futures: {exc}", exc_info=True)
        return False


def find_option_token(df: pd.DataFrame, strike: float, option_type: str) -> dict:
    """
    Find the nearest-expiry option contract for the given strike and type (CE/PE).
    Returns dict with 'symbol' and 'token'.
    """
    expiry = get_nearest_expiry(df, "OPTIDX")
    mask = (
        (df["name"] == INDEX) &
        (df["instrumenttype"] == "OPTIDX") &
        (df["expiry"] == expiry) &
        (df["symbol"].str.endswith(option_type)) &
        (df["strike"] == float(strike))
    )
    matches = df[mask]
    if matches.empty:
        # Fallback: find closest available strike
        candidates = df[
            (df["name"] == INDEX) &
            (df["instrumenttype"] == "OPTIDX") &
            (df["expiry"] == expiry) &
            (df["symbol"].str.endswith(option_type))
        ].copy()
        candidates["strike_diff"] = (candidates["strike"] - strike).abs()
        matches = candidates.sort_values("strike_diff").head(1)

    row = matches.iloc[0]
    return {"symbol": row["symbol"], "token": row["token"], "strike": row["strike"]}

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3: MARKET DATA — 5-min candles & VWAP
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_candles_once(token: str, symbol: str, exchange: str = "NFO",
                        interval: str = "FIVE_MINUTE", lookback_days: int = 1) -> pd.DataFrame:
    """
    Fetch historical OHLCV candles from Angel One Smart API.

    Parameters
    ----------
    token        : Instrument token (string)
    symbol       : Trading symbol (string)
    exchange     : Exchange code ('NFO', 'NSE', etc.)
    interval     : Candle interval — 'ONE_MINUTE', 'FIVE_MINUTE', etc.
    lookback_days: How many calendar days of data to request

    Returns
    -------
    DataFrame with columns: timestamp, open, high, low, close, volume
    Sorted ascending by timestamp. Returns empty DataFrame on error.
    """
    if use_mock_market_data():
        log.info(f"[MOCK DATA] Using generated candles for {symbol}")
        mock_df = generate_mock_candles(symbol)
        _mock_state[symbol] = mock_df
        return mock_df

    if PAPER_TRADE:
        log.info(f"[LIVE DATA] Fetching real candles for {symbol}")

    angel = state["angel"]
    if angel is None:
        log.error(f"Angel One session is unavailable for live candle fetch: {symbol}")
        return pd.DataFrame()

    lookbacks = [lookback_days, 2, 3, 5, 7]
    tried = []
    anchor_date = _previous_market_date()
    market_open_now = is_market_open()
    for days in dict.fromkeys(lookbacks):
        try:
            from_dt = IST.localize(
                datetime.combine(anchor_date - timedelta(days=days - 1), datetime.min.time())
            ).replace(hour=9, minute=15, second=0, microsecond=0)
            if market_open_now:
                to_dt = datetime.now(IST)
            else:
                to_dt = IST.localize(
                    datetime.combine(anchor_date, datetime.min.time())
                ).replace(hour=15, minute=30, second=0, microsecond=0)
            retry_params = {
                "exchange": exchange,
                "symboltoken": token,
                "interval": interval,
                "fromdate": from_dt.strftime("%Y-%m-%d %H:%M"),
                "todate": to_dt.strftime("%Y-%m-%d %H:%M"),
            }
            tried.append(days)
            resp = angel.getCandleData(retry_params)
            if resp.get("status") is False:
                message = resp.get("message")
                _mark_api_failure(rate_limit=_is_rate_limit_message(message))
                log.warning(f"Candle fetch failed for {symbol} (lookback {days}d): {message}")
                continue

            candles = resp.get("data", [])
            if not candles:
                _mark_api_failure(rate_limit=_is_rate_limit_message(resp.get("message")))
                log.warning(f"No candle data returned for {symbol} (lookback {days}d)")
                continue

            df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric)
            df = df.sort_values("timestamp").reset_index(drop=True)
            _update_api_health()
            return df
        except Exception as exc:
            _mark_api_failure(rate_limit=_is_rate_limit_message(exc))
            log.warning(f"Exception fetching candles for {symbol} (lookback {days}d): {exc}", exc_info=True)

    log.error(f"Unable to fetch live candles for {symbol} after retries: {tried}")
    return pd.DataFrame()


def fetch_candles(token: str, symbol: str, exchange: str = "NFO",
                  interval: str = "FIVE_MINUTE", lookback_days: int = 1) -> pd.DataFrame:
    """Fetch candles with retry/backoff protection."""
    if not needs_candle_refresh(symbol):
        cached_df = _get_cached_candles(symbol)
        if cached_df is not None:
            log.info("[CACHE HIT] Using cached candles")
            return cached_df

    log.info("[CACHE MISS] Fetching new candles")

    if use_mock_market_data():
        df = _fetch_candles_once(token, symbol, exchange=exchange, interval=interval, lookback_days=lookback_days)
        if isinstance(df, pd.DataFrame) and not df.empty:
            _store_candle_cache(symbol, df)
        return df

    def _call():
        df = _fetch_candles_once(token, symbol, exchange=exchange, interval=interval, lookback_days=lookback_days)
        return df if isinstance(df, pd.DataFrame) and not df.empty else None

    result = _retry_with_backoff(
        "candle fetch",
        f"Critical candle fetch failure for {symbol}",
        _call,
        critical=True,
    )
    if isinstance(result, pd.DataFrame) and not result.empty:
        _store_candle_cache(symbol, result)
        return result
    return pd.DataFrame()


def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    """
    Calculate VWAP (Volume Weighted Average Price) on a candle DataFrame.

    VWAP = Cumulative(Typical Price × Volume) / Cumulative(Volume)
    Typical Price = (High + Low + Close) / 3

    Resets each trading day (uses only today's candles).
    Returns a pd.Series of VWAP values aligned with df's index.
    """
    df = df.copy()
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["tp_vol"]        = df["typical_price"] * df["volume"]

    # Cumulate from start of session (intraday VWAP resets daily)
    df["cum_tp_vol"] = df["tp_vol"].cumsum()
    df["cum_vol"]    = df["volume"].cumsum()

    vwap = df["cum_tp_vol"] / df["cum_vol"]
    return vwap


def _get_ltp_once(token: str, symbol: str, exchange: str = "NFO") -> float:
    """
    Fetch the Last Traded Price (LTP) for a given instrument.
    Returns 0.0 on failure.
    """
    if use_mock_market_data():
        log.info(f"[MOCK DATA] Using generated LTP for {symbol}")
        return get_mock_ltp(symbol)

    try:
        log.info(f"[LIVE DATA] Fetching real LTP for {symbol}")
        resp = state["angel"].ltpData(exchange, symbol, token)
        if resp.get("status"):
            _update_api_health()
            return float(resp["data"]["ltp"])
        message = resp.get("message")
        _mark_api_failure(rate_limit=_is_rate_limit_message(message))
        log.warning(f"LTP fetch failed for {symbol}: {message}")
        return 0.0
    except Exception as exc:
        _mark_api_failure(rate_limit=_is_rate_limit_message(exc))
        log.error(f"Exception getting LTP for {symbol}: {exc}", exc_info=True)
        return 0.0


def _get_cached_ltp(token: str | None, symbol: str | None) -> float | None:
    cached = state.get("live_ltp") or {}
    for key in (token, symbol):
        if key is None:
            continue
        value = cached.get(str(key))
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None


def get_ltp(token: str, symbol: str, exchange: str = "NFO") -> float:
    """Fetch LTP with retry/backoff protection."""
    cached = _get_cached_ltp(token, symbol)
    if cached is not None and USE_WEBSOCKET:
        return cached

    if use_mock_market_data():
        return _get_ltp_once(token, symbol, exchange=exchange)

    def _call():
        if USE_WEBSOCKET:
            cached_inner = _get_cached_ltp(token, symbol)
            if cached_inner is not None:
                return cached_inner
        value = _get_ltp_once(token, symbol, exchange=exchange)
        return value if value > 0 else None

    result = _retry_with_backoff(
        "LTP fetch",
        f"Critical LTP fetch failure for {symbol}",
        _call,
        critical=True,
    )
    return float(result) if result is not None else 0.0


_websocket_client = None
_websocket_thread = None
_websocket_stop_event = threading.Event()
_websocket_lock = threading.Lock()
_desired_ws_tokens = set()


def _ws_is_enabled() -> bool:
    return USE_WEBSOCKET and SmartWebSocketV2 is not None and auth_enabled()


def _ws_exchange_type_for_token(_: str) -> int:
    return SmartWebSocketV2.NSE_FO if SmartWebSocketV2 else 2


def _set_live_ltp(token: str | None, price: float, symbol: str | None = None):
    if token is None:
        return
    live_ltp = state.setdefault("live_ltp", {})
    live_ltp[str(token)] = price
    if symbol:
        live_ltp[str(symbol)] = price


def _parse_ws_ltp(raw_price) -> float:
    price = _safe_float(raw_price, 0.0)
    if price <= 0:
        return 0.0
    if price > 1000:
        return round(price / 100.0, 2)
    return round(price, 2)


def _ws_on_open(wsapp):
    state["websocket_connected"] = True
    state["websocket_status"] = "CONNECTED"
    _update_api_health()
    publish_shared_state()
    _apply_websocket_subscriptions(force=True)
    log.info("[WEBSOCKET] Connected")


def _ws_on_close(wsapp, *args):
    state["websocket_connected"] = False
    state["websocket_status"] = "DISCONNECTED"
    publish_shared_state()
    log.info("[WEBSOCKET] Disconnected")


def _ws_on_error(wsapp, error):
    state["websocket_connected"] = False
    state["websocket_status"] = "DISCONNECTED"
    log.error(f"[WEBSOCKET] Error: {error}", exc_info=True)


def _ws_on_data(wsapp, parsed_message):
    token = parsed_message.get("token")
    ltp = _parse_ws_ltp(parsed_message.get("last_traded_price"))
    if not token or ltp <= 0:
        return
    symbol = state.get("ws_token_symbol_map", {}).get(str(token))
    _set_live_ltp(str(token), ltp, symbol=symbol)
    _update_api_health()


def _desired_websocket_tokens() -> set[str]:
    tokens = set()
    fut_token = state.get("nifty_futures_token")
    if fut_token:
        tokens.add(str(fut_token))
    opt_token = state.get("selected_option_token")
    if opt_token:
        tokens.add(str(opt_token))
    active_trade = state.get("active_trade") or {}
    if isinstance(active_trade, dict) and active_trade.get("token"):
        tokens.add(str(active_trade["token"]))
    signal = state.get("signal") or {}
    if isinstance(signal, dict) and signal.get("option_token"):
        tokens.add(str(signal["option_token"]))
    return tokens


def _apply_websocket_subscriptions(force: bool = False):
    if not _ws_is_enabled():
        return

    global _websocket_client, _desired_ws_tokens
    desired = _desired_websocket_tokens()
    state.setdefault("ws_token_symbol_map", {})
    symbol_map = state["ws_token_symbol_map"]

    if not force and desired == _desired_ws_tokens:
        return

    with _websocket_lock:
        client = _websocket_client
        if client is None:
            _desired_ws_tokens = desired
            return

        to_add = desired - _desired_ws_tokens
        to_remove = _desired_ws_tokens - desired
        if not to_add and not to_remove and not force:
            return

        if to_remove:
            token_list = [{"exchangeType": _ws_exchange_type_for_token(token), "tokens": [token]} for token in to_remove]
            try:
                client.unsubscribe("VWAP-UNSUB", SmartWebSocketV2.LTP_MODE, token_list)
            except Exception as exc:
                log.warning(f"[WEBSOCKET] Unsubscribe failed: {exc}", exc_info=True)

        if to_add:
            token_list = [{"exchangeType": _ws_exchange_type_for_token(token), "tokens": [token]} for token in to_add]
            try:
                client.subscribe("VWAP-SUB", SmartWebSocketV2.LTP_MODE, token_list)
                for token in to_add:
                    if token == str(state.get("nifty_futures_token")):
                        symbol_map[token] = state.get("nifty_futures_symbol")
                    if state.get("selected_option_token") and token == str(state.get("selected_option_token")):
                        symbol_map[token] = state.get("selected_option")
                    active_trade = state.get("active_trade") or {}
                    if isinstance(active_trade, dict) and token == str(active_trade.get("token")):
                        symbol_map[token] = active_trade.get("symbol")
            except Exception as exc:
                log.warning(f"[WEBSOCKET] Subscribe failed: {exc}", exc_info=True)

        _desired_ws_tokens = desired


def _websocket_loop(stop_event: threading.Event):
    if not _ws_is_enabled():
        return

    global _websocket_client
    while not stop_event.is_set() and state.get("bot_active", True):
        try:
            client = SmartWebSocketV2(
                auth_token=state.get("auth_token"),
                api_key=API_KEY,
                client_code=CLIENT_ID,
                feed_token=state.get("feed_token"),
                max_retry_attempt=1,
            )
            client.on_open = _ws_on_open
            client.on_close = _ws_on_close
            client.on_error = _ws_on_error
            client.on_data = _ws_on_data
            _websocket_client = client
            state["websocket_status"] = "CONNECTING"
            publish_shared_state()
            client.connect()
        except Exception as exc:
            log.error(f"[WEBSOCKET] Connection error: {exc}", exc_info=True)
            state["websocket_connected"] = False
            state["websocket_status"] = "DISCONNECTED"
            publish_shared_state()
        if not stop_event.is_set():
            time.sleep(5)


def start_websocket_feed(stop_event: threading.Event):
    if not _ws_is_enabled():
        state["data_feed"] = "REST"
        state["websocket_status"] = "DISCONNECTED"
        publish_shared_state()
        return None

    thread = threading.Thread(
        target=_websocket_loop,
        args=(stop_event,),
        daemon=True,
        name="ltp-websocket-thread",
    )
    thread.start()
    state["data_feed"] = "WEBSOCKET"
    return thread

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4: ATM CALCULATION
# ─────────────────────────────────────────────────────────────────────────────

def get_atm_strike(ltp: float, step: int = 50) -> float:
    """
    Round LTP to the nearest ATM strike.
    NIFTY options are available at every 50-point interval.
    E.g. LTP=23467 → ATM=23450  (for step=50)
    """
    return round(ltp / step) * step


_TERMINAL_ORDER_STATUSES = {"complete", "completed", "filled", "rejected", "cancelled", "canceled", "cancel"}


def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_order_fill_snapshot(order: dict, requested_qty: int | None = None) -> dict:
    status = str(order.get("status", "unknown")).strip().lower()
    quantity = _safe_int(
        order.get("quantity")
        or order.get("orderquantity")
        or order.get("totalquantity")
        or requested_qty,
        0,
    )
    filled = _safe_int(
        order.get("filledquantity")
        or order.get("filled_quantity")
        or order.get("filledqty")
        or order.get("tradedqty")
        or order.get("tradeqty")
        or 0,
        0,
    )
    remaining = _safe_int(
        order.get("remainingquantity")
        or order.get("remaining_quantity")
        or order.get("pendingquantity")
        or order.get("unfilledquantity")
        or max(quantity - filled, 0),
        0,
    )
    avg_price = _safe_float(
        order.get("averageprice")
        or order.get("average_price")
        or order.get("avgprice")
        or order.get("avg_price")
        or order.get("price")
        or 0.0,
        0.0,
    )
    return {
        "status": status,
        "quantity": quantity,
        "filled_quantity": filled if filled else (quantity - remaining if quantity else 0),
        "remaining_quantity": remaining if remaining >= 0 else 0,
        "average_fill_price": avg_price,
        "raw": order,
    }


def _find_order_row(order_id: str) -> dict | None:
    if state.get("angel") is None:
        return None
    try:
        resp = state["angel"].orderBook()
        if not resp.get("status"):
            return None
        for order in resp.get("data", []) or []:
            if str(order.get("orderid") or order.get("orderId")) == str(order_id):
                _update_api_health()
                return order
        _update_api_health()
        return None
    except Exception as exc:
        log.error(f"Exception fetching order book for {order_id}: {exc}", exc_info=True)
        return None


def _wait_for_live_order_completion(order_id: str, requested_qty: int | None = None, timeout_seconds: int = 30) -> dict:
    """Poll orderBook until a live order reaches a terminal state or times out."""
    deadline = time.time() + timeout_seconds
    last_snapshot = {
        "status": "unknown",
        "quantity": requested_qty or 0,
        "filled_quantity": 0,
        "remaining_quantity": requested_qty or 0,
        "average_fill_price": 0.0,
        "raw": None,
    }

    while time.time() < deadline:
        order_row = _find_order_row(order_id)
        if not order_row:
            time.sleep(1)
            continue
        snapshot = _extract_order_fill_snapshot(order_row, requested_qty=requested_qty)
        last_snapshot = snapshot
        state["filled_quantity"] = snapshot["filled_quantity"]
        state["remaining_quantity"] = snapshot["remaining_quantity"]
        state["average_fill_price"] = snapshot["average_fill_price"]
        if snapshot["status"] in _TERMINAL_ORDER_STATUSES:
            return snapshot
        time.sleep(1)

    return last_snapshot

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5: ORDER MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def _place_order_once(symbol: str, token: str, transaction_type: str,
                      quantity: int, order_type: str = "MARKET",
                      price: float = 0.0, trigger_price: float = 0.0,
                      bypass_entry_guard: bool = False,
                      wait_for_completion: bool = True) -> str | None:
    """
    Place an order via Angel One Smart API.

    Parameters
    ----------
    symbol           : Trading symbol (e.g. "NIFTY25JUN23500CE")
    token            : Instrument token
    transaction_type : "BUY" or "SELL"
    quantity         : Number of units (lots × lot_size)
    order_type       : "MARKET", "LIMIT", "SL", "SL-M"
    price            : Limit price (0 for MARKET)
    trigger_price    : Trigger price for SL orders

    Returns
    -------
    Order ID string on success, None on failure.
    In PAPER_TRADE mode, returns a dummy order ID and logs the intent.
    """
    lot_size = effective_lot_size()
    qty = quantity * lot_size  # Convert lots to quantity
    is_buy = transaction_type.upper() == "BUY"
    state["filled_quantity"] = 0
    state["remaining_quantity"] = qty
    state["average_fill_price"] = None

    if is_buy and state.get("entry_in_progress") and not bypass_entry_guard:
        log.info("[PAPER ORDER]" if PAPER_TRADE else "[ORDER] Duplicate entry blocked by in-progress guard")
        return None

    if PAPER_TRADE:
        dummy_id = _paper_order_id(transaction_type, order_type)
        fill_qty = quantity * lot_size
        state["filled_quantity"] = fill_qty
        state["remaining_quantity"] = 0
        state["average_fill_price"] = price or trigger_price or 0.0
        log.info(
            f"[PAPER ORDER] Simulating {transaction_type} order for {qty} × {symbol} | "
            f"Type: {order_type} | Price: ₹{price:.2f} | Trigger: ₹{trigger_price:.2f} "
            f"→ Order ID: {dummy_id}"
        )
        _update_api_health()
        return dummy_id

    try:
        order_params = {
            "variety":         "NORMAL",
            "tradingsymbol":   symbol,
            "symboltoken":     token,
            "transactiontype": transaction_type,
            "exchange":        EXCHANGE,
            "ordertype":       order_type,
            "producttype":     "INTRADAY",
            "duration":        "DAY",
            "price":           str(round(price, 2)) if price else "0",
            "squareoff":       "0",
            "stoploss":        "0",
            "quantity":        str(qty),
            "triggerprice":    str(round(trigger_price, 2)) if trigger_price else "0",
        }
        resp = state["angel"].placeOrder(order_params)
        if resp.get("status"):
            order_id = resp["data"]["orderid"]
            _update_api_health()
            log.info(
                f"Order placed: {transaction_type} {qty} × {symbol} "
                f"[{order_type}] → ID: {order_id}"
            )
            if wait_for_completion and order_type.upper() not in {"SL", "SL-M", "SLM"}:
                snapshot = _wait_for_live_order_completion(order_id, requested_qty=qty)
                state["filled_quantity"] = snapshot.get("filled_quantity")
                state["remaining_quantity"] = snapshot.get("remaining_quantity")
                state["average_fill_price"] = snapshot.get("average_fill_price")
                state["api_connected"] = snapshot.get("status") in _TERMINAL_ORDER_STATUSES or state.get("api_connected")
            return order_id
        else:
            message = resp.get("message")
            _mark_api_failure(rate_limit=_is_rate_limit_message(message))
            log.error(f"Order failed for {symbol}: {message}")
            return None

    except Exception as exc:
        _mark_api_failure(rate_limit=_is_rate_limit_message(exc))
        log.error(f"Exception placing order for {symbol}: {exc}", exc_info=True)
        return None


def place_order(symbol: str, token: str, transaction_type: str,
                quantity: int, order_type: str = "MARKET",
                price: float = 0.0, trigger_price: float = 0.0,
                bypass_entry_guard: bool = False,
                wait_for_completion: bool = True) -> str | None:
    """Place an order with retry/backoff protection."""
    if PAPER_TRADE:
        return _place_order_once(
            symbol, token, transaction_type, quantity,
            order_type=order_type, price=price, trigger_price=trigger_price,
            bypass_entry_guard=bypass_entry_guard,
            wait_for_completion=wait_for_completion,
        )

    def _call():
        order_id = _place_order_once(
            symbol, token, transaction_type, quantity,
            order_type=order_type, price=price, trigger_price=trigger_price,
            bypass_entry_guard=bypass_entry_guard,
            wait_for_completion=wait_for_completion,
        )
        return order_id if order_id else None

    return _retry_with_backoff(
        "order placement",
        f"Critical order placement failure for {symbol}",
        _call,
        critical=True,
    )


def place_stop_loss_order(symbol: str, token: str, quantity: int, trigger_price: float) -> str | None:
    """Place a broker-side SL-M order, or simulate it in paper mode."""
    order_id = place_order(
        symbol=symbol,
        token=token,
        transaction_type="SELL",
        quantity=quantity,
        order_type="SL-M",
        price=0.0,
        trigger_price=trigger_price,
        wait_for_completion=False,
    )
    if order_id:
        state["sl_order_id"] = order_id
        state["sl_status"] = "PENDING"
        log.info(f"SL order placed | ID: {order_id} | Trigger: ₹{trigger_price:.2f}")
        publish_shared_state()
    return order_id


def cancel_active_sl_order() -> bool:
    """Cancel the current SL order, if one exists."""
    sl_order_id = state.get("sl_order_id")
    if not sl_order_id:
        return False

    success = cancel_order(sl_order_id)
    if success:
        state["sl_status"] = "CANCELLED"
        log.info("SL order cancelled")
        publish_shared_state()
    return success


def cancel_order(order_id: str, variety: str = "NORMAL") -> bool:
    """Cancel a pending order. Returns True on success."""
    if PAPER_TRADE:
        log.info(f"[PAPER] Cancel order: {order_id}")
        _update_api_health()
        return True
    try:
        resp = state["angel"].cancelOrder(order_id, variety)
        success = resp.get("status", False)
        if success:
            _update_api_health()
            log.info(f"Order cancelled: {order_id}")
        else:
            message = resp.get("message")
            _mark_api_failure(rate_limit=_is_rate_limit_message(message))
            log.warning(f"Cancel failed for {order_id}: {message}")
        return success
    except Exception as exc:
        _mark_api_failure(rate_limit=_is_rate_limit_message(exc))
        log.error(f"Exception cancelling order {order_id}: {exc}", exc_info=True)
        return False


def get_order_status(order_id: str) -> str:
    """
    Return the current status string of an order:
    'complete', 'open', 'rejected', 'cancelled', 'pending', 'unknown'
    """
    if PAPER_TRADE:
        _update_api_health()
        return "complete"
    try:
        book = state["angel"].orderBook()
        if book.get("status"):
            for order in book["data"]:
                if order.get("orderid") == order_id:
                    _update_api_health()
                    return order.get("status", "unknown").lower()
        _mark_api_failure(rate_limit=_is_rate_limit_message(book.get("message")))
        return "unknown"
    except Exception as exc:
        _mark_api_failure(rate_limit=_is_rate_limit_message(exc))
        log.error(f"Exception fetching order status for {order_id}: {exc}", exc_info=True)
        return "unknown"


def _normalize_order_status(order: dict) -> str:
    return str(order.get("status", "unknown")).lower()


def _is_open_order_status(status: str) -> bool:
    return status in {"open", "trigger pending", "pending", "triggred", "queued"}


def _extract_positions() -> list[dict]:
    if state.get("angel") is None:
        return []
    try:
        angel = state["angel"]
        if hasattr(angel, "positionBook"):
            resp = angel.positionBook()
        else:
            resp = angel.position()
        if not resp.get("status"):
            _mark_api_failure(rate_limit=_is_rate_limit_message(resp.get("message")))
            return []
        data = resp.get("data", [])
        _update_api_health()
        return data if isinstance(data, list) else []
    except Exception as exc:
        _mark_api_failure(rate_limit=_is_rate_limit_message(exc))
        log.error(f"Exception fetching position book: {exc}", exc_info=True)
        return []


def _extract_orders() -> list[dict]:
    if state.get("angel") is None:
        return []
    try:
        resp = state["angel"].orderBook()
        if not resp.get("status"):
            _mark_api_failure(rate_limit=_is_rate_limit_message(resp.get("message")))
            return []
        data = resp.get("data", [])
        _update_api_health()
        return data if isinstance(data, list) else []
    except Exception as exc:
        _mark_api_failure(rate_limit=_is_rate_limit_message(exc))
        log.error(f"Exception fetching order book: {exc}", exc_info=True)
        return []


def _resolve_open_position_from_broker(positions: list[dict]) -> dict | None:
    for pos in positions:
        try:
            net_qty = int(float(pos.get("netqty") or pos.get("netQty") or pos.get("quantity") or 0))
        except (TypeError, ValueError):
            net_qty = 0
        if net_qty == 0:
            continue

        symbol = pos.get("tradingsymbol") or pos.get("symbol") or pos.get("tradingSymbol")
        token = pos.get("symboltoken") or pos.get("token")
        if not symbol:
            continue

        avg_price = pos.get("averageprice") or pos.get("avgprice") or pos.get("buyavgprice") or pos.get("sellavgprice")
        try:
            avg_price = float(avg_price) if avg_price is not None else None
        except (TypeError, ValueError):
            avg_price = None

        qty_abs = abs(net_qty)
        direction = "LONG" if net_qty > 0 else "SHORT"
        return {
            "symbol": symbol,
            "token": token,
            "direction": direction,
            "quantity": qty_abs,
            "entry_price": avg_price,
        }
    return None


def _find_broker_sl_order(orders: list[dict], trade_symbol: str | None, trade_token: str | None) -> dict | None:
    candidates = []
    for order in orders:
        status = _normalize_order_status(order)
        if not _is_open_order_status(status):
            continue
        tx_type = str(order.get("transactiontype") or order.get("transactionType") or "").upper()
        if tx_type != "SELL":
            continue
        variety = str(order.get("variety") or "").upper()
        order_type = str(order.get("ordertype") or order.get("orderType") or "").upper()
        if order_type not in {"SL", "SL-M", "SLM"} and "SL" not in variety:
            continue

        symbol = order.get("tradingsymbol") or order.get("symbol")
        token = order.get("symboltoken") or order.get("token")
        if trade_symbol and symbol and symbol != trade_symbol:
            continue
        if trade_token and token and str(token) != str(trade_token):
            continue
        candidates.append(order)

    if not candidates:
        return None
    return candidates[-1]


def _find_broker_entry_order(orders: list[dict], trade_symbol: str | None, trade_token: str | None) -> dict | None:
    candidates = []
    for order in orders:
        status = _normalize_order_status(order)
        if status not in {"complete", "filled", "executed"}:
            continue
        tx_type = str(order.get("transactiontype") or order.get("transactionType") or "").upper()
        if tx_type != "BUY":
            continue
        symbol = order.get("tradingsymbol") or order.get("symbol")
        token = order.get("symboltoken") or order.get("token")
        if trade_symbol and symbol and symbol != trade_symbol:
            continue
        if trade_token and token and str(token) != str(trade_token):
            continue
        candidates.append(order)

    if not candidates:
        return None
    return candidates[-1]


def reconcile_broker_state() -> bool:
    """
    Rebuild open trade state from Angel One if the broker has an active position.
    Broker data takes precedence over local persisted state.
    """
    if state.get("angel") is None:
        return False

    positions = _extract_positions()
    orders = _extract_orders()
    open_position = _resolve_open_position_from_broker(positions)

    if not open_position:
        return False

    trade_symbol = open_position["symbol"]
    trade_token = open_position["token"]
    sl_order = _find_broker_sl_order(orders, trade_symbol, trade_token)
    entry_order = _find_broker_entry_order(orders, trade_symbol, trade_token)

    state["active_trade"] = {
        "trade_id": state.get("active_trade", {}).get("trade_id") if isinstance(state.get("active_trade"), dict) else None,
        "symbol": trade_symbol,
        "token": trade_token,
        "direction": open_position["direction"],
        "entry_price": open_position.get("entry_price"),
        "sl": state.get("active_trade", {}).get("sl") if isinstance(state.get("active_trade"), dict) else None,
        "target": state.get("active_trade", {}).get("target") if isinstance(state.get("active_trade"), dict) else None,
        "entry_time": state.get("active_trade", {}).get("entry_time") if isinstance(state.get("active_trade"), dict) else None,
        "filled_quantity": open_position.get("quantity"),
        "remaining_quantity": 0,
        "average_fill_price": open_position.get("entry_price"),
        "order_id": (entry_order.get("orderid") or entry_order.get("orderId")) if entry_order else state.get("trade_order_id"),
    }
    if not state["active_trade"].get("trade_id"):
        state["active_trade"]["trade_id"] = _trade_id_now()
    state["trade_order_id"] = state["active_trade"].get("order_id")

    if sl_order:
        state["sl_order_id"] = sl_order.get("orderid") or sl_order.get("orderId")
        state["sl_status"] = _normalize_order_status(sl_order).upper()
    else:
        state["sl_status"] = "UNKNOWN"

    log.info("Recovered active trade from broker")
    publish_shared_state()
    return True

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6: RISK CONTROLS
# ─────────────────────────────────────────────────────────────────────────────

def check_risk_limits() -> bool:
    """
    Returns True if the bot should STOP trading (a risk limit has been breached).
    Checks:
      • Max daily loss  : daily_pnl ≤ -MAX_DAILY_LOSS
      • Max daily profit: daily_pnl ≥  MAX_DAILY_PROFIT
      • Max trades/day  : trades_today ≥ MAX_TRADES_DAY
    """
    pnl    = state["daily_pnl"]
    trades = state["trades_today"]

    if pnl <= -MAX_DAILY_LOSS:
        log.warning(f"🛑 MAX DAILY LOSS hit: ₹{pnl:.2f} ≤ -₹{MAX_DAILY_LOSS}. Stopping bot.")
        return True

    if pnl >= MAX_DAILY_PROFIT:
        log.info(f"🎯 DAILY PROFIT TARGET hit: ₹{pnl:.2f} ≥ ₹{MAX_DAILY_PROFIT}. Stopping bot.")
        return True

    if trades >= MAX_TRADES_DAY:
        log.info(f"📊 Max trades/day reached: {trades}/{MAX_TRADES_DAY}. No more entries today.")
        return True

    return False


def update_pnl(trade: dict):
    """
    Update daily P&L after a trade is closed.
    trade dict must have 'pnl' key (positive = profit, negative = loss).
    """
    state["daily_pnl"]   += trade["pnl"]
    state["trades_today"] += 1
    state["trade_log"].append(trade)
    publish_shared_state()
    log.info(
        f"Trade closed | P&L: ₹{trade['pnl']:.2f} | "
        f"Daily P&L: ₹{state['daily_pnl']:.2f} | "
        f"Trades today: {state['trades_today']}"
    )

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7: EXIT MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def exit_active_trade(exit_price: float, reason: str, broker_exit: bool = True):
    """
    Close the active trade at the given exit price and update P&L.
    In live mode: places a MARKET SELL order to square off.
    """
    trade = state["active_trade"]
    if trade is None:
        return

    log.info(f"Exiting trade: {trade['symbol']} | Reason: {reason} | Exit: ₹{exit_price:.2f}")

    # Place the exit order
    if broker_exit:
        place_order(
            symbol           = trade["symbol"],
            token            = trade["token"],
            transaction_type = "SELL",
            quantity         = _safe_int(trade.get("filled_quantity"), LOTS * effective_lot_size()),
            order_type       = "MARKET",
        )

    # Calculate P&L: (exit − entry) × quantity × lot_size
    lot_size = effective_lot_size()
    qty = _safe_int(trade.get("filled_quantity"), LOTS * lot_size)
    pnl = (exit_price - trade["entry_price"]) * qty
    trade_id = trade.get("trade_id") or _trade_id_now()

    completed_trade = {
        "trade_id":     trade_id,
        "date":         datetime.now(IST).strftime("%Y-%m-%d"),
        "symbol":       trade["symbol"],
        "direction":    trade["direction"],
        "entry_price":  trade["entry_price"],
        "exit_price":   exit_price,
        "sl":           trade["sl"],
        "target":       trade["target"],
        "filled_quantity": trade.get("filled_quantity", qty),
        "remaining_quantity": trade.get("remaining_quantity", 0),
        "average_fill_price": trade.get("average_fill_price", trade["entry_price"]),
        "pnl":          round(pnl, 2),
        "result":       "WIN" if pnl > 0 else "LOSS",
        "reason":       reason,
        "entry_time":   trade["entry_time"],
        "exit_time":    datetime.now(IST).strftime("%H:%M:%S"),
    }

    update_pnl(completed_trade)
    log_trade_closed({**trade, **completed_trade}, reason)
    log.info(f"Trade result: {completed_trade['result']} | P&L: ₹{pnl:.2f}")
    if reason == "TARGET HIT":
        send_telegram(f"🎯 TARGET HIT\nProfit: ₹{pnl:.2f}")
    elif reason == "STOP LOSS HIT":
        send_telegram(f"🛑 STOP LOSS HIT\nLoss: ₹{abs(pnl):.2f}")
    elif reason == "EOD SQUAREOFF":
        send_telegram(f"🕒 EOD SQUAREOFF\nP&L: ₹{pnl:.2f}")

    # Clear active trade state
    state["active_trade"]    = None
    state["trade_order_id"]  = None
    state["sl_status"]       = None
    state["target_order_id"] = None
    state["entry_in_progress"] = False
    state["filled_quantity"] = None
    state["remaining_quantity"] = None
    state["average_fill_price"] = None
    if USE_WEBSOCKET:
        _apply_websocket_subscriptions(force=True)
    publish_shared_state()


def check_active_trade_exit(ltp: float):
    """
    Monitor an open position every cycle.
    Exits if LTP hits the target or stop loss.
    """
    trade = state["active_trade"]
    if trade is None:
        return

    symbol = trade["symbol"]
    sl     = trade["sl"]
    target = trade["target"]

    log.info(
        f"Monitoring {symbol} | LTP: ₹{ltp:.2f} | "
        f"SL: ₹{sl:.2f} | Target: ₹{target:.2f}"
    )

    trade["current_ltp"] = ltp
    state["last_ltp_update"] = datetime.now(IST).strftime("%H:%M:%S")
    publish_shared_state()

    if ltp >= target:
        log.info("[LTP MONITOR] Target hit")
        sl_order_id = state.get("sl_order_id")
        exit_active_trade(ltp, reason="TARGET HIT", broker_exit=True)
        if sl_order_id:
            if cancel_order(sl_order_id):
                state["sl_status"] = "CANCELLED"
                state["sl_order_id"] = None
                log.info("SL order cancelled")
                publish_shared_state()

    elif ltp <= sl:
        log.info("[LTP MONITOR] SL hit")
        log.info("SL triggered")
        if PAPER_TRADE:
            exit_active_trade(ltp, reason="STOP LOSS HIT", broker_exit=True)
        else:
            exit_active_trade(ltp, reason="STOP LOSS HIT", broker_exit=False)
        state["sl_order_id"] = None
        state["sl_status"] = "TRIGGERED"
        publish_shared_state()

    # If approaching 3:00 PM, square off to avoid overnight position
    now = datetime.now(IST)
    if now.hour == 14 and now.minute >= 55:
        log.info("Approaching market close (2:55 PM). Squaring off position.")
        exit_active_trade(ltp, reason="EOD SQUAREOFF", broker_exit=True)
        cancel_active_sl_order()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 8: SIGNAL MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def is_signal_expired(signal: dict, now: datetime | None = None) -> bool:
    """Return True when the signal has passed its wall-clock expiry time."""
    return _is_signal_expired(signal, now=now)


def _signal_time_as_datetime(signal: dict) -> datetime | None:
    signal_time = signal.get("signal_time") or signal.get("candle_time")
    if isinstance(signal_time, datetime):
        return signal_time
    if isinstance(signal_time, str):
        try:
            return datetime.fromisoformat(signal_time)
        except ValueError:
            return None
    return None


def _signal_created_at_as_datetime(signal: dict) -> datetime | None:
    created_at = signal.get("signal_created_at")
    if isinstance(created_at, datetime):
        return created_at
    if isinstance(created_at, str):
        try:
            return datetime.fromisoformat(created_at)
        except ValueError:
            return None
    return None


def _is_after_signal_candle(signal: dict, now: datetime | None = None) -> bool:
    confirmation_start = _confirmation_candle_start_as_datetime(signal)
    if confirmation_start is None:
        return False
    current = now or datetime.now(IST)
    return current >= confirmation_start


def _confirmation_candle_start_as_datetime(signal: dict) -> datetime | None:
    start_time = signal.get("confirmation_candle_start")
    if isinstance(start_time, datetime):
        return start_time
    if isinstance(start_time, str):
        try:
            return datetime.fromisoformat(start_time)
        except ValueError:
            return None
    signal_time = _signal_time_as_datetime(signal)
    if signal_time is None:
        return None
    return signal_time + timedelta(minutes=5)


def _signal_expiry_time_as_datetime(signal: dict) -> datetime | None:
    expiry_time = signal.get("signal_expiry_time") or signal.get("expiry_time")
    if isinstance(expiry_time, datetime):
        return expiry_time
    if isinstance(expiry_time, str):
        try:
            return datetime.fromisoformat(expiry_time)
        except ValueError:
            return None
    return None


def _is_signal_expired(signal: dict, now: datetime | None = None) -> bool:
    expiry_time = _signal_expiry_time_as_datetime(signal)
    if expiry_time is None:
        return False
    current = now or datetime.now(IST)
    return current > expiry_time


def _clear_signal(reason: str) -> None:
    if state.get("signal") is None:
        return
    log.info(f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} SIGNAL CLEARED | {reason}")
    log_signal_cleared(state.get("signal"), reason)
    state["signal"] = None
    if USE_WEBSOCKET:
        _apply_websocket_subscriptions(force=True)
    publish_shared_state()


def _completed_candles_only(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) < 2:
        return pd.DataFrame()
    completed = df.iloc[:-1].copy()
    if completed.empty:
        return pd.DataFrame()
    latest = completed.iloc[-1]
    log.info(
        "USING COMPLETED CANDLES ONLY | "
        f"Latest completed candle: {latest['timestamp']} | "
        f"Close: {latest['close']:.2f}"
    )
    return completed.reset_index(drop=True)


def detect_vwap_crossover(df: pd.DataFrame, vwap: pd.Series) -> int | None:
    """
    Detect a strict bullish VWAP crossover using only the latest two completed candles.
    """
    if len(df) < 2:
        return None

    prev_close = df.iloc[-2]["close"]
    curr_close = df.iloc[-1]["close"]
    prev_vwap = vwap.iloc[-2]
    curr_vwap = vwap.iloc[-1]
    crossover = prev_close < prev_vwap and curr_close > curr_vwap
    log.info(
        "VWAP CROSSOVER CHECK | "
        f"Prev Close: {prev_close:.2f} | Prev VWAP: {prev_vwap:.2f} | "
        f"Curr Close: {curr_close:.2f} | Curr VWAP: {curr_vwap:.2f} | "
        f"CROSSOVER = {'TRUE' if crossover else 'FALSE'}"
    )
    return len(df) - 1 if crossover else None


def _signal_entry_price(signal: dict) -> float:
    return _safe_float(signal.get("crossover_high"), _safe_float(signal.get("signal_high"), _safe_float(signal.get("entry"))))

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 9: CORE BOT CYCLE (runs every 60 seconds)
# ─────────────────────────────────────────────────────────────────────────────

def _bot_cycle_impl(instruments_df: pd.DataFrame):
    """
    Main loop executed every CHECK_INTERVAL seconds.

    Sequence:
    1.  Check if within trading window (10:30 – 15:00)
    2.  Check risk limits (daily loss/profit/trade count)
    3.  If active trade exists → check SL/Target; skip entry logic
    4.  Fetch NIFTY Futures 5-min candles → calculate VWAP → determine direction
    5.  Select ATM+100 CE (bullish) or ATM−100 PE (bearish)
    6.  Fetch selected option 5-min candles → calculate option VWAP
    7.  Detect latest VWAP crossover → store/refresh signal
    8.  Check signal expiry; discard stale signals
    9.  Wait for the next candle, then fetch option LTP; if LTP >= signal close → execute BUY
    10. Log every action
    """
    now = datetime.now(IST)
    log.info(f"── Bot cycle at {now.strftime('%H:%M:%S')} ──────────────────────")

    # ── 1. Trading window check ──────────────────────────────────────────────
    start_h, start_m = map(int, MARKET_START.split(":"))
    end_h,   end_m   = map(int, MARKET_END.split(":"))
    market_open  = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
    market_close = now.replace(hour=end_h,   minute=end_m,   second=0, microsecond=0)

    if not is_market_open():
        state["market_status"] = "CLOSED"
        log.info("Market closed due to holiday")
        return
    state["market_status"] = "OPEN"

    if not (market_open <= now <= market_close):
        log.info(f"Outside trading window ({MARKET_START}–{MARKET_END} IST). Skipping.")
        return

    # ── 2. Risk limits ───────────────────────────────────────────────────────
    if check_risk_limits():
        state["bot_active"] = False
        return

    # ── 3. Active trade management is handled by the fast LTP monitor loop ──
    if state["active_trade"] is not None:
        log.info("Active trade present. Strategy scan paused.")
        return

    # ── 4. NIFTY Futures → VWAP → direction ─────────────────────────────────
    fut_token  = state["nifty_futures_token"]
    fut_symbol = state["nifty_futures_symbol"]

    fut_df = fetch_candles(fut_token, fut_symbol, exchange="NFO", interval="FIVE_MINUTE")
    if fut_df.empty:
        log.warning("No NIFTY Futures candle data. Skipping cycle.")
        return

    fut_completed_df = _completed_candles_only(fut_df)
    if fut_completed_df.empty or len(fut_completed_df) < 2:
        log.warning("Not enough completed NIFTY Futures candles. Skipping cycle.")
        return

    fut_vwap = calculate_vwap(fut_completed_df)
    fut_last_close = fut_completed_df["close"].iloc[-1]
    fut_last_vwap  = fut_vwap.iloc[-1]
    log.info(
        f"Latest completed candle | Timestamp: {fut_completed_df['timestamp'].iloc[-1]} | "
        f"Close: {fut_last_close:.2f} | VWAP: {fut_last_vwap:.2f}"
    )

    direction = "BULLISH" if fut_last_close > fut_last_vwap else "BEARISH"
    state["direction"] = direction
    log.info(
        f"NIFTY Futures | Close: ₹{fut_last_close:.2f} | "
        f"VWAP: ₹{fut_last_vwap:.2f} | Direction: {direction}"
    )

    # ── 5. Select option (ATM±100) ───────────────────────────────────────────
    nifty_ltp   = get_ltp(fut_token, fut_symbol, exchange="NFO")
    atm_strike  = get_atm_strike(nifty_ltp)

    if direction == "BULLISH":
        target_strike = atm_strike - CE_ITM_OFFSET
        option_type   = "CE"
        itm_distance = CE_ITM_OFFSET
    else:
        target_strike = atm_strike + PE_ITM_OFFSET
        option_type   = "PE"
        itm_distance = PE_ITM_OFFSET

    state["selected_strike"] = target_strike
    state["itm_distance"] = itm_distance
    log.info(f"ATM: {atm_strike} | Selected: {target_strike} {option_type} | ITM Distance: {itm_distance}")

    option_info = find_option_token(instruments_df, target_strike, option_type)
    opt_symbol  = option_info["symbol"]
    opt_token   = option_info["token"]
    state["selected_option"] = opt_symbol
    state["selected_option_token"] = opt_token
    state["atm_strike"] = atm_strike
    log.info(f"Option: {opt_symbol} | Token: {opt_token}")
    if USE_WEBSOCKET:
        state.setdefault("ws_token_symbol_map", {})[str(opt_token)] = opt_symbol
        _apply_websocket_subscriptions(force=True)

    existing_signal = state.get("signal")
    if existing_signal:
        if existing_signal.get("direction") and existing_signal.get("direction") != direction:
            _clear_signal("DIRECTION CHANGED")
        elif existing_signal.get("option_symbol") and existing_signal.get("option_symbol") != opt_symbol:
            _clear_signal("SELECTED OPTION CHANGED")

    # ── 6. Option 5-min candles → VWAP ──────────────────────────────────────
    opt_df = fetch_candles(opt_token, opt_symbol, exchange="NFO", interval="FIVE_MINUTE")
    if opt_df.empty or len(opt_df) < 2:
        log.warning(f"Insufficient option candle data for {opt_symbol}. Skipping.")
        return

    opt_completed_df = _completed_candles_only(opt_df)
    if opt_completed_df.empty or len(opt_completed_df) < 2:
        log.warning(f"Not enough completed option candles for {opt_symbol}. Skipping.")
        return

    opt_vwap = calculate_vwap(opt_completed_df)
    log.info(
        f"Option last completed candle | Timestamp: {opt_completed_df['timestamp'].iloc[-1]} | "
        f"Close: ₹{opt_completed_df['close'].iloc[-1]:.2f} | "
        f"VWAP: ₹{opt_vwap.iloc[-1]:.2f}"
    )

    # ── 7. Detect latest VWAP crossover ──────────────────────────────────────
    crossover_idx = detect_vwap_crossover(opt_completed_df, opt_vwap)
    if crossover_idx is not None:
        crossover_candle = opt_completed_df.iloc[crossover_idx]
        signal_high = crossover_candle["high"]
        signal_low = crossover_candle["low"]
        signal_close = crossover_candle["close"]
        signal_time = crossover_candle["timestamp"]
        signal_created_at = now
        confirmation_candle_start = signal_time + timedelta(minutes=5)
        signal_expiry_time = signal_created_at + timedelta(minutes=5 * SIGNAL_EXPIRY_CANDLES)

        # Discard stale crossovers (older than what we already have)
        if (state["signal"] is None or
                crossover_idx > state["signal"]["crossover_candle_index"]):

            state["signal"] = {
                "option_symbol":          opt_symbol,
                "option_token":           opt_token,
                "crossover_candle_index": crossover_idx,
                "candle_time":            signal_time,
                "signal_time":            signal_time,
                "signal_created_at":      signal_created_at,
                "signal_expiry_time":     signal_expiry_time,
                "confirmation_candle_start": confirmation_candle_start,
                "waiting_for_confirmation": True,
                "crossover_time":         signal_time,
                "crossover_close":        round(signal_close, 2),
                "crossover_high":         round(signal_high, 2),
                "crossover_low":          round(signal_low, 2),
                "entry":                  round(signal_high, 2),
                "atm_strike":             atm_strike,
                "selected_strike":        target_strike,
                "itm_distance":           itm_distance,
                "option_type":            option_type,
                "signal_high":            round(signal_high, 2),
                "signal_low":             round(signal_low, 2),
                "signal_close":           round(signal_close, 2),
                "sl":                     round(signal_low, 2),
                "direction":              direction,
            }
            if USE_WEBSOCKET:
                _apply_websocket_subscriptions(force=True)

            log.info(
                f"{signal_created_at.strftime('%Y-%m-%d %H:%M:%S')} SIGNAL CREATED | "
                f"{opt_symbol} | Direction: {direction} | "
                f"Expiry: {signal_expiry_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            log.info(
                f"CONFIRMATION STARTS AT | {confirmation_candle_start.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            log.info(
                f"🔔 New VWAP crossover signal on {opt_symbol} "
                f"(signal candle #{crossover_idx} at {signal_time}) | "
                f"Close: ₹{signal_close:.2f} | Low: ₹{signal_low:.2f} | "
                f"High: ₹{signal_high:.2f}"
            )
            log_signal_created(state["signal"])
    else:
        log.info("No VWAP crossover detected this cycle.")

    # ── 8. Check signal expiry ───────────────────────────────────────────────
    if state["signal"] is not None:
        signal_now = datetime.now(IST)
        if is_signal_expired(state["signal"], signal_now):
            log.info(
                f"{signal_now.strftime('%Y-%m-%d %H:%M:%S')} SIGNAL EXPIRED | "
                f"{state['signal']['option_symbol']}"
            )
            log_signal_expired(state["signal"], "SIGNAL EXPIRED")
            _clear_signal("SIGNAL EXPIRED")
            return

    if state["signal"] is None:
        log.info("No active signal. Waiting for crossover.")


def _execute_signal_entry(sig: dict, opt_ltp: float, now: datetime) -> bool:
    """Execute the breakout entry for a live or paper signal."""
    if not sig or state.get("signal") is None:
        log.info("[LTP MONITOR] No active signal. BUY skipped.")
        return False
    if state.get("active_trade") is not None:
        log.info("[LTP MONITOR] Active trade already exists. BUY skipped.")
        return False
    if state.get("entry_in_progress"):
        log.info("[LTP MONITOR] Entry already in progress. Skipping duplicate BUY.")
        return False
    if _is_signal_expired(sig, now):
        log.info("[LTP MONITOR] Signal expired before BUY. Skipping entry.")
        _clear_signal("SIGNAL EXPIRED")
        return False
    if not _is_after_signal_candle(sig, now):
        log.info("[LTP MONITOR] Waiting for confirmation candle. BUY skipped.")
        return False

    opt_symbol = sig["option_symbol"]
    opt_token = sig["option_token"]
    signal_close = _safe_float(sig.get("signal_close"), _safe_float(sig.get("entry")))
    signal_high = _signal_entry_price(sig)
    signal_low = _safe_float(sig.get("signal_low"), _safe_float(sig.get("sl")))
    state["entry_in_progress"] = True
    trade_id = _trade_id_now()

    try:
        log.info(f"[LTP MONITOR] Breakout detected for {opt_symbol}")
        log.info(f"Trade ID: {trade_id}")
        log.info(
            f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} TRADE EXECUTED | "
            f"{opt_symbol} | Trade ID: {trade_id}"
        )
        log.info(
            f"WAITING FOR HIGH BREAKOUT | Current LTP: {opt_ltp:.2f} | "
            f"Breakout Level: {signal_high:.2f}"
        )
        log.info(
            f"BUY EXECUTED: Reason: High breakout confirmation | "
            f"LTP: {opt_ltp:.2f} | Breakout Level: {signal_high:.2f}"
        )
        log.info(f"🚀 CONFIRMED! LTP ₹{opt_ltp:.2f} >= Signal Close ₹{signal_close:.2f} — placing BUY order")

        order_id = place_order(
            symbol=opt_symbol,
            token=opt_token,
            transaction_type="BUY",
            quantity=LOTS,
            order_type="MARKET",
            bypass_entry_guard=True,
        )

        if not order_id:
            log.error("Order placement failed. Will retry on next breakout.")
            return False

        fill_qty = _safe_int(state.get("filled_quantity"), LOTS * effective_lot_size())
        avg_fill = _safe_float(state.get("average_fill_price"), opt_ltp)
        remaining_qty = _safe_int(state.get("remaining_quantity"), 0)
        if fill_qty <= 0:
            log.error("BUY order did not fill. Will retry on next breakout.")
            return False

        entry_price = signal_high
        risk = max(entry_price - signal_low, 0.0)
        target = entry_price + (2 * risk)

        state["active_trade"] = {
            "trade_id":     trade_id,
            "symbol":       opt_symbol,
            "token":        opt_token,
            "direction":    sig["direction"],
            "crossover_time": sig.get("crossover_time") or sig.get("signal_time") or sig.get("candle_time"),
            "crossover_close": sig.get("crossover_close") or sig.get("signal_close"),
            "crossover_high": sig.get("crossover_high") or sig.get("signal_high"),
            "crossover_low": sig.get("crossover_low") or sig.get("signal_low"),
            "entry_trigger_price": signal_high,
            "entry_price":  entry_price,
            "current_ltp":  opt_ltp,
            "sl":           signal_low,
            "target":       target,
            "entry_time":   now.strftime("%H:%M:%S"),
            "order_id":     order_id,
            "filled_quantity": fill_qty,
            "remaining_quantity": remaining_qty,
            "average_fill_price": entry_price,
        }
        state["last_ltp_update"] = datetime.now(IST).strftime("%H:%M:%S")
        state.setdefault("ws_token_symbol_map", {})[str(opt_token)] = opt_symbol
        state["trade_order_id"] = order_id
        _clear_signal("TRADE EXECUTED")

        sl_order_id = place_stop_loss_order(
            symbol=opt_symbol,
            token=opt_token,
            quantity=fill_qty,
            trigger_price=signal_low,
        )
        if not sl_order_id and not PAPER_TRADE:
            log.error("Failed to place broker-side SL order. Squaring off immediately.")
            exit_active_trade(opt_ltp, reason="SL ORDER FAILED", broker_exit=True)
            return False

        log.info(
            f"✅ Trade opened: {opt_symbol} @ ₹{opt_ltp:.2f} | "
            f"SL: ₹{signal_low:.2f} | Target: ₹{target:.2f} | "
            f"R:R 1:2 | Order ID: {order_id}"
        )
        send_telegram(
            f"🚀 BUY {opt_symbol}\n"
            f"Trade ID: {trade_id}\n"
            f"Entry: {entry_price:.2f}\n"
            f"SL: {signal_low:.2f}\n"
            f"Target: {target:.2f}"
        )
        if USE_WEBSOCKET:
            _apply_websocket_subscriptions(force=True)
        log_signal_executed(
            sig,
            trade_id,
            signal_high,
            opt_ltp,
            "High breakout confirmation",
        )
        publish_shared_state()
        return True
    finally:
        state["entry_in_progress"] = False


def ltp_monitor_loop(stop_event: threading.Event):
    """Fast loop that watches breakout entries and open positions every second."""
    state["ltp_monitor_running"] = True
    publish_shared_state()
    log.info("[LTP MONITOR] Started")

    try:
        while not stop_event.is_set() and state.get("bot_active", True):
            try:
                active_trade = state.get("active_trade")
                if active_trade is not None:
                    ltp = get_ltp(active_trade["token"], active_trade["symbol"])
                    if ltp > 0:
                        check_active_trade_exit(ltp)
                    time.sleep(LTP_CHECK_INTERVAL)
                    continue

                sig = state.get("signal")
                if sig is None:
                    time.sleep(LTP_CHECK_INTERVAL)
                    continue

                if _is_signal_expired(sig, datetime.now(IST)):
                    log.info(
                        f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} SIGNAL EXPIRED | "
                        f"{sig.get('option_symbol')}"
                    )
                    _clear_signal("SIGNAL EXPIRED")
                    time.sleep(LTP_CHECK_INTERVAL)
                    continue

                current_now = datetime.now(IST)
                if not _is_after_signal_candle(sig, current_now):
                    confirmation_start = _confirmation_candle_start_as_datetime(sig)
                    if confirmation_start is not None:
                        log.info(
                            f"{current_now.strftime('%Y-%m-%d %H:%M:%S')} CONFIRMATION CANDLE ACTIVE | "
                            f"Starts At: {confirmation_start.strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                    state["last_ltp_update"] = datetime.now(IST).strftime("%H:%M:%S")
                    time.sleep(LTP_CHECK_INTERVAL)
                    continue

                opt_ltp = get_ltp(sig["option_token"], sig["option_symbol"])
                if opt_ltp <= 0:
                    time.sleep(LTP_CHECK_INTERVAL)
                    continue

                breakout_level = _signal_entry_price(sig)
                if opt_ltp >= breakout_level:
                    log.info(
                        f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} ENTRY TRIGGER TOUCHED | "
                        f"{sig.get('option_symbol')} | LTP: {opt_ltp:.2f} | Trigger: {breakout_level:.2f}"
                    )
                    _execute_signal_entry(sig, opt_ltp, datetime.now(IST))
                else:
                    log.info(
                        f"WAITING FOR HIGH BREAKOUT | Current LTP: {opt_ltp:.2f} | "
                        f"Breakout Level: {breakout_level:.2f}"
                    )
                time.sleep(LTP_CHECK_INTERVAL)
            except Exception as exc:
                log.error(f"[LTP MONITOR] Error: {exc}", exc_info=True)
                time.sleep(LTP_CHECK_INTERVAL)
    finally:
        state["ltp_monitor_running"] = False
        publish_shared_state()
        log.info("[LTP MONITOR] Stopped")


def bot_cycle(instruments_df: pd.DataFrame):
    """Public wrapper that guarantees dashboard state refresh after each cycle."""
    try:
        state["last_check_time"] = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        return _bot_cycle_impl(instruments_df)
    finally:
        publish_shared_state()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 10: DAILY RESET & SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

def print_daily_summary():
    """Print end-of-day P&L and trade summary to log."""
    log.info("═" * 60)
    log.info("END OF DAY SUMMARY")
    log.info("═" * 60)
    log.info(f"Total trades      : {state['trades_today']}")
    log.info(f"Daily P&L         : ₹{state['daily_pnl']:.2f}")

    wins   = [t for t in state["trade_log"] if t["result"] == "WIN"]
    losses = [t for t in state["trade_log"] if t["result"] == "LOSS"]
    log.info(f"Wins              : {len(wins)}")
    log.info(f"Losses            : {len(losses)}")
    if state["trades_today"] > 0:
        log.info(f"Win rate          : {len(wins)/state['trades_today']*100:.1f}%")

    for i, t in enumerate(state["trade_log"], 1):
        log.info(
            f"  Trade {i}: {t['symbol']} | {t['result']} | "
            f"Entry ₹{t['entry_price']:.2f} → Exit ₹{t['exit_price']:.2f} | "
            f"P&L ₹{t['pnl']:.2f} | {t['reason']}"
        )
    log.info("═" * 60)
    send_telegram(
        "📊 DAILY SUMMARY\n"
        f"Trades: {state['trades_today']}\n"
        f"Daily P&L: ₹{state['daily_pnl']:.2f}"
    )
    publish_shared_state()


def reset_daily_state():
    """Reset daily counters at start of each session."""
    state["daily_pnl"]      = 0.0
    state["trades_today"]   = 0
    state["trade_log"]      = []
    state["active_trade"]   = None
    state["signal"]         = None
    state["bot_active"]     = True
    log.info("Daily state reset.")
    publish_shared_state()


def reset_paper_state() -> bool:
    """
    Clear paper-trading runtime state while preserving credentials, instruments, and caches.
    """
    if not PAPER_TRADE:
        log.info("Reset disabled in live trading mode.")
        return False

    state.update(
        {
            "active_trade": None,
            "signal": None,
            "trade_order_id": None,
            "sl_order_id": None,
            "sl_status": None,
            "target_order_id": None,
            "daily_pnl": 0.0,
            "trades_today": 0,
            "trade_log": [],
            "trade_log_public": [],
            "entry_in_progress": False,
            "total_api_calls": 0,
            "failed_api_calls": 0,
            "rate_limit_hits": 0,
            "direction": None,
            "selected_option": None,
            "selected_option_token": None,
            "selected_strike": None,
            "itm_distance": None,
            "atm_strike": None,
            "public_signal": None,
            "active_trade_public": None,
            "last_ltp_update": "--",
            "setup_status": "Idle",
            "last_check_time": None,
            "sl_status": None,
            "bot_active": False,
            "bot_running": False,
            "bot_start_time": None,
            "ltp_monitor_running": False,
            "websocket_connected": False,
            "websocket_status": "DISCONNECTED",
            "data_feed": "REST",
            "live_ltp": {},
            "filled_quantity": None,
            "remaining_quantity": None,
            "average_fill_price": None,
        }
    )

    try:
        if STATE_FILE.exists():
            STATE_FILE.unlink()
        log.info("Paper state reset successfully.")
    except Exception as exc:
        log.error(f"Failed to delete {STATE_FILE.name}: {exc}", exc_info=True)

    publish_shared_state(persist=False)
    return True


def start_ltp_monitor_thread(stop_event: threading.Event) -> threading.Thread:
    """Launch the fast LTP monitor loop as a daemon thread."""
    thread = threading.Thread(
        target=ltp_monitor_loop,
        args=(stop_event,),
        daemon=True,
        name="ltp-monitor-thread",
    )
    thread.start()
    return thread


def run_bot_service(external_state: dict | None = None, stop_event: threading.Event | None = None):
    """
    Run the strategy in a background-friendly loop for the Flask app.
    """
    if external_state is not None:
        attach_shared_state(external_state)

    stop_event = stop_event or threading.Event()
    state["bot_active"] = True
    recovered = load_state()
    if recovered:
        log.info("Recovered bot state from state.json")
    state["bot_start_time"] = datetime.now(IST).isoformat()
    state["last_ltp_update"] = "--"
    state["entry_in_progress"] = False
    publish_shared_state()

    log.info("Starting bot service loop.")
    log.info(f"Order mode: {'PAPER' if PAPER_TRADE else 'LIVE'}")
    log.info(f"Data source: {'MOCK' if use_mock_market_data() else 'LIVE'}")
    log.info(f"Authentication: {'ENABLED' if auth_enabled() else 'DISABLED'}")
    send_telegram("🚀 Bot started")

    if auth_enabled() and not all([API_KEY, CLIENT_ID, MPIN, TOTP_SECRET]):
        log.error("Missing API credentials. Set them in .env file. Exiting.")
        state["bot_active"] = False
        publish_shared_state()
        return

    if auth_enabled():
        if not login():
            log.error("Authentication failed. Exiting.")
            state["bot_active"] = False
            publish_shared_state()
            return
    else:
        log.info("[PAPER] Skipping live API authentication.")
        state["angel"] = None

    instruments_df = download_instruments()
    log.info(f"Loaded {len(instruments_df)} NFO instruments.")

    if not resolve_nifty_futures(instruments_df):
        log.error("Could not resolve NIFTY Futures token. Exiting.")
        state["bot_active"] = False
        publish_shared_state()
        return

    validate_lot_size()
    reconcile_broker_state()
    log.info(f"Index: {INDEX}")
    log.info(f"Configured Lots: {LOTS}")
    log.info(f"Detected Lot Size: {effective_lot_size()}")
    log.info(f"Effective Quantity: {LOTS * effective_lot_size()}")

    start_websocket_feed(stop_event)
    ltp_thread = start_ltp_monitor_thread(stop_event)

    while not stop_event.is_set() and state.get("bot_active", True):
        bot_cycle(instruments_df)
        if stop_event.wait(CHECK_INTERVAL):
            break

    state["bot_active"] = False
    stop_event.set()
    publish_shared_state()
    print_daily_summary()
    send_telegram("🛑 Bot stopped")
    log.info("Bot service loop stopped.")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 11: MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    recovered = load_state()
    if recovered:
        log.info("Recovered bot state from state.json")
    state["bot_start_time"] = datetime.now(IST).isoformat()
    state["last_ltp_update"] = "--"
    state["entry_in_progress"] = False
    publish_shared_state()
    log.info("=" * 60)
    log.info("  VWAP STRATEGY BOT — Starting up")
    log.info(f"  Mode        : {'PAPER TRADING 📄' if PAPER_TRADE else '🔴 LIVE TRADING'}")
    log.info(f"  Order Mode  : {'PAPER' if PAPER_TRADE else 'LIVE'}")
    log.info(f"  Data Source : {'MOCK' if use_mock_market_data() else 'LIVE'}")
    log.info(f"  Authentication: {'ENABLED' if auth_enabled() else 'DISABLED'}")
    log.info(f"  Index       : {INDEX}")
    log.info(f"  ATM Offset  : ±{ATM_OFFSET} points")
    log.info(f"  CE ITM Off  : {CE_ITM_OFFSET} points")
    log.info(f"  PE ITM Off  : {PE_ITM_OFFSET} points")
    log.info(f"  Configured Lots: {LOTS}")
    log.info(f"  Window      : {MARKET_START} – {MARKET_END} IST")
    log.info(f"  Interval    : Every {CHECK_INTERVAL}s")
    log.info(f"  Max loss    : ₹{MAX_DAILY_LOSS}")
    log.info(f"  Max profit  : ₹{MAX_DAILY_PROFIT}")
    log.info("=" * 60)
    send_telegram("🚀 Bot started")

    # ── Authenticate ─────────────────────────────────────────────────────────
    if auth_enabled():
        if not all([API_KEY, CLIENT_ID, MPIN, TOTP_SECRET]):
            log.error("Missing API credentials. Set them in .env file. Exiting.")
            sys.exit(1)
        if not login():
            log.error("Authentication failed. Exiting.")
            sys.exit(1)
    else:
        log.info("[MOCK DATA] Skipping live API authentication.")
        # Still create a dummy SmartConnect to avoid None errors in mock-data mode
        state["angel"] = None

    # ── Load instrument master ────────────────────────────────────────────────
    instruments_df = download_instruments()
    log.info(f"Loaded {len(instruments_df)} NFO instruments.")

    if not resolve_nifty_futures(instruments_df):
        log.error("Could not resolve NIFTY Futures token. Exiting.")
        sys.exit(1)

    # ── Schedule the bot cycle ────────────────────────────────────────────────
    reconcile_broker_state()
    main_stop_event = threading.Event()
    start_websocket_feed(main_stop_event)
    start_ltp_monitor_thread(main_stop_event)
    scheduler = BlockingScheduler(timezone=IST)

    # Main strategy cycle: every CHECK_INTERVAL seconds
    scheduler.add_job(
        func     = lambda: bot_cycle(instruments_df),
        trigger  = "interval",
        seconds  = CHECK_INTERVAL,
        id       = "bot_cycle",
        name     = "VWAP Strategy Cycle",
        misfire_grace_time = 30,
    )

    # Daily reset at 9:00 AM IST
    scheduler.add_job(
        func    = reset_daily_state,
        trigger = "cron",
        hour    = 9, minute = 0,
        id      = "daily_reset",
        name    = "Daily State Reset",
    )

    # End-of-day summary at 3:05 PM IST
    scheduler.add_job(
        func    = print_daily_summary,
        trigger = "cron",
        hour    = 15, minute = 5,
        id      = "eod_summary",
        name    = "End-of-Day Summary",
    )

    # Session refresh at 12:00 PM IST (tokens expire)
    if auth_enabled():
        scheduler.add_job(
            func    = refresh_session,
            trigger = "cron",
            hour    = 12, minute = 0,
            id      = "session_refresh",
            name    = "API Session Refresh",
        )

    log.info("Scheduler started. Bot is running.")
    log.info(f"Next trade window: {MARKET_START} – {MARKET_END} IST")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        main_stop_event.set()
        log.info("Bot stopped by user.")
        print_daily_summary()
        send_telegram("🛑 Bot stopped")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    main()
