"""
Pre-flight Connectivity & Readiness Check
==========================================
Verifies every live-trading dependency WITHOUT placing any orders.
Run this before switching PAPER_TRADE=False.

Usage:
    python preflight_check.py

Each check prints PASS or FAIL with details.
Final verdict: READY or NOT READY.
"""

import os
import sys
import json
import socket
import threading
import time
import requests
import pyotp
import pytz
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from SmartApi import SmartConnect

load_dotenv()

# ── Credentials ───────────────────────────────────────────────────────────────
API_KEY        = os.getenv("ANGEL_API_KEY", "")
CLIENT_ID      = os.getenv("ANGEL_CLIENT_ID", "")
MPIN           = os.getenv("ANGEL_MPIN", "")
TOTP_SECRET    = os.getenv("ANGEL_TOTP_SECRET", "")
INDEX          = os.getenv("INDEX", "NIFTY")
LOT_SIZE       = int(os.getenv("LOT_SIZE", "75"))
LOTS           = int(os.getenv("LOTS", "1"))
PAPER_TRADE    = os.getenv("PAPER_TRADE", "True").strip().lower() == "true"
USE_WEBSOCKET  = os.getenv("USE_WEBSOCKET", "False").strip().lower() == "true"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")

IST = pytz.timezone("Asia/Kolkata")

# ── Output helpers ────────────────────────────────────────────────────────────
PASS  = "[PASS]"
FAIL  = "[FAIL]"
WARN  = "[WARN]"
INFO  = "[INFO]"
SEP   = "-" * 60

results = []   # list of (label, passed, detail)

def check(label: str, passed: bool, detail: str = ""):
    tag = PASS if passed else FAIL
    msg = f"  {tag}  {label}"
    if detail:
        msg += f"\n         {detail}"
    print(msg)
    results.append((label, passed, detail))
    return passed

def warn(label: str, detail: str = ""):
    msg = f"  {WARN}  {label}"
    if detail:
        msg += f"\n         {detail}"
    print(msg)
    results.append((label, None, detail))   # None = warning, not fail

def section(title: str):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 1 — Configuration
# ══════════════════════════════════════════════════════════════════════════════
section("1. Configuration (.env)")

check("API_KEY present",    bool(API_KEY),    API_KEY[:4] + "****" if API_KEY else "MISSING")
check("CLIENT_ID present",  bool(CLIENT_ID),  CLIENT_ID if CLIENT_ID else "MISSING")
check("MPIN present",       bool(MPIN),       "****" if MPIN else "MISSING")
check("TOTP_SECRET present",bool(TOTP_SECRET),"****" if TOTP_SECRET else "MISSING")

if PAPER_TRADE:
    warn("PAPER_TRADE=True",
         "Set PAPER_TRADE=False in .env before going live")
else:
    check("PAPER_TRADE=False (live mode)", True)

check(f"LOT_SIZE={LOT_SIZE} configured", LOT_SIZE >= 1,
      f"{LOTS} lot(s) × {LOT_SIZE} = {LOTS * LOT_SIZE} qty per order")

check(f"LOTS={LOTS}",        LOTS >= 1,        f"Trades will use {LOTS} lot(s) = {LOTS * LOT_SIZE} qty")
check("USE_WEBSOCKET=True",  USE_WEBSOCKET,     "Set USE_WEBSOCKET=True for real-time LTP feed")


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 2 — Network / DNS
# ══════════════════════════════════════════════════════════════════════════════
section("2. Network Connectivity")

HOSTS = [
    ("apiconnect.angelone.in",   443, "Angel One API"),
    ("margincalculator.angelbroking.com", 443, "Instrument master"),
    ("api.telegram.org",         443, "Telegram alerts"),
]

for host, port, name in HOSTS:
    is_optional = "telegram" in host.lower()
    try:
        socket.create_connection((host, port), timeout=5).close()
        check(f"{name} reachable ({host})", True)
    except Exception as e:
        if is_optional:
            warn(f"{name} unreachable ({host}) — alerts disabled, trading unaffected")
        else:
            check(f"{name} reachable ({host})", False, str(e))


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 3 — Angel One Login
# ══════════════════════════════════════════════════════════════════════════════
section("3. Angel One Authentication")

angel = None
feed_token = None

try:
    totp_code = pyotp.TOTP(TOTP_SECRET).now()
    print(f"  {INFO}  TOTP generated: {totp_code}")
    angel = SmartConnect(api_key=API_KEY)
    resp  = angel.generateSession(CLIENT_ID, MPIN, totp_code)
    if resp.get("status") is False:
        check("Login", False, resp.get("message", "Unknown error"))
        print("\n  Cannot continue without login. Exiting.")
        sys.exit(1)
    feed_token = resp["data"]["feedToken"]
    check("Login", True, f"JWT obtained  |  Feed token: {feed_token[:10]}...")
except Exception as e:
    check("Login", False, str(e))
    print("\n  Cannot continue without login. Exiting.")
    sys.exit(1)

# Account profile
try:
    profile = angel.getProfile(resp["data"]["refreshToken"])
    name    = profile.get("data", {}).get("name", "N/A")
    check("Account profile", True, f"Name: {name}  |  Client: {CLIENT_ID}")
except Exception as e:
    check("Account profile", False, str(e))

# Available margin
try:
    rms = angel.rmsLimit()
    if rms.get("status"):
        data         = rms.get("data") or {}
        net_avail    = data.get("net", "N/A")
        used_margin  = data.get("utilisedpayout", "N/A")
        check("Margin / funds readable", True,
              f"Net available: Rs {net_avail}  |  Used: Rs {used_margin}")
    else:
        check("Margin / funds readable", False, rms.get("message"))
except Exception as e:
    check("Margin / funds readable", False, str(e))


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 4 — Instrument Master
# ══════════════════════════════════════════════════════════════════════════════
section("4. Instrument Master & Token Lookup")

fut_token  = None
fut_symbol = None
instruments_df = None

try:
    cache = f"instruments_{datetime.now(IST).strftime('%Y%m%d')}.json"
    if os.path.exists(cache):
        with open(cache) as f:
            data = json.load(f)
        print(f"  {INFO}  Using cached instruments ({cache})")
    else:
        url  = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        r    = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        with open(cache, "w") as f:
            json.dump(data, f)

    instruments_df = pd.DataFrame(data)
    instruments_df = instruments_df[instruments_df["exch_seg"] == "NFO"]
    instruments_df["expiry"] = pd.to_datetime(instruments_df["expiry"], format="%d%b%Y", errors="coerce")
    instruments_df["strike"] = pd.to_numeric(instruments_df["strike"], errors="coerce") / 100
    check("Instrument master downloaded", True, f"{len(instruments_df):,} NFO instruments")

    today      = datetime.now(IST).replace(tzinfo=None).date()
    fut_mask   = (
        (instruments_df["name"] == INDEX) &
        (instruments_df["instrumenttype"] == "FUTIDX") &
        (instruments_df["expiry"].dt.date >= today)
    )
    fut_row    = instruments_df[fut_mask].sort_values("expiry").iloc[0]
    fut_token  = str(fut_row["token"])
    fut_symbol = str(fut_row["symbol"])
    detected_lot = int(fut_row.get("lotsize") or LOT_SIZE)
    check(f"{INDEX} Futures token found",  True,
          f"{fut_symbol}  |  Token: {fut_token}  |  Expiry: {fut_row['expiry'].date()}  |  Lot: {detected_lot}")

    if detected_lot != LOT_SIZE:
        warn(f"Lot size mismatch: instrument={detected_lot}, .env LOT_SIZE={LOT_SIZE}",
             "Update LOT_SIZE in .env to match the detected lot size")

except Exception as e:
    check("Instrument master / token lookup", False, str(e))


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 5 — Market Data (Candles)
# ══════════════════════════════════════════════════════════════════════════════
section("5. Market Data — Candles (getCandleData)")

if fut_token:
    try:
        to_dt   = datetime.now(IST)
        from_dt = to_dt - timedelta(days=2)
        params  = {
            "exchange":    "NFO",
            "symboltoken": fut_token,
            "interval":    "FIVE_MINUTE",
            "fromdate":    from_dt.strftime("%Y-%m-%d %H:%M"),
            "todate":      to_dt.strftime("%Y-%m-%d %H:%M"),
        }
        resp2 = angel.getCandleData(params)
        if resp2.get("status") is False:
            msg = str(resp2.get("message") or "")
            if "rate" in msg.lower() or "access denied" in msg.lower() or "exceeded" in msg.lower():
                warn("Candle data fetch (rate-limited)",
                     "Hit Angel One rate limit — API is reachable. "
                     "Wait 1 min before retrying. The live bot has throttling built in.")
            else:
                check("Candle data fetch", False, msg)
        else:
            candles = resp2.get("data", [])
            if candles:
                last = candles[-1]
                check("Candle data fetch", True,
                      f"{len(candles)} candles  |  Last: {last[0]}  close={last[4]}")
            else:
                warn("Candle data returned empty",
                     "Outside market hours — normal; candles available only during/after session")
    except Exception as e:
        msg = str(e)
        if "rate" in msg.lower() or "access denied" in msg.lower() or "exceeded" in msg.lower():
            warn("Candle data fetch (rate-limited)",
                 "Hit Angel One rate limit — API is reachable. Wait 1 min and retry.")
        else:
            check("Candle data fetch", False, msg)
else:
    warn("Candle data skipped (no futures token)")


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 6 — Market Data (LTP via REST)
# ══════════════════════════════════════════════════════════════════════════════
section("6. Market Data — LTP (REST fallback — only used when WebSocket is stale)")

if fut_token and fut_symbol:
    try:
        # ltpData(exchange, tradingsymbol, symboltoken) — symbol first, token second
        ltp_resp = angel.ltpData("NFO", fut_symbol, fut_token)
        if ltp_resp.get("status") is False:
            if USE_WEBSOCKET:
                warn("LTP (REST fallback)",
                     ltp_resp.get("message") +
                     " — not critical: WebSocket is primary LTP source")
            else:
                check("LTP (REST)", False, ltp_resp.get("message"))
        else:
            ltp = ltp_resp.get("data", {}).get("ltp", 0)
            check("LTP (REST fallback)", True,
                  f"{fut_symbol}  LTP = Rs {ltp}  (fallback path works)")
    except Exception as e:
        if USE_WEBSOCKET:
            warn("LTP (REST fallback)", str(e) + " — not critical: WebSocket is primary")
        else:
            check("LTP (REST)", False, str(e))
else:
    warn("LTP check skipped (no futures token)")


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 7 — WebSocket Feed
# ══════════════════════════════════════════════════════════════════════════════
section("7. WebSocket Feed (SmartWebSocketV2)")

if not USE_WEBSOCKET:
    warn("WebSocket skipped (USE_WEBSOCKET=False in .env)",
         "Set USE_WEBSOCKET=True for real-time LTP — required for fast SL/target detection")
elif not fut_token:
    warn("WebSocket skipped (no futures token)")
else:
    tick_received  = threading.Event()
    ws_error       = []
    ws_connected   = threading.Event()
    received_ltp   = [None]

    try:
        from SmartApi.smartWebSocketV2 import SmartWebSocketV2

        sws = SmartWebSocketV2(
            resp["data"]["jwtToken"],
            API_KEY,
            CLIENT_ID,
            feed_token,
        )

        def on_open(ws):
            ws_connected.set()
            sws.subscribe(
                correlation_id="preflight",
                mode=1,
                token_list=[{"exchangeType": 2, "tokens": [fut_token]}],
            )

        def on_data(ws, message):
            ltp_raw = message.get("last_traded_price")
            if ltp_raw:
                received_ltp[0] = ltp_raw / 100.0
                tick_received.set()

        def on_close(ws, code, reason):
            pass

        def on_error(ws, error):
            ws_error.append(str(error))
            tick_received.set()

        sws.on_open  = on_open
        sws.on_data  = on_data
        sws.on_close = on_close
        sws.on_error = on_error

        ws_thread = threading.Thread(target=sws.connect, daemon=True)
        ws_thread.start()

        connected = ws_connected.wait(timeout=8)
        if not connected:
            check("WebSocket connect", False, "Timed out waiting for connection (8s)")
        else:
            check("WebSocket connect", True)
            tick_ok = tick_received.wait(timeout=10)
            if ws_error:
                check("WebSocket tick received", False, ws_error[0])
            elif tick_ok and received_ltp[0]:
                check("WebSocket tick received", True,
                      f"{fut_symbol}  LTP = Rs {received_ltp[0]:.2f}  (real-time)")
            else:
                warn("No tick within 10s",
                     "Market may be closed — WS connected OK; ticks arrive only during market hours")
                check("WebSocket connect", True, "Connected but no tick (outside market hours)")

        try:
            sws.close_connection()
        except Exception:
            pass

    except ImportError:
        check("WebSocket (SmartWebSocketV2)", False,
              "SmartWebSocketV2 not found — run: pip install smartapi-python")
    except Exception as e:
        check("WebSocket", False, str(e))


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 8 — Order Book & Positions (read-only)
# ══════════════════════════════════════════════════════════════════════════════
section("8. Order Book & Positions (read-only — no orders placed)")

try:
    ob = angel.orderBook()
    if ob.get("status") is False:
        check("Order book readable", False, ob.get("message"))
    else:
        orders = ob.get("data") or []
        check("Order book readable", True,
              f"{len(orders)} order(s) on record today")
except Exception as e:
    check("Order book readable", False, str(e))

try:
    pos = angel.position()
    if pos.get("status") is False:
        check("Positions readable", False, pos.get("message"))
    else:
        positions = pos.get("data") or []
        open_pos  = [p for p in positions if abs(float(p.get("netqty") or 0)) > 0]
        if open_pos:
            symbols = ", ".join(p.get("tradingsymbol","?") for p in open_pos)
            warn(f"Open positions found: {symbols}",
                 "Close these before starting the bot or it may conflict with active trades")
        else:
            check("Positions readable (no open positions)", True)
except Exception as e:
    check("Positions readable", False, str(e))


# ══════════════════════════════════════════════════════════════════════════════
# CHECK 9 — Telegram Alerts
# ══════════════════════════════════════════════════════════════════════════════
section("9. Telegram Alerts")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
    warn("Telegram not configured",
         "Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to .env for trade alerts")
else:
    try:
        url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        now_str = datetime.now(IST).strftime("%H:%M:%S")
        payload = {
            "chat_id":                  TELEGRAM_CHAT,
            "text":                     f"[Pre-flight check] Bot connectivity OK at {now_str} IST",
            "disable_web_page_preview": True,
        }
        r = requests.post(url, data=payload, timeout=10)
        ok = r.json().get("ok", False)
        if ok:
            check("Telegram message sent", True,
                  "Check your Telegram — you should see the test message")
        else:
            warn("Telegram message sent",
                 r.json().get("description", "Unknown error") +
                 " (alerts disabled but trading still works)")
    except Exception as e:
        # Telegram being blocked/unreachable does not affect trading
        warn("Telegram unreachable",
             str(e)[:120] + " — alerts will be skipped but trading is unaffected")


# ══════════════════════════════════════════════════════════════════════════════
# FINAL VERDICT
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'=' * 60}")
print("  RESULTS SUMMARY")
print(f"{'=' * 60}")

passed  = [r for r in results if r[1] is True]
failed  = [r for r in results if r[1] is False]
warned  = [r for r in results if r[1] is None]

for label, ok, detail in results:
    if ok is True:
        print(f"  {PASS}  {label}")
    elif ok is False:
        print(f"  {FAIL}  {label}")
        if detail:
            print(f"         -> {detail}")
    else:
        print(f"  {WARN}  {label}")

print(f"\n  Passed : {len(passed)}")
print(f"  Failed : {len(failed)}")
print(f"  Warns  : {len(warned)}")

print(f"\n{'=' * 60}")
if failed:
    print("  VERDICT:  NOT READY")
    print("  Fix the FAIL items above before going live.")
else:
    if PAPER_TRADE:
        print("  VERDICT:  CONNECTIVITY OK  (still in paper mode)")
        print("  All checks passed. Set PAPER_TRADE=False when ready.")
    else:
        print("  VERDICT:  READY FOR LIVE TRADING")
        print("  All checks passed. Bot can connect and trade live.")
print(f"{'=' * 60}\n")
