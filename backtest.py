"""
VWAP Crossover Backtest -- NIFTY Futures (5-minute candles)

Replays historical NIFTY Futures data through the same VWAP crossover logic
used by the live bot. Simulates confirmation-candle entries with 1:2 R:R.

NOTE: This backtest uses NIFTY Futures price as a proxy for the option premium.
      It validates *signal quality* (win rate, signal frequency, edge) rather
      than exact option P&L. Option P&L would require historical strike data.

Usage:
    python backtest.py              # last 30 days
    python backtest.py --days 60    # last 60 days

Output:
    backtest_results.csv  (one row per trade)
    Terminal summary with win rate, P&L, daily breakdown
"""

import os
import sys
import json
import argparse
import requests
import pyotp
import pytz
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from SmartApi import SmartConnect

load_dotenv()

# ── credentials (from .env) ───────────────────────────────────────────────────
API_KEY     = os.getenv("ANGEL_API_KEY", "")
CLIENT_ID   = os.getenv("ANGEL_CLIENT_ID", "")
MPIN        = os.getenv("ANGEL_MPIN", "")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET", "")
INDEX       = os.getenv("INDEX", "NIFTY")
LOT_SIZE    = int(os.getenv("LOT_SIZE", "75"))

# ── backtest config ───────────────────────────────────────────────────────────
SIGNAL_EXPIRY_CANDLES = int(os.getenv("SIGNAL_EXPIRY_CANDLES", "2"))
RISK_REWARD           = 2.0          # target = entry + 2 * risk
OUTPUT_CSV            = "backtest_results.csv"
IST                   = pytz.timezone("Asia/Kolkata")

# Market hours filter (inclusive, IST)
MARKET_START_MIN = 9  * 60 + 15   # 9:15
MARKET_END_MIN   = 15 * 60 + 20   # 15:20 (captures the 15:15 candle)
EOD_CLOSE_MIN    = 15 * 60 + 20   # force-close any open trade at this bar


# ── login ─────────────────────────────────────────────────────────────────────

def login() -> SmartConnect:
    print("Logging in to Angel One...")
    angel = SmartConnect(api_key=API_KEY)
    totp  = pyotp.TOTP(TOTP_SECRET).now()
    resp  = angel.generateSession(CLIENT_ID, MPIN, totp)
    if resp.get("status") is False:
        print(f"ERROR: Login failed -- {resp.get('message')}")
        sys.exit(1)
    print(f"Login OK.  Client: {CLIENT_ID}")
    return angel


# ── instrument lookup ─────────────────────────────────────────────────────────

def get_futures_token(angel: SmartConnect) -> tuple:
    """
    Download Angel One instrument master, find nearest-expiry NIFTY Futures
    and return (token, symbol).  Result is cached per day.
    """
    cache = f"instruments_{datetime.now(IST).strftime('%Y%m%d')}.json"
    if os.path.exists(cache):
        with open(cache) as f:
            data = json.load(f)
        print(f"Instruments loaded from cache ({cache})")
    else:
        url  = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        print("Downloading instrument master...")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        with open(cache, "w") as f:
            json.dump(data, f)

    df = pd.DataFrame(data)
    df = df[df["exch_seg"] == "NFO"]
    df["expiry"] = pd.to_datetime(df["expiry"], format="%d%b%Y", errors="coerce")

    today = datetime.now(IST).replace(tzinfo=None).date()
    mask  = (
        (df["name"] == INDEX) &
        (df["instrumenttype"] == "FUTIDX") &
        (df["expiry"].dt.date >= today)
    )
    candidates = df[mask].sort_values("expiry")
    if candidates.empty:
        print("ERROR: No NIFTY Futures contract found.")
        sys.exit(1)

    row = candidates.iloc[0]
    token  = str(row["token"])
    symbol = str(row["symbol"])
    print(f"Contract: {symbol}  |  Token: {token}  |  Expiry: {row['expiry'].date()}")
    return token, symbol


# ── data fetch ────────────────────────────────────────────────────────────────

def fetch_candles(angel: SmartConnect, token: str, symbol: str,
                  lookback_days: int = 30) -> pd.DataFrame:
    """Fetch 5-minute OHLCV candles for the last `lookback_days` calendar days."""
    to_dt   = datetime.now(IST)
    from_dt = to_dt - timedelta(days=lookback_days)

    params = {
        "exchange":    "NFO",
        "symboltoken": token,
        "interval":    "FIVE_MINUTE",
        "fromdate":    from_dt.strftime("%Y-%m-%d %H:%M"),
        "todate":      to_dt.strftime("%Y-%m-%d %H:%M"),
    }
    print(f"Fetching {symbol} candles from {params['fromdate']} ...")
    resp = angel.getCandleData(params)
    if resp.get("status") is False:
        print(f"ERROR fetching candles: {resp.get('message')}")
        sys.exit(1)

    candles = resp.get("data", [])
    if not candles:
        print("ERROR: No candle data returned.")
        sys.exit(1)

    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df[["open", "high", "low", "close", "volume"]] = (
        df[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric)
    )
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Keep only market-hours bars
    mins = df["timestamp"].apply(lambda ts: ts.hour * 60 + ts.minute)
    df   = df[(mins >= MARKET_START_MIN) & (mins <= MARKET_END_MIN)].reset_index(drop=True)

    print(f"Got {len(df)} candles  |  {df['timestamp'].iloc[0]} -> {df['timestamp'].iloc[-1]}")
    return df


# ── VWAP ─────────────────────────────────────────────────────────────────────

def calculate_vwap_daily(df: pd.DataFrame) -> pd.Series:
    """
    Cumulative VWAP that resets at the start of each trading day.
    Returns a Series aligned with df's index.
    """
    result = pd.Series(index=df.index, dtype=float)
    for _, group in df.groupby(df["timestamp"].dt.date):
        tp      = (group["high"] + group["low"] + group["close"]) / 3
        vol     = group["volume"]
        cum_vol = vol.cumsum().replace(0, float("nan"))
        vwap    = (tp * vol).cumsum() / cum_vol
        result.loc[group.index] = vwap.values
    return result


# ── simulation ────────────────────────────────────────────────────────────────

def run_backtest(df: pd.DataFrame) -> pd.DataFrame:
    """
    Walk through every bar.  State machine:

        FLAT  ->  SIGNAL  ->  IN_TRADE  ->  FLAT

    Crossover detection mirrors detect_vwap_crossover() in the live bot:
      - Bullish (LONG):  prev_close < prev_vwap  AND  curr_close > curr_vwap
      - Bearish (SHORT): prev_close > prev_vwap  AND  curr_close < curr_vwap

    Entry: next candle breaks above (LONG) or below (SHORT) the crossover bar's
           high / low.  Conservative fill: SL wins if SL and target are both
           touched in the same candle.
    """
    vwap   = calculate_vwap_daily(df)
    trades = []

    sim_state = "FLAT"
    signal    = None
    trade     = None

    def record_trade(entry_ts, exit_ts, direction, entry, sl, target, exit_price, result):
        pnl_pts = (exit_price - entry) * (1 if direction == "LONG" else -1)
        trades.append({
            "entry_time": entry_ts,
            "exit_time":  exit_ts,
            "direction":  direction,
            "entry":      round(entry,      2),
            "sl":         round(sl,         2),
            "target":     round(target,     2),
            "exit_price": round(exit_price, 2),
            "pnl_pts":    round(pnl_pts,    2),
            "pnl_rs":     round(pnl_pts * LOT_SIZE, 2),
            "result":     result,
        })

    for i in range(2, len(df)):
        ts    = df.at[i, "timestamp"]
        high  = df.at[i, "high"]
        low   = df.at[i, "low"]
        close = df.at[i, "close"]
        bar_min = ts.hour * 60 + ts.minute

        # ── Force end-of-day close ───────────────────────────────────────────
        if bar_min >= EOD_CLOSE_MIN:
            if sim_state == "IN_TRADE":
                record_trade(
                    trade["entry_ts"], ts, trade["direction"],
                    trade["entry"], trade["sl"], trade["target"],
                    close, "EOD_CLOSE",
                )
            sim_state = "FLAT"
            signal    = None
            trade     = None
            continue

        # ── IN_TRADE: monitor SL / target ────────────────────────────────────
        if sim_state == "IN_TRADE":
            d          = trade["direction"]
            sl_hit     = low <= trade["sl"]    if d == "LONG" else high >= trade["sl"]
            target_hit = high >= trade["target"] if d == "LONG" else low <= trade["target"]

            if sl_hit or target_hit:
                exit_price = trade["sl"] if sl_hit else trade["target"]
                result     = "SL"        if sl_hit else "TARGET"
                record_trade(
                    trade["entry_ts"], ts, d,
                    trade["entry"], trade["sl"], trade["target"],
                    exit_price, result,
                )
                sim_state = "FLAT"
                signal    = None
                trade     = None
            continue

        # ── SIGNAL: wait for confirmation entry ──────────────────────────────
        if sim_state == "SIGNAL":
            if i > signal["expiry_idx"]:
                sim_state = "FLAT"
                signal    = None
                continue

            d = signal["direction"]
            entered = (d == "LONG" and high >= signal["entry"]) or \
                      (d == "SHORT" and low  <= signal["entry"])

            if entered:
                trade = {
                    "entry":    signal["entry"],
                    "sl":       signal["sl"],
                    "target":   signal["target"],
                    "direction": d,
                    "entry_ts": ts,
                }
                sim_state = "IN_TRADE"
                signal    = None

                # Check SL / target on the entry candle itself
                sl_hit     = low  <= trade["sl"]     if d == "LONG" else high >= trade["sl"]
                target_hit = high >= trade["target"]  if d == "LONG" else low  <= trade["target"]
                if sl_hit or target_hit:
                    exit_price = trade["sl"] if sl_hit else trade["target"]
                    result     = "SL"        if sl_hit else "TARGET"
                    record_trade(
                        trade["entry_ts"], ts, d,
                        trade["entry"], trade["sl"], trade["target"],
                        exit_price, result,
                    )
                    sim_state = "FLAT"
                    trade     = None
            continue

        # ── FLAT: scan for crossover on last 2 completed candles ─────────────
        # Completed at step i = bars [0 .. i-1]; last two are i-2 and i-1
        prev_vwap = vwap.at[i - 2]
        curr_vwap = vwap.at[i - 1]
        if pd.isna(prev_vwap) or pd.isna(curr_vwap):
            continue

        prev_close = df.at[i - 2, "close"]
        curr_close = df.at[i - 1, "close"]
        curr_bar   = df.iloc[i - 1]

        bullish = prev_close < prev_vwap and curr_close > curr_vwap
        bearish = prev_close > prev_vwap and curr_close < curr_vwap

        if bullish:
            direction   = "LONG"
            entry_level = curr_bar["high"]
            sl_level    = curr_bar["low"]
        elif bearish:
            direction   = "SHORT"
            entry_level = curr_bar["low"]
            sl_level    = curr_bar["high"]
        else:
            continue

        risk = abs(entry_level - sl_level)
        if risk < 1.0:          # ignore noise-level signals
            continue

        target = entry_level + risk * RISK_REWARD * (1 if direction == "LONG" else -1)
        signal = {
            "entry":      entry_level,
            "sl":         sl_level,
            "target":     target,
            "direction":  direction,
            "expiry_idx": i + SIGNAL_EXPIRY_CANDLES,
        }
        sim_state = "SIGNAL"

    return pd.DataFrame(trades)


# ── reporting ─────────────────────────────────────────────────────────────────

def print_summary(trades_df: pd.DataFrame, lookback_days: int):
    SEP = "=" * 58

    if trades_df.empty:
        print(f"\n{SEP}")
        print("  No trades generated in this period.")
        print(SEP)
        return

    total     = len(trades_df)
    wins      = (trades_df["result"] == "TARGET").sum()
    losses    = (trades_df["result"] == "SL").sum()
    eod       = (trades_df["result"] == "EOD_CLOSE").sum()
    win_rate  = wins / total * 100 if total else 0.0

    gross_pts = trades_df["pnl_pts"].sum()
    gross_rs  = trades_df["pnl_rs"].sum()
    avg_pts   = trades_df["pnl_pts"].mean()
    best      = trades_df["pnl_pts"].max()
    worst     = trades_df["pnl_pts"].min()

    long_trades  = trades_df[trades_df["direction"] == "LONG"]
    short_trades = trades_df[trades_df["direction"] == "SHORT"]

    print(f"\n{SEP}")
    print(f"  VWAP CROSSOVER BACKTEST  |  Last {lookback_days} days")
    print(SEP)
    print(f"  Instrument    : NIFTY Futures (5-min, proxy for option)")
    print(f"  Lot size      : {LOT_SIZE}  |  R:R target: 1:{RISK_REWARD:.0f}")
    print(f"  Signal expiry : {SIGNAL_EXPIRY_CANDLES} candles")
    print()
    print(f"  Total trades  : {total}  ({total / lookback_days:.1f}/day avg)")
    print(f"  Long          : {len(long_trades)}")
    print(f"  Short         : {len(short_trades)}")
    print()
    print(f"  Wins (Target) : {wins}")
    print(f"  Losses (SL)   : {losses}")
    print(f"  EOD closes    : {eod}")
    print(f"  Win rate      : {win_rate:.1f}%")
    print()
    print(f"  Total P&L     : {gross_pts:+.1f} pts   (Rs {gross_rs:+,.0f})")
    print(f"  Avg per trade : {avg_pts:+.1f} pts")
    print(f"  Best trade    : {best:+.1f} pts")
    print(f"  Worst trade   : {worst:+.1f} pts")

    # Long vs short breakdown
    if not long_trades.empty:
        lw = (long_trades["result"] == "TARGET").sum()
        print(f"\n  Long  win rate : {lw}/{len(long_trades)} = {lw/len(long_trades)*100:.0f}%  "
              f"|  P&L: {long_trades['pnl_pts'].sum():+.1f} pts")
    if not short_trades.empty:
        sw = (short_trades["result"] == "TARGET").sum()
        print(f"  Short win rate : {sw}/{len(short_trades)} = {sw/len(short_trades)*100:.0f}%  "
              f"|  P&L: {short_trades['pnl_pts'].sum():+.1f} pts")

    # Daily P&L bar chart
    trades_df["date"] = pd.to_datetime(trades_df["entry_time"]).dt.date
    daily = trades_df.groupby("date")["pnl_pts"].sum()
    pos   = (daily > 0).sum()
    neg   = (daily <= 0).sum()

    print(f"\n  Daily P&L  ({pos} green / {neg} red):")
    for d, pnl in daily.items():
        bar = "#" * min(int(abs(pnl) / 20), 20)
        print(f"  {d}  {pnl:+8.1f} pts  {bar}")

    print(f"\n{SEP}\n")


# ── entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="VWAP Crossover Backtest — NIFTY Futures")
    parser.add_argument("--days", type=int, default=30,
                        help="How many calendar days of history to fetch (default: 30)")
    args = parser.parse_args()

    angel          = login()
    token, symbol  = get_futures_token(angel)
    df             = fetch_candles(angel, token, symbol, lookback_days=args.days)
    trades_df      = run_backtest(df)

    if not trades_df.empty:
        trades_df.to_csv(OUTPUT_CSV, index=False)
        print(f"\nTrade log -> {OUTPUT_CSV}")

    print_summary(trades_df, args.days)


if __name__ == "__main__":
    main()
