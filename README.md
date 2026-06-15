# VWAP Strategy Bot

This project runs an intraday options strategy for NSE F&O using Angel One SmartAPI. It includes a local Flask dashboard so you can start and stop the bot from your browser and watch its state in real time.

## What each file does

- `app.py` runs the Flask server on `http://localhost:5000`, starts the bot in a background thread, and exposes the API endpoints used by the dashboard.
- `vwap_strategy_bot.py` contains the strategy engine, paper-trading mocks, order logic, risk checks, and shared state updates.
- `dashboard.html` is the browser UI for bot status, signal details, the trade log, and recent activity messages.
- `.env` stores your personal Angel One credentials and strategy settings.
- `.env.example` is a template you can copy to create your local `.env`.
- `requirements.txt` lists the Python dependencies.

## How the strategy works

In plain English:

1. The bot checks NIFTY futures and compares the latest close with VWAP to decide whether the market looks bullish or bearish.
2. If bullish, it picks a CE option around ATM plus the configured offset. If bearish, it picks a PE option around ATM minus the offset.
3. It then watches the selected option for a bullish VWAP crossover on the 5-minute chart.
4. When a crossover appears, the bot marks the candle high as the entry trigger, the candle low as stop loss, and sets a target at 2 times the risk.
5. If live price breaks above the signal candle high, it enters the trade.
6. The position is monitored until target, stop loss, or end of day.

Paper mode uses generated mock candles and mock LTP values so you can test the full flow without live credentials.

## Setup on Windows

1. Install Python 3.11 from python.org.
2. Open Command Prompt in the project folder.
3. Run `pip install -r requirements.txt`.
4. Copy `.env.example` to `.env`.
5. Open `.env` in Notepad and fill in your Angel One credentials.
6. Run `python app.py`.
7. Open `http://localhost:5000` in your browser.
8. Click `Start Bot` and watch it run in paper mode.
9. When you are satisfied, set `PAPER_TRADE=False` in `.env` for live trading.

## How to read the trade log

- `Time` shows when the trade exited or was recorded.
- `Option` is the option symbol traded.
- `Entry`, `SL`, and `Target` are the planned levels.
- `Exit` is the realized exit price.
- `P&L` is the profit or loss for the completed trade.
- `Result` shows `WIN` or `LOSS`.

## Risk warning

This is an intraday trading bot, not a guaranteed profit system. Options trading can lose money quickly. Keep `PAPER_TRADE=True` until you have tested the flow carefully, and only switch to live mode if you understand the risks and have checked your credentials, lot size, and risk limits.
