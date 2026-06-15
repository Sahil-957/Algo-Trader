import csv
import io
import json
import threading
from pathlib import Path

from flask import Flask, jsonify, send_file, send_from_directory
from flask_cors import CORS

import vwap_strategy_bot as bot


BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__, static_folder=str(BASE_DIR), static_url_path="")
CORS(app)


shared_state = {}
bot.attach_shared_state(shared_state)
if bot.load_state():
    bot.log.info("Recovered bot state from state.json")
    bot.publish_shared_state()

bot_thread = None
stop_event = threading.Event()
thread_lock = threading.Lock()


def _snapshot_status():
    state = dict(shared_state)
    state.pop("candle_cache", None)
    state["bot_thread_alive"] = bool(bot_thread and bot_thread.is_alive())
    state["bot_running"] = state["bot_thread_alive"]
    state["bot_active"] = state["bot_thread_alive"]
    state["connection_status"] = "running" if state["bot_thread_alive"] else "stopped"
    lot_size = state.get("lot_size")
    lots = state.get("lots", bot.LOTS)
    try:
        lot_size_int = int(lot_size) if lot_size is not None else None
    except (TypeError, ValueError):
        lot_size_int = None
    try:
        lots_int = int(lots) if lots is not None else None
    except (TypeError, ValueError):
        lots_int = None
    state["lot_size"] = lot_size_int
    state["lots"] = lots_int
    if lot_size_int is not None and lots_int is not None:
        state["effective_quantity"] = lots_int * lot_size_int
    else:
        state["effective_quantity"] = None
    return json.loads(json.dumps(state, default=str))


def _stop_running_bot(wait: bool = False):
    global bot_thread
    stop_event.set()
    shared_state["bot_active"] = False
    bot.publish_shared_state()
    if wait and bot_thread and bot_thread.is_alive():
        bot_thread.join()
    return jsonify({"ok": True, "message": "Bot stop requested."})


def _stop_and_wait_for_bot_thread():
    global bot_thread
    stop_event.set()
    shared_state["bot_active"] = False
    bot.publish_shared_state()
    if bot_thread and bot_thread.is_alive():
        bot_thread.join()
    bot_thread = None


def _reset_paper_session():
    global bot_thread
    if not bot.PAPER_TRADE:
        return jsonify({"ok": False, "message": "Reset disabled in live trading mode."}), 400

    with thread_lock:
        if bot_thread and bot_thread.is_alive():
            _stop_and_wait_for_bot_thread()

        bot.reset_paper_state()
        bot_thread = None
        stop_event.clear()
    return jsonify({"ok": True, "message": "Paper trading state reset."})


@app.route("/")
def index():
    return send_from_directory(str(BASE_DIR), "dashboard.html")


@app.route("/api/status", methods=["GET"])
def api_status():
    return jsonify(_snapshot_status())


@app.route("/api/trades", methods=["GET"])
def api_trades():
    trades = shared_state.get("trade_log_public") or shared_state.get("trade_log") or []
    return jsonify({"trades": trades, "count": len(trades)})


@app.route("/api/start", methods=["POST"])
def api_start():
    global bot_thread, stop_event
    with thread_lock:
        if bot_thread and bot_thread.is_alive():
            return jsonify({"ok": True, "message": "Bot is already running."})

        stop_event = threading.Event()
        bot_thread = threading.Thread(
            target=bot.run_bot_service,
            kwargs={"external_state": shared_state, "stop_event": stop_event},
            daemon=True,
            name="vwap-bot-thread",
        )
        bot_thread.start()

    return jsonify({"ok": True, "message": "Bot started."})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    return _stop_running_bot()


@app.route("/api/reset", methods=["POST"])
def api_reset():
    return _reset_paper_session()


@app.route("/api/emergency_exit", methods=["POST"])
def api_emergency_exit():
    active_trade = shared_state.get("active_trade")
    if not active_trade:
        return _stop_running_bot()

    try:
        bot.cancel_active_sl_order()
        exit_price = bot.get_ltp(active_trade["token"], active_trade["symbol"])
        if not exit_price or exit_price <= 0:
            exit_price = float(active_trade.get("entry_price") or 0.0)
        bot.exit_active_trade(exit_price, reason="EMERGENCY EXIT", broker_exit=True)
        shared_state["sl_order_id"] = None
        shared_state["sl_status"] = "CANCELLED"
        bot.publish_shared_state()
        bot.send_telegram("🚨 EMERGENCY EXIT EXECUTED")
    finally:
        _stop_running_bot()

    return jsonify({"ok": True, "message": "Emergency exit executed."})


@app.route("/api/export_trades", methods=["GET"])
def api_export_trades():
    trades = shared_state.get("trade_log") or []
    rows = []
    for trade in trades:
        trade_id = trade.get("trade_id") or "--"
        trade_date = trade.get("date") or bot.datetime.now(bot.IST).strftime("%Y-%m-%d")
        rows.append(
            {
                "Trade ID": trade_id,
                "Date": trade_date,
                "Symbol": trade.get("symbol", "--"),
                "Direction": trade.get("direction", "--"),
                "Entry Price": trade.get("entry_price", "--"),
                "Exit Price": trade.get("exit_price", "--"),
                "PnL": trade.get("pnl", "--"),
                "Result": trade.get("result", "--"),
                "Reason": trade.get("reason", "--"),
            }
        )

    output = io.StringIO()
    fieldnames = ["Trade ID", "Date", "Symbol", "Direction", "Entry Price", "Exit Price", "PnL", "Result", "Reason"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
    filename = f"trades_{bot.datetime.now(bot.IST).strftime('%Y%m%d')}.csv"
    return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=filename)


@app.route("/api/export_signal_audit", methods=["GET"])
def api_export_signal_audit():
    audit_path = bot.SIGNAL_AUDIT_FILE
    if audit_path.exists():
        return send_file(
            audit_path,
            mimetype="text/csv",
            as_attachment=True,
            download_name=audit_path.name,
        )

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=bot.SIGNAL_AUDIT_FIELDS)
    writer.writeheader()
    csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
    return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=audit_path.name)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, threaded=True, debug=False)
