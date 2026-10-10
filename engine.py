 a
"""
engine.py - Nova Trade v3.1 (Active Scalping & Dynamic Trend Following)
Capital inițial: $150.00 | Scanner pe Forex, Metale, Energie și Crypto (24/7)
"""

import datetime
import hmac
import html
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, quote, urlparse

import ccxt
import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None

MT5_AVAILABLE = os.name == "nt" and mt5 is not None

from storage.mongo_db import MongoManager

# ------------------------- PARAMETRI -------------------------
EXCHANGE = os.environ.get("EXCHANGE", "binance")
TIMEFRAME = "15m"
BAR_MINUTES = 15
START_CAPITAL = 150.0
FEE = 0.001
SLIPPAGE = 0.0002
MAX_BARS = 48
COST_RT = 2 * (FEE + SLIPPAGE)
POLL_SECONDS = 30
MAX_SIGNAL_AGE = 120
STALE_PRICE_SECONDS = 900

# Piețele monitorizate, inclusiv Crypto pentru weekend (24/7)
MARKETS = {
    "EURUSD": dict(src="yf", yf="EURUSD=X", fee=0.00005, slip=0.00005, max_age=300),
    "GBPUSD": dict(src="yf", yf="GBPUSD=X", fee=0.00005, slip=0.00005, max_age=300),
    "USDJPY": dict(src="yf", yf="USDJPY=X", fee=0.00005, slip=0.00005, max_age=300),
    "GOLD": dict(src="yf", yf="GC=F", fee=0.0001, slip=0.00015, max_age=300),
    "OIL": dict(src="yf", yf="CL=F", fee=0.0001, slip=0.0003, max_age=300),
    "BTCUSD": dict(src="yf", yf="BTC-USD", fee=0.0002, slip=0.0002, max_age=300),
    "ETHUSD": dict(src="yf", yf="ETH-USD", fee=0.0002, slip=0.0002, max_age=300),
}

SYMBOL = "EURUSD"

STATE_COLLECTION = "motor_state"
CONTROL_COLLECTION = "motor_control"
TRADES_COLLECTION = "trades"

DEFAULTS = dict(
    stopped=False,
    allow_new_trades=True,
    close_now=False,
    vol_adjust_stop=True,
    trailing_stop=True,
    tp_extend=False,
    tp_extend_atr=0.5,
    tp_lock_atr=0.2,
    risk_per_trade=0.015,
    trade_amount=50.0,
    execution_mode="paper",
    daily_loss_limit=0.05,
    capital_override=None,
    atr_stop_mult=1.2,
    trail_activate_atr=0.6,
    trail_atr=0.8,
    manual_stop=None,
    manual_tp=None,
)


# --------------------------- UTILITARE ---------------------------
def utc_now():
    return pd.Timestamp.now(tz="UTC").tz_localize(None)

def _num(x):
    try:
        v = float(x)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None

def fnum(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

def rnd(x):
    x = float(x)
    return round(x, 2 if abs(x) >= 100 else 6)

def fmt_price(x):
    return f"{x:,.2f}" if abs(x) >= 100 else f"{x:.6f}"

def quote_to_usd(symbol, amount, price):
    return amount / price if symbol == "USDJPY" and price > 0 else amount

def usd_notional(symbol, qty, price):
    return qty if symbol == "USDJPY" else qty * price

def ser_pos(p):
    out = {}
    for k, v in p.items():
        if isinstance(v, pd.Timestamp):
            out[k] = v.isoformat()
        elif isinstance(v, np.floating):
            out[k] = float(v)
        elif isinstance(v, np.integer):
            out[k] = int(v)
        else:
            out[k] = v
    return out

def deser_pos(raw):
    if not raw:
        return None
    need = ("entry", "stop", "tp", "qty", "fee_in", "atr0", "hh", "t", "t_open")
    if any(k not in raw for k in need):
        return None
    p = dict(raw)
    try:
        p["t"] = pd.Timestamp(p["t"])
        p["t_open"] = pd.Timestamp(p["t_open"])
        for k in ("entry", "stop", "tp", "qty", "fee_in", "atr0", "hh"):
            p[k] = float(p[k])
        p["bars"] = int(p.get("bars", 0))
        if p.get("mt5_ticket") is not None:
            p["mt5_ticket"] = int(p["mt5_ticket"])
        if p.get("mt5_identifier") is not None:
            p["mt5_identifier"] = int(p["mt5_identifier"])
        p["symbol"] = p.get("symbol") or SYMBOL
        mk = MARKETS.get(p["symbol"], MARKETS[SYMBOL])
        p["fee"] = float(p.get("fee", mk["fee"]))
        p["slip"] = float(p.get("slip", mk["slip"]))
    except Exception as e:
        print("Eroare la restaurarea poziției:", e)
        return None
    return p


# --------------------------- STOCARE ---------------------------
class Store:
    def __init__(self, mongo):
        self.mongo = mongo
        self.lock = threading.Lock()
        self.mem_state = {}
        self.mem_control = {}
        self.mem_trades = []

    def _col(self, name):
        try:
            db = getattr(self.mongo, "db", None) if self.mongo else None
            return db[name] if db is not None else None
        except Exception:
            return None

    def save_state(self, data):
        with self.lock:
            self.mem_state = dict(data)
        col = self._col(STATE_COLLECTION)
        if col is None:
            return
        try:
            col.update_one({"_id": "current_state"}, {"$set": data}, upsert=True)
        except Exception as e:
            print("Eroare la salvarea stării:", e)

    def load_state(self):
        col = self._col(STATE_COLLECTION)
        if col is not None:
            try:
                res = col.find_one({"_id": "current_state"})
                if res:
                    res.pop("_id", None)
                    return res
            except Exception:
                pass
        with self.lock:
            return dict(self.mem_state) or None

    def get_control(self):
        cfg = dict(DEFAULTS)
        stored = None
        col = self._col(CONTROL_COLLECTION)
        if col is not None:
            try:
                stored = col.find_one({"_id": "control"})
                if stored:
                    stored.pop("_id", None)
                    with self.lock:
                        self.mem_control = dict(stored)
            except Exception:
                stored = None
        if stored is None:
            with self.lock:
                stored = dict(self.mem_control)
        cfg.update({k: v for k, v in stored.items() if k in DEFAULTS})
        return cfg

    def set_control(self, **kw):
        with self.lock:
            self.mem_control.update(kw)
        col = self._col(CONTROL_COLLECTION)
        if col is None:
            return
        try:
            col.update_one({"_id": "control"}, {"$set": kw}, upsert=True)
        except Exception:
            pass

    def add_trade(self, rec):
        with self.lock:
            self.mem_trades.insert(0, dict(rec))
            self.mem_trades = self.mem_trades[:100]
        col = self._col(TRADES_COLLECTION)
        if col is None:
            return
        try:
            col.insert_one(dict(rec))
        except Exception:
            pass

    def recent_trades(self, limit=20):
        col = self._col(TRADES_COLLECTION)
        if col is None:
            with self.lock:
                return list(self.mem_trades[:limit])
        try:
            rows = list(col.find({}).sort("t_close", -1).limit(limit))
            for row in rows:
                row.pop("_id", None)
            return rows
        except Exception:
            with self.lock:
                return list(self.mem_trades[:limit])


class MT5Bridge:
    SYMBOL_DEFAULTS = {
        "EURUSD": "EURUSD", "GBPUSD": "GBPUSD", "USDJPY": "USDJPY",
        "GOLD": "XAUUSD", "OIL": "XTIUSD", "BTCUSD": "BTCUSD", "ETHUSD": "ETHUSD",
    }

    def __init__(self, mode):
        if os.name != "nt":
            raise RuntimeError("Integrarea Python MT5 necesită Windows și terminalul MetaTrader 5 pornit; pe Linux păstrează simularea sau rulează botul pe un Windows/VPS cu MT5.")
        if mt5 is None:
            raise RuntimeError("Lipsește pachetul MetaTrader5.")
        self.mode = mode
        prefix = "MT5_DEMO" if mode == "mt5_demo" else "MT5_LIVE"
        login = os.environ.get(f"{prefix}_LOGIN", "")
        password = os.environ.get(f"{prefix}_PASSWORD", "")
        server = os.environ.get(f"{prefix}_SERVER", "")
        if not (login and password and server):
            raise RuntimeError(f"Configurează {prefix}_LOGIN, {prefix}_PASSWORD și {prefix}_SERVER.")
        if not mt5.initialize(login=int(login), password=password, server=server):
            raise RuntimeError(f"Conectarea MT5 a eșuat: {mt5.last_error()}")
        self.account = mt5.account_info()
        expected = mt5.ACCOUNT_TRADE_MODE_DEMO if mode == "mt5_demo" else mt5.ACCOUNT_TRADE_MODE_REAL
        if self.account is None or self.account.trade_mode != expected:
            mt5.shutdown()
            raise RuntimeError("Tipul contului MT5 nu corespunde.")
        # FIX 8: interfața și calculele sunt în USD, deci contul trebuie să fie în USD
        if str(getattr(self.account, "currency", "")).upper() != "USD":
            cur = getattr(self.account, "currency", "?")
            mt5.shutdown()
            raise RuntimeError(f"Moneda contului MT5 este {cur}, dar botul lucrează în USD.")
        self.magic = 731031
        self.time_offset_seconds = self._detect_time_offset()
        if self.time_offset_seconds:
            sign = "+" if self.time_offset_seconds > 0 else ""
            print(f"MT5 time offset calibrat față de UTC: {sign}{self.time_offset_seconds // 3600}h", flush=True)

    def _detect_time_offset(self):
        configured = os.environ.get("MT5_TIME_OFFSET_SECONDS")
        if configured:
            return int(configured)
        now = time.time()
        for alias in self.SYMBOL_DEFAULTS:
            try:
                name = self.symbol(alias)
                tick = mt5.symbol_info_tick(name)
                if tick is None:
                    continue
                tick_epoch = float(getattr(tick, "time_msc", 0) or 0) / 1000.0
                if tick_epoch <= 0:
                    tick_epoch = float(getattr(tick, "time", 0) or 0)
                skew = tick_epoch - now
                if abs(skew) <= 120:
                    return 0
                nearest_hour = round(skew / 3600) * 3600
                # Broker server clocks commonly place the raw MT5 epoch a few hours ahead.
                # Require a near-hour offset and a timestamp that becomes current after correction.
                if 3600 <= nearest_hour <= 5 * 3600 and abs(skew - nearest_hour) <= 180:
                    corrected_age = (tick_epoch - nearest_hour) - now
                    if abs(corrected_age) <= 180:
                        return int(nearest_hour)
            except Exception:
                continue
        return 0

    def utc_epoch(self, broker_epoch):
        return float(broker_epoch) - self.time_offset_seconds

    def symbol(self, alias):
        name = os.environ.get(f"MT5_SYMBOL_{alias}", self.SYMBOL_DEFAULTS.get(alias, alias))
        info = mt5.symbol_info(name)
        if info is None:
            raise RuntimeError(f"Simbolul {name} nu există.")
        if not info.visible and not mt5.symbol_select(name, True):
            raise RuntimeError(f"Simbolul {name} nu poate fi activat.")
        return name

    def fetch_ohlcv(self, alias, timeframe, limit=500):
        name = self.symbol(alias)
        rates = mt5.copy_rates_from_pos(name, mt5.TIMEFRAME_M15, 0, limit)
        if rates is None or len(rates) == 0:
            raise RuntimeError("Fără lumânări MT5.")
        df = pd.DataFrame(rates)
        df["ts"] = pd.to_datetime(df["time"] - self.time_offset_seconds, unit="s", utc=True).dt.tz_convert(None)
        return df.rename(columns={"tick_volume": "volume"})[["ts", "open", "high", "low", "close", "volume"]]

    def fetch_ticker(self, alias):
        name = self.symbol(alias)
        tick = mt5.symbol_info_tick(name)
        if tick is None:
            raise RuntimeError("Fără cotație MT5.")
        raw_time = float(getattr(tick, "time_msc", 0) or 0) / 1000.0
        if raw_time <= 0:
            raw_time = float(getattr(tick, "time", 0) or 0)
        return {"bid": float(tick.bid), "ask": float(tick.ask), "last": float(tick.last or tick.bid),
                "time_utc": self.utc_epoch(raw_time)}

    def filling_mode(self, symbol):
        info = mt5.symbol_info(symbol)
        available = int(info.filling_mode) if info is not None else 0
        if available & int(getattr(mt5, "SYMBOL_FILLING_IOC", 2)):
            return mt5.ORDER_FILLING_IOC
        if available & int(getattr(mt5, "SYMBOL_FILLING_FOK", 1)):
            return mt5.ORDER_FILLING_FOK
        return mt5.ORDER_FILLING_RETURN

    def normalize_price(self, symbol, price):
        info = mt5.symbol_info(symbol)
        if info is None:
            raise RuntimeError(f"Nu pot citi precizia simbolului {symbol}.")
        tick_size = float(getattr(info, "trade_tick_size", 0) or info.point or 0)
        if tick_size > 0:
            price = round(round(float(price) / tick_size) * tick_size, int(info.digits))
        else:
            price = round(float(price), int(info.digits))
        return price

    # FIX 2: metoda lipsă, apelată din check_position_realtime și _close
    def position(self, ticket):
        rows = mt5.positions_get(ticket=int(ticket))
        if rows is None:
            raise RuntimeError(f"Nu pot verifica poziția MT5 {ticket}: {mt5.last_error()}")
        return rows[0] if rows else None

    def managed_positions(self):
        rows = mt5.positions_get()
        if rows is None:
            raise RuntimeError(f"Nu pot căuta pozițiile Nova în MT5: {mt5.last_error()}")
        return [p for p in rows if int(p.magic) == self.magic]

    def alias_for_symbol(self, broker_symbol):
        for alias in self.SYMBOL_DEFAULTS:
            configured = os.environ.get(f"MT5_SYMBOL_{alias}", self.SYMBOL_DEFAULTS[alias])
            if configured == broker_symbol:
                return alias
        raise RuntimeError(f"Poziția MT5 {broker_symbol} nu corespunde niciunui simbol configurat MT5_SYMBOL_*.")

    def open_long(self, alias, risk_cash, max_margin, stop, target):
        self.account = mt5.account_info()
        expected = mt5.ACCOUNT_TRADE_MODE_DEMO if self.mode == "mt5_demo" else mt5.ACCOUNT_TRADE_MODE_REAL
        if self.account is None or self.account.trade_mode != expected:
            raise RuntimeError("Contul MT5 conectat nu mai corespunde modului selectat; ordin blocat.")
        if self.mode == "mt5_live" and os.environ.get("ENABLE_LIVE_TRADING", "").lower() != "true":
            raise RuntimeError("Tranzacționarea MT5 Real este dezactivată; ordin blocat.")
        existing = mt5.positions_get()
        # FIX 6: dacă nu putem verifica pozițiile existente, nu trimitem ordin
        if existing is None:
            raise RuntimeError(f"Nu pot verifica pozițiile MT5 existente: {mt5.last_error()}")
        if any(int(p.magic) == self.magic for p in existing):
            raise RuntimeError("Există deja o poziție deschisă.")
        symbol = self.symbol(alias)
        info = mt5.symbol_info(symbol)
        tick = mt5.symbol_info_tick(symbol)
        if info is None or tick is None:
            raise RuntimeError(f"Datele MT5 lipsesc pentru {symbol}.")
        stop = self.normalize_price(symbol, stop)
        target = self.normalize_price(symbol, target)
        price = float(tick.ask)
        one_lot_loss = abs(float(mt5.order_calc_profit(mt5.ORDER_TYPE_BUY, symbol, 1.0, price, stop) or 0))
        one_lot_margin = float(mt5.order_calc_margin(mt5.ORDER_TYPE_BUY, symbol, 1.0, price) or 0)
        if one_lot_loss <= 0 or one_lot_margin <= 0 or self.account.margin_free <= 0:
            raise RuntimeError(f"MT5 nu poate calcula riscul sau marja pentru {symbol}; ordin blocat.")
        raw_volume = min(risk_cash / one_lot_loss, max_margin / one_lot_margin, self.account.margin_free * 0.95 / one_lot_margin)
        step = float(info.volume_step or 0.01)
        if step <= 0:
            raise RuntimeError(f"Pasul de volum MT5 este invalid pentru {symbol}.")
        raw_volume = min(raw_volume, float(info.volume_max))
        volume = round((raw_volume // step) * step, 8)
        # FIX 5: dacă volumul calculat e sub minimul brokerului, blocăm ordinul (nu îl mărim la minim)
        if volume < float(info.volume_min):
            raise RuntimeError(
                f"Volum calculat {volume} sub minimul brokerului {info.volume_min} pentru {symbol}; ordin blocat."
            )
        request = {
            "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": volume,
            "type": mt5.ORDER_TYPE_BUY, "price": price, "sl": float(stop), "tp": float(target),
            "deviation": 20, "magic": self.magic, "comment": "Nova Trade",
            "type_time": mt5.ORDER_TIME_GTC, "type_filling": self.filling_mode(symbol),
        }
        check = mt5.order_check(request)
        if check is None or check.retcode != 0:
            raise RuntimeError(f"Verificarea ordinului MT5 a eșuat: {check}")
        res = mt5.order_send(request)
        if res is None or res.retcode not in (mt5.TRADE_RETCODE_DONE, getattr(mt5, "TRADE_RETCODE_DONE_PARTIAL", -1)):
            raise RuntimeError(f"Ordin respins: {res}")
        rows = mt5.positions_get(symbol=symbol)
        if rows is None:
            raise RuntimeError(f"Ordin acceptat, dar nu pot citi pozițiile MT5: {mt5.last_error()}; verifică terminalul.")
        ours = [p for p in (rows or []) if p.magic == self.magic]
        if not ours:
            raise RuntimeError("Ordinul a fost acceptat dar poziția nu a putut fi identificată; verifică MT5 manual.")
        return max(ours, key=lambda p: p.time)

    def close_position(self, pos):
        tick = mt5.symbol_info_tick(pos.symbol)
        if tick is None:
            raise RuntimeError(f"Nu există cotație MT5 pentru închiderea poziției {pos.symbol}.")
        request = {
            "action": mt5.TRADE_ACTION_DEAL, "symbol": pos.symbol, "position": pos.ticket,
            "volume": pos.volume, "type": mt5.ORDER_TYPE_SELL, "price": float(tick.bid),
            "deviation": 20, "magic": self.magic, "comment": "Nova close",
            "type_time": mt5.ORDER_TIME_GTC, "type_filling": self.filling_mode(pos.symbol),
        }
        res = mt5.order_send(request)
        # FIX 7: verificăm rezultatul brokerului
        if res is None or res.retcode not in (mt5.TRADE_RETCODE_DONE, getattr(mt5, "TRADE_RETCODE_DONE_PARTIAL", -1)):
            raise RuntimeError(f"Închidere respinsă: {res}")
        remaining = mt5.positions_get(ticket=int(pos.ticket))
        if remaining is None:
            raise RuntimeError(f"Nu pot verifica dacă poziția {pos.ticket} s-a închis: {mt5.last_error()}")
        if remaining:
            raise RuntimeError(f"Poziția MT5 {pos.ticket} a rămas deschisă parțial; verifică terminalul.")
        return res

    def update_stops(self, pos, stop, target):
        stop = self.normalize_price(pos.symbol, stop)
        target = self.normalize_price(pos.symbol, target)
        res = mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "symbol": pos.symbol, "position": pos.ticket, "sl": float(stop), "tp": float(target), "magic": self.magic})
        # FIX 7: verificăm rezultatul brokerului
        if res is None or res.retcode != mt5.TRADE_RETCODE_DONE:
            raise RuntimeError(f"Actualizare SL/TP respinsă: {res}")
        return res

    def closed_result(self, ticket):
        deals = mt5.history_deals_get(position=int(ticket))
        if deals is None or len(deals) == 0:
            raise RuntimeError(f"Istoricul poziției MT5 {ticket} nu este disponibil încă: {mt5.last_error()}")
        pnl = sum(float(d.profit) + float(d.commission) + float(d.swap) + float(getattr(d, "fee", 0)) for d in deals)
        close_entries = {
            getattr(mt5, "DEAL_ENTRY_OUT", -1),
            getattr(mt5, "DEAL_ENTRY_OUT_BY", -2),
            getattr(mt5, "DEAL_ENTRY_INOUT", -3),
        }
        exits = [d for d in deals if getattr(d, "entry", None) in close_entries and float(getattr(d, "volume", 0)) > 0]
        if not exits:
            raise RuntimeError(f"Dealul de închidere MT5 pentru poziția {ticket} nu este disponibil încă.")
        exit_volume = sum(float(d.volume) for d in exits)
        exit_price = sum(float(d.price) * float(d.volume) for d in exits) / exit_volume
        return pnl, exit_price

    def shutdown(self):
        if mt5 is not None: mt5.shutdown()


# --------------------------- DASHBOARD WEB ---------------------------
CSS = """
body { background-color: #0f172a; color: #f8fafc; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; }
.container { max-width: 900px; margin: 0 auto; }
header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; padding-bottom: 15px; margin-bottom: 25px; }
h1 { margin: 0; font-size: 24px; color: #38bdf8; }
.badge { color: white; padding: 4px 12px; border-radius: 12px; font-size: 12px; font-weight: bold; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 25px; }
.card { background: #1e293b; padding: 20px; border-radius: 10px; border: 1px solid #334155; }
.card h3 { margin: 0 0 10px 0; font-size: 14px; color: #94a3b8; text-transform: uppercase; }
.card .value { font-size: 22px; font-weight: bold; }
.pos-box { background: #1e293b; padding: 20px; border-radius: 10px; border: 1px solid #334155; margin-bottom: 25px; }
.actions { display: flex; gap: 15px; margin-top: 25px; flex-wrap: wrap; }
.btn { background: #0284c7; color: white; border: none; padding: 10px 20px; border-radius: 6px; font-weight: bold; cursor: pointer; }
.btn-danger { background: #dc2626; }
.note { color: #94a3b8; font-size: 13px; }
.settings { display: flex; gap: 14px; align-items: end; flex-wrap: wrap; }
.settings label { display: grid; gap: 6px; color: #cbd5e1; font-size: 13px; }
.settings input { background: #0f172a; color: #f8fafc; border: 1px solid #475569; border-radius: 5px; padding: 9px; width: 190px; }
.settings select { background: #0f172a; color: #f8fafc; border: 1px solid #475569; border-radius: 5px; padding: 9px; }
.position-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; }
.position-grid div { background: #0f172a; border-radius: 6px; padding: 10px; }
.position-grid span, .position-grid b { display: block; }
.position-grid span { color: #94a3b8; font-size: 12px; margin-bottom: 5px; }
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { border-bottom: 1px solid #334155; padding: 9px; text-align: left; white-space: nowrap; }
th { color: #94a3b8; }
button:disabled { opacity: .5; cursor: not-allowed; }
.chart-container { position: relative; height: 300px; width: 100%; margin-top: 15px; }
"""

def render_dashboard(state, ctl, can_act, token_configured, token, history=None):
    history = history or []
    cap = fnum(state.get("capital"), START_CAPITAL)
    mid = fnum(state.get("mid_price"))
    psym = html.escape(str(state.get("price_symbol") or SYMBOL))
    scan = state.get("scan") or []
    scan_html = html.escape(" | ".join(str(s) for s in scan)) if scan else "n/a"
    wins = int(fnum(state.get("wins")))
    losses = int(fnum(state.get("losses")))
    pnl = fnum(state.get("current_pnl"))
    stamp = html.escape(str(state.get("timestamp", "N/A")))
    status = html.escape(str(state.get("status", "HOLD")))
    pos = state.get("active_position")
    # FIX 4: istoric real de prețuri pentru grafic
    ph = state.get("price_history") or []
    chart_labels = json.dumps([str(x[0]) for x in ph])
    chart_data = json.dumps([fnum(x[1]) for x in ph])
    if pos:
        pos_html = "<div class=\"position-grid\">" + "".join(
            f"<div><span>{label}</span><b>{html.escape(str(pos.get(key, '—')))}</b></div>"
            for label, key in (("Instrument", "symbol"), ("Direcție", "direction"), ("Intrare", "entry_price"), ("Cantitate", "qty"), (pos.get("notional_label", "Valoare poziție"), "notional"), ("Stop-loss", "target_sl"), ("Take-profit", "target_tp"), ("Lumânări", "bars"))
        ) + "</div>"
    else:
        pos_html = "Nicio poziție deschisă momentan"
    stopped = bool(ctl.get("stopped"))
    badge_color = "#dc2626" if stopped else "#22c55e"
    mode_badges = {"paper": "SIMULARE INTERNĂ", "mt5_demo": "MT5 DEMO", "mt5_live": "MT5 REAL"}
    badge_text = "OPRIT" if stopped else mode_badges.get(ctl.get("execution_mode", "paper"), "MOD NECUNOSCUT")
    pnl_color = "#22c55e" if pnl >= 0 else "#ef4444"

    if can_act:
        k = html.escape(token, quote=True)
        selected_mode = str(ctl.get("execution_mode", "paper"))
        mt5_disabled = "" if MT5_AVAILABLE else "disabled"
        toggle = (
            f'<form method="post" action="/start"><input type="hidden" name="k" value="{k}"><button class="btn">▶ Pornește</button></form>'
            if stopped else
            f'<form method="post" action="/stop"><input type="hidden" name="k" value="{k}"><button class="btn btn-danger">🛑 Oprește</button></form>'
        )
        actions = f'<form method="post" action="/close_now"><input type="hidden" name="k" value="{k}"><button class="btn btn-danger">🚨 Închide poziția acum</button></form>' + toggle
        settings = f'''<section class="card"><h3>Setări bot</h3>
          <form method="post" action="/settings" class="settings">
            <input type="hidden" name="k" value="{k}">
            <label>Mod de tranzacționare
              <select name="execution_mode">
                <option value="paper" {'selected' if selected_mode == 'paper' else ''}>Simulare internă</option>
                <option value="mt5_demo" {'selected' if selected_mode == 'mt5_demo' else ''} {mt5_disabled}>MT5 Demo</option>
                <option value="mt5_live" {'selected' if selected_mode == 'mt5_live' else ''} {mt5_disabled}>MT5 Real</option>
              </select>
            </label>
            <label>Sumă maximă alocată (USD)
              <input type="number" name="trade_amount" min="1" step="1" max="{cap}" value="{min(fnum(ctl.get('trade_amount'), 50), cap):.2f}" required>
            </label>
            <label>Capital simulării (USD)
              <input type="number" name="capital" min="1" step="0.01" value="{cap:.2f}" {'disabled' if selected_mode != 'paper' else ''}>
            </label>
            <button class="btn">Salvează setările</button>
          </form>
          <p class="note">Poți modifica oricând capitalul și suma maximă per poziție (plafonată automat la valoarea capitalului curent).{' MT5 este disponibil numai pe Windows cu terminalul pornit; pe acest server folosește Simulare internă.' if not MT5_AVAILABLE else ''}</p>
        </section>'''
    else:
        actions = '<p class="note">Comenzi inactive (necesită token).</p>'
        settings = '<p class="note">Setările simulării necesită token.</p>'

    history_rows = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(row.get(key, '')))}</td>" for key in ("t_close", "symbol", "entry", "exit", "qty", "pnl", "why")) + "</tr>"
        for row in history
    ) or '<tr><td colspan="7">Nu există tranzacții închise salvate.</td></tr>'

    return f"""<!DOCTYPE html>
<html lang="ro">
<head>
<meta charset="UTF-8"><title>Nova Trade v3.1 Scalper</title><meta http-equiv="refresh" content="3">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>{CSS}</style>
</head>
<body>
<div class="container">
  <header><h1>⚡ Nova Trade v3.1 - Scalper Dinamic</h1><span class="badge" style="background: {badge_color};">{badge_text}</span></header>
  <div class="grid">
    <div class="card"><h3>Capital</h3><div class="value" style="color: #38bdf8;">${cap:.2f}</div></div>
    <div class="card"><h3>Preț {psym}</h3><div class="value">${fmt_price(mid)}</div></div>
    <div class="card"><h3>Win / Loss</h3><div class="value"><span style="color: #22c55e;">{wins}W</span> / <span style="color: #ef4444;">{losses}L</span></div></div>
    <div class="card"><h3>UTC</h3><div class="value" style="font-size: 16px;">{stamp}</div></div>
  </div>
  <div class="pos-box">
    <h3>Status Execuție & Diagramă Live ({psym})</h3>
    <p><b>Motor:</b> {status}</p>
    <p><b>Scanner:</b> <span class="note">{scan_html}</span></p>
    <p><b>Poziție Activă:</b> {pos_html}</p>
    <p><b>PnL Curent:</b> <span style="color: {pnl_color};">${pnl:+.4f}</span></p>
    <div class="chart-container">
      <canvas id="liveChart"></canvas>
    </div>
  </div>
  {settings}
  <div class="actions">{actions}</div>
  <div class="pos-box"><h3>Istoric tranzacții închise (ultimele 20)</h3>
    <div class="table-wrap"><table><thead><tr><th>Închisă la</th><th>Simbol</th><th>Intrare</th><th>Ieșire</th><th>Cantitate</th><th>PnL net</th><th>Motiv</th></tr></thead>
    <tbody>{history_rows}</tbody></table></div>
  </div>
</div>
<script>
const ctx = document.getElementById('liveChart').getContext('2d');
new Chart(ctx, {{
    type: 'line',
    data: {{
        labels: {chart_labels},
        datasets: [{{
            label: 'Preț {psym}',
            data: {chart_data},
            borderColor: '#38bdf8',
            backgroundColor: 'rgba(56, 189, 248, 0.1)',
            borderWidth: 2,
            fill: true,
            tension: 0.2
        }}]
    }},
    options: {{
        responsive: true,
        maintainAspectRatio: false,
        plugins: {{ legend: {{ display: false }} }},
        scales: {{
            x: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }} }},
            y: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }} }}
        }}
    }}
}});
</script>
</body>
</html>"""

class DashboardHandler(BaseHTTPRequestHandler):
    store = None
    @staticmethod
    def _authorized(supplied):
        token = os.environ.get("DASHBOARD_TOKEN", "")
        return bool(token) and hmac.compare_digest(supplied.encode(), token.encode())

    def _send(self, code, body, ctype="text/html; charset=utf-8", headers=None):
        data = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        k = (parse_qs(u.query).get("k") or [""])[0]
        if u.path == "/healthz":
            self._send(200, "ok", "text/plain")
        elif u.path in ("/", "/index.html"):
            st = DashboardHandler.store
            page = render_dashboard(st.load_state() or {}, st.get_control(), self._authorized(k), bool(os.environ.get("DASHBOARD_TOKEN")), k, st.recent_trades())
            self._send(200, page)
        else:
            self._send(404, "Not found", "text/plain")

    def do_POST(self):
        u = urlparse(self.path)
        length = min(int(self.headers.get("Content-Length") or 0), 4096)
        form = parse_qs(self.rfile.read(length).decode("utf-8", errors="ignore"))
        k = (form.get("k") or [""])[0]
        if not self._authorized(k):
            self._send(403, "Interzis", "text/plain")
            return
        st = DashboardHandler.store
        if u.path == "/stop": st.set_control(stopped=True)
        elif u.path == "/start": st.set_control(stopped=False)
        elif u.path == "/close_now": st.set_control(close_now=True)
        elif u.path == "/settings":
            try:
                amount = float((form.get("trade_amount") or [""])[0])
                if not 1 <= amount <= 1_000_000:
                    raise ValueError("Sumă maximă invalidă.")
                mode = (form.get("execution_mode") or ["paper"])[0]
                if mode not in ("paper", "mt5_demo", "mt5_live"):
                    raise ValueError("Mod de execuție invalid.")
                if mode in ("mt5_demo", "mt5_live") and not MT5_AVAILABLE:
                    self._send(409, "MT5 necesită Windows și terminalul MetaTrader 5; selectează Simulare internă pe acest server.", "text/plain")
                    return
                if mode == "mt5_live" and os.environ.get("ENABLE_LIVE_TRADING", "").lower() != "true":
                    self._send(403, "Modul MT5 Real este blocat.", "text/plain")
                    return
                current_state = st.load_state() or {}
                active = current_state.get("active_position")
                raw_pos = current_state.get("active_position_raw") or {}
                if active:
                    if raw_pos.get("mt5_ticket") is not None:
                        open_mode = raw_pos.get("mt5_mode")
                        if mode == "paper" or (open_mode and mode != open_mode):
                            self._send(409, "Închide poziția MT5 în modul în care a fost deschisă înainte să schimbi modul.", "text/plain")
                            return
                        if not open_mode and mode != st.get_control().get("execution_mode", "paper"):
                            self._send(409, "Poziția MT5 salvată nu conține modul inițial. Închide-o manual în terminal înainte de a schimba modul.", "text/plain")
                            return
                    elif mode != "paper":
                        self._send(409, "Închide poziția curentă înainte să schimbi modulul de tranzacționare.", "text/plain")
                        return
                capital_text = (form.get("capital") or [""])[0].strip()
                capital = None
                state_capital = fnum(current_state.get("capital"), START_CAPITAL)
                if capital_text and mode == "paper":
                    requested_capital = float(capital_text)
                    if not 1 <= requested_capital <= 100_000_000:
                        raise ValueError("Capital invalid.")
                    if active and abs(requested_capital - state_capital) > 0.01:
                        self._send(409, "Capitalul simulării nu poate fi schimbat cât timp există o poziție deschisă.", "text/plain")
                        return
                    if not active:
                        capital = requested_capital
                max_cap = capital if capital is not None else state_capital
                if amount > max_cap: amount = max_cap
                updates = {"trade_amount": amount, "execution_mode": mode}
                if capital is not None:
                    updates["capital_override"] = capital
                st.set_control(**updates)
            except (ValueError, TypeError):
                self._send(400, "Valori invalide.", "text/plain")
                return
        self._send(303, "", "text/plain", {"Location": "/?k=" + quote(k)})

    def log_message(self, format, *args): pass

def start_health_server():
    port = int(os.environ.get("PORT", 10000))
    HTTPServer(("0.0.0.0", port), DashboardHandler).serve_forever()


# --------------------------- INDICATORI & SEMNAL ---------------------------
def to_frame(rows):
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    return df

def add_indicators(df):
    df = df.copy()
    c = df["close"]
    df["ema_fast"] = c.ewm(span=9, adjust=False).mean()
    df["ema_trend"] = c.ewm(span=50, adjust=False).mean()
    tr = pd.concat([df["high"] - df["low"], (df["high"] - c.shift()).abs(), (df["low"] - c.shift()).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / 14, adjust=False).mean()
    df["breakout_high"] = df["high"].rolling(20).max().shift(1)
    return df

def signal(r, cost_rt=COST_RT):
    close = r["close"]
    ema_fast = r.get("ema_fast", close)
    ema_trend = r.get("ema_trend", close)
    return bool(
        close > ema_fast
        and ema_fast > ema_trend
        and close > r.get("breakout_high", float("inf"))
        and (close / ema_fast - 1) >= cost_rt
    )


# --------------------------- DATE & SCANNER ---------------------------
def _naive_utc(idx):
    idx = pd.DatetimeIndex(idx)
    return idx.tz_convert("UTC").tz_localize(None) if idx.tz is not None else idx

def fetch_candles(ex, sym):
    if isinstance(ex, MT5Bridge):
        return ex.fetch_ohlcv(sym, TIMEFRAME, limit=500)
    m = MARKETS[sym]
    if m["src"] == "ccxt":
        return to_frame(ex.fetch_ohlcv(sym, TIMEFRAME, limit=200))
    if yf is None: raise RuntimeError("yfinance lipsă")
    raw = yf.Ticker(m["yf"]).history(period="5d", interval="15m", auto_adjust=False)
    if raw is None or raw.empty: raise ValueError("fără date")
    df = pd.DataFrame({
        "ts": _naive_utc(raw.index),
        "open": raw["Open"].to_numpy(dtype=float),
        "high": raw["High"].to_numpy(dtype=float),
        "low": raw["Low"].to_numpy(dtype=float),
        "close": raw["Close"].to_numpy(dtype=float),
        "volume": raw["Volume"].to_numpy(dtype=float) if "Volume" in raw else 0.0,
    })
    return df.dropna().drop_duplicates("ts").sort_values("ts").reset_index(drop=True)

def fetch_price(ex, sym, side="buy"):
    if isinstance(ex, MT5Bridge):
        tick = ex.fetch_ticker(sym)
        age = time.time() - tick["time_utc"]
        fresh = -120 <= age <= STALE_PRICE_SECONDS
        return fnum(tick["ask"] if side == "buy" else tick["bid"]), fresh
    m = MARKETS[sym]
    if m["src"] == "ccxt":
        t = ex.fetch_ticker(sym)
        return fnum(t.get("last") or t.get("close")), True
    if yf is None: raise RuntimeError("yfinance lipsă")
    h = yf.Ticker(m["yf"]).history(period="1d", interval="1m", auto_adjust=False)
    if h is None or h.empty: return 0.0, False
    return fnum(h["Close"].iloc[-1]), (utc_now() - _naive_utc(h.index)[-1]).total_seconds() <= STALE_PRICE_SECONDS

def scan_markets(ex, now, slot, scanned, scan_info):
    best = None
    age = (now - slot).total_seconds()
    for sym, m in MARKETS.items():
        if scanned.get(sym) == slot: continue
        try:
            df = add_indicators(fetch_candles(ex, sym))
        except Exception:
            scan_info[sym] = "eroare date"
            continue
        closed = df[df["ts"] + pd.Timedelta(minutes=BAR_MINUTES) <= slot].dropna().reset_index(drop=True)
        if len(closed) == 0:
            scan_info[sym] = "date insuficiente"
            continue
        bar = closed.iloc[-1]
        scanned[sym] = slot
        cost_rt = 2 * (m["fee"] + m["slip"])
        if age <= m["max_age"] and signal(bar, cost_rt):
            score = float((bar["close"] - bar["ema_fast"]) / bar["atr"])
            scan_info[sym] = f"SEMNAL (scor {score:.2f})"
            if best is None or score > best[0]:
                best = (score, sym, bar)
        else:
            scan_info[sym] = "fără semnal"
    return best


# --------------------------- MOTOR ---------------------------
class Engine:
    def __init__(self, capital, cfg=None, on_trade=None):
        self.cfg = dict(DEFAULTS, **(cfg or {}))
        self.on_trade = on_trade
        self.equity = capital
        self.pos = None
        self.wins = 0
        self.losses = 0
        self.day = None
        self.day_start = capital

    def log(self, msg): print(msg, flush=True)

    def recover_mt5_position(self, broker_pos, bridge, mode):
        alias = bridge.alias_for_symbol(broker_pos.symbol)
        if int(broker_pos.type) != int(mt5.POSITION_TYPE_BUY):
            raise RuntimeError(f"Poziția Nova {broker_pos.symbol} nu este LONG; nu o pot administra în siguranță.")
        opened = pd.to_datetime(bridge.utc_epoch(broker_pos.time), unit="s", utc=True).tz_convert(None)
        frame = add_indicators(bridge.fetch_ohlcv(alias, TIMEFRAME, limit=500))
        # Preferăm să deducem ATR-ul inițial din SL-ul deja plasat de broker;
        # astfel trailing-ul recuperat păstrează distanța inițială a riscului.
        stop = float(broker_pos.sl or 0.0)
        stop_mult = max(0.0, fnum(self.cfg.get("atr_stop_mult"), 0.0))
        atr = (float(broker_pos.price_open) - stop) / stop_mult if stop > 0 and stop_mult > 0 else 0.0
        if atr <= 0:
            before_open = frame.loc[frame["ts"] <= opened, "atr"].dropna()
            valid_atr = before_open if not before_open.empty else frame["atr"].dropna()
            if valid_atr.empty or float(valid_atr.iloc[-1]) <= 0:
                raise RuntimeError(f"Nu pot calcula ATR pentru recuperarea poziției MT5 {broker_pos.symbol}.")
            atr = float(valid_atr.iloc[-1])
        # OHLC-ul nu arată ce s-a întâmplat înainte de intrare în lumânarea curentă;
        # excludem acea lumânare parțială pentru a nu inventa un high-water prea mare.
        after_open = frame.loc[frame["ts"] >= opened.ceil(f"{BAR_MINUTES}min"), "high"].dropna()
        current_tick = bridge.fetch_ticker(alias)
        high_water = max(
            [float(broker_pos.price_open), float(current_tick["bid"])]
            + ([float(after_open.max())] if not after_open.empty else [])
        )
        target = float(broker_pos.tp or 0.0)
        margin = float(mt5.order_calc_margin(mt5.ORDER_TYPE_BUY, broker_pos.symbol, broker_pos.volume, broker_pos.price_open) or 0.0)
        self.pos = dict(
            entry=float(broker_pos.price_open), stop=stop, tp=target, qty=float(broker_pos.volume),
            bars=max(0, int((utc_now() - opened).total_seconds() // (BAR_MINUTES * 60))),
            hh=high_water, atr0=atr, fee_in=0.0, t=opened, t_open=opened,
            margin=margin, symbol=alias, mt5_ticket=int(broker_pos.ticket),
            mt5_identifier=int(getattr(broker_pos, "identifier", broker_pos.ticket)),
            mt5_mode=mode, fee=MARKETS[alias]["fee"], slip=MARKETS[alias]["slip"],
            broker_profit=float(broker_pos.profit),
        )
        self.log(f"{utc_now()} RECOVERED MT5 {alias} ticket {broker_pos.ticket} @ {rnd(broker_pos.price_open)}")

    def roll_day(self, today):
        if today != self.day: self.day, self.day_start = today, self.equity

    def trading_allowed(self):
        c = self.cfg
        return bool(c["allow_new_trades"]) and not c["stopped"] and self.equity > self.day_start * (1 - c["daily_loss_limit"])

    def unrealized(self, price):
        p = self.pos
        if p is None: return 0.0
        if p.get("mt5_ticket") is not None:
            return fnum(p.get("broker_profit"))
        symbol = p.get("symbol", SYMBOL)
        gross = quote_to_usd(symbol, p["qty"] * (price - p["entry"]), price)
        fee_out = quote_to_usd(symbol, p["qty"] * price * p.get("fee", FEE), price)
        return gross - p["fee_in"] - fee_out

    def _open(self, bar, price, symbol, ex=None):
        c = self.cfg
        mode = c.get("execution_mode", "paper")
        mk = MARKETS[symbol]
        fee, slip = mk["fee"], mk["slip"]
        atr = float(bar["atr"])
        mult = c["atr_stop_mult"]

        if mode in ("mt5_demo", "mt5_live") and isinstance(ex, MT5Bridge):
            try:
                entry_tick = ex.fetch_ticker(symbol)
                entry = entry_tick["ask"]
                stop, tp = entry - mult * atr, entry + 1.5 * atr
                risk_budget = self.equity * c["risk_per_trade"]
                max_margin = min(c.get("trade_amount", 50.0), self.equity)
                pos = ex.open_long(symbol, risk_budget, max_margin, stop, tp)
                if pos is not None:
                    # FIX 3: TradePosition nu are atributul "margin"; îl calculăm
                    pos_margin = float(mt5.order_calc_margin(mt5.ORDER_TYPE_BUY, pos.symbol, pos.volume, pos.price_open) or 0.0)
                    self.pos = dict(
                        entry=pos.price_open, stop=pos.sl, tp=pos.tp, qty=pos.volume,
                        bars=0, hh=pos.price_open, atr0=atr, fee_in=0.0,
                        t=bar["ts"], t_open=utc_now(), margin=pos_margin,
                        symbol=symbol, mt5_ticket=int(pos.ticket), mt5_identifier=int(pos.identifier),
                        mt5_mode=mode, fee=fee, slip=slip, broker_profit=float(pos.profit)
                    )
                    self.log(f"{utc_now()} MT5 BUY {symbol} {pos.volume} @ {rnd(pos.price_open)}")
            except Exception as e:
                self.log(f"Eroare deschidere MT5: {e}")
            return

        # Simulare internă (Paper)
        entry = float(price) * (1 + slip)
        stop, tp = entry - mult * atr, entry + 1.5 * atr
        if stop <= 0 or stop >= entry: return
        risk_budget = self.equity * c["risk_per_trade"]
        risk_per_unit = quote_to_usd(symbol, entry - stop, entry)
        qty = risk_budget / risk_per_unit
        max_allowed_amount = min(c.get("trade_amount", 50.0), self.equity)
        qty = min(qty, max_allowed_amount / entry, self.equity * 0.99 / entry)
        if qty <= 0: return
        self.pos = dict(
            entry=entry, stop=stop, tp=tp, qty=float(qty), bars=0, hh=entry,
            atr0=atr, fee_in=float(quote_to_usd(symbol, qty * entry * fee, entry)), t=bar["ts"],
            t_open=utc_now(), margin=round(self.equity * c["risk_per_trade"], 2),
            symbol=symbol, fee=fee, slip=slip,
        )
        self.log(f"{utc_now()} PAPER BUY {symbol} {qty:.6f} @ {rnd(entry)}")

    def check_position_realtime(self, price, now, ex=None):
        p, c = self.pos, self.cfg
        if p is None or price <= 0: return False, "HOLD"
        mode = c.get("execution_mode", "paper")

        if mode in ("mt5_demo", "mt5_live") and isinstance(ex, MT5Bridge) and "mt5_ticket" in p:
            mt5_pos = ex.position(p["mt5_ticket"])
            if mt5_pos is None:
                self._record_mt5_exit(p, now, "MT5_EXIT", ex)
                return True, "MT5_CLOSED"
            p["broker_profit"] = float(mt5_pos.profit)
            p["entry"] = float(mt5_pos.price_open)
            p["stop"] = float(mt5_pos.sl or p["stop"])
            p["tp"] = float(mt5_pos.tp or p["tp"])
            p["last_price"] = float(price)
            p["bars"] = int((now - p["t_open"]).total_seconds() // (BAR_MINUTES * 60))
            if c["trailing_stop"]:
                p["hh"] = max(p["hh"], price)
                if p["hh"] >= p["entry"] + c["trail_activate_atr"] * p["atr0"]:
                    new_stop = max(p["stop"], p["entry"] * (1 + 2 * p.get("slip", 0)), p["hh"] - c["trail_atr"] * p["atr0"])
                    if new_stop > p["stop"]:
                        try:
                            ex.update_stops(mt5_pos, new_stop, p["tp"])
                            p["stop"] = ex.normalize_price(mt5_pos.symbol, new_stop)
                        except Exception as e:
                            self.log(f"Trailing MT5 respins; păstrez stopul anterior și continui gestionarea poziției: {e}")
            if p["bars"] >= MAX_BARS:
                self._close(price, "TIME_EXIT", now, ex)
                return self.pos is None, "TIME_EXIT" if self.pos is None else "HOLD_ACTIVE"
            return False, "HOLD_ACTIVE"

        slip = p.get("slip", SLIPPAGE)
        p["bars"] = int((now - p["t_open"]).total_seconds() // (BAR_MINUTES * 60))

        if c["trailing_stop"]:
            p["hh"] = max(p["hh"], price)
            if p["hh"] >= p["entry"] + c["trail_activate_atr"] * p["atr0"]:
                p["stop"] = max(p["stop"], p["entry"] * (1 + 2 * slip), p["hh"] - c["trail_atr"] * p["atr0"])

        if price <= p["stop"]:
            self._close(min(price, p["stop"]) * (1 - slip), "STOP", now, ex)
            return True, "STOP_LOSS"
        if price >= p["tp"]:
            self._close(p["tp"], "TP", now, ex)
            return True, "TAKE_PROFIT"
        if p["bars"] >= MAX_BARS:
            self._close(price * (1 - slip), "TIME", now, ex)
            return True, "TIME_EXIT"
        return False, "HOLD_ACTIVE"

    def _close(self, px, why, ts, ex=None):
        p = self.pos
        if p.get("mt5_ticket") and isinstance(ex, MT5Bridge):
            try:
                mt5_pos = ex.position(p["mt5_ticket"])
                if mt5_pos: ex.close_position(mt5_pos)
                self._record_mt5_exit(p, ts, why, ex)
            except Exception as e:
                # FIX 7: dacă închiderea a eșuat, poziția rămâne în evidență (nu o ștergem)
                self.log(f"Eroare închidere MT5: {e}")
                return
            self.pos = None
            return

        symbol = p.get("symbol", SYMBOL)
        gross = quote_to_usd(symbol, p["qty"] * (px - p["entry"]), px)
        fee_out = quote_to_usd(symbol, p["qty"] * px * p.get("fee", FEE), px)
        pnl = gross - p["fee_in"] - fee_out
        self.equity += pnl
        if pnl > 0: self.wins += 1
        else: self.losses += 1
        rec = dict(
            t=str(p["t"]), t_open=str(p["t_open"]), t_close=str(ts),
            symbol=p.get("symbol", SYMBOL), entry=rnd(p["entry"]), exit=rnd(px),
            qty=round(p["qty"], 6), pnl=round(float(pnl), 4), why=why,
            equity_after=round(self.equity, 4),
        )
        if self.on_trade:
            try: self.on_trade(rec)
            except Exception: pass
        self.pos = None

    def _record_mt5_exit(self, p, ts, why, ex):
        position_id = p.get("mt5_identifier", p["mt5_ticket"])
        pnl, exit_price = ex.closed_result(position_id)
        account = mt5.account_info()
        self.equity = float(account.equity) if account is not None else self.equity + pnl
        if pnl > 0: self.wins += 1
        else: self.losses += 1
        rec = dict(
            t=str(p["t"]), t_open=str(p["t_open"]), t_close=str(ts),
            symbol=p.get("symbol", SYMBOL), entry=rnd(p["entry"]),
            exit=rnd(exit_price), qty=round(p["qty"], 6),
            pnl=round(float(pnl), 4), why=why, equity_after=round(self.equity, 4),
        )
        if self.on_trade:
            try: self.on_trade(rec)
            except Exception: pass
        self.log(f"{ts} MT5 CLOSED {rec['symbol']} PnL {pnl:+.2f} Equity {self.equity:.2f}")
        self.pos = None

    def force_close(self, price, now, ex=None):
        if self.pos is not None:
            self._close(price * (1 - self.pos.get("slip", SLIPPAGE)), "MANUAL", now, ex)


# ------------------------- BUCLA LIVE --------------------
def run_paper_loop():
    mongo = MongoManager()
    store = Store(mongo)
    DashboardHandler.store = store
    threading.Thread(target=start_health_server, daemon=True).start()

    ccxt_ex = getattr(ccxt, EXCHANGE)({"enableRateLimit": True})
    eng = Engine(START_CAPITAL, store.get_control(), on_trade=store.add_trade)

    saved = store.load_state()
    if saved:
        eng.equity = fnum(saved.get("equity", saved.get("capital")), START_CAPITAL)
        eng.wins = int(fnum(saved.get("wins")))
        eng.losses = int(fnum(saved.get("losses")))
        eng.pos = deser_pos(saved.get("active_position_raw"))

    slot_unit = f"{BAR_MINUTES}min"
    scanned, scan_info = {}, {}
    current_mt5_bridge = None
    active_mode = None
    last_price = {}              # FIX 1: ultimul preț valid per simbol
    price_hist, hist_sym = [], None   # FIX 4: istoric real pentru grafic

    print("\n--- [NOVA TRADE v3.1] Bot pornit ---", flush=True)

    while True:
        try:
            cfg = store.get_control()
            mode = cfg.get("execution_mode", "paper")

            if eng.pos is not None:
                saved_mt5 = eng.pos.get("mt5_ticket") is not None
                saved_mode = eng.pos.get("mt5_mode")
                if saved_mode and mode != saved_mode:
                    print(f"Modul din control ({mode}) nu corespunde poziției ({saved_mode}); restabilesc modulul poziției.", flush=True)
                    mode = saved_mode
                    cfg["execution_mode"] = mode
                    store.set_control(execution_mode=mode)
                elif saved_mt5 and mode == "paper":
                    raise RuntimeError("Există o poziție MT5 deschisă. Selectează modul MT5 original și închide poziția înainte să revii la simulare.")
                elif not saved_mt5 and mode != "paper":
                    print("Poziția deschisă este paper; restabilesc modul Simulare internă.", flush=True)
                    mode = "paper"
                    cfg["execution_mode"] = mode
                    store.set_control(execution_mode=mode)

            if mode != active_mode:
                previous_mode = active_mode
                if eng.pos is not None and active_mode is not None:
                    raise RuntimeError("Nu schimb modul cât timp există o poziție deschisă.")
                if current_mt5_bridge and eng.pos is None:
                    current_mt5_bridge.shutdown()
                    current_mt5_bridge = None
                if mode in ("mt5_demo", "mt5_live"):
                    try:
                        current_mt5_bridge = MT5Bridge(mode)
                        if current_mt5_bridge.account:
                            eng.equity = float(current_mt5_bridge.account.balance)
                    except Exception as e:
                        current_mt5_bridge = None
                        active_mode = None
                        raise RuntimeError(f"MT5 nu este disponibil; modul selectat rămâne {mode} și nu se execută tranzacții: {e}") from e
                active_mode = mode
                cfg["execution_mode"] = mode
                if eng.pos is None and previous_mode is not None:
                    eng.day = utc_now().date()
                    eng.day_start = eng.equity

            ex = current_mt5_bridge if mode in ("mt5_demo", "mt5_live") and current_mt5_bridge else ccxt_ex

            if current_mt5_bridge is not None:
                account = mt5.account_info()
                if account is None:
                    try: current_mt5_bridge.shutdown()
                    except Exception: pass
                    current_mt5_bridge = None
                    active_mode = None
                    raise RuntimeError("Sesiunea MT5 s-a deconectat; voi încerca reconectarea la următoarea verificare. Nicio poziție salvată nu este trecută în simulare.")
                eng.equity = float(account.equity)
                broker_positions = current_mt5_bridge.managed_positions()
                if eng.pos is None and broker_positions:
                    if len(broker_positions) != 1:
                        raise RuntimeError(f"Am găsit {len(broker_positions)} poziții Nova în MT5. Botul gestionează una singură; nu va deschide poziții noi până la verificarea lor.")
                    eng.recover_mt5_position(broker_positions[0], current_mt5_bridge, mode)
                elif eng.pos is not None and eng.pos.get("mt5_ticket") is not None:
                    extras = [p for p in broker_positions if int(p.ticket) != int(eng.pos["mt5_ticket"])]
                    if extras:
                        raise RuntimeError("Există poziții Nova suplimentare în MT5 față de starea salvată; nu deschid ordine noi până la verificarea lor.")

            if cfg.get("capital_override") is not None and mode == "paper":
                new_capital = fnum(cfg.get("capital_override"))
                if new_capital >= 1:
                    eng.equity = new_capital
                    eng.day_start = new_capital
                store.set_control(capital_override=None)
                cfg["capital_override"] = None

            # Păstrăm administrarea unei poziții existente, dar nu permitem intrări
            # noi pe bani reali dacă protecția explicită nu este activată.
            if mode == "mt5_live" and os.environ.get("ENABLE_LIVE_TRADING", "").lower() != "true":
                cfg["allow_new_trades"] = False
            eng.cfg = cfg
            now = utc_now()

            cur_sym = eng.pos["symbol"] if eng.pos is not None else SYMBOL
            price, price_ok = fetch_price(ex, cur_sym, "sell" if eng.pos is not None else "buy")
            # FIX 1: dacă prețul nu s-a putut citi, nu mai folosim 1.0 pentru decizii
            price_valid = bool(price_ok and price > 0)
            if price_valid:
                last_price[cur_sym] = price
            else:
                price = last_price.get(cur_sym, 1.0)

            # FIX 4: adăugăm prețul real în istoricul graficului
            if price_valid:
                if hist_sym != cur_sym:
                    price_hist, hist_sym = [], cur_sym
                price_hist.append([now.strftime("%H:%M:%S"), rnd(price)])
                price_hist = price_hist[-120:]

            eng.roll_day(now.date())

            if cfg["close_now"]:
                # FIX 1: nu închidem manual la un preț invalid; cererea rămâne în așteptare
                if eng.pos is None or price_valid or isinstance(ex, MT5Bridge):
                    if eng.pos is not None:
                        eng.force_close(price, now, ex)
                        for s in MARKETS: scanned[s] = now.floor(slot_unit)
                    store.set_control(close_now=False)

            status = "HOLD"
            if eng.pos is not None:
                # FIX 1: fără preț valid, nu verificăm STOP/TP/TIME
                if price_valid:
                    _, status = eng.check_position_realtime(price, now, ex)
                else:
                    status = "HOLD_ACTIVE"
            elif eng.trading_allowed():
                slot = now.floor(slot_unit)
                if any(scanned.get(s) != slot for s in MARKETS):
                    best = scan_markets(ex, now, slot, scanned, scan_info)
                    if best is not None:
                        _, sym, bar = best
                        px2, ok2 = fetch_price(ex, sym)
                        if ok2 and px2 > 0:
                            eng._open(bar, px2, sym, ex)
                            if eng.pos is not None:
                                cur_sym, price = sym, px2
                                last_price[sym] = px2
                                # FIX 4: istoricul grafic urmează noul simbol
                                price_hist, hist_sym = [[now.strftime("%H:%M:%S"), rnd(px2)]], sym
            elif cfg["stopped"]:
                status = "STOPPED"

            if eng.pos is not None and status in ("HOLD", "STOPPED"):
                status = "HOLD_ACTIVE"

            pos_dash, raw_pos = None, None
            if eng.pos is not None:
                p = eng.pos
                pos_dash = {
                    "symbol": p.get("symbol", SYMBOL),
                    "entry_price": rnd(p["entry"]),
                    "qty": round(p["qty"], 6),
                    "margin": p.get("margin", 0.0),
                    "direction": "LONG",
                    "target_tp": rnd(p["tp"]),
                    "target_sl": rnd(p["stop"]),
                    "notional": round(usd_notional(p.get("symbol", SYMBOL), p["qty"], p["entry"]), 2),
                    "notional_label": "Marjă poziție" if mode != "paper" else "Valoare poziție",
                    "bars": p["bars"],
                }
                raw_pos = ser_pos(p)

            store.save_state({
                "timestamp": now.strftime("%H:%M:%S"),
                "capital": round(eng.equity, 2),
                "equity": float(eng.equity),
                "mid_price": rnd(price),
                "price_symbol": cur_sym,
                "price_history": price_hist,
                "scan": [f"{s}: {t}" for s, t in scan_info.items()],
                "wins": eng.wins,
                "losses": eng.losses,
                "trades_count": eng.wins + eng.losses,
                "active_position": pos_dash,
                "active_position_raw": raw_pos,
                "current_pnl": round(eng.unrealized(price), 4),
                "day": eng.day.isoformat() if eng.day else None,
                "day_start": float(eng.day_start),
                "status": status,
            })
        except Exception as e:
            print("Eroare buclă:", e, flush=True)
            try:
                saved_state = store.load_state() or {}
                saved_state["timestamp"] = utc_now().strftime("%H:%M:%S")
                saved_state["status"] = f"EROARE: {str(e)[:180]}"
                store.save_state(saved_state)
            except Exception:
                pass

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    run_paper_loop()