"""
engine.py - Nova Trade v3.0 (paper trading, persistență MongoDB, execuție real-time, multi-piață)
Capital inițial: $150.00 | Scanner pe Forex, Metale, Energie (15m)

Variabile de mediu:
  PORT            portul serverului web (Render îl setează automat)
  MONGO_*         cele folosite de storage.mongo_db.MongoManager
  DASHBOARD_TOKEN token secret pentru butoanele Stop / Start / Închide poziția
                  (fără el, dashboard-ul e doar de citit). Deschizi /?k=TOKEN
  EXCHANGE        opțional, implicit "binance" (ex: "kraken" dacă Binance dă 451)

v3: marcajele "# v3" arată fiecare zonă adăugată sau modificată față de v2.3.
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

try:  # v3: yfinance e folosit doar pentru piețele non-crypto
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None

from storage.mongo_db import MongoManager

# ------------------------- PARAMETRI -------------------------
EXCHANGE = os.environ.get("EXCHANGE", "binance")
TIMEFRAME = "15m"
BAR_MINUTES = 15
START_CAPITAL = 150.0
FEE = 0.001
SLIPPAGE = 0.0002
MAX_BARS = 24             # ieșire forțată după 24 lumânări (6 ore)
RSI_BUY = 30
TREND_TOL = 0.02
COST_RT = 2 * (FEE + SLIPPAGE)
MIN_EDGE = 3 * COST_RT
POLL_SECONDS = 30
MAX_SIGNAL_AGE = 120     # secunde: nu intrăm pe un semnal mai vechi de atât
STALE_PRICE_SECONDS = 900  # v3: prețul non-crypto mai vechi de 15 min = piață închisă

# v3: piețele reale stabilite (Forex, Metale, Energie).
MARKETS = {
    "EURUSD": dict(src="yf", yf="EURUSD=X", fee=0.0, slip=0.00005, max_age=300),
    "GBPUSD": dict(src="yf", yf="GBPUSD=X", fee=0.0, slip=0.00005, max_age=300),
    "USDJPY": dict(src="yf", yf="USDJPY=X", fee=0.0, slip=0.00005, max_age=300),
    "GOLD": dict(src="yf", yf="GC=F", fee=0.0, slip=0.00015, max_age=300),
    "OIL": dict(src="yf", yf="CL=F", fee=0.0, slip=0.0003, max_age=300),
}

# Definim un simbol principal implicit pentru starea inițială
SYMBOL = "EURUSD"

STATE_COLLECTION = "motor_state"
CONTROL_COLLECTION = "motor_control"
TRADES_COLLECTION = "trades"

DEFAULTS = dict(
    stopped=False,           # True = nu deschide poziții noi (poziția deschisă rămâne gestionată)
    allow_new_trades=True,
    close_now=False,
    vol_adjust_stop=True,
    trailing_stop=True,      # v3: era False
    tp_extend=True,          # v3: când prețul atinge TP, TP-ul se mută mai sus în loc să închidă
    tp_extend_atr=1.0,       # v3: cu cât se mută TP-ul (în ATR)
    tp_lock_atr=0.5,         # v3: stop-ul urcă la vechiul TP minus atât (în ATR)
    risk_per_trade=0.015,
    daily_loss_limit=0.05,
    atr_stop_mult=1.5,
    trail_activate_atr=1.0,
    trail_atr=1.0,
    manual_stop=None,
    manual_tp=None,
)


# --------------------------- UTILITARE ---------------------------
def utc_now():
    """Timp UTC naiv (la fel ca lumânările din ccxt)."""
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
    """v3: rotunjire adaptivă (2 zecimale pentru prețuri mari, 6 pentru EURUSD etc.)."""
    x = float(x)
    return round(x, 2 if abs(x) >= 100 else 6)


def fmt_price(x):
    """v3: afișare adaptivă a prețului în dashboard."""
    return f"{x:,.2f}" if abs(x) >= 100 else f"{x:.6f}"


def ser_pos(p):
    """Poziția -> dict simplu, serializabil în Mongo."""
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
    """dict din Mongo -> poziție utilizabilă de motor (sau None dacă e incompletă)."""
    if not raw:
        return None
    need = ("entry", "stop", "tp", "qty", "fee_in", "atr0", "hh", "t", "t_open")
    if any(k not in raw for k in need):
        print("Poziția salvată e incompletă, o ignor.")
        return None
    p = dict(raw)
    try:
        p["t"] = pd.Timestamp(p["t"])
        p["t_open"] = pd.Timestamp(p["t_open"])
        for k in ("entry", "stop", "tp", "qty", "fee_in", "atr0", "hh"):
            p[k] = float(p[k])
        p["bars"] = int(p.get("bars", 0))
        p["symbol"] = p.get("symbol") or SYMBOL
        mk = MARKETS.get(p["symbol"], MARKETS[SYMBOL])
        p["fee"] = float(p.get("fee", mk["fee"]))
        p["slip"] = float(p.get("slip", mk["slip"]))
    except Exception as e:
        print("Eroare la restaurarea poziției:", e)
        return None
    return p


# ------------------------- STOCARE (MongoDB + fallback în memorie) -------------------------
class Store:
    def __init__(self, mongo):
        self.mongo = mongo
        self.lock = threading.Lock()
        self.mem_state = {}
        self.mem_control = {}

    def _col(self, name):
        try:
            db = getattr(self.mongo, "db", None) if self.mongo else None
            return db[name] if db is not None else None
        except Exception:
            return None

    # --- stare ---
    def save_state(self, data):
        with self.lock:
            self.mem_state = dict(data)
        col = self._col(STATE_COLLECTION)
        if col is None:
            return
        try:
            col.update_one({"_id": "current_state"}, {"$set": data}, upsert=True)
        except Exception as e:
            print("Eroare la salvarea stării în MongoDB:", e)

    def load_state(self):
        col = self._col(STATE_COLLECTION)
        if col is not None:
            try:
                res = col.find_one({"_id": "current_state"})
                if res:
                    res.pop("_id", None)
                    return res
            except Exception as e:
                print("Eroare la citirea stării din MongoDB:", e)
        with self.lock:
            return dict(self.mem_state) or None

    # --- control ---
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
            except Exception as e:
                print("Eroare la citirea controlului din MongoDB:", e)
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
        except Exception as e:
            print("Eroare la scrierea controlului în MongoDB:", e)

    # --- tranzacții ---
    def add_trade(self, rec):
        col = self._col(TRADES_COLLECTION)
        if col is None:
            return
        try:
            col.insert_one(dict(rec))
        except Exception as e:
            print("Eroare la salvarea tranzacției:", e)


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
pre { background: #0f172a; padding: 10px; border-radius: 6px; overflow-x: auto; }
.actions { display: flex; gap: 15px; margin-top: 25px; flex-wrap: wrap; }
.btn { background: #0284c7; color: white; border: none; padding: 10px 20px; border-radius: 6px; font-weight: bold; cursor: pointer; }
.btn-danger { background: #dc2626; }
.note { color: #94a3b8; font-size: 13px; }
.footer { text-align: center; color: #64748b; font-size: 12px; margin-top: 40px; }
"""


def render_dashboard(state, ctl, can_act, token_configured, token):
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
    margin = fnum(pos.get("margin")) if isinstance(pos, dict) else 0.0
    pos_html = (
        "<pre>" + html.escape(json.dumps(pos, indent=2)) + "</pre>"
        if pos else "Nicio poziție deschisă momentan"
    )
    stopped = bool(ctl.get("stopped"))
    badge_color = "#dc2626" if stopped else "#22c55e"
    badge_text = "OPRIT (fără poziții noi)" if stopped else "LIVE 24/7"
    pnl_color = "#22c55e" if pnl >= 0 else "#ef4444"

    if can_act:
        k = html.escape(token, quote=True)
        toggle = (
            f'<form method="post" action="/start"><input type="hidden" name="k" value="{k}">'
            f'<button class="btn">▶ Pornește tranzacționarea</button></form>'
            if stopped else
            f'<form method="post" action="/stop" onsubmit="return confirm(\'Oprești deschiderea de poziții noi?\')">'
            f'<input type="hidden" name="k" value="{k}"><button class="btn btn-danger">🛑 Oprește tranzacționarea</button></form>'
        )
        actions = (
            f'<form method="post" action="/close_now" onsubmit="return confirm(\'Închizi poziția curentă?\')">'
            f'<input type="hidden" name="k" value="{k}"><button class="btn btn-danger">🚨 Închide poziția acum</button></form>'
            + toggle
        )
    elif not token_configured:
        actions = '<p class="note">Comenzile sunt dezactivate: setează variabila DASHBOARD_TOKEN.</p>'
    else:
        actions = '<p class="note">Pentru comenzi deschide pagina cu ?k=TOKEN.</p>'

    return f"""<!DOCTYPE html>
<html lang="ro">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Nova Trade v3 - Live Dashboard</title>
<meta http-equiv="refresh" content="3">
<style>{CSS}</style>
</head>
<body>
<div class="container">
  <header>
    <h1>⚡ Nova Trade v3 Dashboard</h1>
    <span class="badge" style="background: {badge_color};">{badge_text}</span>
  </header>
  <div class="grid">
    <div class="card"><h3>Capital Total</h3><div class="value" style="color: #38bdf8;">${cap:.2f}</div></div>
    <div class="card"><h3>Preț {psym} (Mid)</h3><div class="value">${fmt_price(mid)} USD</div></div>
    <div class="card"><h3>Win / Loss</h3><div class="value"><span style="color: #22c55e;">{wins}W</span> / <span style="color: #ef4444;">{losses}L</span></div></div>
    <div class="card"><h3>Ultima Actualizare (UTC)</h3><div class="value" style="font-size: 16px; color: #cbd5e1;">{stamp}</div></div>
  </div>
  <div class="pos-box">
    <h3>Stare Poziție Curentă &amp; Execuție</h3>
    <p><b>Status Motor:</b> {status}</p>
    <p><b>Scanner piețe:</b> <span class="note">{scan_html}</span></p>
    <p><b>Risc asumat în poziție:</b> ${margin:.2f}</p>
    <p><b>Poziție Activă:</b> {pos_html}</p>
    <p><b>PnL Curent (estimat, cu comisioane):</b> <span style="color: {pnl_color};">${pnl:+.4f}</span></p>
  </div>
  <div class="actions">{actions}</div>
  <div class="footer">Nova Trade v3 &bull; Paper trading &bull; Se actualizează la fiecare 3 secunde.</div>
</div>
</body>
</html>"""


class DashboardHandler(BaseHTTPRequestHandler):
    store = None

    @staticmethod
    def _authorized(supplied):
        token = os.environ.get("DASHBOARD_TOKEN", "")
        return bool(token) and hmac.compare_digest(supplied.encode(), token.encode())

    def _send(self, code, body, ctype="text/html; charset=utf-8", extra=None):
        data = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        k = (parse_qs(u.query).get("k") or [""])[0]
        if u.path == "/healthz":
            self._send(200, "ok", "text/plain; charset=utf-8")
        elif u.path in ("/", "/index.html"):
            st = DashboardHandler.store
            state = st.load_state() or {}
            ctl = st.get_control()
            page = render_dashboard(
                state, ctl,
                can_act=self._authorized(k),
                token_configured=bool(os.environ.get("DASHBOARD_TOKEN")),
                token=k,
            )
            self._send(200, page)
        else:
            self._send(404, "Not found", "text/plain; charset=utf-8")

    def do_POST(self):
        u = urlparse(self.path)
        try:
            length = min(int(self.headers.get("Content-Length") or 0), 4096)
        except ValueError:
            length = 0
        form = parse_qs(self.rfile.read(length).decode("utf-8", errors="ignore"))
        k = (form.get("k") or [""])[0]
        if not self._authorized(k):
            self._send(403, "Acces interzis", "text/plain; charset=utf-8")
            return
        st = DashboardHandler.store
        if u.path == "/stop":
            st.set_control(stopped=True)
        elif u.path == "/start":
            st.set_control(stopped=False)
        elif u.path == "/close_now":
            st.set_control(close_now=True)
        else:
            self._send(404, "Not found", "text/plain; charset=utf-8")
            return
        self._send(303, "", "text/plain; charset=utf-8", {"Location": "/?k=" + quote(k)})

    def log_message(self, format, *args):
        pass


def start_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), DashboardHandler)
    server.serve_forever()


# --------------------------- INDICATORI ----------------------------
def to_frame(rows):
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    return df


def add_indicators(df):
    df = df.copy()
    c = df["close"]
    delta = c.diff()
    up = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    dn = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    df["rsi"] = 100 - 100 / (1 + up / dn.replace(0, np.nan))
    ma, sd = c.rolling(20).mean(), c.rolling(20).std()
    df["bb_mid"], df["bb_low"] = ma, ma - 2 * sd
    tr = pd.concat([df["high"] - df["low"],
                    (df["high"] - c.shift()).abs(),
                    (df["low"] - c.shift()).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / 14, adjust=False).mean()
    df["atr_avg"] = df["atr"].rolling(100).mean()
    df["ema_trend"] = c.ewm(span=200, adjust=False).mean()
    return df


def signal(r, cost_rt=COST_RT):
    return bool(
        r["close"] < r["bb_low"]
        and r["rsi"] < RSI_BUY
        and r["close"] > r["ema_trend"] * (1 - TREND_TOL)
        and (r["bb_mid"] / r["close"] - 1) > 3 * cost_rt
    )


# ------------------------- DATE DE PIAȚĂ (v3) -------------------------
def _naive_utc(idx):
    idx = pd.DatetimeIndex(idx)
    return idx.tz_convert("UTC").tz_localize(None) if idx.tz is not None else idx


def fetch_candles(ex, sym):
    m = MARKETS[sym]
    if m["src"] == "ccxt":
        return to_frame(ex.fetch_ohlcv(sym, TIMEFRAME, limit=400))
    if yf is None:
        raise RuntimeError("yfinance nu e instalat")
    raw = yf.Ticker(m["yf"]).history(period="10d", interval="15m", auto_adjust=False)
    if raw is None or raw.empty:
        raise ValueError("fără date de la Yahoo")
    df = pd.DataFrame({
        "ts": _naive_utc(raw.index),
        "open": raw["Open"].to_numpy(dtype=float),
        "high": raw["High"].to_numpy(dtype=float),
        "low": raw["Low"].to_numpy(dtype=float),
        "close": raw["Close"].to_numpy(dtype=float),
        "volume": raw["Volume"].to_numpy(dtype=float) if "Volume" in raw else 0.0,
    })
    df = df.dropna(subset=["open", "high", "low", "close"])
    return df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)


def fetch_price(ex, sym):
    m = MARKETS[sym]
    if m["src"] == "ccxt":
        t = ex.fetch_ticker(sym)
        return fnum(t.get("last") or t.get("close")), True
    if yf is None:
        raise RuntimeError("yfinance nu e instalat")
    h = yf.Ticker(m["yf"]).history(period="1d", interval="1m", auto_adjust=False)
    if h is None or h.empty:
        return 0.0, False
    px = fnum(h["Close"].iloc[-1])
    age = (utc_now() - _naive_utc(h.index)[-1]).total_seconds()
    return px, age <= STALE_PRICE_SECONDS


def scan_markets(ex, now, slot, scanned, scan_info):
    best = None
    age = (now - slot).total_seconds()
    expected = slot - pd.Timedelta(minutes=BAR_MINUTES)
    for sym, m in MARKETS.items():
        if scanned.get(sym) == slot:
            continue
        if m["src"] == "yf" and yf is None:
            scan_info[sym] = "yfinance lipsă"
            scanned[sym] = slot
            continue
        try:
            df = add_indicators(fetch_candles(ex, sym))
        except Exception as e:
            print(f"Eroare date {sym}:", e, flush=True)
            scan_info[sym] = "eroare date"
            if age > m["max_age"]:
                scanned[sym] = slot
            continue
        closed = df[df["ts"] + pd.Timedelta(minutes=BAR_MINUTES) <= slot].dropna().reset_index(drop=True)
        if len(closed) == 0:
            scan_info[sym] = "date insuficiente"
            if age > m["max_age"]:
                scanned[sym] = slot
            continue
        bar = closed.iloc[-1]
        if bar["ts"] != expected:
            stale_market = bar["ts"] < expected - pd.Timedelta(minutes=2 * BAR_MINUTES)
            scan_info[sym] = "piață închisă" if stale_market else "aștept bara nouă"
            if stale_market or age > m["max_age"]:
                scanned[sym] = slot
            continue
        scanned[sym] = slot
        cost_rt = 2 * (m["fee"] + m["slip"])
        if age <= m["max_age"] and signal(bar, cost_rt):
            score = float((bar["bb_mid"] - bar["close"]) / bar["atr"])
            scan_info[sym] = f"SEMNAL (scor {score:.2f})"
            if best is None or score > best[0]:
                best = (score, sym, bar)
        else:
            scan_info[sym] = "fără semnal"
    return best


# --------------------------- MOTOR ---------------------------
class Engine:
    def __init__(self, capital, cfg=None, on_trade=None, verbose=True):
        self.cfg = dict(DEFAULTS, **(cfg or {}))
        self.verbose = verbose
        self.on_trade = on_trade
        self.equity = capital
        self.pos = None
        self.wins = 0
        self.losses = 0
        self.day = None
        self.day_start = capital

    def log(self, msg):
        if self.verbose:
            print(msg, flush=True)

    def roll_day(self, today):
        if today != self.day:
            self.day, self.day_start = today, self.equity

    def trading_allowed(self):
        c = self.cfg
        return (
            bool(c["allow_new_trades"])
            and not c["stopped"]
            and self.equity > self.day_start * (1 - c["daily_loss_limit"])
        )

    def unrealized(self, price):
        p = self.pos
        if p is None:
            return 0.0
        return p["qty"] * (price - p["entry"]) - p["fee_in"] - p["qty"] * price * p.get("fee", FEE)

    def _open(self, bar, price, symbol=SYMBOL):
        c = self.cfg
        mk = MARKETS[symbol]
        fee, slip = mk["fee"], mk["slip"]
        entry = float(price) * (1 + slip)
        atr, atr_avg, bb_mid = float(bar["atr"]), float(bar["atr_avg"]), float(bar["bb_mid"])
        mult = c["atr_stop_mult"]
        if c["vol_adjust_stop"] and atr > 1.5 * atr_avg:
            mult *= 1.33
        stop, tp = entry - mult * atr, bb_mid
        if stop <= 0 or stop >= entry or tp <= entry:
            return
        qty = (self.equity * c["risk_per_trade"]) / (entry - stop)
        qty = min(qty, self.equity * 0.99 / (entry * (1 + fee)))
        if qty <= 0:
            return
        self.pos = dict(
            entry=entry, stop=stop, tp=tp, qty=float(qty), bars=0, hh=entry,
            atr0=atr, fee_in=float(qty * entry * fee), t=bar["ts"],
            t_open=utc_now(), margin=round(self.equity * c["risk_per_trade"], 2),
            symbol=symbol, fee=fee, slip=slip,
        )
        self.log(f"{utc_now()} BUY {symbol} {qty:.6f} @ {rnd(entry)} SL {rnd(stop)} TP {rnd(tp)}")

    def check_position_realtime(self, price, now):
        p, c = self.pos, self.cfg
        if p is None or price <= 0:
            return False, "HOLD"

        slip = p.get("slip", SLIPPAGE)
        cost_rt = 2 * (p.get("fee", FEE) + slip)

        p["bars"] = int((now - p["t_open"]).total_seconds() // (BAR_MINUTES * 60))

        ms, mt = _num(c.get("manual_stop")), _num(c.get("manual_tp"))
        if ms:
            p["stop"] = ms
        elif c["trailing_stop"]:
            p["hh"] = max(p["hh"], price)
            if p["hh"] >= p["entry"] + c["trail_activate_atr"] * p["atr0"]:
                p["stop"] = max(p["stop"], p["entry"] * (1 + cost_rt),
                                p["hh"] - c["trail_atr"] * p["atr0"])
        if mt:
            p["tp"] = mt

        if price <= p["stop"]:
            self._close(min(price, p["stop"]) * (1 - slip), "STOP", now)
            return True, "STOP_LOSS"
        if price >= p["tp"]:
            if c.get("tp_extend") and c["trailing_stop"] and not mt and not ms:
                old_tp = p["tp"]
                p["tp"] = max(price, old_tp) + c["tp_extend_atr"] * p["atr0"]
                p["stop"] = max(p["stop"], old_tp - c["tp_lock_atr"] * p["atr0"])
                self.log(f"{now} TP extins pentru {p.get('symbol', SYMBOL)}: nou TP {rnd(p['tp'])}, SL {rnd(p['stop'])}")
                return False, "HOLD_ACTIVE"
            self._close(p["tp"], "TP", now)
            return True, "TAKE_PROFIT"
        if p["bars"] >= MAX_BARS:
            self._close(price * (1 - slip), "TIME", now)
            return True, "TIME_EXIT"
        return False, "HOLD_ACTIVE"

    def _close(self, px, why, ts):
        p = self.pos
        pnl = p["qty"] * (px - p["entry"]) - p["fee_in"] - p["qty"] * px * p.get("fee", FEE)
        self.equity += pnl
        if pnl > 0:
            self.wins += 1
        else:
            self.losses += 1
        rec = dict(
            t=str(p["t"]), t_open=str(p["t_open"]), t_close=str(ts),
            symbol=p.get("symbol", SYMBOL),
            entry=rnd(p["entry"]), exit=rnd(px),
            qty=round(p["qty"], 6), pnl=round(float(pnl), 4), why=why,
            equity_after=round(self.equity, 4),
        )
        if self.on_trade:
            try:
                self.on_trade(rec)
            except Exception as e:
                print("Eroare la logarea tranzacției:", e)
        self.log(f"{ts} SELL {rec['symbol']} @ {rnd(px)} ({why}) PnL {pnl:+.2f} Equity {self.equity:.2f}")
        self.pos = None

    def force_close(self, price, now):
        if self.pos is not None:
            self._close(price * (1 - self.pos.get("slip", SLIPPAGE)), "MANUAL", now)


# ------------------------- BUCLA PRINCIPALĂ LIVE --------------------
def run_paper_loop():
    mongo = MongoManager()
    store = Store(mongo)
    DashboardHandler.store = store
    threading.Thread(target=start_health_server, daemon=True).start()

    ex = getattr(ccxt, EXCHANGE)({"enableRateLimit": True})
    eng = Engine(START_CAPITAL, store.get_control(), on_trade=store.add_trade)

    saved = store.load_state()
    if saved:
        eng.equity = fnum(saved.get("equity", saved.get("capital")), START_CAPITAL)
        eng.wins = int(fnum(saved.get("wins")))
        eng.losses = int(fnum(saved.get("losses")))
        try:
            if saved.get("day"):
                eng.day = datetime.date.fromisoformat(saved["day"])
                eng.day_start = fnum(saved.get("day_start"), eng.equity)
        except ValueError:
            pass
        eng.pos = deser_pos(saved.get("active_position_raw"))
        if eng.pos:
            print("Poziție restaurată din MongoDB.")

    slot_unit = f"{BAR_MINUTES}min"
    scanned = {}
    scan_info = {}
    print(f"\n--- [NOVA TRADE v3] Pornit cu capital {eng.equity:.2f}$ (MongoDB) ---", flush=True)

    while True:
        try:
            cfg = store.get_control()
            eng.cfg = cfg
            now = utc_now()

            cur_sym = eng.pos["symbol"] if eng.pos is not None else SYMBOL
            price, price_ok = fetch_price(ex, cur_sym)
            if price <= 0:
                raise ValueError("preț invalid de la sursa de date")

            eng.roll_day(now.date())

            if cfg["close_now"]:
                if eng.pos is not None:
                    eng.force_close(price, now)
                    for s in MARKETS:
                        scanned[s] = now.floor(slot_unit)
                store.set_control(close_now=False)

            status = "HOLD"
            if eng.pos is not None:
                if price_ok:
                    _, status = eng.check_position_realtime(price, now)
                else:
                    status = "MARKET_CLOSED"
            elif eng.trading_allowed():
                slot = now.floor(slot_unit)
                if any(scanned.get(s) != slot for s in MARKETS):
                    best = scan_markets(ex, now, slot, scanned, scan_info)
                    if best is not None:
                        _, sym, bar = best
                        px2, ok2 = fetch_price(ex, sym)
                        if ok2 and px2 > 0:
                            eng._open(bar, px2, sym)
                            if eng.pos is not None:
                                cur_sym, price = sym, px2
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
                    "margin": p.get("margin", 0.0),
                    "direction": "LONG",
                    "target_tp": rnd(p["tp"]),
                    "target_sl": rnd(p["stop"]),
                    "notional": round(p["qty"] * p["entry"], 2),
                    "bars": p["bars"],
                }
                raw_pos = ser_pos(p)

            store.save_state({
                "timestamp": now.strftime("%H:%M:%S"),
                "capital": round(eng.equity, 2),
                "equity": float(eng.equity),
                "mid_price": rnd(price),
                "price_symbol": cur_sym,
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
            print("Eroare în bucla live:", e, flush=True)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    run_paper_loop()