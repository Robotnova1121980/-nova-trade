"""
engine.py
Nova Trade v0.6 - Capital Total 150$ | Marjă Tranzacție 50$ + Web Dashboard integrat
"""

import uuid
import datetime
import time
import json
import threading
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from storage.mongo_db import MongoManager
from storage.repositories import EventRepository, TradeDNARepository
from market.market_state import MarketStateEngine
from market.live_feed import fetch_live_market_data

FEE_RATE = 0.0004         
SLIPPAGE_RATE = 0.0001    
LEVERAGE = 5

# Server HTTP cu Dashboard Web Integrat care citește starea motorului
class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            
            # Încercăm să citim starea curentă din motor_state.json
            state = {
                "timestamp": "N/A",
                "capital": 150.0,
                "mid_price": 0.0,
                "wins": 0,
                "losses": 0,
                "active_position": None,
                "current_pnl": 0.0,
                "status": "INITIALIZING"
            }
            try:
                if os.path.exists("motor_state.json"):
                    with open("motor_state.json", "r") as f:
                        state = json.load(f)
            except Exception:
                pass

            html_content = f"""
            <!DOCTYPE html>
            <html lang="ro">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Nova Trade Engine - Live Dashboard</title>
                <meta http-equiv="refresh" content="3">
                <style>
                    body {{ background-color: #0f172a; color: #f8fafc; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; }}
                    .container {{ max-width: 900px; margin: 0 auto; }}
                    header {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; padding-bottom: 15px; margin-bottom: 25px; }}
                    h1 {{ margin: 0; font-size: 24px; color: #38bdf8; }}
                    .badge {{ background: #22c55e; color: white; padding: 4px 12px; border-radius: 12px; font-size: 12px; font-weight: bold; }}
                    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 25px; }}
                    .card {{ background: #1e293b; padding: 20px; border-radius: 10px; border: 1px solid #334155; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); }}
                    .card h3 {{ margin: 0 0 10px 0; font-size: 14px; color: #94a3b8; text-transform: uppercase; }}
                    .card .value {{ font-size: 22px; font-weight: bold; }}
                    .pos-box {{ background: #1e293b; padding: 20px; border-radius: 10px; border: 1px solid #334155; margin-bottom: 25px; }}
                    .actions {{ display: flex; gap: 15px; margin-top: 25px; }}
                    .btn {{ background: #0284c7; color: white; border: none; padding: 10px 20px; border-radius: 6px; font-weight: bold; cursor: pointer; transition: background 0.2s; }}
                    .btn:hover {{ background: #0369a1; }}
                    .btn-danger {{ background: #dc2626; }}
                    .btn-danger:hover {{ background: #b91c1c; }}
                    .footer {{ text-align: center; color: #64748b; font-size: 12px; margin-top: 40px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <header>
                        <h1>⚡ Nova Trade Dashboard</h1>
                        <span class="badge">LIVE 24/7</span>
                    </header>
                    
                    <div class="grid">
                        <div class="card">
                            <h3>Capital Total</h3>
                            <div class="value" style="color: #38bdf8;">${state.get('capital', 150.0):.2f}</div>
                        </div>
                        <div class="card">
                            <h3>Preț BTC (Mid)</h3>
                            <div class="value">${state.get('mid_price', 0):,.2f} USD</div>
                        </div>
                        <div class="card">
                            <h3>Win / Loss</h3>
                            <div class="value"><span style="color: #22c55e;">{state.get('wins', 0)}W</span> / <span style="color: #ef4444;">{state.get('losses', 0)}L</span></div>
                        </div>
                        <div class="card">
                            <h3>Ultima Actualizare</h3>
                            <div class="value" style="font-size: 16px; color: #cbd5e1;">{state.get('timestamp', 'N/A')}</div>
                        </div>
                    </div>

                    <div class="pos-box">
                        <h3>Stare Poziție Curentă & Execuție</h3>
                        <p><b>Status Motor:</b> {state.get('status', 'HOLD')}</p>
                        <p><b>Poziție Activă:</b> {json.dumps(state.get('active_position', None), indent=2) if state.get('active_position') else 'Nicio poziție deschisă momentan'}</p>
                        <p><b>PnL Nerealizat / Curent:</b> <span style="color: {'#22c55e' if state.get('current_pnl', 0) >= 0 else '#ef4444'};">${state.get('current_pnl', 0):+.4f}</span></p>
                    </div>

                    <div class="actions">
                        <button class="btn btn-danger" onclick="alert('Funcționalitate de oprire manuală în curând!')">🛑 Oprire Manuală</button>
                        <button class="btn" onclick="alert('Setare marjă/fonduri în curând!')">⚙️ Gestionează Fonduri</button>
                    </div>

                    <div class="footer">
                        Nova Trade Autonomous Engine &bull; Se actualizează automat la fiecare 3 secunde.
                    </div>
                </div>
            </body>
            </html>
            """
            self.wfile.write(html_content.encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass

def start_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), DashboardHandler)
    server.serve_forever()

class AutonomousScalperEngine:
    def __init__(self, initial_capital=150.0):
        self.capital = initial_capital
        self.active_position = None
        self.wins = 0
        self.losses = 0
        self.trade_counter = 0
        self.last_trade_time = 0

    def evaluate_dna_memory(self, dna_repo, market_state):
        try:
            if hasattr(dna_repo, "collection"):
                recent_history = list(dna_repo.collection.find({"market_state": market_state}).sort("_id", -1).limit(10))
            elif hasattr(dna_repo, "db"):
                recent_history = list(dna_repo.db["trade_dna"].find({"market_state": market_state}).sort("_id", -1).limit(10))
            else:
                return 1.0, 75

            if not recent_history:
                return 1.0, 75  

            wins = sum(1 for t in recent_history if t.get("result", {}).get("realized_edge", 0) > 0)
            total = len(recent_history)
            win_rate = (wins / total) * 100 if total > 0 else 50
            confidence_score = int(win_rate)
            
            if win_rate < 30:
                return 0.5, confidence_score
            
            return 1.2, confidence_score
        except Exception:
            return 1.0, 70

    def execute_trade(self, bid, ask, market_state, dna_repo):
        self.trade_counter += 1
        direction = "LONG" if self.trade_counter % 2 != 0 else "SHORT"
        entry_price = ask if direction == "LONG" else bid

        _, confidence_score = self.evaluate_dna_memory(dna_repo, market_state)
        
        dynamic_margin = 50.0
        if self.capital < dynamic_margin:
            dynamic_margin = self.capital

        notional = dynamic_margin * LEVERAGE  
        entry_fee = notional * FEE_RATE

        base_tp_dist = entry_price * 0.0015  
        base_sl_dist = entry_price * 0.0010  

        if direction == "LONG":
            target_tp = entry_price + base_tp_dist
            target_sl = entry_price - base_sl_dist
        else:
            target_tp = entry_price - base_tp_dist
            target_sl = entry_price + base_sl_dist

        self.active_position = {
            "entry_price": entry_price,
            "margin": dynamic_margin,
            "leverage": LEVERAGE,
            "notional": notional,
            "direction": direction,
            "target_tp": target_tp,
            "target_sl": target_sl,
            "entry_fee": entry_fee,
            "open_time": time.time(),
            "highest_price": entry_price,  
            "lowest_price": entry_price    
        }
        
        self.last_trade_time = time.time()
        print(f"    └─ [OPEN {direction}] Marjă Tranzacție: ${dynamic_margin:.2f} | Notional (x5): ${notional:.2f} | Preț: ${entry_price:,.2f}")

    def calculate_pnl(self, current_price):
        if not self.active_position:
            return 0.0

        p = self.active_position
        if p["direction"] == "LONG":
            movement = (current_price - p["entry_price"]) / p["entry_price"]
        else:
            movement = (p["entry_price"] - current_price) / p["entry_price"]

        gross_pnl = p["notional"] * movement
        exit_fee = p["notional"] * FEE_RATE
        slippage_cost = p["notional"] * SLIPPAGE_RATE

        net_pnl = gross_pnl - p["entry_fee"] - exit_fee - slippage_cost
        return net_pnl

    def update_and_check(self, bid, ask):
        if not self.active_position:
            return 0.0, "HOLD"

        direction = self.active_position["direction"]
        current_price = bid if direction == "LONG" else ask
        pnl = self.calculate_pnl(current_price)

        tp = self.active_position["target_tp"]
        sl = self.active_position["target_sl"]

        if direction == "LONG":
            hit_tp = (current_price >= tp)
            hit_sl = (current_price <= sl)
        else:
            hit_tp = (current_price <= tp)
            hit_sl = (current_price >= sl)

        if hit_tp:
            self.capital += pnl
            self.wins += 1
            ex_pos = self.active_position
            self.active_position = None
            print(f"    └─ [TAKE PROFIT] Poziție {ex_pos['direction']} închisă! PnL net: ${pnl:+.4f}")
            return pnl, "TAKE_PROFIT"

        elif hit_sl:
            self.capital += pnl
            self.losses += 1
            ex_pos = self.active_position
            self.active_position = None
            print(f"    └─ [STOP LOSS] Poziție închisă. PnL net: ${pnl:+.4f}")
            return pnl, "STOP_LOSS"

        return pnl, "HOLD_ACTIVE"

def run_live_trading_loop():
    threading.Thread(target=start_health_server, daemon=True).start()
    
    print("\n--- [NOVA TRADE v0.6] Motor Pornit cu Web Dashboard Integrat ---")
    mongo = MongoManager()
    events = EventRepository(mongo)
    dna_repo = TradeDNARepository(mongo)
    market_engine = MarketStateEngine()
    
    scalper = AutonomousScalperEngine(initial_capital=150.0)

    try:
        while True:
            correlation_id = f"CORR-{uuid.uuid4().hex[:8]}"
            symbol = "BTCUSDT"
            
            real_market_data = fetch_live_market_data(symbol)
            if not real_market_data:
                time.sleep(2)
                continue

            bid = float(real_market_data.get("best_bid", real_market_data.get("price", 0)))
            ask = float(real_market_data.get("best_ask", real_market_data.get("price", 0)))
            
            if bid <= 0 or ask <= 0:
                time.sleep(2)
                continue

            mid_price = (bid + ask) / 2
            timestamp_str = datetime.datetime.now().strftime('%H:%M:%S')
            current_state = market_engine.classify(real_market_data)

            print(f"[{timestamp_str}] Capital Total: ${scalper.capital:.4f} | BTC Mid: ${mid_price:,.2f} | W/L: {scalper.wins}W / {scalper.losses}L")

            pnl = 0.0
            action = "HOLD"
            if scalper.active_position:
                pnl, action = scalper.update_and_check(bid, ask)
                print(f"    └─ [{scalper.active_position['direction'] if scalper.active_position else 'CLOSED'}] PnL curent: ${pnl:+.4f} | Status: {action}")
                
                if action in ["TAKE_PROFIT", "STOP_LOSS"]:
                    events.log(
                        "POSITION_CLOSED", "INFO", "execution", correlation_id,
                        {"pnl": round(pnl, 4), "reason": action, "new_capital": scalper.capital}
                    )
            else:
                if scalper.capital >= 50 and (time.time() - scalper.last_trade_time > 15):
                    scalper.execute_trade(bid, ask, current_state, dna_repo)
                    dir_used = scalper.active_position['direction']
                    
                    try:
                        if hasattr(dna_repo, "save_dna"):
                            dna_repo.save_dna(
                                trade_id=f"TRD-{uuid.uuid4().hex[:6]}",
                                symbol=symbol,
                                direction=dir_used,
                                market_state=current_state,
                                opportunity={"score": 85, "momentum": real_market_data.get("momentum", 0)},
                                execution={"price": scalper.active_position['entry_price'], "margin": scalper.active_position['margin']},
                                risk={"risk_pct": 50.0, "firewall": "PASS"},
                                result={"expected_edge": 0.002, "realized_edge": 0.0}
                            )
                    except Exception:
                        pass

            state_data = {
                "timestamp": timestamp_str,
                "capital": round(scalper.capital, 2),
                "mid_price": round(mid_price, 2),
                "wins": scalper.wins,
                "losses": scalper.losses,
                "active_position": scalper.active_position,
                "current_pnl": round(pnl, 4),
                "status": action if scalper.active_position else "HOLD_ACTIVE"
            }
            try:
                with open("motor_state.json", "w") as f:
                    json.dump(state_data, f)
            except Exception:
                pass

            print("-" * 60)
            time.sleep(2)

    except KeyboardInterrupt:
        print(f"\n[STOP] Oprit. Capital final: ${scalper.capital:.4f}")

if __name__ == "__main__":
    run_live_trading_loop()