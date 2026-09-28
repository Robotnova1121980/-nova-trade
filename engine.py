import sqlite3
from datetime import datetime, timezone
import time

# ==========================================
# 1. FUNCȚIILE ADAPTIVE ȘI DE BAZĂ DE DATE
# ==========================================

def init_db():
    conn = sqlite3.connect('trading_history.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            open_time TEXT,
            direction TEXT,
            entry_price REAL,
            exit_price REAL,
            session TEXT,
            volatility REAL,
            mae REAL,
            mfe REAL,
            result TEXT,
            pnl REAL
        )
    ''')
    conn.commit()
    conn.close()

def get_current_session():
    now_utc = datetime.now(timezone.utc)
    hour = now_utc.hour
    
    if 13 <= hour < 16:
        return "Overlap_London_NY"
    elif 7 <= hour < 16:
        return "London"
    elif 13 <= hour < 22:
        return "New_York"
    elif 0 <= hour < 8:
        return "Tokyo"
    else:
        return "Off_Hours"

def calculate_adaptive_targets(entry_price, direction, current_volatility):
    session = get_current_session()
    
    if session == "Overlap_London_NY":
        reward_factor = 2.0 
        vol_multiplier = 1.2
    elif session == "Tokyo":
        reward_factor = 1.2 
        vol_multiplier = 0.8
    else:
        reward_factor = 1.5
        vol_multiplier = 1.0

    safe_volatility = max(0.001, current_volatility * vol_multiplier)
    
    if direction == "SHORT":
        sl = entry_price + (safe_volatility * 1.5)
        tp = entry_price - (safe_volatility * 1.5 * reward_factor)
    else: # LONG
        sl = entry_price - (safe_volatility * 1.5)
        tp = entry_price + (safe_volatility * 1.5 * reward_factor)
        
    return sl, tp, session

def log_completed_trade(trade_data, result, pnl, mae, mfe):
    conn = sqlite3.connect('trading_history.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO trades (open_time, direction, entry_price, exit_price, session, volatility, mae, mfe, result, pnl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        str(trade_data.get('open_time')),
        trade_data.get('direction'),
        trade_data.get('entry_price'),
        trade_data.get('exit_price', 0.0),
        trade_data.get('session', 'Unknown'),
        trade_data.get('volatility', 0.0),
        mae,
        mfe,
        result,
        pnl
    ))
    conn.commit()
    conn.close()


# ==========================================
# 2. MOTORUL PRINCIPAL DE TRADING
# ==========================================

def run_trading_bot():
    # Inițializăm baza de date la pornire
    init_db()
    print("Botul a pornit. Baza de date SQLite este pregătită.")

    while True:
        try:
            # Exemplu pentru momentul când botul detectează un semnal de intrare:
            # (Aici pui logica ta existentă de preluare preț și semnal)
            
            # Exemplu de date simulate/pre luate din piață:
            entry_price = 83000.0  # Prețul curent
            direction = "LONG"     # sau SHORT
            current_volatility = 150.0  # ATR-ul calculat din piață
            
            # APELUL 1: Calculăm țintele adaptive în funcție de sesuie și volatilitate
            sl, tp, current_session = calculate_adaptive_targets(entry_price, direction, current_volatility)
            
            trade_data = {
                "open_time": datetime.now(timezone.utc).isoformat(),
                "direction": direction,
                "entry_price": entry_price,
                "target_sl": sl,
                "target_tp": tp,
                "session": current_session,
                "volatility": current_volatility
            }
            
            print(f"Trade deschis: {direction} | Sesiune: {current_session} | SL: {sl} | TP: {tp}")
            
            # Simulare tranzacție activă... (aici botul monitorizează prețul)
            # ...
            
            # APELUL 2: Când tranzacția s-a închis, o salvăm în baza de date
            # (Aici pui rezultatul real obținut din piață)
            exit_price = tp # Exemplu că a lovit Take Profit
            result = "WIN"
            pnl = 12.50
            mae = 20.0
            mfe = 150.0
            
            trade_data["exit_price"] = exit_price
            log_completed_trade(trade_data, result, pnl, mae, mfe)
            print("Tranzacție închisă și salvată în baza de date SQLite.")

            break # Scoate break-ul în codul tău real ca să ruleze în buclă continuă
            
        except Exception as e:
            print(fനാളEroare în bucla botului: {e}")
            time.sleep(10)

if __name__ == "__main__":
    run_trading_bot()