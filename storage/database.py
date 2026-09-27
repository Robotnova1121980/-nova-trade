"""
storage/database.py
Gestionează persistența datelor locale (SQLite) pentru Trade DNA.
"""

import sqlite3
import os
from config.settings import SYSTEM_CONFIG

class DatabaseManager:
    def __init__(self, db_name: str = SYSTEM_CONFIG.DB_NAME):
        db_path = os.path.join("storage", db_name)
        self.db_path = db_path
        self.init_db()

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def init_db(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS trade_dna (
            trade_id TEXT PRIMARY KEY, timestamp TEXT, symbol TEXT, regime TEXT, direction TEXT,
            entry_price REAL, exit_price REAL, pnl_pct REAL, opportunity_score REAL,
            momentum REAL, order_flow REAL, liquidity REAL, volatility REAL, spread REAL,
            expected_edge REAL, execution_quality REAL, signal_quality REAL,
            regret_main_failure TEXT, status TEXT
        )
        """)
        conn.commit()
        conn.close()

    def log_trade(self, trade_data: dict):
        """Salvează sau actualizează un Trade DNA în baza de date SQLite."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        columns = ", ".join(trade_data.keys())
        placeholders = ", ".join([":" + k for k in trade_data.keys()])
        
        query = f"INSERT OR REPLACE INTO trade_dna ({columns}) VALUES ({placeholders})"
        cursor.execute(query, trade_data)
        
        conn.commit()
        conn.close()

if __name__ == "__main__":
    db = DatabaseManager()
    print("Baza de date a fost initializata corect cu suport pentru log_trade!")