"""
storage/mongo_db.py
Gestionează conexiunea și stocarea multisursă în MongoDB pentru Nova Trade v0.2.
"""

import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Încarcă automat variabilele din fișierul .env
load_dotenv()

class MongoManager:
    def __init__(self):
        # Preia URL-ul de conexiune din variabilele de mediu (.env sau Docker)
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        self.client = MongoClient(mongo_uri)
        self.db = self.client["adaptive_scalper_db"]
        
        # Colecțiile oficiale Nova Trade v0.2
        self.trades_coll = self.db["trades"]
        self.signals_coll = self.db["signals"]
        self.rejected_coll = self.db["rejected_signals"]
        self.market_states_coll = self.db["market_states"]
        self.trade_dna_coll = self.db["trade_dna"]
        self.risk_events_coll = self.db["risk_events"]
        self.events_coll = self.db["events"]  # Noul Event Ledger
        
        # Aplică automat regulile TTL pentru a proteja spațiul din planul gratuit (512MB)
        self._setup_ttl_indexes()

    def _setup_ttl_indexes(self):
        """Configurează expirarea automată a datelor temporare în MongoDB."""
        try:
            # Șterge market_states după 24 ore
            self.market_states_coll.create_index("timestamp", expireAfterSeconds=86400)
            # Șterge semnalele și semnalele refuzate după 7 zile
            self.signals_coll.create_index("timestamp", expireAfterSeconds=604800)
            self.rejected_coll.create_index("timestamp", expireAfterSeconds=604800)
            # Șterge evenimentele de rutină din Ledger după 3 zile
            self.events_coll.create_index("timestamp", expireAfterSeconds=259200)
        except Exception as e:
            print(f"[MONGO WARNING] Nu s-au putut seta indexurile TTL: {e}")

    def log_signal(self, signal_record: dict):
        """Salvează un semnal detectat de sistem."""
        try:
            self.signals_coll.insert_one(signal_record)
        except Exception as e:
            print(f"[MONGO ERROR] Nu s-a putut salva semnalul: {e}")

    def log_rejected_signal(self, rejected_record: dict):
        """CRUCIAL: Salvează semnalele refuzate de Risk Firewall (ex: spread prea mare)."""
        try:
            self.rejected_coll.insert_one(rejected_record)
            print(f"[MONGO] Semnal refuzat salvat (DNA/Audit): {rejected_record.get('reason', 'N/A')}")
        except Exception as e:
            print(f"[MONGO ERROR] Nu s-a putut salva semnalul refuzat: {e}")

    def log_trade(self, trade_record: dict):
        """Salvează tranzacția executată și amprenta sa Trade DNA."""
        try:
            self.trades_coll.insert_one(trade_record)
            self.trade_dna_coll.insert_one(trade_record)
            print("[MONGO] Trade & Trade DNA salvate cu succes.")
        except Exception as e:
            print(f"[MONGO ERROR] Nu s-a putut salva trade-ul: {e}")

    def log_market_state(self, state_record: dict):
        """Salvează starea pieței la momente cheie."""
        try:
            self.market_states_coll.insert_one(state_record)
        except Exception as e:
            print(f"[MONGO ERROR] Nu s-a putut salva starea pieței: {e}")

    def log_event(self, event_record: dict):
        """Înregistrează evenimente operaționale în Event Ledger (cu correlation_id)."""
        try:
            self.events_coll.insert_one(event_record)
            print(f"[MONGO EVENT] {event_record.get('event_type', 'UNKNOWN')} salvat.")
        except Exception as e:
            print(f"[MONGO ERROR] Nu s-a putut salva evenimentul: {e}")

    def get_recent_trades(self, limit: int = 10):
        """Returnează ultimele tranzacții pentru analiză."""
        try:
            return list(self.trades_coll.find({}, {"_id": 0}).sort("timestamp", -1).limit(limit))
        except:
            return []