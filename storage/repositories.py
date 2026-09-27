"""
storage/repositories.py
Repository pattern pentru decuplarea logicii de business de baza de date.
"""

from storage.mongo_db import MongoManager
import datetime

class BaseRepository:
    def __init__(self, mongo_manager: MongoManager):
        self.db = mongo_manager

class EventRepository(BaseRepository):
    def log(self, event_type: str, severity: str, component: str, correlation_id: str, payload: dict = None):
        record = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc),
            "event_type": event_type,
            "severity": severity,
            "component": component,
            "correlation_id": correlation_id,
            "payload": payload or {}
        }
        self.db.events_coll.insert_one(record)

class TradeDNARepository(BaseRepository):
    def save_dna(self, trade_id: str, symbol: str, direction: str, market_state: str, opportunity: dict, execution: dict, risk: dict, result: dict):
        record = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc),
            "trade_id": trade_id,
            "symbol": symbol,
            "direction": direction,
            "market_state": market_state,
            "opportunity": opportunity,
            "execution": execution,
            "risk": risk,
            "result": result
        }
        self.db.trade_dna_coll.insert_one(record)
        print(f"[DNA] Amprentă stocată pentru tranzacția {trade_id}")

class SignalRepository(BaseRepository):
    def log_rejection(self, signal_id: str, symbol: str, reason: str, score: float, correlation_id: str):
        record = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc),
            "signal_id": signal_id,
            "symbol": symbol,
            "reason": reason,
            "score": score,
            "correlation_id": correlation_id
        }
        self.db.rejected_coll.insert_one(record)
        print(f"[SIGNAL REJECTED] {symbol} - Motiv: {reason}")