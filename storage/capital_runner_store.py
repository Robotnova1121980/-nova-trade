"""Mongo-backed controls and durable scan state for the Capital Demo runner."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo import MongoClient


class CapitalRunnerStore:
    """Use the project's Mongo database for runner control and restart-safe state."""

    def __init__(self, local_state_path: Path):
        self.local_state_path = local_state_path
        uri = os.getenv("MONGO_URI", "").strip()
        require_mongo = os.getenv("CAPITAL_DEMO_REQUIRE_MONGO", "false").lower() == "true"
        if not uri:
            if require_mongo:
                raise RuntimeError("MONGO_URI este obligatoriu pentru runnerul Capital de pe Render.")
            self.client = None
            self.db = None
            return

        self.client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        self.client.admin.command("ping")
        self.db = self.client["adaptive_scalper_db"]

    def get_control(self) -> dict[str, Any]:
        if self.db is None:
            return {}
        document = self.db["motor_control"].find_one({"_id": "control"}) or {}
        document.pop("_id", None)
        return document

    def load_state(self) -> dict[str, str]:
        if self.db is not None:
            document = self.db["capital_demo_state"].find_one({"_id": "runner"}) or {}
            state = document.get("scanned_bars")
            return state if isinstance(state, dict) else {}
        try:
            raw = json.loads(self.local_state_path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}

    def save_state(self, state: dict[str, str], *, status: str = "running") -> None:
        if self.db is not None:
            self.db["capital_demo_state"].update_one(
                {"_id": "runner"},
                {"$set": {
                    "scanned_bars": state,
                    "status": status,
                    "updated_at": datetime.now(timezone.utc),
                }},
                upsert=True,
            )
            return
        self.local_state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.local_state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state, indent=2), encoding="utf-8")
        temporary.replace(self.local_state_path)

    def log_event(self, event_type: str, message: str, **fields: Any) -> None:
        if self.db is None:
            return
        self.db["capital_demo_events"].insert_one({
            "event_type": event_type,
            "message": message,
            "timestamp": datetime.now(timezone.utc),
            **fields,
        })

    def close(self) -> None:
        if self.client is not None:
            self.client.close()
