"""Mongo-backed control panel for the single Capital.com Demo runner."""

from __future__ import annotations

import os
import time
import hmac
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from storage.mongo_db import MongoManager

st.set_page_config(page_title="Nova Trade — Capital Demo", page_icon="📈", layout="wide")
st.title("Nova Trade — Capital.com Demo")
st.caption("Un singur runner Capital.com Demo; setările și starea sunt păstrate în MongoDB.")

dashboard_token = os.getenv("DASHBOARD_TOKEN", "")
if not dashboard_token:
    st.error("DASHBOARD_TOKEN lipsește din variabilele private Render; panoul este blocat.")
    st.stop()
if not st.session_state.get("dashboard_authenticated"):
    with st.form("dashboard_login"):
        supplied_token = st.text_input("Parola panoului", type="password")
        login_clicked = st.form_submit_button("Autentificare")
    if login_clicked and hmac.compare_digest(supplied_token, dashboard_token):
        st.session_state["dashboard_authenticated"] = True
        st.rerun()
    st.info("Introdu parola privată a panoului.")
    st.stop()

@st.cache_resource
def get_db():
    manager = MongoManager()
    manager.client.admin.command("ping")
    return manager

try:
    mongo = get_db()
    control_collection = mongo.db["motor_control"]
    runner_collection = mongo.db["capital_demo_state"]
    event_collection = mongo.db["capital_demo_events"]
    control = control_collection.find_one({"_id": "control"}) or {}
    state = runner_collection.find_one({"_id": "runner"}) or {}
except Exception as exc:
    st.error(f"Nu pot citi MongoDB. Verifică MONGO_URI în Render. Detaliu: {exc}")
    st.stop()

default_epics = os.getenv(
    "CAPITAL_DEMO_EPICS",
    "EURUSD,GBPUSD,USDJPY,AUDUSD,USDCAD,USDCHF,NZDUSD,EURJPY,GBPJPY,US500,US100,US30,DE40,UK100,GOLD,SILVER,OIL_CRUDE,OIL_BRENT,BTCUSD,ETHUSD",
)
with st.sidebar.form("capital_demo_control"):
    st.header("Setări bot Demo")
    allow_entries = st.checkbox(
        "Permite intrări noi",
        value=bool(control.get("allow_new_trades", True)) and not bool(control.get("stopped", False)),
    )
    orders_enabled = st.checkbox(
        "Trimite ordine pe contul Demo",
        value=bool(control.get("capital_demo_enable_orders", os.getenv("CAPITAL_DEMO_ENABLE_ORDERS", "false").lower() == "true")),
    )
    risk_percent = st.number_input(
        "Risc estimat per ordin (%)", min_value=0.05, max_value=5.0, step=0.05,
        value=float(control.get("capital_demo_risk_percent", os.getenv("CAPITAL_DEMO_RISK_PERCENT", "0.5"))),
    )
    max_positions = st.selectbox(
        "Poziții simultane, pe piețe diferite", options=[1, 2, 3],
        index=max(0, min(2, int(control.get("capital_demo_max_open_positions", 3)) - 1)),
    )
    bar_minutes = st.selectbox(
        "Interval lumânare (minute)", options=[5, 15],
        index=0 if int(control.get("capital_demo_bar_minutes", 5)) == 5 else 1,
    )
    epics_text = st.text_area(
        "Piețe Capital (epic-uri separate prin virgulă)",
        value=str(control.get("capital_demo_epics", default_epics)), height=100,
    )
    save_settings = st.form_submit_button("Salvează setările")

if save_settings:
    epics = list(dict.fromkeys(epic.strip() for epic in epics_text.split(",") if epic.strip()))
    if not epics:
        st.sidebar.error("Adaugă cel puțin un epic Capital valid.")
    else:
        control_collection.update_one(
            {"_id": "control"},
            {"$set": {
                "allow_new_trades": allow_entries,
                "stopped": not allow_entries,
                "capital_demo_enable_orders": orders_enabled,
                "capital_demo_risk_percent": float(risk_percent),
                "capital_demo_max_open_positions": int(max_positions),
                "capital_demo_bar_minutes": int(bar_minutes),
                "capital_demo_epics": epics,
            }},
            upsert=True,
        )
        st.sidebar.success("Setările au fost salvate în MongoDB; runnerul le preia la următoarea verificare.")
        st.rerun()

status = str(state.get("status", "waiting"))
updated = state.get("updated_at")
if isinstance(updated, datetime):
    updated_text = updated.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
else:
    updated_text = "încă nu există heartbeat"
scanned = state.get("scanned_bars") or {}

col1, col2, col3 = st.columns(3)
col1.metric("Starea runnerului", status)
col2.metric("Ultimul heartbeat", updated_text)
col3.metric("Piețe cu lumânare procesată", len(scanned))

st.subheader("Setarea curentă")
st.write(
    f"Ordine Demo: **{'activate' if control.get('capital_demo_enable_orders', os.getenv('CAPITAL_DEMO_ENABLE_ORDERS', 'false').lower() == 'true') else 'dezactivate'}** · "
    f"Intrări noi: **{'permise' if control.get('allow_new_trades', True) and not control.get('stopped', False) else 'oprite'}** · "
    f"Limită: **{control.get('capital_demo_max_open_positions', 3)} poziții** · "
    f"Lumânări: **{control.get('capital_demo_bar_minutes', 5)} minute** · "
    f"Risc: **{control.get('capital_demo_risk_percent', 0.5)}% per ordin**"
)

st.subheader("Ultimele semnale și ordine confirmate")
try:
    events = list(event_collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(30))
except Exception:
    events = []
if events:
    st.dataframe(pd.DataFrame(events), use_container_width=True, hide_index=True)
else:
    st.info("Nu există încă semnale sau ordine înregistrate de runner.")

if st.checkbox("Reîmprospătează automat la fiecare 15 secunde", value=True):
    time.sleep(15)
    st.rerun()
