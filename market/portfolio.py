"""
dashboard.py
Centrul de Comandă Vizual pentru Nova Trade v0.3 (Conectat la MongoDB și Portofoliu Virtual)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import time
from storage.mongo_db import MongoManager
from market.live_feed import fetch_live_market_data
from market.market_state import MarketStateEngine

st.set_page_config(
    page_title="Nova Trade v0.3 - Live Dashboard",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Nova Trade v0.3 — Live Research & Portfolio Dashboard")
st.markdown("Monitorizare în timp real a portofoliului virtual de **$50.00**, stărilor de piață și Trade DNA.")

# 1. Inițializare conexiune MongoDB
@st.cache_resource
def get_db():
    return MongoManager()

db = get_db()
market_engine = MarketStateEngine()

# Sidebar pentru setări
st.sidebar.header("🎛️ Control Panel")
auto_refresh = st.sidebar.checkbox("Actualizare automată (Live)", value=True)
refresh_rate = st.sidebar.slider("Interval (secunde)", 2, 10, 5)

# 2. Preluare date live de pe Binance pentru afișare
live_data = fetch_live_market_data("BTCUSDT")
if live_data:
    current_price = live_data["best_ask"]
    spread = live_data["spread_bps"]
    market_state = market_engine.classify(live_data)
else:
    current_price = 0.0
    spread = 0.0
    market_state = "UNKNOWN"

# 3. Metrice Principale sus în pagină (Capital, Portofoliu, Stare)
col1, col2, col3, col4 = st.columns(4)

col1.metric("Capital Virtual Total", "$50.00", "+$0.00 (Paper Trading)")
col2.metric("Preț Live BTCUSDT", f"${current_price:,.2f}", f"Spread: {spread} bps")
col3.metric("Regim de Piață Detectat", market_state, "Senzor Binance Activ")
col4.metric("Risk Firewall", "SECURE", "Limită 20% / trade")

st.markdown("---")

# 4. Secțiunea Vizuală: Grafic și Stări din MongoDB
row_col1, row_col2 = st.columns([2, 1])

with row_col1:
    st.subheader("📊 Istoricul Stărilor de Piață (MongoDB Atlas)")
    try:
        states_cursor = db.market_states_coll.find({}, {"_id": 0}).sort("timestamp", -1).limit(10)
        states_df = pd.DataFrame(list(states_cursor))
        if not states_df.empty:
            st.dataframe(states_df[["timestamp", "symbol", "market_state"]], use_container_width=True)
        else:
            st.info("Încă nu sunt stări înregistrate. Asigură-te că rulezi `python engine.py` în fundal.")
    except Exception as e:
        st.error(f"Eroare la citirea bazei de date: {e}")

with row_col2:
    st.subheader("🧬 Trade DNA & Rejecții")
    try:
        dna_cursor = db.trade_dna_coll.find({}, {"_id": 0}).sort("timestamp", -1).limit(5)
        dna_df = pd.DataFrame(list(dna_cursor))
        if not dna_df.empty:
            st.dataframe(dna_df[["trade_id", "symbol", "direction", "market_state"]], use_container_width=True)
        else:
            st.info("Niciun Trade DNA salvat încă.")
    except Exception as e:
        st.error(f"Eroare la citirea Trade DNA: {e}")

st.markdown("---")
st.subheader("📋 Event Ledger (Jurnalul de Urmărire End-to-End)")
try:
    events_cursor = db.events_coll.find({}, {"_id": 0}).sort("timestamp", -1).limit(10)
    events_df = pd.DataFrame(list(events_cursor))
    if not events_df.empty:
        st.dataframe(events_df[["timestamp", "event_type", "severity", "component", "correlation_id"]], use_container_width=True)
    else:
        st.info("Jurnalul de evenimente este gol momentan.")
except Exception as e:
    st.error(f"Eroare la citirea evenimentelor: {e}")

# Reîmprospătare automată
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()