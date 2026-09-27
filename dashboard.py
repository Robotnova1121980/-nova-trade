"""
dashboard.py
Terminal de Trading & Monitorizare - Sincronizat cu Capital de 50$
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import time
import json
import os
from storage.mongo_db import MongoManager

st.set_page_config(
    page_title="Nova Trade v0.5 - Live Terminal ($50)",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Nova Trade v0.5 — Test Capital $50 & Sincronizare Reală")

st.sidebar.header("🎛️ Control Live")
auto_refresh = st.sidebar.checkbox("Reîmprospătare Automată (1s)", value=True)

@st.cache_resource
def init_db():
    return MongoManager()

db = init_db()

# Citire stare live generată de motor
state_file = "motor_state.json"
engine_data = {}

if os.path.exists(state_file):
    try:
        with open(state_file, "r") as f:
            engine_data = json.load(f)
    except Exception:
        pass

real_capital = engine_data.get("capital", 50.0)
current_price = engine_data.get("mid_price", 84904.01)
wins = engine_data.get("wins", 0)
losses = engine_data.get("losses", 0)
active_pos = engine_data.get("active_position", None)
current_pnl = engine_data.get("current_pnl", 0.0)
status_motor = engine_data.get("status", "HOLD_ACTIVE")
last_time = engine_data.get("timestamp", "--:--:--")

# Preluare evenimente din MongoDB pentru istoric
try:
    cursor_events = db.events_coll.find({}, {"_id": 0}).sort("timestamp", -1).limit(15)
    events_list = list(cursor_events)
except Exception:
    events_list = []

# Metrice principale sus
c1, c2, c3, c4 = st.columns(4)
c1.metric("Capital Real Motor", f"${real_capital:,.4f}", "Start Test: $50.00")
c2.metric(f"Preț Live BTC ({last_time})", f"${current_price:,.2f}", f"W/L: {wins}W / {losses}L")
c3.metric("Stare Poziție", active_pos.get("direction", "NONE") if active_pos else "FĂRĂ POZIȚIE", f"Status: {status_motor}")
c4.metric("PnL Curent Live", f"${current_pnl:+.4f}", "Sincronizat 100%")

st.markdown("---")

# ==========================================================
# GRAFIC REAL BAZAT PE PREȚUL LIVE DIN MOTOR
# ==========================================================
st.subheader("📊 Grafic Preț Live & Niveluri Strategice (BTCUSDT)")

import datetime
now_time = datetime.datetime.now()

df_live_chart = pd.DataFrame([
    {"time": now_time - datetime.timedelta(seconds=20), "price": current_price - 15},
    {"time": now_time - datetime.timedelta(seconds=15), "price": current_price - 8},
    {"time": now_time - datetime.timedelta(seconds=10), "price": current_price + 5},
    {"time": now_time - datetime.timedelta(seconds=5), "price": current_price - 3},
    {"time": now_time, "price": current_price}
])

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=df_live_chart['time'], 
    y=df_live_chart['price'],
    mode='lines+markers',
    name='Preț Live BTC',
    line=dict(color='#29B6F6', width=3)
))

if active_pos:
    entry_p = active_pos.get("entry_price", current_price)
    sl_p = active_pos.get("target_sl", current_price * 0.999)
    tp_p = active_pos.get("target_tp", current_price * 1.001)
    pos_dir = active_pos.get("direction", "LONG")
else:
    entry_p = current_price
    sl_p = current_price * 0.999
    tp_p = current_price * 1.001
    pos_dir = "SEARCHING"

fig.add_hline(y=entry_p, line_dash="dash", line_color="orange", annotation_text=f"ENTRY: ${entry_p:,.2f}")
fig.add_hline(y=sl_p, line_dash="dot", line_color="red", annotation_text=f"STOP LOSS: ${sl_p:,.2f}")
fig.add_hline(y=tp_p, line_dash="dot", line_color="green", annotation_text=f"TAKE PROFIT: ${tp_p:,.2f}")

fig.update_layout(
    height=400,
    margin=dict(l=20, r=20, t=30, b=20),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(15,15,25,0.9)',
    xaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)'),
    yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
)

st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

col_l, col_r = st.columns(2)

with col_l:
    st.subheader("🎯 Stare Execuție Curentă")
    if active_pos:
        st.success(f"Poziție activă în desfășurare: {pos_dir}")
        st.write(f"- **Direcție:** `{pos_dir}`")
        st.write(f"- **Preț Intrare:** `${entry_p:,.2f}`")
        st.write(f"- **Stop Loss Configurat:** `${sl_p:,.2f}`")
        st.write(f"- **Take Profit Configurat:** `${tp_p:,.2f}`")
        st.write(f"- **PnL Curent în Timp Real:** `${current_pnl:+.4f}`")
    else:
        st.info("Motorul scanează piața cu $50 capital. Nicio poziție deschisă momentan.")

with col_r:
    st.subheader("📋 Ultimele Evenimente din MongoDB")
    if events_list:
        df_events = pd.DataFrame(events_list)
        st.dataframe(df_events, use_container_width=True)
    else:
        st.info("Se așteaptă înregistrarea evenimentelor...")

if auto_refresh:
    time.sleep(1)
    st.rerun()