"""
market/live_feed.py
Preluarea datelor reale de piață prin API-ul public Binance.
"""

import requests

def fetch_live_market_data(symbol: str = "BTCUSDT") -> dict:
    """
    Interoghează Binance pentru a obține prețuri, spread real și volum live.
    """
    try:
        # 1. Preluare date ticker 24h (pentru volum și variație)
        ticker_url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
        ticker_res = requests.get(ticker_url, timeout=5).json()

        # 2. Preluare Order Book (pentru a calcula spread-ul real bid/ask)
        depth_url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=5"
        depth_res = requests.get(depth_url, timeout=5).json()

        best_bid = float(depth_res['bids'][0][0])
        best_ask = float(depth_res['asks'][0][0])
        
        # Calculăm spread-ul în puncte de bază (bps)
        mid_price = (best_bid + best_ask) / 2
        spread_bps = ((best_ask - best_bid) / mid_price) * 10000

        volume = float(ticker_res.get('volume', 0))
        price_change_pct = float(ticker_res.get('priceChangePercent', 0))

        # Construim snapshot-ul real pentru motorul de stări
        snapshot = {
            "symbol": symbol,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread_bps": round(spread_bps, 2),
            "volatility": abs(price_change_pct) * 15,  # Indicator orientativ de volatilitate bazat pe piață
            "volume": volume,
            "momentum": 50.0 + (price_change_pct * 5)  # Indicator de direcție bazat pe randament
        }
        
        return snapshot

    except Exception as e:
        print(f"[API ERROR] Nu s-au putut prelua datele live de la Binance: {e}")
        return None