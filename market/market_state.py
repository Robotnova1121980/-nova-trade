"""
market/market_state.py
Clasificatorul stărilor pieței pentru Nova Trade v0.3.
"""

class MarketStateEngine:
    def __init__(self):
        self.max_allowable_spread = 4.0 # bps
        self.high_volatility_threshold = 75.0

    def classify(self, market_snapshot: dict) -> str:
        spread = market_snapshot.get("spread_bps", 1.0)
        volatility = market_snapshot.get("volatility", 50.0)
        volume = market_snapshot.get("volume", 500)
        momentum = market_snapshot.get("momentum", 50.0)

        if spread > self.max_allowable_spread:
            return "HIGH_SPREAD_NO_TRADE"
        if volatility > self.high_volatility_threshold:
            return "HIGH_VOLATILITY_CHAOTIC"
        if momentum > 70 and volume > 800:
            return "MICRO_TREND"
        
        return "RANGE_BOUND"