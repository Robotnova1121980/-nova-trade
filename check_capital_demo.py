"""Verify Capital.com Demo API credentials and discover eligible markets.

This script is read-only. It never creates, changes, or closes a position.
"""

from __future__ import annotations

from dotenv import load_dotenv

from execution.adapters.capital_com_demo import CapitalComDemoClient


SEARCHES = {
    # Major FX pairs and commonly traded crosses
    "EURUSD": "EURUSD", "GBPUSD": "GBPUSD", "USDJPY": "USDJPY",
    "AUDUSD": "AUDUSD", "USDCAD": "USDCAD", "USDCHF": "USDCHF",
    "NZDUSD": "NZDUSD", "EURJPY": "EURJPY", "GBPJPY": "GBPJPY",
    # Major indices, metals, energy, and crypto
    "US500": "US500", "US100": "US100", "US30": "US30",
    "DE40": "Germany 40", "UK100": "UK100",
    "GOLD": "gold", "SILVER": "silver", "OIL_CRUDE": "crude oil",
    "OIL_BRENT": "brent oil", "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum",
}


def main() -> int:
    load_dotenv()
    client = CapitalComDemoClient()
    try:
        accounts = client.accounts()
        print(f"Autentificare API Demo: OK ({len(accounts)} cont(uri) returnat(e)).")
        for account in accounts:
            # Never print account IDs, credentials, or exact balances.
            print(
                "Cont:"
                f" tip={account.get('accountType', 'necunoscut')}"
                f" | monedă={account.get('currency') or account.get('currencyIsoCode', 'necunoscut')}"
                f" | preferat={account.get('preferred', 'necunoscut')}"
            )

        for alias, term in SEARCHES.items():
            try:
                markets = client.search_markets(term)
            except Exception as exc:
                print(f"\n{alias}: căutarea a eșuat: {exc}")
                continue
            print(f"\n{alias} — rezultate pentru «{term}»:")
            if not markets:
                print("  Nu s-au găsit piețe.")
                continue
            tradeable = [
                item for item in markets
                if str(((item.get("snapshot") or item).get("marketStatus") or "")).upper() == "TRADEABLE"
            ]
            if not tradeable:
                print("  Nu s-a găsit momentan o piață TRADEABLE în rezultate.")
                continue
            shown = 0
            for item in tradeable:
                if shown >= 3:
                    break
                epic = str(item.get("epic") or "")
                rules = item.get("dealingRules") or {}
                snapshot = item.get("snapshot") or item
                try:
                    details = client.market_details(epic) if epic else {}
                    rules = details.get("dealingRules") or rules
                    snapshot = details.get("snapshot") or snapshot
                except Exception:
                    # Keep the search result visible; detailed rules are needed
                    # before implementing order sizing, not for this discovery.
                    pass
                print(
                    f"  epic={epic or '—'}"
                    f" | nume={item.get('instrumentName') or item.get('symbol') or '—'}"
                    f" | status={snapshot.get('marketStatus', '—')}"
                    f" | bid={snapshot.get('bid', '—')}"
                    f" | ask={snapshot.get('offer', '—')}"
                    f" | minSize={((rules.get('minDealSize') or {}).get('value', '—'))} {((rules.get('minDealSize') or {}).get('unit', ''))}"
                    f" | sizeStep={((rules.get('minSizeIncrement') or {}).get('value', '—'))} {((rules.get('minSizeIncrement') or {}).get('unit', ''))}"
                    f" | minStop={((rules.get('minStopOrProfitDistance') or {}).get('value', '—'))} {((rules.get('minStopOrProfitDistance') or {}).get('unit', ''))}"
                    f" | pnlCurrency={((details.get('instrument') or {}).get('currency', '—'))}"
                    f" | contractSize={((details.get('instrument') or {}).get('lotSize', '—'))}"
                    f" | marginFactor={((details.get('instrument') or {}).get('marginFactor', '—'))} {((details.get('instrument') or {}).get('marginFactorUnit', ''))}"
                )
                shown += 1
    finally:
        client.close()
    print("\nVerificarea s-a încheiat fără a trimite ordine.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
