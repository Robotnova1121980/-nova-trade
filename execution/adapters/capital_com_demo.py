"""Capital.com Demo read and order clients.

The discovery client is GET-only. A separate Demo-only trading client can
submit a risk-sized position after validating the market, levels, and account
position limits.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote

import requests


DEMO_BASE_URL = "https://demo-api-capital.backend-capital.com/api/v1"


class CapitalComDemoError(RuntimeError):
    """Raised when the Capital.com Demo API cannot safely answer a request."""


def validate_open_position_capacity(
    positions: list[dict[str, Any]], epic: str, max_positions: int = 3
) -> None:
    """Allow one position per market and enforce the account-wide position cap."""
    if not 1 <= max_positions <= 3:
        raise CapitalComDemoError("Limita pozițiilor deschise trebuie să fie între 1 și 3.")

    open_epics: list[str] = []
    for item in positions:
        position = item.get("position") or {}
        market = item.get("market") or {}
        open_epic = market.get("epic") or position.get("epic") or item.get("epic")
        if open_epic:
            open_epics.append(str(open_epic))

    if epic in open_epics:
        raise CapitalComDemoError(
            f"Există deja o poziție deschisă pe {epic}; botul permite o singură poziție per piață."
        )
    if len(positions) >= max_positions:
        raise CapitalComDemoError(
            f"Limita de {max_positions} poziții Demo simultane este atinsă."
        )


class CapitalComDemoClient:
    """Small synchronous client fixed to the Capital.com Demo endpoint."""

    def __init__(self, timeout: float = 12.0):
        self.identifier = os.getenv("CAPITAL_DEMO_IDENTIFIER", "").strip()
        self.api_key = os.getenv("CAPITAL_DEMO_API_KEY", "").strip()
        self.api_password = os.getenv("CAPITAL_DEMO_API_PASSWORD", "").strip()
        if not all((self.identifier, self.api_key, self.api_password)):
            raise CapitalComDemoError(
                "Lipsesc CAPITAL_DEMO_IDENTIFIER, CAPITAL_DEMO_API_KEY sau "
                "CAPITAL_DEMO_API_PASSWORD din variabilele de mediu."
            )
        self.timeout = timeout
        self.http = requests.Session()
        self.cst: str | None = None
        self.security_token: str | None = None
        self.login()

    def login(self) -> None:
        """Create a session against the Demo host; never uses the Live host."""
        try:
            response = self.http.post(
                f"{DEMO_BASE_URL}/session",
                headers={"X-CAP-API-KEY": self.api_key},
                json={"identifier": self.identifier, "password": self.api_password},
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise CapitalComDemoError(f"Nu pot contacta API-ul Demo: {exc}") from exc
        if response.status_code != 200:
            raise CapitalComDemoError(
                f"Autentificarea Demo a eșuat (HTTP {response.status_code}): "
                f"{response.text[:300]}"
            )
        self.cst = response.headers.get("CST")
        self.security_token = response.headers.get("X-SECURITY-TOKEN")
        if not self.cst or not self.security_token:
            raise CapitalComDemoError("Răspunsul de autentificare nu conține tokenurile API necesare.")

    def request(self, method: str, path: str, *, params: dict[str, Any] | None = None,
                payload: dict[str, Any] | None = None, retry_auth: bool = True) -> dict[str, Any]:
        if method.upper() != "GET":
            raise CapitalComDemoError("Clientul de verificare este read-only; sunt permise doar cereri GET.")
        if not self.cst or not self.security_token:
            self.login()
        headers = {"CST": self.cst or "", "X-SECURITY-TOKEN": self.security_token or ""}
        try:
            response = self.http.request(
                method,
                f"{DEMO_BASE_URL}{path}",
                headers=headers,
                params=params,
                json=payload,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise CapitalComDemoError(f"Eroare de rețea la {method} {path}: {exc}") from exc

        # Only retry safe reads after refreshing an expired session. Never
        # replay a trading POST/PUT/DELETE because its execution could be unclear.
        if response.status_code == 401 and method.upper() == "GET" and retry_auth:
            self.login()
            return self.request(method, path, params=params, payload=payload, retry_auth=False)
        if response.status_code < 200 or response.status_code >= 300:
            detail = response.text[:400]
            raise CapitalComDemoError(f"API {method} {path}: HTTP {response.status_code}: {detail}")
        if not response.content:
            return {}
        try:
            result = response.json()
        except ValueError as exc:
            raise CapitalComDemoError(f"API-ul a returnat un răspuns JSON invalid la {path}.") from exc
        if not isinstance(result, dict):
            raise CapitalComDemoError(f"API-ul a returnat un format neașteptat la {path}.")
        return result

    def accounts(self) -> list[dict[str, Any]]:
        result = self.request("GET", "/accounts")
        accounts = result.get("accounts")
        if not isinstance(accounts, list):
            raise CapitalComDemoError("Răspunsul /accounts nu conține lista de conturi.")
        return accounts

    def session_details(self) -> dict[str, Any]:
        return self.request("GET", "/session")

    def search_markets(self, term: str) -> list[dict[str, Any]]:
        result = self.request("GET", "/markets", params={"searchTerm": term})
        markets = result.get("markets")
        if not isinstance(markets, list):
            raise CapitalComDemoError(f"Căutarea pieței «{term}» nu a returnat o listă validă.")
        return markets

    def market_details(self, epic: str) -> dict[str, Any]:
        return self.request("GET", f"/markets/{quote(epic, safe='')}")

    def ticker(self, epic: str) -> dict[str, Any]:
        result = self.market_details(epic)
        snapshot = result.get("snapshot") or {}
        bid, ask = snapshot.get("bid"), snapshot.get("offer")
        if bid is None or ask is None:
            raise CapitalComDemoError(f"Piața {epic} nu oferă momentan bid/offer.")
        bid, ask = float(bid), float(ask)
        if bid <= 0 or ask <= 0 or ask < bid:
            raise CapitalComDemoError(f"Cotație invalidă pentru piața {epic}.")
        return {
            "epic": epic,
            "bid": bid,
            "ask": ask,
            "last": (bid + ask) / 2.0,
            "market_status": snapshot.get("marketStatus"),
            "update_time_utc": snapshot.get("updateTimeUTC"),
            "update_time": snapshot.get("updateTime"),
            "delay_time": snapshot.get("delayTime"),
            "spread": ask - bid,
        }

    def candles_5m(self, epic: str, limit: int = 500) -> list[dict[str, Any]]:
        return self._candles(epic, limit, "MINUTE_5")

    def candles_15m(self, epic: str, limit: int = 500) -> list[dict[str, Any]]:
        return self._candles(epic, limit, "MINUTE_15")

    def _candles(
        self, epic: str, limit: int, resolution: str,
        *, from_utc: str | None = None, to_utc: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "resolution": resolution,
            "max": max(2, min(int(limit), 1000)),
        }
        if from_utc:
            params["from"] = from_utc
        if to_utc:
            params["to"] = to_utc
        result = self.request(
            "GET", f"/prices/{quote(epic, safe='')}",
            params=params,
        )
        prices = result.get("prices")
        if not isinstance(prices, list):
            raise CapitalComDemoError(f"API-ul nu a returnat lumânări {resolution} pentru {epic}.")
        rows: list[dict[str, Any]] = []
        for bar in prices:
            timestamp = bar.get("snapshotTimeUTC")
            if not timestamp:
                continue
            values: dict[str, float] = {}
            for out_key, api_key in (("open", "openPrice"), ("high", "highPrice"),
                                     ("low", "lowPrice"), ("close", "closePrice")):
                quote_data = bar.get(api_key) or {}
                bid, ask = quote_data.get("bid"), quote_data.get("ask")
                if bid is None or ask is None:
                    break
                bid, ask = float(bid), float(ask)
                values[out_key] = (bid + ask) / 2.0
                values[f"{out_key}_bid"] = bid
                values[f"{out_key}_ask"] = ask
            if len(values) != 12:
                continue
            rows.append({
                "ts": timestamp,
                **values,
                "volume": float(bar.get("lastTradedVolume") or 0.0),
            })
        if not rows:
            raise CapitalComDemoError(f"Nu există lumânări complete {resolution} pentru {epic}.")
        return rows

    def historical_candles(
        self, epic: str, resolution: str, start_utc: str, end_utc: str, limit: int = 1000
    ) -> list[dict[str, Any]]:
        """Read a bounded historical price window. This method is GET-only."""
        allowed = {"MINUTE_5", "MINUTE_15"}
        if resolution not in allowed:
            raise CapitalComDemoError(f"Rezoluție istorică neacceptată: {resolution}.")
        return self._candles(
            epic, limit, resolution, from_utc=start_utc, to_utc=end_utc
        )

    def close(self) -> None:
        self.http.close()


class CapitalComDemoTradingClient:
    """Explicit order client fixed to Capital.com Demo.

    The read-only discovery client above remains GET-only. This separate
    client is intended for a reviewed Demo runner; it never uses the Live
    hostname and never retries an order submission whose outcome is unclear.
    """

    def __init__(self, timeout: float = 12.0):
        self.read = CapitalComDemoClient(timeout=timeout)
        self.timeout = timeout

    def open_position(
        self,
        *,
        epic: str,
        direction: str,
        size: float,
        stop_level: float,
        profit_level: float,
        max_open_positions: int | None = None,
    ) -> dict[str, Any]:
        direction = direction.upper()
        if direction not in {"BUY", "SELL"}:
            raise CapitalComDemoError("Direcția ordinului trebuie să fie BUY sau SELL.")
        if not epic or not all(float(x) > 0 for x in (size, stop_level, profit_level)):
            raise CapitalComDemoError("Epic, mărime, stop-loss și take-profit trebuie să fie pozitive.")

        market = self.read.market_details(epic)
        snapshot = market.get("snapshot") or {}
        if snapshot.get("marketStatus") != "TRADEABLE":
            raise CapitalComDemoError(f"Piața {epic} nu este TRADEABLE.")
        rules = market.get("dealingRules") or {}
        min_size = float((rules.get("minDealSize") or {}).get("value") or 0)
        size_step = float((rules.get("minSizeIncrement") or {}).get("value") or 0)
        if min_size <= 0 or size_step <= 0:
            raise CapitalComDemoError(f"Regulile de mărime pentru {epic} nu pot fi validate.")
        steps = (float(size) - min_size) / size_step
        if size < min_size or abs(steps - round(steps)) > 1e-7:
            raise CapitalComDemoError(
                f"Mărimea ordinului nu respectă minDealSize/sizeStep pentru {epic}."
            )

        bid = float(snapshot.get("bid") or 0)
        ask = float(snapshot.get("offer") or 0)
        reference = ask if direction == "BUY" else bid
        stop_level, profit_level = float(stop_level), float(profit_level)
        if reference <= 0:
            raise CapitalComDemoError(f"Nu există cotație validă pentru {epic}.")
        if direction == "BUY" and not (stop_level < reference < profit_level):
            raise CapitalComDemoError("Pentru BUY, stop-loss < preț curent < take-profit.")
        if direction == "SELL" and not (profit_level < reference < stop_level):
            raise CapitalComDemoError("Pentru SELL, take-profit < preț curent < stop-loss.")

        if max_open_positions is None:
            try:
                max_open_positions = int(os.getenv("CAPITAL_DEMO_MAX_OPEN_POSITIONS", "3"))
            except ValueError as exc:
                raise CapitalComDemoError(
                    "CAPITAL_DEMO_MAX_OPEN_POSITIONS trebuie să fie un număr întreg între 1 și 3."
                ) from exc
        max_positions = int(max_open_positions)
        if not 1 <= max_positions <= 3:
            raise CapitalComDemoError(
                "CAPITAL_DEMO_MAX_OPEN_POSITIONS trebuie să fie între 1 și 3."
            )
        positions = self.read.request("GET", "/positions").get("positions") or []
        validate_open_position_capacity(positions, epic, max_positions)

        payload = {
            "epic": epic,
            "direction": direction,
            "size": float(size),
            "stopLevel": stop_level,
            "profitLevel": profit_level,
            "guaranteedStop": False,
        }
        headers = {
            "CST": self.read.cst or "",
            "X-SECURITY-TOKEN": self.read.security_token or "",
            "X-CAP-API-KEY": self.read.api_key,
        }
        try:
            response = self.read.http.post(
                f"{DEMO_BASE_URL}/positions",
                headers=headers,
                json=payload,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise CapitalComDemoError(
                "Rezultatul trimiterii ordinului este necunoscut; verifică pozițiile Demo "
                "înainte de orice reluare manuală. Cererea nu a fost retrimisă automat."
            ) from exc
        if response.status_code < 200 or response.status_code >= 300:
            raise CapitalComDemoError(
                f"Capital.com a respins ordinul Demo (HTTP {response.status_code})."
            )
        try:
            deal_reference = response.json().get("dealReference")
        except (ValueError, AttributeError):
            deal_reference = None
        if not deal_reference:
            raise CapitalComDemoError(
                "API-ul nu a returnat dealReference; verifică pozițiile Demo înainte de reluare."
            )

        confirmation = self.read.request("GET", f"/confirms/{quote(str(deal_reference), safe='')}")
        if confirmation.get("dealStatus") != "ACCEPTED":
            raise CapitalComDemoError(
                f"Ordinul nu este confirmat ACCEPTED (status={confirmation.get('dealStatus', 'necunoscut')})."
            )
        return {
            "deal_reference": str(deal_reference),
            "deal_status": confirmation.get("dealStatus"),
            "affected_deals": confirmation.get("affectedDeals") or [],
            "epic": epic,
            "direction": direction,
            "size": float(size),
            "stop_level": stop_level,
            "profit_level": profit_level,
        }

    def close(self) -> None:
        self.read.close()
