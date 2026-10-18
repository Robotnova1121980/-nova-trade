"""Capital.com Demo runner using the Nova 5/15-minute breakout signal.

Order submission is disabled unless CAPITAL_DEMO_ENABLE_ORDERS=true is set.
This runner is permanently bound to the Capital.com Demo API client.
"""

from __future__ import annotations

import math
import os
import re
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from pathlib import Path
from typing import Any
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from execution.adapters.capital_com_demo import (
    CapitalComDemoClient,
    CapitalComDemoError,
    CapitalComDemoTradingClient,
)
from storage.capital_runner_store import CapitalRunnerStore


BAR_MINUTES = 5
FAST_EMA = 9
TREND_EMA = 50
ATR_PERIOD = 14
BREAKOUT_PERIOD = 20
ATR_STOP_MULTIPLIER = 1.2
ATR_TARGET_MULTIPLIER = 1.5
MAX_SPREAD_PCT = 0.001
SLIPPAGE_ESTIMATE_PCT = 0.0002
ADX_MIN_TREND = 25.0
MAX_SPREAD_TO_STOP = 0.15
POLL_SECONDS = 15
DEFAULT_RISK_PERCENT = 0.5
MAX_RISK_PERCENT = 5.0
STATE_PATH = Path(__file__).resolve().parent / "storage" / "capital_demo_runner_state.json"


def fetch_candles(client: CapitalComDemoClient, epic: str, limit: int = 250) -> list[dict[str, Any]]:
    method_name = "candles_5m" if BAR_MINUTES == 5 else "candles_15m"
    method = getattr(client, method_name, None)
    if not callable(method):
        raise CapitalComDemoError(
            f"Adaptorul Capital.com nu implementează {method_name} pentru intervalul de {BAR_MINUTES} minute."
        )
    return method(epic, limit=limit)


def configured_bar_minutes(control: dict[str, Any] | None = None) -> int:
    control = control or {}
    value = str(control.get("capital_demo_bar_minutes", os.getenv("CAPITAL_DEMO_BAR_MINUTES", "5"))).strip()
    if value not in {"5", "15"}:
        raise ValueError("CAPITAL_DEMO_BAR_MINUTES acceptă doar 5 sau 15.")
    return int(value)


def configured_markets(control: dict[str, Any] | None = None) -> dict[str, str]:
    control = control or {}
    configured_epics = control.get("capital_demo_epics", os.getenv("CAPITAL_DEMO_EPICS", ""))
    if isinstance(configured_epics, (list, tuple)):
        epics = [str(epic).strip() for epic in configured_epics if str(epic).strip()]
    else:
        configured_epics = str(configured_epics or "").strip()
        epics = [epic.strip() for epic in configured_epics.split(",") if epic.strip()]
    if configured_epics:
        # Keep the first occurrence and preserve the user's chosen order.
        unique_epics = list(dict.fromkeys(epics))
        return {epic: epic for epic in unique_epics}
    return {
        alias: os.getenv(env_name, "").strip()
        for alias, env_name in (
            ("EURUSD", "CAPITAL_EPIC_EURUSD"),
            ("GOLD", "CAPITAL_EPIC_GOLD"),
            ("OIL", "CAPITAL_EPIC_OIL"),
        )
        if os.getenv(env_name, "").strip()
    }


def closed_candles(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["ts"]).drop_duplicates("ts").sort_values("ts")
    cutoff = pd.Timestamp.now(tz="UTC")
    frame = frame[frame["ts"] + pd.Timedelta(minutes=BAR_MINUTES) <= cutoff].copy()
    if frame.empty:
        return frame
    close = frame["close"].astype(float)
    frame["ema_fast"] = close.ewm(span=FAST_EMA, adjust=False).mean()
    frame["ema_trend"] = close.ewm(span=TREND_EMA, adjust=False).mean()
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            frame["high"].astype(float) - frame["low"].astype(float),
            (frame["high"].astype(float) - previous_close).abs(),
            (frame["low"].astype(float) - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    frame["atr"] = true_range.ewm(alpha=1 / ATR_PERIOD, adjust=False).mean()
    frame["breakout_high"] = frame["high"].astype(float).rolling(BREAKOUT_PERIOD).max().shift(1)
    frame["breakout_low"] = frame["low"].astype(float).rolling(BREAKOUT_PERIOD).min().shift(1)

    # Wilder-smoothed ADX, same definition as the regime filter in the backtest.
    up = frame["high"].astype(float).diff()
    down = -frame["low"].astype(float).diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=frame.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=frame.index)
    smooth_atr = true_range.ewm(alpha=1 / ATR_PERIOD, min_periods=ATR_PERIOD, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / ATR_PERIOD, min_periods=ATR_PERIOD, adjust=False).mean() / smooth_atr
    minus_di = 100 * minus_dm.ewm(alpha=1 / ATR_PERIOD, min_periods=ATR_PERIOD, adjust=False).mean() / smooth_atr
    denominator = (plus_di + minus_di).replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / denominator
    frame["adx"] = dx.ewm(alpha=1 / ATR_PERIOD, min_periods=ATR_PERIOD, adjust=False).mean()
    return frame.dropna(subset=["ema_trend", "atr", "breakout_high", "breakout_low"])


def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError:
        return default
    return value if math.isfinite(value) else default


def breakout_direction(bar: pd.Series, spread_pct: float) -> str | None:
    # Use mirrored trend/breakout rules for long and short entries.
    total_cost_estimate = spread_pct + (2 * SLIPPAGE_ESTIMATE_PCT)

    # Regime filter: skip breakouts while the market is not trending.
    # A missing ADX value (not enough bars) blocks the signal.
    adx_min = _env_float("CAPITAL_DEMO_ADX_MIN", ADX_MIN_TREND)
    if not (float(bar["adx"]) >= adx_min):
        print(f"   motiv respingere: ADX {float(bar['adx']):.1f} < {adx_min:g}")
        return None

    # Cost filter relative to the stop distance: skip entries where the spread
    # alone would consume too large a share of the planned 1R risk.
    stop_pct = ATR_STOP_MULTIPLIER * float(bar["atr"]) / float(bar["close"])
    max_spread_to_stop = _env_float("CAPITAL_DEMO_MAX_SPREAD_TO_STOP", MAX_SPREAD_TO_STOP)
    if not (stop_pct > 0) or spread_pct / stop_pct > max_spread_to_stop:
        ratio = spread_pct / stop_pct if stop_pct > 0 else float("nan")
        print(f"   motiv respingere: spread/stop {ratio:.3f} > {max_spread_to_stop:g}")
        return None

    buy_signal = bool(
        float(bar["close"]) > float(bar["ema_fast"])
        and float(bar["ema_fast"]) > float(bar["ema_trend"])
        and float(bar["close"]) > float(bar["breakout_high"])
        and (float(bar["close"]) / float(bar["ema_fast"]) - 1) >= total_cost_estimate
    )
    sell_signal = bool(
        float(bar["close"]) < float(bar["ema_fast"])
        and float(bar["ema_fast"]) < float(bar["ema_trend"])
        and float(bar["close"]) < float(bar["breakout_low"])
        and (float(bar["ema_fast"]) / float(bar["close"]) - 1) >= total_cost_estimate
    )
    if buy_signal:
        return "BUY"
    if sell_signal:
        return "SELL"
    print(
        f"   motiv respingere: fără breakout valid (close={float(bar['close']):.4f}, "
        f"max20={float(bar['breakout_high']):.4f}, min20={float(bar['breakout_low']):.4f}, "
        f"ema9={float(bar['ema_fast']):.4f}, ema50={float(bar['ema_trend']):.4f})"
    )
    return None


def fresh_tradeable_quote(client: CapitalComDemoClient, epic: str) -> dict[str, Any]:
    quote = client.ticker(epic)
    if quote.get("market_status") != "TRADEABLE":
        raise CapitalComDemoError(f"Piața {epic} nu este TRADEABLE.")
    timestamp = quote.get("update_time_utc")
    if timestamp:
        try:
            updated = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - updated.astimezone(timezone.utc)).total_seconds()
        except (TypeError, ValueError):
            age = None
        if age is not None and (age < -60 or age > 180):
            raise CapitalComDemoError(f"Cotația pentru {epic} este prea veche ({age:.0f}s).")
    else:
        delay = quote.get("delay_time")
        if delay not in (None, ""):
            try:
                delay_seconds = float(delay)
            except (TypeError, ValueError):
                raise CapitalComDemoError(f"Întârzierea cotației pentru {epic} nu poate fi verificată.")
            if delay_seconds > 180:
                raise CapitalComDemoError(f"Cotația pentru {epic} este întârziată ({delay_seconds:.0f}s).")
        # Some Capital.com snapshots omit both timestamp fields. In that case
        # this value came from the just-completed REST request; market status,
        # delayTime (when present), and spread checks still apply.
    if quote["spread"] / quote["bid"] > MAX_SPREAD_PCT:
        raise CapitalComDemoError(f"Spread prea mare pentru {epic}; semnal blocat.")
    return quote


def _normalise_pair(value: Any) -> str:
    return re.sub(r"[^A-Z]", "", str(value or "").upper())


def active_cfd_account(client: CapitalComDemoClient) -> dict[str, Any]:
    accounts = client.accounts()
    session = client.session_details()
    current_id = str(session.get("accountId") or "")
    current = [a for a in accounts if current_id and str(a.get("accountId") or "") == current_id]
    if len(current) == 1 and str(current[0].get("accountType") or "").upper() == "CFD":
        return current[0]
    cfds = [a for a in accounts if str(a.get("accountType") or "").upper() == "CFD"]
    preferred = [a for a in cfds if a.get("preferred") is True]
    if len(preferred) == 1:
        return preferred[0]
    if not preferred and len(cfds) == 1:
        return cfds[0]
    raise CapitalComDemoError("Nu pot identifica fără ambiguitate contul CFD Demo activ; ordin blocat.")


def account_currency_conversion(
    client: CapitalComDemoClient, source_currency: str, account_currency: str
) -> float:
    """Return source-currency -> account-currency rate using a fresh broker FX quote."""
    source = source_currency.upper()
    target = account_currency.upper()
    if len(source) != 3 or len(target) != 3:
        raise CapitalComDemoError("Moneda instrumentului sau a contului nu poate fi validată.")
    if source == target:
        return 1.0

    for pair, inverse in ((source + target, False), (target + source, True)):
        try:
            candidates = client.search_markets(pair)
        except CapitalComDemoError:
            continue
        for item in candidates:
            if str(item.get("instrumentType") or "").upper() != "CURRENCIES":
                continue
            identifiers = (item.get("symbol"), item.get("instrumentName"), item.get("epic"))
            if not any(_normalise_pair(v) == pair for v in identifiers):
                continue
            epic = str(item.get("epic") or "").strip()
            if not epic:
                continue
            quote = fresh_tradeable_quote(client, epic)
            rate = 1.0 / quote["ask"] if inverse else quote["bid"]
            if math.isfinite(rate) and rate > 0:
                return rate
    raise CapitalComDemoError(
        f"Nu am găsit o cotație Forex proaspătă pentru conversia {source}->{target}; ordin blocat."
    )


def calculate_risk_sized_order(
    *, client: CapitalComDemoClient, market: dict[str, Any], account: dict[str, Any],
    entry: float, stop: float, spread: float, risk_percent: float | None = None,
) -> tuple[float, float, float, float]:
    """Size an order so estimated loss at its stop stays within the risk budget.

    Position size is interpreted in the broker's market units. The calculation
    uses price-distance P/L, broker min/max/increment rules, account conversion,
    a slippage cushion, and the broker's margin percentage. Unknown fields fail closed.
    """
    if risk_percent is None:
        try:
            risk_percent = float(os.getenv("CAPITAL_DEMO_RISK_PERCENT", str(DEFAULT_RISK_PERCENT)))
        except ValueError as exc:
            raise CapitalComDemoError("CAPITAL_DEMO_RISK_PERCENT trebuie să fie un număr.") from exc
    if not math.isfinite(risk_percent) or not (0 < risk_percent <= MAX_RISK_PERCENT):
        raise CapitalComDemoError(f"CAPITAL_DEMO_RISK_PERCENT trebuie să fie între 0 și {MAX_RISK_PERCENT}.")

    balance_data = account.get("balance") or {}
    equity = float(balance_data.get("balance") or 0)
    available = float(balance_data.get("available") or 0)
    account_currency = str(account.get("currency") or account.get("currencyIsoCode") or "").upper()
    instrument = market.get("instrument") or {}
    pnl_currency = str(instrument.get("currency") or market.get("currency") or "").upper()
    if equity <= 0 or available <= 0 or not account_currency or not pnl_currency:
        raise CapitalComDemoError("Lipsesc soldul disponibil sau moneda necesară dimensionării riscului.")

    conversion = account_currency_conversion(client, pnl_currency, account_currency)
    rules = market.get("dealingRules") or {}
    try:
        minimum = Decimal(str((rules.get("minDealSize") or {})["value"]))
        increment = Decimal(str((rules.get("minSizeIncrement") or {})["value"]))
        maximum = Decimal(str((rules.get("maxDealSize") or {})["value"]))
        margin_factor = float(instrument["marginFactor"])
        margin_unit = str(instrument["marginFactorUnit"]).upper()
        contract_size = float(instrument["lotSize"])
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise CapitalComDemoError("Regulile min/max/pas sau factorul de marjă lipsesc; ordin blocat.") from exc
    if minimum <= 0 or increment <= 0 or maximum < minimum:
        raise CapitalComDemoError("Regulile de mărime ale brokerului sunt invalide.")
    if not math.isfinite(contract_size) or contract_size <= 0:
        raise CapitalComDemoError("Mărimea contractului lipsește sau este invalidă; ordin blocat.")
    if margin_unit != "PERCENTAGE" or not math.isfinite(margin_factor) or margin_factor <= 0:
        raise CapitalComDemoError("Unitatea factorului de marjă nu este validată; ordin blocat.")

    risk_budget = equity * risk_percent / 100.0
    # The added spread and slippage buffer make the planned loss estimate more
    # conservative than the distance between entry and stop alone.
    risk_distance = abs(entry - stop) + max(0.0, spread) + (2 * SLIPPAGE_ESTIMATE_PCT * entry)
    risk_per_size = risk_distance * conversion * contract_size
    margin_per_size = entry * conversion * contract_size * (margin_factor / 100.0)
    if not all(math.isfinite(x) and x > 0 for x in (risk_budget, risk_per_size, margin_per_size)):
        raise CapitalComDemoError("Nu se poate calcula un buget de risc/marjă pozitiv; ordin blocat.")

    # Keep a 10% free-margin reserve, and never round up to the broker minimum.
    risk_limited_size = risk_budget / risk_per_size
    margin_limited_size = (available * 0.90) / margin_per_size
    max_size = min(float(maximum), risk_limited_size, margin_limited_size)
    if max_size < float(minimum):
        min_risk = float(minimum) * risk_per_size
        raise CapitalComDemoError(
            f"Ordin blocat: minimul brokerului ar risca estimativ {min_risk:.2f} {account_currency}, "
            f"peste bugetul de {risk_budget:.2f} {account_currency} sau depășește marja disponibilă."
        )

    min_decimal = minimum
    step_decimal = increment
    max_decimal = Decimal(str(max_size))
    steps = ((max_decimal - min_decimal) / step_decimal).to_integral_value(rounding=ROUND_FLOOR)
    size = min_decimal + steps * step_decimal
    size = min(size, maximum)
    if size < minimum:
        raise CapitalComDemoError("Mărimea calculată este sub minimul brokerului; ordin blocat.")
    size_float = float(size)
    estimated_risk = size_float * risk_per_size
    estimated_margin = size_float * margin_per_size
    if estimated_risk > risk_budget + 1e-8 or estimated_margin > available * 0.90 + 1e-8:
        raise CapitalComDemoError("Rotunjirea mărimii ar depăși bugetul; ordin blocat.")
    return size_float, estimated_risk, risk_budget, estimated_margin


def run() -> int:
    global BAR_MINUTES
    load_dotenv()
    try:
        runner_store = CapitalRunnerStore(STATE_PATH)
        initial_control = runner_store.get_control()
        BAR_MINUTES = configured_bar_minutes(initial_control)
    except Exception as exc:
        print(f"Nu pot inițializa baza de date/configurația: {exc}")
        return 2

    orders_authorized = os.getenv("CAPITAL_DEMO_ENABLE_ORDERS", "false").strip().lower() == "true"
    orders_enabled = orders_authorized and bool(initial_control.get("capital_demo_enable_orders", True))
    print("Trimiterea ordinelor Demo este activată explicit." if orders_enabled else "Mod doar semnale: ordine dezactivate.")

    markets = configured_markets(initial_control)
    if not markets:
        print("Nu sunt piețe Capital configurate în Mongo sau în variabilele Render.")
        runner_store.close()
        return 1

    state = runner_store.load_state()
    try:
        trading_client: CapitalComDemoTradingClient | None = (
            CapitalComDemoTradingClient() if orders_authorized else None
        )
        read_client = trading_client.read if trading_client is not None else CapitalComDemoClient()
    except Exception as exc:
        runner_store.close()
        print(f"Conectarea la Capital.com Demo a eșuat: {exc}")
        return 2

    health_server = None
    port = os.getenv("PORT", "").strip()
    if port:
        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path.split("?", 1)[0] not in {"/", "/health"}:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(b'{"status":"ok","runner":"capital-demo"}')

            def log_message(self, _format, *_args):
                return

        try:
            health_server = ThreadingHTTPServer(("0.0.0.0", int(port)), HealthHandler)
            threading.Thread(target=health_server.serve_forever, daemon=True).start()
            print(f"Endpoint Render /health activ pe portul {port}.")
        except Exception as exc:
            read_client.close()
            runner_store.close()
            print(f"Nu pot porni endpointul de sănătate Render: {exc}")
            return 2

    try:
        print(f"Runner pornit pe Capital.com Demo. Strategie: breakout BUY/SELL pe lumânări de {BAR_MINUTES} minute.")
        if orders_enabled:
            try:
                configured_risk = float(initial_control.get(
                    "capital_demo_risk_percent",
                    os.getenv("CAPITAL_DEMO_RISK_PERCENT", str(DEFAULT_RISK_PERCENT)),
                ))
            except ValueError:
                configured_risk = DEFAULT_RISK_PERCENT
            print(
                f"Dimensionare după risc: maximum {configured_risk:g}% din sold la stop, "
                "cu buffer de spread/slippage și limită de marjă; maximum 3 poziții simultane, "
                "pe piețe diferite."
            )

        # Establish a baseline on first run so an old breakout is never traded at startup.
        baseline_needed = not state
        while True:
            try:
                control = runner_store.get_control()
                BAR_MINUTES = configured_bar_minutes(control)
                markets = configured_markets(control)
                if not markets:
                    raise ValueError("Nu sunt piețe Capital configurate.")
            except Exception as exc:
                print(f"Configurarea Mongo nu poate fi citită; intrările sunt suspendate: {exc}")
                runner_store.save_state(state, status="config_error")
                time.sleep(max(10, int(os.getenv("CAPITAL_DEMO_POLL_SECONDS", str(POLL_SECONDS)))))
                continue

            if bool(control.get("stopped", False)) or not bool(control.get("allow_new_trades", True)):
                status = "stopped" if control.get("stopped") else "entries_paused"
                runner_store.save_state(state, status=status)
                time.sleep(max(10, int(os.getenv("CAPITAL_DEMO_POLL_SECONDS", str(POLL_SECONDS)))))
                continue

            orders_enabled = orders_authorized and bool(control.get("capital_demo_enable_orders", True))
            for alias, epic in markets.items():
                try:
                    bars = closed_candles(fetch_candles(read_client, epic, limit=250))
                    if bars.empty:
                        print(f"[{alias}] Nu sunt suficiente lumânări închise încă.")
                        continue
                    bar = bars.iloc[-1]
                    bar_id = bar["ts"].isoformat()
                    state_key = f"{BAR_MINUTES}:{epic}"
                    if baseline_needed or state_key not in state:
                        state[state_key] = bar_id
                        runner_store.save_state(state)
                        print(f"[{alias}] Referință stabilită; aștept următoarea lumânare închisă.")
                        continue
                    if state.get(state_key) == bar_id:
                        continue

                    # Persist before any order call. An uncertain response must not trigger a retry.
                    state[state_key] = bar_id
                    runner_store.save_state(state)
                    quote = fresh_tradeable_quote(read_client, epic)
                    direction = breakout_direction(bar, quote["spread"] / quote["bid"])
                    if direction is None:
                        print(f"[{alias}] Fără semnal nou ({bar_id}).")
                        continue
                    runner_store.log_event(
                        "SIGNAL", f"Semnal {direction} pe {alias}.",
                        epic=epic, alias=alias, direction=direction, bar_id=bar_id,
                    )

                    if not orders_enabled:
                        runner_store.log_event(
                            "SIGNAL_ONLY", f"Semnal {direction}; ordinul este dezactivat.",
                            epic=epic, alias=alias, direction=direction, bar_id=bar_id,
                        )
                        print(f"[{alias}] Semnal {direction} detectat ({bar_id}); ordinul nu a fost trimis.")
                        continue

                    details = read_client.market_details(epic)
                    entry = float(quote["ask"] if direction == "BUY" else quote["bid"])
                    atr = float(bar["atr"])
                    if direction == "BUY":
                        stop = entry - (ATR_STOP_MULTIPLIER * atr)
                        target = entry + (ATR_TARGET_MULTIPLIER * atr)
                        valid_levels = stop < entry < target
                    else:
                        stop = entry + (ATR_STOP_MULTIPLIER * atr)
                        target = entry - (ATR_TARGET_MULTIPLIER * atr)
                        valid_levels = target < entry < stop
                    if atr <= 0 or target <= 0 or not valid_levels:
                        print(f"[{alias}] Stop/țintă invalide; ordin blocat.")
                        continue

                    account = active_cfd_account(read_client)
                    risk_percent = float(control.get(
                        "capital_demo_risk_percent",
                        os.getenv("CAPITAL_DEMO_RISK_PERCENT", str(DEFAULT_RISK_PERCENT)),
                    ))
                    size, estimated_risk, risk_budget, estimated_margin = calculate_risk_sized_order(
                        client=read_client,
                        market=details,
                        account=account,
                        entry=entry,
                        stop=stop,
                        spread=quote["spread"],
                        risk_percent=risk_percent,
                    )
                    currency = account.get("currency") or account.get("currencyIsoCode") or "monedă cont"
                    print(
                        f"[{alias}] Semnal {direction}; mărime={size:g}, "
                        f"risc estimat la distanța ATR={estimated_risk:.2f}/{risk_budget:.2f} {currency}, "
                        f"stop monetar solicitat={os.getenv('CAPITAL_DEMO_MAX_LOSS_AMOUNT', '1.0')} {currency}, "
                        f"marjă estimată={estimated_margin:.2f} {currency}. Trimit ordin Demo."
                    )
                    if trading_client is None:
                        raise CapitalComDemoError("Clientul de ordine Demo nu este inițializat.")
                    result = trading_client.open_position(
                        epic=epic,
                        direction=direction,
                        size=size,
                        stop_level=stop,
                        profit_level=target,
                        max_open_positions=int(control.get(
                            "capital_demo_max_open_positions",
                            os.getenv("CAPITAL_DEMO_MAX_OPEN_POSITIONS", "3"),
                        )),
                    )
                    print(
                        f"[{alias}] Ordin confirmat: status={result['deal_status']} "
                        f"| referință={result['deal_reference']} "
                        f"| stop monetar activ={result['stop_amount']:g} {currency}"
                    )
                    runner_store.log_event(
                        "ORDER_ACCEPTED",
                        f"Ordin {direction} confirmat pe {alias}; stop monetar configurat.",
                        epic=epic, alias=alias, direction=direction, size=size,
                        atr_stop_reference=stop, stop_amount=result["stop_amount"],
                        trailing_stop=False, profit_level=target,
                        deal_reference=result["deal_reference"],
                    )
                except CapitalComDemoError as exc:
                    print(f"[{alias}] Blocat/eroare API: {exc}")
                except Exception as exc:
                    print(f"[{alias}] Eroare; nu se trimite alt ordin pe această lumânare: {exc}")
            baseline_needed = False
            runner_store.save_state(state, status="running")
            time.sleep(max(10, int(os.getenv("CAPITAL_DEMO_POLL_SECONDS", str(POLL_SECONDS)))))
    except KeyboardInterrupt:
        print("Runner oprit manual. Pozițiile deschise rămân gestionate de stop-loss/take-profit la broker.")
        return 0
    finally:
        if health_server is not None:
            health_server.shutdown()
            health_server.server_close()
        if trading_client is not None:
            trading_client.close()
        else:
            read_client.close()
        runner_store.close()


if __name__ == "__main__":
    raise SystemExit(run())