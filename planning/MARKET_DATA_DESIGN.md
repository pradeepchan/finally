# Market Data Backend — Implementation Design

This document is the authoritative implementation reference for the market data subsystem
(`backend/app/market/`). It covers every module, the design rationale behind each decision,
and concrete code examples for every public interface. All code shown here matches what is
actually in the repository.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Model: PriceUpdate](#2-data-model-priceupdate)
3. [Price Cache](#3-price-cache)
4. [Abstract Interface: MarketDataSource](#4-abstract-interface-marketdatasource)
5. [GBM Simulator](#5-gbm-simulator)
6. [Massive API Client](#6-massive-api-client)
7. [Factory: create_market_data_source](#7-factory-create_market_data_source)
8. [SSE Streaming Endpoint](#8-sse-streaming-endpoint)
9. [Public API and Integration](#9-public-api-and-integration)
10. [Testing Patterns](#10-testing-patterns)
11. [Adding a New Data Source](#11-adding-a-new-data-source)

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│  Market Data Subsystem  (backend/app/market/)                    │
│                                                                  │
│   ┌─────────────────────┐    ┌──────────────────────────────┐   │
│   │  SimulatorDataSource│    │      MassiveDataSource       │   │
│   │  (GBM, default)     │    │  (Polygon.io REST, optional) │   │
│   └──────────┬──────────┘    └──────────────┬───────────────┘   │
│              │  MarketDataSource (ABC)       │                   │
│              └──────────────┬───────────────┘                   │
│                             │  writes to                        │
│                             ▼                                   │
│                    ┌─────────────────┐                          │
│                    │   PriceCache    │  thread-safe, in-memory  │
│                    └────────┬────────┘                          │
│                             │  reads from                       │
│          ┌──────────────────┼──────────────────┐               │
│          ▼                  ▼                  ▼               │
│   GET /api/stream     Portfolio API       Trade execution       │
│      /prices          (/api/portfolio)    (/api/portfolio/trade)│
└──────────────────────────────────────────────────────────────────┘
```

### Design Principles

**Strategy pattern**: `SimulatorDataSource` and `MassiveDataSource` both implement
`MarketDataSource`. All downstream code—portfolio valuation, trade execution, SSE
streaming—interacts only with the cache and never imports from the simulator or Massive
client directly.

**PriceCache as single source of truth**: producers write to it, consumers read from it.
No direct coupling between data sources and API handlers.

**Single background task**: one asyncio task (simulator loop or Massive poller) runs
concurrently with FastAPI request handling. The task writes to the thread-safe cache;
FastAPI handlers read from it without any locks in the hot path.

**Cold-start safety**: the cache is empty until the first update. `SimulatorDataSource`
seeds the cache synchronously in `start()` before launching the background task, giving
sub-millisecond time-to-first-price. `MassiveDataSource` does an immediate first poll
in `start()` so data arrives within one HTTP round-trip (typically under a second).

---

## 2. Data Model: PriceUpdate

**File:** `backend/app/market/models.py`

`PriceUpdate` is an immutable, frozen dataclass. Every cache write creates a new
instance. This makes it safe to hand off to multiple readers without copying.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change. Rounded to 4 decimal places."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous price. Returns 0.0 if previous == 0."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### JSON Wire Format

The `to_dict()` output is what gets sent in each SSE event:

```json
{
  "ticker": "AAPL",
  "price": 190.50,
  "previous_price": 189.75,
  "timestamp": 1718300000.0,
  "change": 0.75,
  "change_percent": 0.3953,
  "direction": "up"
}
```

**Timestamp is Unix seconds** (not milliseconds). The Massive API returns milliseconds;
`MassiveDataSource` divides by 1000 before writing to the cache.

**First update semantics**: on the first `PriceCache.update()` for a ticker,
`previous_price` equals `price`, so `direction` is `"flat"` and `change` is `0.0`.

### Usage Examples

```python
from app.market.models import PriceUpdate

update = PriceUpdate(ticker="AAPL", price=191.00, previous_price=190.00)

print(update.change)         # 1.0
print(update.change_percent) # 0.5263
print(update.direction)      # "up"
print(update.to_dict())
# {"ticker": "AAPL", "price": 191.0, "previous_price": 190.0,
#  "timestamp": ..., "change": 1.0, "change_percent": 0.5263, "direction": "up"}

# Immutability: frozen=True raises AttributeError on assignment
update.price = 200.00  # AttributeError!
```

---

## 3. Price Cache

**File:** `backend/app/market/cache.py`

`PriceCache` is the in-memory store that decouples producers (data sources) from
consumers (API handlers). It is thread-safe via a `threading.Lock` and safe to call
from asyncio tasks, background threads, and FastAPI handlers simultaneously.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing counter

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Write a new price. Returns the created PriceUpdate.

        Thread-safe. If this is the first update for the ticker,
        previous_price == price (direction = 'flat').
        Prices are rounded to 2 decimal places on write.
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest PriceUpdate for a ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Monotonic counter. Increments on every update. Used for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Version Counter for SSE Change Detection

The `version` property enables efficient SSE polling without iterating the full price map:

```python
last_version = -1
while True:
    current_version = price_cache.version
    if current_version != last_version:
        last_version = current_version
        prices = price_cache.get_all()
        # Serialize and send SSE event
    await asyncio.sleep(0.5)
```

This is O(1) change detection. The SSE loop only serializes prices when something
actually changed—no spurious events on idle connections.

### Usage Examples

```python
cache = PriceCache()

# First update: direction is "flat"
update = cache.update("AAPL", 190.00)
assert update.direction == "flat"
assert update.previous_price == 190.00

# Second update: direction tracks the change
update = cache.update("AAPL", 191.50)
assert update.direction == "up"
assert update.change == 1.50

# Reading
price: float | None = cache.get_price("AAPL")   # 191.5
full: PriceUpdate | None = cache.get("AAPL")     # PriceUpdate(...)
all_prices: dict[str, PriceUpdate] = cache.get_all()

# Membership and length
assert "AAPL" in cache
assert len(cache) == 1

# Removal (e.g., user removes from watchlist)
cache.remove("AAPL")
assert cache.get("AAPL") is None

# Custom timestamp (used by MassiveDataSource with API-provided timestamps)
cache.update("MSFT", 420.00, timestamp=1718300000.0)

# Cold-start: always check for None before using
price = cache.get_price("TSLA")
if price is None:
    display = "--"
else:
    display = f"${price:.2f}"
```

### Thread Safety Notes

- `update()`, `get()`, `get_all()`, `remove()`, `__len__`, `__contains__` all acquire
  the lock for the duration of the operation.
- `version` does not acquire the lock (integer read is atomic in CPython). Safe to read
  in the SSE poll loop without blocking.
- `get_all()` returns a **shallow copy** of the internal dict. Callers own the returned
  dict but not the `PriceUpdate` values (which are immutable anyway).

---

## 4. Abstract Interface: MarketDataSource

**File:** `backend/app/market/interface.py`

All downstream code that interacts with a data source uses this interface only.

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])   # Starts background task
        # ... app runs, handling requests ...
        await source.add_ticker("PYPL")              # Dynamic addition
        await source.remove_ticker("NFLX")           # Dynamic removal
        # ... shutdown ...
        await source.stop()                          # Cancel background task
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates. Starts a background asyncio task.
        Must be called exactly once. Seeds the cache before returning.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task. Safe to call multiple times."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.
        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set and from the PriceCache.
        No-op if not present.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Type Annotation Pattern

Downstream code that accepts either implementation:

```python
from app.market import MarketDataSource, PriceCache

async def handle_watchlist_add(
    ticker: str,
    market_source: MarketDataSource,
    price_cache: PriceCache,
) -> dict:
    await market_source.add_ticker(ticker)
    price = price_cache.get_price(ticker)
    return {"ticker": ticker, "price": price}  # price may be None on cold start
```

---

## 5. GBM Simulator

**Files:** `backend/app/market/simulator.py`, `backend/app/market/seed_prices.py`

The simulator generates realistic-looking stock prices using Geometric Brownian Motion
with sector-correlated moves and occasional shock events. It runs entirely in-process
with no external dependencies.

### 5.1 The GBM Formula

```
S(t + dt) = S(t) * exp((mu - 0.5 * sigma²) * dt + sigma * sqrt(dt) * Z)
```

| Variable | Meaning |
|---|---|
| `S(t)` | Current price |
| `mu` | Annualized drift (expected return). E.g., `0.05` = 5% per year |
| `sigma` | Annualized volatility. E.g., `0.25` = 25% per year |
| `dt` | Time step as a fraction of a trading year |
| `Z` | Standard normal random variable (correlated across tickers) |

**Why GBM?**
- Prices can never go negative (multiplicative structure)
- Log-normal returns match real equity data
- Each ticker gets its own `mu` and `sigma` — TSLA can be wilder than JPM
- Correlated moves via Cholesky decomposition

**dt calculation:**

```python
# 252 trading days * 6.5 trading hours/day * 3600 seconds/hour = 5,896,800 seconds/year
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800

# For 500ms ticks:
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ≈ 8.48e-8
```

At this dt, each tick produces sub-cent moves. With `sigma=0.25`, a single 500ms tick
moves the price by roughly `0.025%` on average, which accumulates naturally to the
annualized volatility over a full trading year.

### 5.2 Seed Prices and Per-Ticker Parameters

```python
# backend/app/market/seed_prices.py

SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM":  195.00,
    "V":    280.00,
    "NFLX": 600.00,
}

TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},  # Moderate vol
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},  # Lowest vol
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},  # High vol, lower drift
    "NVDA":  {"sigma": 0.40, "mu": 0.08},  # High vol, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},  # Low vol (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},  # Lowest vol (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

# Dynamically added tickers not in the list get these defaults
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}
INTRA_TECH_CORR    = 0.6   # Tech stocks move together
INTRA_FINANCE_CORR = 0.5   # Finance stocks move together
CROSS_GROUP_CORR   = 0.3   # Between sectors / unknown tickers
TSLA_CORR          = 0.3   # TSLA does its own thing
```

Dynamically added tickers not in `SEED_PRICES` receive a random starting price in
`$50–$300` and `DEFAULT_PARAMS`.

### 5.3 Correlated Moves via Cholesky Decomposition

Real stocks in the same sector move together. The simulator builds a correlation matrix,
decomposes it with Cholesky, and applies it to independent normal draws each tick.

**Step-by-step:**

1. Build an `n×n` correlation matrix `C`. `C[i][j]` is the pairwise correlation between
   ticker `i` and ticker `j`, determined by sector group membership.
2. Compute `L = cholesky(C)` so that `L @ L.T == C`.
3. Each tick: draw `z_ind` (n independent standard normals), then `z_corr = L @ z_ind`.
4. Use `z_corr[i]` as the `Z` term for ticker `i` in the GBM formula.

The resulting `z_corr` vector has the exact pairwise correlations from `C` built in.

```python
def _rebuild_cholesky(self) -> None:
    n = len(self._tickers)
    if n <= 1:
        self._cholesky = None  # No correlation needed for a single ticker
        return

    corr = np.eye(n)  # Start with identity (self-correlation = 1.0)
    for i in range(n):
        for j in range(i + 1, n):
            rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
            corr[i, j] = rho
            corr[j, i] = rho  # Symmetric matrix

    self._cholesky = np.linalg.cholesky(corr)
    # L such that L @ L.T == corr

@staticmethod
def _pairwise_correlation(t1: str, t2: str) -> float:
    tech    = CORRELATION_GROUPS["tech"]
    finance = CORRELATION_GROUPS["finance"]

    if t1 == "TSLA" or t2 == "TSLA":
        return TSLA_CORR                           # 0.3

    if t1 in tech and t2 in tech:
        return INTRA_TECH_CORR                     # 0.6

    if t1 in finance and t2 in finance:
        return INTRA_FINANCE_CORR                  # 0.5

    return CROSS_GROUP_CORR                        # 0.3
```

**IMPORTANT:** Never set any correlation to `1.0`. A correlation of exactly 1.0 makes
the matrix singular, and `np.linalg.cholesky` raises `LinAlgError`.

The Cholesky matrix is rebuilt whenever tickers are added or removed. This is O(n²) but
n < 50 in practice, so it's negligible.

### 5.4 GBMSimulator: The Core Math Engine

`GBMSimulator` is a pure synchronous class with no asyncio dependency. It is designed
to be tested in isolation without any event loop.

```python
import math
import random
import numpy as np

from .seed_prices import (
    SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS,
    CORRELATION_GROUPS, CROSS_GROUP_CORR, INTRA_FINANCE_CORR,
    INTRA_TECH_CORR, TSLA_CORR,
)


class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers one dt. Returns {ticker: new_price}.

        This is the hot path — called every 500ms.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        # n independent standard normal draws
        z_independent = np.random.standard_normal(n)

        # Apply Cholesky to get correlated draws
        if self._cholesky is not None:
            z_correlated = self._cholesky @ z_independent
        else:
            z_correlated = z_independent  # Single ticker

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu    = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]

            # GBM: S(t+dt) = S(t) * exp((mu - sigma²/2)*dt + sigma*sqrt(dt)*Z)
            drift     = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random shock: ~0.1% chance per ticker per tick
            # Expected event rate: 0.001 * 10 tickers * 2 ticks/sec = 1 event/50s
            if random.random() < self._event_prob:
                magnitude = random.uniform(0.02, 0.05)         # 2-5% shock
                sign      = random.choice([-1, 1])
                self._prices[ticker] *= 1 + magnitude * sign

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add without rebuilding Cholesky (used in batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))
```

### 5.5 SimulatorDataSource: asyncio Integration

`SimulatorDataSource` wraps `GBMSimulator` in an asyncio task that calls `step()` every
500ms and writes results to the `PriceCache`.

```python
import asyncio
import logging

from .cache import PriceCache
from .interface import MarketDataSource
from .simulator import GBMSimulator

logger = logging.getLogger(__name__)


class SimulatorDataSource(MarketDataSource):
    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,       # seconds between steps
        event_probability: float = 0.001,    # shock probability per tick per ticker
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)

        # Seed cache before launching the loop — no cold-start delay
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            # Seed cache immediately — ticker has a price right away
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
                # Never crash the loop — GBM errors are programming bugs,
                # but we guard here as a catch-all.
            await asyncio.sleep(self._interval)
```

### 5.6 Shock Events

```python
# Expected event frequency calculation:
# probability = 0.001 per ticker per tick
# 10 tickers * 2 ticks/second = 20 opportunities/second
# Expected events/second = 20 * 0.001 = 0.02
# Expected seconds between events = 1 / 0.02 = 50 seconds

if random.random() < self._event_prob:
    magnitude = random.uniform(0.02, 0.05)  # 2-5% move
    sign = random.choice([-1, 1])           # up or down
    self._prices[ticker] *= 1 + magnitude * sign
```

Events produce sudden 2-5% moves on a single ticker, simulating earnings surprises or
news. With 10 tickers at default settings, expect one event roughly every 50 seconds
across the watchlist — visually dramatic without being chaotic.

### 5.7 Tuning Guide

| Goal | Parameter |
|---|---|
| Faster / more volatile prices | Increase `sigma` in `TICKER_PARAMS` |
| Stronger upward/downward trend | Increase/decrease `mu` |
| More frequent shock events | Increase `event_probability` (e.g., `0.005`) |
| Slower tick rate | Increase `update_interval` in `SimulatorDataSource` (e.g., `1.0`) |
| Stronger sector correlation | Increase `INTRA_TECH_CORR` (keep strictly < 1.0) |
| More independent movement | Decrease `INTRA_TECH_CORR` toward `CROSS_GROUP_CORR` |

**Adding a new default ticker:**

1. Add to `SEED_PRICES` in `seed_prices.py`
2. Add to `TICKER_PARAMS` (or omit to use `DEFAULT_PARAMS`)
3. Optionally add to a `CORRELATION_GROUPS` set
4. Add to the default watchlist seed data in `backend/schema/`

---

## 6. Massive API Client

**File:** `backend/app/market/massive_client.py`

`MassiveDataSource` polls the Massive (formerly Polygon.io) REST API for real market
data. It uses `asyncio.to_thread()` to run the synchronous `RESTClient` without
blocking the event loop.

### 6.1 Rate Limits and Poll Interval

| Tier | Requests/min | Data delay | Recommended interval |
|---|---|---|---|
| Free (Starter) | 5 | 15 min | 15 s (default) |
| Advanced | Unlimited | Real-time | 2–5 s |
| Business | Unlimited | Real-time | 2–5 s |

The default `poll_interval=15.0` fits the free tier. Set a lower interval if a paid key
is configured:

```python
# In factory.py or via environment variable:
source = MassiveDataSource(
    api_key=api_key,
    price_cache=cache,
    poll_interval=5.0,   # 5s for paid tiers
)
```

### 6.2 Full Implementation

```python
from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers), self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            # Ticker will appear on the next poll cycle (up to poll_interval away)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Fetch snapshots for all tickers and write to cache."""
        if not self._tickers or not self._client:
            return
        try:
            # RESTClient is synchronous — run in a thread pool to avoid
            # blocking the asyncio event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price     = snap.last_trade.price
                    # Massive returns Unix milliseconds; cache expects seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(
                        ticker=snap.ticker,
                        price=price,
                        timestamp=timestamp,
                    )
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning(
                        "Skipping snapshot for %s: %s",
                        getattr(snap, "ticker", "???"), e,
                    )
            logger.debug(
                "Massive poll: updated %d/%d tickers", processed, len(self._tickers)
            )
        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — loop retries on next interval.
            # 401 = bad key, 429 = rate limit exceeded, network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous REST call. Runs in asyncio.to_thread()."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### 6.3 Snapshot Response Fields

The `get_snapshot_all()` call returns objects with these key attributes:

| Attribute | Type | Notes |
|---|---|---|
| `snap.ticker` | str | Ticker symbol |
| `snap.last_trade.price` | float | Last trade price (the "current" price) |
| `snap.last_trade.timestamp` | int | Unix **milliseconds** — divide by 1000 |
| `snap.last_trade.size` | int | Last trade size in shares |
| `snap.todays_change_perc` | float | % change from prior close |
| `snap.day.open` | float | Today's open |
| `snap.day.high` | float | Today's high |
| `snap.day.low` | float | Today's low |
| `snap.prev_day.close` | float | Previous day's close |

We only use `last_trade.price` and `last_trade.timestamp`. The daily change percentage
from the API could be used if exposing real "today's change %" is desired (for the
simulator, change % is calculated from the seed price baseline).

### 6.4 Error Handling

Common failures and what happens:

| Error | Cause | Behavior |
|---|---|---|
| `401 Unauthorized` | Bad or missing API key | Logged as error, loop retries |
| `403 Forbidden` | Endpoint requires higher tier | Logged as error, loop retries |
| `429 Too Many Requests` | Rate limit exceeded | Logged as error, loop backs off naturally (next poll at `poll_interval`) |
| Network error | Connectivity issue | Logged as error, loop retries |
| `AttributeError` on snap | Ticker had no last trade data | Logged as warning, ticker skipped, rest of batch processed |

The design intentionally does **not** crash the process on API errors. The cache simply
goes stale until the next successful poll. The frontend displays the last known price.

### 6.5 Adding a Ticker with Massive

New tickers added via `add_ticker()` will appear in the next poll cycle:

```python
await market_source.add_ticker("PYPL")
# Cache for PYPL is empty until next poll (up to 15s on free tier)
# Frontend shows "--" during this window, then updates automatically
price = price_cache.get_price("PYPL")  # None until next poll
```

This differs from the simulator, which seeds the cache immediately in `add_ticker()`.

---

## 7. Factory: create_market_data_source

**File:** `backend/app/market/factory.py`

A single function selects the implementation based on `MASSIVE_API_KEY` in the
environment. No other code needs to know which implementation is active.

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select and return the appropriate market data source.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise                         → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

### Configuration via Environment Variables

```bash
# .env (project root — gitignored)

# Use the GBM simulator (no key needed):
MASSIVE_API_KEY=

# Use real Massive API data:
MASSIVE_API_KEY=your_api_key_here
```

The factory reads the environment at call time, not at import time. This means tests can
set `os.environ["MASSIVE_API_KEY"]` before calling `create_market_data_source()` and
get the expected implementation.

```python
import os

# Test: force simulator even if MASSIVE_API_KEY is set in the environment
os.environ.pop("MASSIVE_API_KEY", None)
source = create_market_data_source(cache)
assert isinstance(source, SimulatorDataSource)

# Test: force Massive client
os.environ["MASSIVE_API_KEY"] = "test-key"
source = create_market_data_source(cache)
assert isinstance(source, MassiveDataSource)
```

---

## 8. SSE Streaming Endpoint

**File:** `backend/app/market/stream.py`

The SSE endpoint pushes all ticker prices to connected clients every ~500ms. It uses
a factory function pattern to inject the `PriceCache` without module-level globals.

### 8.1 Implementation

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with an injected PriceCache reference."""

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint: GET /api/stream/prices (Content-Type: text/event-stream)

        Pushes a full price snapshot every ~500ms when anything in the cache
        has changed. Clients use the browser EventSource API.
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Prevent nginx from buffering events
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted price events until the client disconnects."""

    # Tell the browser to reconnect after 1 second if the connection drops.
    # EventSource handles reconnection automatically.
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    payload = json.dumps(data)
                    yield f"data: {payload}\n\n"  # SSE requires \n\n to end an event

            await asyncio.sleep(interval)

    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### 8.2 Wire Format

Each event is a single JSON object mapping ticker symbols to their full `PriceUpdate`
dict. The client receives the entire watchlist in every event — no delta logic needed.

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":189.75,"timestamp":1718300000.0,"change":0.75,"change_percent":0.3953,"direction":"up"},"MSFT":{...},...}

data: {"AAPL":{"ticker":"AAPL","price":190.52,...},"MSFT":{...},...}
```

SSE text format rules:
- Each event ends with `\n\n` (two newlines)
- `data:` field contains the payload
- `retry:` sets the browser reconnect delay in milliseconds

### 8.3 Frontend EventSource Usage

```typescript
// Connect (browser / Next.js client component)
const es = new EventSource("/api/stream/prices");

es.onmessage = (event) => {
  const prices: Record<string, PriceUpdate> = JSON.parse(event.data);
  // prices["AAPL"].price, prices["AAPL"].direction, etc.
};

es.onerror = () => {
  // EventSource automatically reconnects after the retry delay (1000ms)
  // No manual reconnect logic needed
};

// Cleanup
es.close();
```

### 8.4 Change Detection Design

The SSE generator tracks the cache's `version` counter. Events are only emitted when
the version changes, meaning no CPU is wasted serializing unchanged data:

```python
current_version = price_cache.version
if current_version != last_version:
    last_version = current_version
    # Only now do we serialize (one JSON.dumps per 500ms tick)
    data = {ticker: update.to_dict() for ticker, update in price_cache.get_all().items()}
    payload = json.dumps(data)
    yield f"data: {payload}\n\n"
```

On an idle connection (no new prices), the loop spins at 2 Hz but does nothing except
check the version counter and call `request.is_disconnected()`.

### 8.5 Disconnect Detection

```python
if await request.is_disconnected():
    break
```

FastAPI's `request.is_disconnected()` detects TCP connection closure. When the browser
tab is closed, navigates away, or calls `es.close()`, this returns `True` and the
generator stops, freeing the connection.

### 8.6 Headers Explained

| Header | Value | Purpose |
|---|---|---|
| `Cache-Control` | `no-cache` | Prevent proxies from caching the stream |
| `Connection` | `keep-alive` | Keep the TCP connection open |
| `X-Accel-Buffering` | `no` | Tell nginx not to buffer the response body |

The `X-Accel-Buffering` header is critical when nginx proxies the container. Without it,
nginx buffers the response and clients receive events in bursts rather than in real-time.

---

## 9. Public API and Integration

### 9.1 Public API (`__init__.py`)

```python
# backend/app/market/__init__.py
from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

All imports from downstream code use the package-level names:

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource
from app.market import create_market_data_source, create_stream_router
```

### 9.2 FastAPI App Integration

The market data subsystem integrates into FastAPI via the lifespan context manager:

```python
# backend/app/main.py
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.market import PriceCache, create_market_data_source, create_stream_router

DEFAULT_TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
                   "NVDA", "META", "JPM", "V", "NFLX"]

# Module-level state (accessed by request handlers via dependency injection or globals)
price_cache = PriceCache()
market_source = create_market_data_source(price_cache)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await market_source.start(DEFAULT_TICKERS)
    yield
    # Shutdown
    await market_source.stop()


app = FastAPI(lifespan=lifespan)

# Register SSE router
app.include_router(create_stream_router(price_cache))

# Mount Next.js static export
app.mount("/", StaticFiles(directory="static", html=True), name="static")
```

### 9.3 Watchlist API Integration

When the user adds or removes a ticker, the watchlist handler calls the market source:

```python
# backend/app/routers/watchlist.py
from fastapi import APIRouter
from app.market import PriceCache, MarketDataSource

router = APIRouter(prefix="/api/watchlist")


@router.post("")
async def add_to_watchlist(
    body: dict,
    market_source: MarketDataSource,   # injected via FastAPI dependency
    price_cache: PriceCache,
):
    ticker = body["ticker"].upper().strip()
    # ... database insert ...
    await market_source.add_ticker(ticker)
    # Price may be None for a few hundred milliseconds (simulator) or up to
    # poll_interval seconds (Massive). Frontend handles None gracefully.
    price = price_cache.get_price(ticker)
    return {"ticker": ticker, "price": price}


@router.delete("/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    market_source: MarketDataSource,
):
    ticker = ticker.upper().strip()
    # ... database delete ...
    await market_source.remove_ticker(ticker)
    # Cache entry is removed immediately by remove_ticker()
    return {"ticker": ticker, "removed": True}
```

### 9.4 Portfolio and Trade Execution

Portfolio valuation reads from the cache to calculate current market values:

```python
# backend/app/routers/portfolio.py
from app.market import PriceCache

def calculate_portfolio_value(
    positions: list[dict],
    cash_balance: float,
    price_cache: PriceCache,
) -> dict:
    total_market_value = 0.0
    for pos in positions:
        ticker = pos["ticker"]
        quantity = pos["quantity"]
        avg_cost = pos["avg_cost"]

        # Always handle None — cache may not have this ticker yet
        current_price = price_cache.get_price(ticker)
        if current_price is None:
            market_value = None
            unrealized_pnl = None
        else:
            market_value = current_price * quantity
            unrealized_pnl = (current_price - avg_cost) * quantity
            total_market_value += market_value

        pos["current_price"] = current_price
        pos["market_value"] = market_value
        pos["unrealized_pnl"] = unrealized_pnl

    return {
        "positions": positions,
        "cash_balance": cash_balance,
        "total_value": cash_balance + total_market_value,
    }
```

Trade execution uses the cache for the fill price:

```python
async def execute_trade(
    ticker: str,
    side: str,
    quantity: float,
    price_cache: PriceCache,
    db,
) -> dict:
    current_price = price_cache.get_price(ticker)
    if current_price is None:
        raise ValueError(f"No price available for {ticker} — cannot execute trade")

    fill_price = current_price  # Market order: fill at current price
    # ... update positions, cash balance, record trade in DB ...
```

### 9.5 Watchlist API Response Shape

The `GET /api/watchlist` response returns `null` for price fields during cold start:

```python
# What the watchlist endpoint returns:
[
  {"ticker": "AAPL",  "price": 190.50, "change_percent": 0.39, "direction": "up"},
  {"ticker": "GOOGL", "price": null,   "change_percent": null,  "direction": null},
  # ^ null during cold start (cache not yet populated for this ticker)
]
```

The frontend must handle `null` prices and display `--` or a loading skeleton.

---

## 10. Testing Patterns

### 10.1 Unit Tests for PriceUpdate (test_models.py)

```python
# backend/tests/market/test_models.py
from app.market.models import PriceUpdate


class TestPriceUpdate:
    def test_direction_up(self):
        u = PriceUpdate(ticker="AAPL", price=191.0, previous_price=190.0, timestamp=0.0)
        assert u.direction == "up"
        assert u.change == 1.0
        assert u.change_percent == pytest.approx(0.5263, rel=1e-3)

    def test_direction_flat_on_first_update(self):
        # Simulates what PriceCache does for the first update
        u = PriceUpdate(ticker="AAPL", price=190.0, previous_price=190.0, timestamp=0.0)
        assert u.direction == "flat"
        assert u.change == 0.0

    def test_immutability(self):
        u = PriceUpdate(ticker="AAPL", price=190.0, previous_price=189.0, timestamp=0.0)
        with pytest.raises(AttributeError):
            u.price = 200.0

    def test_to_dict_shape(self):
        u = PriceUpdate(ticker="AAPL", price=190.5, previous_price=190.0, timestamp=1234.0)
        d = u.to_dict()
        assert set(d.keys()) == {
            "ticker", "price", "previous_price", "timestamp",
            "change", "change_percent", "direction"
        }
```

### 10.2 Unit Tests for PriceCache (test_cache.py)

```python
# backend/tests/market/test_cache.py
from app.market.cache import PriceCache


class TestPriceCache:
    def test_first_update_is_flat(self):
        cache = PriceCache()
        update = cache.update("AAPL", 190.0)
        assert update.direction == "flat"
        assert update.previous_price == 190.0

    def test_version_increments_on_each_write(self):
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.0)
        assert cache.version == v0 + 1
        cache.update("AAPL", 191.0)
        assert cache.version == v0 + 2

    def test_remove_clears_ticker(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        cache.remove("AAPL")
        assert cache.get("AAPL") is None
        assert "AAPL" not in cache

    def test_cold_start_returns_none(self):
        cache = PriceCache()
        assert cache.get_price("ANYTHING") is None

    def test_prices_rounded_to_two_decimals(self):
        cache = PriceCache()
        update = cache.update("AAPL", 190.12345)
        assert update.price == 190.12

    def test_custom_timestamp(self):
        cache = PriceCache()
        ts = 1718300000.0
        update = cache.update("AAPL", 190.0, timestamp=ts)
        assert update.timestamp == ts
```

### 10.3 Unit Tests for GBMSimulator (test_simulator.py)

```python
# backend/tests/market/test_simulator.py
import numpy as np
from app.market.simulator import GBMSimulator
from app.market.seed_prices import SEED_PRICES


class TestGBMSimulator:
    def test_prices_stay_positive_over_1000_steps(self):
        sim = GBMSimulator(["AAPL", "GOOGL", "TSLA"], event_probability=0.0)
        for _ in range(1000):
            prices = sim.step()
            assert all(p > 0 for p in prices.values())

    def test_starts_near_seed_price(self):
        sim = GBMSimulator(["AAPL"], event_probability=0.0)
        prices = sim.step()
        # First step is very close to seed (tiny dt)
        assert abs(prices["AAPL"] - SEED_PRICES["AAPL"]) < 5.0

    def test_dynamic_ticker_add(self):
        sim = GBMSimulator(["AAPL"])
        sim.add_ticker("PYPL")
        prices = sim.step()
        assert "PYPL" in prices
        assert prices["PYPL"] > 0
        assert sim.get_price("PYPL") is not None

    def test_dynamic_ticker_remove(self):
        sim = GBMSimulator(["AAPL", "MSFT"])
        sim.remove_ticker("MSFT")
        prices = sim.step()
        assert "MSFT" not in prices
        assert sim.get_price("MSFT") is None

    def test_cholesky_valid_for_full_default_watchlist(self):
        # If the correlation matrix were invalid, cholesky would raise LinAlgError.
        tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "NFLX"]
        sim = GBMSimulator(tickers)
        assert sim._cholesky is not None

    def test_unknown_ticker_gets_default_params(self):
        sim = GBMSimulator(["AAPL"])
        sim.add_ticker("UNKNWN")
        prices = sim.step()
        assert "UNKNWN" in prices
        assert 50.0 <= prices["UNKNWN"] <= 1000.0  # within reasonable seed range

    def test_single_ticker_no_cholesky(self):
        sim = GBMSimulator(["AAPL"])
        assert sim._cholesky is None  # No correlation needed for 1 ticker
        prices = sim.step()
        assert "AAPL" in prices
```

### 10.4 Integration Tests for SimulatorDataSource (test_simulator_source.py)

```python
# backend/tests/market/test_simulator_source.py
import pytest
from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource


@pytest.mark.asyncio
async def test_start_seeds_cache_immediately():
    """Cache should have data immediately after start(), before any sleep."""
    cache = PriceCache()
    source = SimulatorDataSource(cache)
    await source.start(["AAPL"])
    # No sleep here — data should be available right now
    assert cache.get_price("AAPL") is not None
    await source.stop()


@pytest.mark.asyncio
async def test_add_ticker_seeds_cache_immediately():
    """Adding a ticker should give it a price right away."""
    cache = PriceCache()
    source = SimulatorDataSource(cache)
    await source.start(["AAPL"])
    await source.add_ticker("PYPL")
    assert cache.get_price("PYPL") is not None
    await source.stop()


@pytest.mark.asyncio
async def test_remove_ticker_clears_cache():
    cache = PriceCache()
    source = SimulatorDataSource(cache)
    await source.start(["AAPL", "MSFT"])
    await source.remove_ticker("MSFT")
    assert cache.get("MSFT") is None
    assert "MSFT" not in source.get_tickers()
    await source.stop()


@pytest.mark.asyncio
async def test_stop_cancels_background_task():
    import asyncio
    cache = PriceCache()
    source = SimulatorDataSource(cache)
    await source.start(["AAPL"])
    await source.stop()
    # After stop, the version should not increment
    v = cache.version
    await asyncio.sleep(0.6)  # Wait more than one tick
    assert cache.version == v  # No more updates
```

### 10.5 Factory Tests (test_factory.py)

```python
# backend/tests/market/test_factory.py
import os
import pytest
from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


class TestCreateMarketDataSource:
    def test_returns_simulator_when_no_key(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        source = create_market_data_source(PriceCache())
        assert isinstance(source, SimulatorDataSource)

    def test_returns_simulator_when_key_is_empty(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "")
        source = create_market_data_source(PriceCache())
        assert isinstance(source, SimulatorDataSource)

    def test_returns_massive_when_key_is_set(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "test-api-key-123")
        source = create_market_data_source(PriceCache())
        assert isinstance(source, MassiveDataSource)

    def test_strips_whitespace_from_key(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "  ")  # whitespace only
        source = create_market_data_source(PriceCache())
        assert isinstance(source, SimulatorDataSource)
```

### 10.6 Massive Client Tests (test_massive.py)

Tests mock the `RESTClient` to avoid real API calls:

```python
# backend/tests/market/test_massive.py
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource


def make_mock_snapshot(ticker: str, price: float, timestamp_ms: int) -> MagicMock:
    """Build a fake snapshot object matching the Massive API shape."""
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade = MagicMock()
    snap.last_trade.price = price
    snap.last_trade.timestamp = timestamp_ms  # milliseconds
    return snap


@pytest.mark.asyncio
async def test_start_does_immediate_poll():
    cache = PriceCache()
    source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)

    mock_snap = make_mock_snapshot("AAPL", 190.50, 1718300000000)

    with patch("app.market.massive_client.RESTClient") as MockClient:
        source._client = MockClient()
        source._client.get_snapshot_all.return_value = [mock_snap]

        # Simulate what start() does (but without launching the background task)
        await source._poll_once()

    assert cache.get_price("AAPL") == 190.50
    # Timestamp should be in seconds
    assert cache.get("AAPL").timestamp == pytest.approx(1718300000.0, rel=1e-3)


@pytest.mark.asyncio
async def test_snapshot_missing_last_trade_is_skipped():
    """Snapshots with no last_trade data should be skipped without crashing."""
    cache = PriceCache()
    source = MassiveDataSource(api_key="test-key", price_cache=cache)

    bad_snap = MagicMock()
    bad_snap.ticker = "BROKEN"
    bad_snap.last_trade = None  # No trade data

    with patch("app.market.massive_client.RESTClient") as MockClient:
        source._client = MockClient()
        source._client.get_snapshot_all.return_value = [bad_snap]
        await source._poll_once()

    assert cache.get("BROKEN") is None  # Skipped, not crashed


@pytest.mark.asyncio
async def test_poll_error_does_not_crash_loop():
    """An exception from the REST API should be caught, not re-raised."""
    cache = PriceCache()
    source = MassiveDataSource(api_key="test-key", price_cache=cache)

    with patch("app.market.massive_client.RESTClient") as MockClient:
        source._client = MockClient()
        source._client.get_snapshot_all.side_effect = Exception("401 Unauthorized")
        await source._poll_once()  # Should not raise

    assert len(cache) == 0  # Nothing written, but process alive


@pytest.mark.asyncio
async def test_remove_ticker_clears_cache():
    cache = PriceCache()
    source = MassiveDataSource(api_key="test-key", price_cache=cache)
    source._tickers = ["AAPL", "MSFT"]
    cache.update("AAPL", 190.0)

    await source.remove_ticker("AAPL")

    assert "AAPL" not in source._tickers
    assert cache.get("AAPL") is None
```

### 10.7 Running the Tests

```bash
cd backend
uv sync --extra dev

# Run all market data tests
uv run --extra dev pytest tests/market/ -v

# Run with coverage
uv run --extra dev pytest tests/market/ --cov=app/market --cov-report=term-missing

# Run a specific test module
uv run --extra dev pytest tests/market/test_simulator.py -v

# Run linting
uv run --extra dev ruff check app/ tests/
```

Expected output: **73 tests, all passing**, 84% overall coverage.

---

## 11. Adding a New Data Source

To add a third market data provider (e.g., Alpaca, IEX Cloud, Yahoo Finance):

**Step 1:** Create `backend/app/market/my_source.py`:

```python
# backend/app/market/my_source.py
from __future__ import annotations

import asyncio
import logging

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MyDataSource(MarketDataSource):
    def __init__(self, api_key: str, price_cache: PriceCache) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._tickers = list(tickers)
        await self._fetch_and_write()  # Immediate first fetch
        self._task = asyncio.create_task(self._poll_loop(), name="my-source-poller")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def add_ticker(self, ticker: str) -> None:
        if ticker not in self._tickers:
            self._tickers.append(ticker.upper().strip())

    async def remove_ticker(self, ticker: str) -> None:
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(10.0)
            await self._fetch_and_write()

    async def _fetch_and_write(self) -> None:
        try:
            # Your API call here. Use asyncio.to_thread() if the client is synchronous.
            data = await asyncio.to_thread(self._call_api)
            for ticker, price, timestamp in data:
                self._cache.update(ticker=ticker, price=price, timestamp=timestamp)
        except Exception:
            logger.exception("MyDataSource poll failed")

    def _call_api(self) -> list[tuple[str, float, float]]:
        # Synchronous API call — returns [(ticker, price, unix_seconds), ...]
        ...
```

**Step 2:** Update `factory.py`:

```python
# backend/app/market/factory.py
import os
from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .my_source import MyDataSource
from .simulator import SimulatorDataSource


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    massive_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    my_key      = os.environ.get("MY_API_KEY", "").strip()

    if massive_key:
        return MassiveDataSource(api_key=massive_key, price_cache=price_cache)
    elif my_key:
        return MyDataSource(api_key=my_key, price_cache=price_cache)
    else:
        return SimulatorDataSource(price_cache=price_cache)
```

**Step 3:** Add tests to `backend/tests/market/test_my_source.py` following the
patterns in [Section 10.6](#106-massive-client-tests-test_massivepy).

**No other files need to change.** The SSE endpoint, portfolio valuation, trade
execution, and watchlist handlers all interact only with `PriceCache` and
`MarketDataSource`—they are completely unaware of which implementation is running.

---

## Module Map (Quick Reference)

```
backend/app/market/
├── __init__.py          Public API re-exports (5 names)
├── models.py            PriceUpdate — immutable frozen dataclass
├── cache.py             PriceCache — thread-safe, version-tracked store
├── interface.py         MarketDataSource — abstract base class (5 methods)
├── seed_prices.py       Seed prices, GBM params, correlation constants
├── simulator.py         GBMSimulator + SimulatorDataSource
├── massive_client.py    MassiveDataSource — Polygon.io/Massive REST poller
├── factory.py           create_market_data_source() — env-driven selection
└── stream.py            create_stream_router() — FastAPI SSE endpoint

backend/tests/market/
├── test_models.py           11 tests — PriceUpdate (100% coverage)
├── test_cache.py            13 tests — PriceCache (100% coverage)
├── test_simulator.py        17 tests — GBMSimulator (98% coverage)
├── test_simulator_source.py 10 tests — SimulatorDataSource (integration)
├── test_factory.py           7 tests — create_market_data_source (100% coverage)
└── test_massive.py          13 tests — MassiveDataSource (56% — API mocked)

backend/market_data_demo.py  Rich terminal dashboard demo (uv run market_data_demo.py)
```
