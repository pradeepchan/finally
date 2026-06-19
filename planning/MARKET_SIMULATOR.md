# Market Simulator — Design and Code Structure

The simulator is the default market data source for FinAlly. It runs entirely in-process with no external dependencies, generates realistic-looking price movements using Geometric Brownian Motion (GBM), and produces correlated moves across related stocks.

**Status: Implemented.** Code lives in `backend/app/market/simulator.py` and `backend/app/market/seed_prices.py`.

---

## Why GBM?

Geometric Brownian Motion is the standard mathematical model for stock prices in the Black-Scholes framework. Key properties that make it appropriate here:

- **Prices stay positive** — the multiplicative structure (`S * exp(...)`) means prices can never go negative.
- **Log-normal returns** — daily returns have a roughly log-normal distribution, matching real equity data.
- **Parameterizable** — each ticker gets its own drift (`mu`) and volatility (`sigma`), so NVDA can be wilder than JPM.
- **Correlated moves** — via Cholesky decomposition, tech stocks can be made to rise and fall together.
- **Simple and fast** — one `numpy` array operation per tick for the full watchlist.

---

## GBM Formula

```
S(t + dt) = S(t) * exp((mu - 0.5 * sigma²) * dt + sigma * sqrt(dt) * Z)
```

Where:
- `S(t)` — current price
- `mu` — annualized drift (expected return, e.g. 0.05 = 5% per year)
- `sigma` — annualized volatility (e.g. 0.25 = 25% per year)
- `dt` — time step as a fraction of a trading year
- `Z` — correlated standard normal random variable

**dt calculation:**

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # = 5,896,800 seconds
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ≈ 8.48e-8  (for 500ms ticks)
```

At this dt, each tick produces sub-cent price moves that accumulate naturally. The drift and volatility scale correctly to annual figures, so `sigma=0.25` (25% annual vol) produces about 0.025% moves per 500ms tick on average.

---

## Correlated Moves via Cholesky Decomposition

Real stocks in the same sector move together. The simulator models this by generating **correlated** random variables rather than independent ones.

### Approach

1. Build an `n×n` correlation matrix `C` for all tracked tickers.
2. Compute the Cholesky factor: `L = cholesky(C)`, where `L @ L.T = C`.
3. Each tick: draw `n` independent standard normals `z_ind`, then `z_corr = L @ z_ind`.
4. Use `z_corr[i]` as the `Z` for ticker `i` in the GBM formula.

The resulting `z_corr` vector has the target correlations built in.

### Correlation Structure

```python
# backend/app/market/seed_prices.py
CORRELATION_GROUPS = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

INTRA_TECH_CORR    = 0.6   # Tech stocks move together
INTRA_FINANCE_CORR = 0.5   # Finance stocks move together
CROSS_GROUP_CORR   = 0.3   # Between sectors / for unknown tickers
TSLA_CORR          = 0.3   # TSLA classified as tech but behaves independently
```

**Pairwise correlation rules:**

| Ticker pair | Correlation |
|---|---|
| Two tech stocks (not TSLA) | 0.6 |
| Two finance stocks | 0.5 |
| TSLA with anything | 0.3 |
| Cross-sector pairs | 0.3 |
| Unknown ticker with anything | 0.3 |

### Matrix rebuild

The Cholesky matrix is rebuilt whenever tickers are added or removed. This is `O(n²)` but `n < 50` in practice, so it's negligible.

```python
def _rebuild_cholesky(self) -> None:
    n = len(self._tickers)
    if n <= 1:
        self._cholesky = None
        return
    corr = np.eye(n)
    for i in range(n):
        for j in range(i + 1, n):
            rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
            corr[i, j] = rho
            corr[j, i] = rho
    self._cholesky = np.linalg.cholesky(corr)
```

---

## Random Shock Events

To add visual drama, each tick has a small probability of triggering a sudden 2-5% move on any ticker. This simulates earnings surprises, news events, etc.

```python
EVENT_PROBABILITY = 0.001   # 0.1% chance per ticker per tick

if random.random() < self._event_prob:
    shock_magnitude = random.uniform(0.02, 0.05)   # 2-5%
    shock_sign = random.choice([-1, 1])
    self._prices[ticker] *= 1 + shock_magnitude * shock_sign
```

With 10 tickers at 2 ticks/second: `0.001 × 10 × 2 = 0.02` events/second → an event roughly every 50 seconds across the watchlist. Visually dramatic without being chaotic.

---

## Seed Prices and Per-Ticker Parameters

```python
# backend/app/market/seed_prices.py

SEED_PRICES = {
    "AAPL":  190.00,
    "GOOGL": 175.00,
    "MSFT":  420.00,
    "AMZN":  185.00,
    "TSLA":  250.00,
    "NVDA":  800.00,
    "META":  500.00,
    "JPM":   195.00,
    "V":     280.00,
    "NFLX":  600.00,
}

TICKER_PARAMS = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},  # Moderate vol
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},  # Low vol
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},  # High vol, lower drift
    "NVDA":  {"sigma": 0.40, "mu": 0.08},  # High vol, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},  # Low vol (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},  # Lowest vol (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},

    # Default for dynamically-added tickers not in the list:
    # DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}
}
```

**Dynamically added tickers** (not in `SEED_PRICES`) receive a random starting price in the `$50–$300` range and use `DEFAULT_PARAMS`.

---

## Class: `GBMSimulator`

The pure simulation math, with no asyncio dependency. Designed to be testable in isolation.

```python
class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ≈ 8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None: ...
```

**Public methods:**

| Method | Description |
|---|---|
| `step() -> dict[str, float]` | Advance all tickers by one dt. Returns `{ticker: new_price}`. |
| `add_ticker(ticker: str)` | Add a ticker. Rebuilds correlation matrix. |
| `remove_ticker(ticker: str)` | Remove a ticker. Rebuilds correlation matrix. |
| `get_price(ticker: str) -> float \| None` | Current price for a ticker. |
| `get_tickers() -> list[str]` | Currently tracked tickers. |

**`step()` is the hot path — called every 500ms:**

```python
def step(self) -> dict[str, float]:
    n = len(self._tickers)
    z_independent = np.random.standard_normal(n)

    if self._cholesky is not None:
        z_correlated = self._cholesky @ z_independent
    else:
        z_correlated = z_independent  # Single ticker: no correlation needed

    result = {}
    for i, ticker in enumerate(self._tickers):
        mu, sigma = self._params[ticker]["mu"], self._params[ticker]["sigma"]
        drift    = (mu - 0.5 * sigma**2) * self._dt
        diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
        self._prices[ticker] *= math.exp(drift + diffusion)

        # Optional shock event
        if random.random() < self._event_prob:
            self._prices[ticker] *= 1 + random.uniform(0.02, 0.05) * random.choice([-1, 1])

        result[ticker] = round(self._prices[ticker], 2)
    return result
```

---

## Class: `SimulatorDataSource`

The asyncio wrapper that integrates `GBMSimulator` with the `MarketDataSource` interface and `PriceCache`.

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None: ...
```

**Lifecycle:**

```
start(tickers)
   └─ creates GBMSimulator
   └─ seeds cache with initial prices (no cold-start delay)
   └─ creates asyncio.Task(_run_loop)

_run_loop()  [runs forever until stop()]
   └─ sim.step()            → dict[ticker, price]
   └─ cache.update(...)     for each ticker
   └─ asyncio.sleep(0.5)

add_ticker(ticker)
   └─ sim.add_ticker(ticker) → rebuilds Cholesky
   └─ cache.update(ticker, sim.get_price(ticker))  ← immediate

remove_ticker(ticker)
   └─ sim.remove_ticker(ticker) → rebuilds Cholesky
   └─ cache.remove(ticker)

stop()
   └─ task.cancel()
   └─ await task (catches CancelledError)
```

**Key behavior:** `start()` seeds the cache _before_ launching the background loop. This means the SSE endpoint has data within microseconds of the first client connecting, not 500ms later.

---

## Error Handling

The `_run_loop` catches all exceptions from `sim.step()` and logs them without crashing:

```python
async def _run_loop(self) -> None:
    while True:
        try:
            if self._sim:
                prices = self._sim.step()
                for ticker, price in prices.items():
                    self._cache.update(ticker=ticker, price=price)
        except Exception:
            logger.exception("Simulator step failed")
        await asyncio.sleep(self._interval)
```

In practice, GBM math cannot raise (numpy errors would be programming bugs), but this guard protects the event loop from any unexpected failure.

---

## Testing

Tests are in `backend/tests/market/`. The simulator is the best-tested module in the codebase.

### Unit tests for `GBMSimulator` (`test_simulator.py`, 17 tests)

Key scenarios:

```python
def test_prices_stay_positive():
    sim = GBMSimulator(["AAPL", "GOOGL", "TSLA"], event_probability=0.0)
    for _ in range(1000):
        prices = sim.step()
        assert all(p > 0 for p in prices.values())

def test_prices_start_from_seed():
    sim = GBMSimulator(["AAPL"])
    # First step should be very close to seed price (tiny dt)
    prices = sim.step()
    assert abs(prices["AAPL"] - SEED_PRICES["AAPL"]) < 5.0

def test_add_ticker_dynamically():
    sim = GBMSimulator(["AAPL"])
    sim.add_ticker("PYPL")
    prices = sim.step()
    assert "PYPL" in prices
    assert prices["PYPL"] > 0

def test_remove_ticker():
    sim = GBMSimulator(["AAPL", "MSFT"])
    sim.remove_ticker("MSFT")
    prices = sim.step()
    assert "MSFT" not in prices

def test_correlation_produces_positive_definite_matrix():
    # Cholesky requires a positive-definite matrix
    # If the correlation structure is invalid, np.linalg.cholesky raises
    sim = GBMSimulator(["AAPL", "GOOGL", "MSFT", "TSLA", "JPM"])
    assert sim._cholesky is not None  # No exception = matrix was valid
```

### Integration tests for `SimulatorDataSource` (`test_simulator_source.py`, 10 tests)

```python
@pytest.mark.asyncio
async def test_start_seeds_cache_immediately():
    cache = PriceCache()
    source = SimulatorDataSource(cache)
    await source.start(["AAPL"])
    assert cache.get_price("AAPL") is not None  # Immediately available
    await source.stop()

@pytest.mark.asyncio
async def test_add_remove_ticker():
    cache = PriceCache()
    source = SimulatorDataSource(cache)
    await source.start(["AAPL"])
    await source.add_ticker("PYPL")
    assert "PYPL" in source.get_tickers()
    await source.remove_ticker("PYPL")
    assert "PYPL" not in source.get_tickers()
    assert cache.get("PYPL") is None
    await source.stop()
```

---

## Tuning Guide

| Goal | Parameter to adjust |
|---|---|
| Faster price movement | Increase `sigma` in `TICKER_PARAMS` |
| Stronger trend up/down | Increase/decrease `mu` |
| More frequent shock events | Increase `event_probability` (e.g., `0.005`) |
| Slower update rate | Increase `update_interval` (e.g., `1.0`) |
| Stronger sector correlation | Increase `INTRA_TECH_CORR` (max < 1.0) |
| More independent behavior | Decrease `INTRA_TECH_CORR` toward `CROSS_GROUP_CORR` |

**Adding a new default ticker:**

1. Add seed price to `SEED_PRICES` in `seed_prices.py`
2. Add GBM params to `TICKER_PARAMS` (or omit to use `DEFAULT_PARAMS`)
3. Optionally add to a `CORRELATION_GROUPS` set for sector-aware correlation
4. Add to the default watchlist in the database seed data

**Important:** Do not set any correlation coefficient to 1.0 — this makes the correlation matrix singular and `np.linalg.cholesky` will raise `LinAlgError`. Keep all pairwise correlations strictly below 1.0.
