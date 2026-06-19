# Market Data Interface — Unified Python API Design

This document describes the unified market data interface used in FinAlly. The design isolates all downstream code (SSE streaming, portfolio valuation, trade execution) from the details of _how_ prices are obtained.

**Status: Implemented.** The code described here lives in `backend/app/market/`. This document is the authoritative design reference for agents building on top of this subsystem.

---

## Design Goals

1. **Source-agnostic downstream** — portfolio, trade, and SSE code never imports from `massive` or the simulator directly.
2. **Transparent switching** — a single environment variable selects the implementation; no code changes needed.
3. **Dynamic watchlists** — tickers can be added or removed at runtime without restarting.
4. **Thread safety** — the price cache is written from a background thread/task and read from FastAPI request handlers concurrently.
5. **Cold-start safety** — the cache may be empty at startup; all readers must handle `None` prices.

---

## Module Layout

```
backend/app/market/
├── __init__.py          # Public API re-exports
├── models.py            # PriceUpdate dataclass
├── cache.py             # PriceCache — thread-safe price store
├── interface.py         # MarketDataSource — abstract base class
├── simulator.py         # SimulatorDataSource + GBMSimulator
├── massive_client.py    # MassiveDataSource — Polygon.io/Massive REST poller
├── factory.py           # create_market_data_source() — env-driven selector
└── stream.py            # FastAPI SSE endpoint (create_stream_router)
```

---

## Data Model: `PriceUpdate`

```python
# backend/app/market/models.py
@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: float          # Unix seconds (not milliseconds)

    @property
    def change(self) -> float: ...          # Absolute change
    @property
    def change_percent(self) -> float: ... # Percent change
    @property
    def direction(self) -> str: ...        # "up" | "down" | "flat"

    def to_dict(self) -> dict: ...         # JSON-serializable dict
```

`PriceUpdate` is **immutable** (`frozen=True`). Every cache write creates a new instance. The `to_dict()` output is what gets sent in SSE events:

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

> **Note:** The SSE payload does not include a `seed_price` field currently. The frontend calculates daily change % using the first price it receives after page load as its baseline, which approximates but does not equal the simulator seed price. If an authoritative daily change % is needed, expose seed prices via a dedicated endpoint.

---

## Price Cache: `PriceCache`

```python
# backend/app/market/cache.py
class PriceCache:
    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate
    def get(self, ticker: str) -> PriceUpdate | None
    def get_price(self, ticker: str) -> float | None
    def get_all(self) -> dict[str, PriceUpdate]
    def remove(self, ticker: str) -> None
    @property
    def version(self) -> int   # Monotonic counter; increments on every write
```

The `version` counter is used by the SSE endpoint to detect changes without iterating the full price map:

```python
# Only send a new SSE event if something changed
if price_cache.version != last_version:
    last_version = price_cache.version
    send_snapshot(price_cache.get_all())
```

**Thread safety:** `PriceCache` uses a `threading.Lock` internally. Safe to call from asyncio tasks, background threads, and FastAPI request handlers simultaneously.

**First update semantics:** On the first `update()` for a ticker, `previous_price` is set equal to `price` (direction = "flat"). Subsequent updates track the true previous price.

---

## Abstract Interface: `MarketDataSource`

```python
# backend/app/market/interface.py
class MarketDataSource(ABC):
    @abstractmethod
    async def start(self, tickers: list[str]) -> None: ...
    # Begin producing prices. Creates a background asyncio task.
    # Call exactly once. Do an immediate first update before returning.

    @abstractmethod
    async def stop(self) -> None: ...
    # Cancel the background task. Safe to call multiple times.

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None: ...
    # Add a ticker to the active set. No-op if already present.
    # Picked up on the next update cycle.

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None: ...
    # Remove a ticker. Also removes it from the PriceCache.

    @abstractmethod
    def get_tickers(self) -> list[str]: ...
    # Currently tracked tickers.
```

Both implementations conform to this interface. Downstream code only imports `MarketDataSource` for type hints.

---

## Implementations

### SimulatorDataSource

Wraps `GBMSimulator`. Runs an asyncio loop calling `GBMSimulator.step()` every 500ms, writing results to the cache. Seeds the cache immediately on `start()` so SSE clients see data within milliseconds of connection.

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,    # seconds between steps
        event_probability: float = 0.001, # probability of a shock event per tick
    )
```

### MassiveDataSource

Polls `GET /v2/snapshot/locale/us/markets/stocks/tickers` for all watched tickers in one API call. Uses `asyncio.to_thread()` to run the synchronous `RESTClient` without blocking the event loop.

```python
class MassiveDataSource(MarketDataSource):
    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,  # 15s for free tier (5 req/min)
    )
```

The `_fetch_snapshots` method runs synchronously in a thread pool:

```python
def _fetch_snapshots(self) -> list:
    return self._client.get_snapshot_all(
        market_type=SnapshotMarketType.STOCKS,
        tickers=self._tickers,
    )
```

Timestamp conversion (Massive returns milliseconds, cache expects seconds):

```python
timestamp = snap.last_trade.timestamp / 1000.0
```

Error handling: polling errors are logged and the loop retries on the next interval. Common errors (401, 429, network) do not crash the process.

---

## Factory: `create_market_data_source`

```python
# backend/app/market/factory.py
def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        return SimulatorDataSource(price_cache=price_cache)
```

---

## Public API (re-exported from `__init__.py`)

```python
from app.market import (
    PriceUpdate,
    PriceCache,
    MarketDataSource,
    create_market_data_source,
    create_stream_router,
)
```

---

## Usage Pattern (FastAPI app startup)

```python
# In backend app startup (e.g., lifespan context manager)
from app.market import PriceCache, create_market_data_source, create_stream_router

DEFAULT_TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
                   "NVDA", "META", "JPM", "V", "NFLX"]

# App state
price_cache = PriceCache()
market_source = create_market_data_source(price_cache)

# Startup
await market_source.start(DEFAULT_TICKERS)

# Register SSE router
app.include_router(create_stream_router(price_cache))

# ... app runs ...

# Add a ticker (e.g., from watchlist POST endpoint)
await market_source.add_ticker("PYPL")

# Remove a ticker (e.g., from watchlist DELETE endpoint)
await market_source.remove_ticker("NFLX")

# Read prices (e.g., in portfolio valuation)
update = price_cache.get("AAPL")       # PriceUpdate | None
price = price_cache.get_price("AAPL") # float | None
all_prices = price_cache.get_all()    # dict[str, PriceUpdate]

# Shutdown
await market_source.stop()
```

---

## SSE Endpoint

`create_stream_router(price_cache)` returns a FastAPI `APIRouter` with:

```
GET /api/stream/prices
Content-Type: text/event-stream
```

Event format (sent every ~500ms when the cache version changes):

```
retry: 1000

data: {"AAPL": {"ticker": "AAPL", "price": 190.50, "previous_price": 189.75, "timestamp": 1718300000.0, "change": 0.75, "change_percent": 0.3953, "direction": "up"}, "MSFT": {...}, ...}

data: {"AAPL": {...}, ...}
```

The `retry: 1000` directive tells the browser to reconnect after 1 second if the connection drops. `EventSource` handles reconnection automatically.

---

## Handling Null Prices

The cache is empty until the first update arrives (~500ms after startup for the simulator, ~15s for the Massive poller on the free tier). All consumers must handle `None`:

```python
# In a watchlist API response
price = price_cache.get_price(ticker)
return {
    "ticker": ticker,
    "price": price,          # May be None on cold start
    "change_percent": ...,   # Omit or None if price is None
}
```

Frontend displays `--` when price is `None`.

---

## Extension Points

To add a new data source (e.g., Alpaca, IEX Cloud):

1. Create `backend/app/market/my_source.py`
2. Implement `MarketDataSource` — `start`, `stop`, `add_ticker`, `remove_ticker`, `get_tickers`
3. Update `factory.py` to return the new source based on an env var
4. Add tests in `backend/tests/market/test_my_source.py`

No other files need to change. The SSE endpoint, portfolio code, and trade execution are all cache-readers and remain untouched.
