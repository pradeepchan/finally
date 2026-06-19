# Massive API — Stock Market Data Reference

Massive (formerly Polygon.io, rebranded October 30 2025) provides REST and WebSocket APIs for US stock market data. Existing Polygon.io API keys continue to work unchanged. The base URL redirects from `api.polygon.io` to `api.massive.com`.

---

## Authentication

All requests require an API key passed as a query parameter or via the Python client.

**Environment variable (recommended):**
```bash
export MASSIVE_API_KEY=your_api_key_here
```

**Direct in Python:**
```python
from massive import RESTClient
client = RESTClient(api_key="your_api_key_here")
```

**Raw HTTP header:**
```
Authorization: Bearer your_api_key_here
```

**Query parameter (legacy):**
```
GET https://api.massive.com/v2/snapshot/...?apiKey=your_api_key_here
```

---

## Python Package

```bash
pip install massive      # formerly: pip install polygon-api-client
```

Requires Python 3.9+. The `RESTClient` is **synchronous** — wrap calls with `asyncio.to_thread()` in async contexts.

---

## Rate Limits

| Tier | Requests / minute | Recommended poll interval |
|---|---|---|
| Free (Starter/Developer) | 5 | 15 s |
| Advanced | Unlimited | 2–5 s |
| Business | Unlimited | 2–5 s |

- Free tier data is **15-minute delayed** for snapshot endpoints.
- Advanced and Business tiers receive **real-time** data.
- The Fair Market Value (`fmv`) field is Business-tier only.

---

## Snapshot Endpoints

Snapshot endpoints return current market data (latest trade, quote, and daily bar) for one or more tickers. These are the primary endpoints for a trading dashboard.

### 1. Full Market Snapshot (batch — all or filtered tickers)

```
GET /v2/snapshot/locale/us/markets/stocks/tickers
```

Query parameters:

| Parameter | Type | Description |
|---|---|---|
| `tickers` | string (CSV) | Comma-separated ticker list; omit for all ~10,000+ tickers |
| `include_otc` | boolean | Include OTC securities (default: false) |

**Python:**
```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient()  # reads MASSIVE_API_KEY from env

# Fetch snapshots for specific tickers (up to all ~10,000 in one call)
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
)

for snap in snapshots:
    last_price = snap.last_trade.price
    pct_change = snap.todays_change_perc
    print(f"{snap.ticker}: ${last_price:.2f}  ({pct_change:+.2f}%)")
```

**curl:**
```bash
curl "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT" \
  -H "Authorization: Bearer $MASSIVE_API_KEY"
```

**Response shape:**
```json
{
  "status": "OK",
  "tickers": [
    {
      "ticker": "AAPL",
      "todaysChange": 2.35,
      "todaysChangePerc": 1.25,
      "updated": 1718300000000,
      "day": {
        "o": 187.50,
        "h": 191.20,
        "l": 186.90,
        "c": 190.50,
        "v": 52341200,
        "vw": 189.42
      },
      "lastTrade": {
        "p": 190.50,
        "s": 100,
        "t": 1718300000000,
        "x": 4
      },
      "lastQuote": {
        "P": 190.52,
        "S": 5,
        "p": 190.49,
        "s": 3
      },
      "min": {
        "o": 190.10,
        "h": 190.55,
        "l": 190.05,
        "c": 190.50,
        "v": 234100,
        "vw": 190.28
      },
      "prevDay": {
        "o": 188.10,
        "h": 189.90,
        "l": 187.80,
        "c": 188.15,
        "v": 48234000,
        "vw": 188.73
      },
      "fmv": 190.48
    }
  ]
}
```

**Key response fields:**

| Field | Type | Description |
|---|---|---|
| `ticker` | string | Ticker symbol |
| `lastTrade.p` | float | Last trade price (the current price) |
| `lastTrade.s` | int | Last trade size (shares) |
| `lastTrade.t` | int | Timestamp (Unix **milliseconds**) |
| `lastQuote.P` | float | Ask price |
| `lastQuote.p` | float | Bid price |
| `day.o/h/l/c` | float | Today's open/high/low/close |
| `day.v` | float | Today's volume |
| `day.vw` | float | Today's VWAP |
| `prevDay.c` | float | Previous day's close |
| `todaysChange` | float | Dollar change from prev close |
| `todaysChangePerc` | float | Percentage change from prev close |
| `updated` | int | Last update timestamp (Unix milliseconds) |
| `fmv` | float | Fair market value (Business tier only) |

> **Timestamp note:** Massive timestamps are Unix **milliseconds**. Convert to seconds: `ts_seconds = snap.last_trade.timestamp / 1000.0`

---

### 2. Single Ticker Snapshot

```
GET /v2/snapshot/locale/us/markets/stocks/tickers/{stocksTicker}
```

Same response shape as above but for one ticker.

**Python:**
```python
snap = client.get_snapshot("AAPL", market_type=SnapshotMarketType.STOCKS)
print(f"AAPL: ${snap.last_trade.price:.2f}")
```

**curl:**
```bash
curl "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers/AAPL" \
  -H "Authorization: Bearer $MASSIVE_API_KEY"
```

---

### 3. Unified Snapshot (cross-asset, up to 250 tickers)

```
GET /v3/snapshot
```

Supports stocks, options, forex, and crypto in a single call.

| Parameter | Type | Description |
|---|---|---|
| `ticker.any_of` | string (CSV) | Up to 250 tickers |
| `type` | string | Asset class filter: `stocks`, `options`, `forex`, `crypto` |
| `limit` | int | Results per page (max 250, default 10) |
| `order` | string | Sort direction |
| `sort` | string | Field to sort by |

**Python:**
```python
results = []
for snap in client.list_universal_snapshots(
    params={"ticker.any_of": "AAPL,GOOGL,MSFT,AMZN,TSLA,NVDA,META,JPM,V,NFLX"}
):
    results.append(snap)
```

**curl:**
```bash
curl "https://api.massive.com/v3/snapshot?ticker.any_of=AAPL,GOOGL,MSFT&limit=250" \
  -H "Authorization: Bearer $MASSIVE_API_KEY"
```

**Response shape:**
```json
{
  "status": "OK",
  "results": [
    {
      "ticker": "AAPL",
      "type": "stocks",
      "name": "Apple Inc.",
      "market_status": "open",
      "session": {
        "open": 187.50,
        "high": 191.20,
        "low": 186.90,
        "close": 190.50,
        "volume": 52341200,
        "change": 2.35,
        "change_percent": 1.25
      },
      "last_trade": {
        "price": 190.50,
        "size": 100,
        "timestamp": 1718300000000000
      },
      "last_quote": {
        "bid": 190.49,
        "ask": 190.52,
        "bid_size": 3,
        "ask_size": 5
      }
    }
  ],
  "next_url": "https://api.massive.com/v3/snapshot?cursor=abc123"
}
```

---

## End-of-Day / Historical Endpoints

### Previous Day Bar

```
GET /v2/aggs/ticker/{ticker}/prev
```

Returns the previous trading day's OHLCV bar.

**Python:**
```python
prev = client.get_previous_close("AAPL")
print(f"AAPL prev close: ${prev[0].close:.2f}")
```

**curl:**
```bash
curl "https://api.massive.com/v2/aggs/ticker/AAPL/prev" \
  -H "Authorization: Bearer $MASSIVE_API_KEY"
```

---

### Aggregate Bars (Historical OHLCV)

```
GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}
```

| Parameter | Values | Description |
|---|---|---|
| `multiplier` | integer | Size of the timespan (e.g., 1) |
| `timespan` | `minute`, `hour`, `day`, `week`, `month` | Granularity |
| `from` | `YYYY-MM-DD` | Start date |
| `to` | `YYYY-MM-DD` | End date |
| `adjusted` | boolean | Adjusted for splits/dividends (default: true) |
| `sort` | `asc`, `desc` | Sort order |
| `limit` | integer | Max bars (default 5000, max 50000) |

**Python:**
```python
aggs = []
for bar in client.list_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="day",
    from_="2024-01-01",
    to="2024-12-31",
    adjusted=True,
    sort="asc",
    limit=50000,
):
    aggs.append(bar)

# bar fields: open, high, low, close, volume, vwap, timestamp (ms), transactions
```

**curl:**
```bash
curl "https://api.massive.com/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-12-31?adjusted=true&sort=asc&limit=365" \
  -H "Authorization: Bearer $MASSIVE_API_KEY"
```

**Response shape:**
```json
{
  "ticker": "AAPL",
  "status": "OK",
  "resultsCount": 252,
  "results": [
    {
      "o": 185.10,
      "h": 187.90,
      "l": 184.20,
      "c": 187.15,
      "v": 55231000,
      "vw": 186.42,
      "t": 1704067200000,
      "n": 412340
    }
  ]
}
```

**Aggregate bar field meanings:**

| Field | Description |
|---|---|
| `o` | Open |
| `h` | High |
| `l` | Low |
| `c` | Close |
| `v` | Volume |
| `vw` | Volume-weighted average price |
| `t` | Timestamp (Unix milliseconds, start of bar) |
| `n` | Number of transactions |

---

## Real-Time Quotes and Trades

### Last Trade

```python
trade = client.get_last_trade("AAPL")
# trade.price, trade.size, trade.timestamp (ms)
```

### Last Quote (NBBO)

```python
quote = client.get_last_quote("AAPL")
# quote.bid_price, quote.ask_price, quote.bid_size, quote.ask_size
```

---

## WebSocket Streaming

For real-time tick-by-tick data (Advanced/Business tiers only):

```python
from massive import WebSocketClient

def handle_messages(msgs):
    for m in msgs:
        if hasattr(m, "price"):
            print(f"{m.sym}: ${m.price}")

client = WebSocketClient(
    api_key="your_api_key",
    subscriptions=["T.AAPL", "T.MSFT"],  # T.* = trades, Q.* = quotes, A.* = minute aggs
)
client.run(["T.AAPL", "T.MSFT"], handle_messages)
```

Subscription prefixes:
- `T.*` — Trades (individual transactions)
- `Q.*` — Quotes (NBBO bid/ask updates)
- `A.*` — Minute aggregate bars
- `AM.*` — Second aggregate bars

---

## Error Handling

Common HTTP status codes:

| Code | Meaning | Action |
|---|---|---|
| 200 | OK | Success |
| 401 | Unauthorized | Bad or missing API key |
| 403 | Forbidden | Endpoint requires higher tier |
| 429 | Too Many Requests | Rate limit exceeded; back off and retry |
| 500 | Server Error | Retry with exponential backoff |

Python pattern for production polling:

```python
import time
import logging

def fetch_snapshots_with_retry(client, tickers, max_retries=3):
    for attempt in range(max_retries):
        try:
            return client.get_snapshot_all(
                market_type=SnapshotMarketType.STOCKS,
                tickers=tickers,
            )
        except Exception as e:
            if "429" in str(e) and attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s, 4s
                logging.warning("Rate limited, retrying in %ds", wait)
                time.sleep(wait)
            else:
                raise
```

---

## Market Hours

Snapshot data resets daily at **3:30 AM EST** and resumes updating from **4:00 AM EST** as pre-market activity begins. Outside trading hours, the `lastTrade` reflects the most recent trade (which may be hours old). Check `market_status` in the unified snapshot or use the market status endpoint:

```python
status = client.get_market_status()
# status.market: "open" | "closed" | "extended-hours"
```

---

## Useful Links

- API documentation: https://massive.com/docs/rest/stocks/overview
- Python client: https://github.com/massive-com/client-python
- Pricing tiers: https://massive.com/pricing
- System status: https://massive.com/system
