# CoinGecko

Cryptocurrency market data from the [CoinGecko API](https://www.coingecko.com/en/api) (free tier).

## Coverage

**Scope:** Top 1,000 coins by market cap, ranked daily. This covers 99%+ of total crypto market capitalization. CoinGecko lists 10,000+ coins but most beyond the top 1,000 are illiquid or defunct.

**History:** 365 days of daily data per coin (free tier limit). Full historical data requires a paid API plan.

**Datasets produced:**

| Dataset | Description | Key |
|---------|-------------|-----|
| `coingecko_prices_daily` | Daily price, volume, and market cap for each coin | `coin_id`, `date` |

## Limitations

- Free API tier: 5-15 calls/minute, 365-day history cap
- Coin list refreshed daily; prices fetched incrementally with checkpoint state
- No real-time or intraday data
