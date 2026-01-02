"""Transform CoinGecko price data into clean daily prices dataset."""

import os
import pyarrow as pa
from datetime import datetime
from subsets_utils import load_raw_json, upload_data, publish, get_data_dir
from .test import test

DATASET_ID = "coingecko_prices_daily"

METADATA = {
    "id": DATASET_ID,
    "title": "CoinGecko Cryptocurrency Prices (Daily)",
    "description": "Daily cryptocurrency prices, trading volumes, and market capitalizations from CoinGecko. Covers top 1000+ coins by market cap with 365 days of history.",
    "column_descriptions": {
        "date": "Date of observation (YYYY-MM-DD)",
        "coin_id": "CoinGecko coin identifier (e.g., 'bitcoin', 'ethereum')",
        "price_usd": "Closing price in USD",
        "volume_usd": "24-hour trading volume in USD",
        "market_cap_usd": "Market capitalization in USD",
    }
}


def run():
    """Transform per-coin price files into a single unified dataset."""
    prices_dir = os.path.join(get_data_dir(), "raw", "prices")

    if not os.path.exists(prices_dir):
        print("  No price data found")
        return

    coin_files = [f for f in os.listdir(prices_dir) if f.endswith('.json')]
    print(f"  Processing {len(coin_files)} coins...")

    records = []

    for coin_file in sorted(coin_files):
        coin_id = coin_file.replace('.json', '')
        data = load_raw_json(f"prices/{coin_id}")

        prices = data.get("prices", [])
        volumes = data.get("total_volumes", [])
        market_caps = data.get("market_caps", [])

        # Zip together - all arrays should be same length
        for i, price_point in enumerate(prices):
            timestamp_ms = price_point[0]
            date = datetime.utcfromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d")

            records.append({
                "date": date,
                "coin_id": coin_id,
                "price_usd": price_point[1] if len(price_point) > 1 else None,
                "volume_usd": volumes[i][1] if i < len(volumes) and len(volumes[i]) > 1 else None,
                "market_cap_usd": market_caps[i][1] if i < len(market_caps) and len(market_caps[i]) > 1 else None,
            })

    if not records:
        print("  No records to transform")
        return

    schema = pa.schema([
        ("date", pa.string()),
        ("coin_id", pa.string()),
        ("price_usd", pa.float64()),
        ("volume_usd", pa.float64()),
        ("market_cap_usd", pa.float64()),
    ])

    table = pa.Table.from_pylist(records, schema=schema)
    print(f"  Transformed {len(table):,} records from {len(coin_files)} coins")

    test(table)

    upload_data(table, DATASET_ID, mode="overwrite")
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
