
"""Transform CoinGecko price data."""

import os
import pyarrow as pa
from datetime import datetime, timezone
from subsets_utils import load_raw_json, upload_data, publish, get_data_dir


def run():
    """Transform prices from per-coin files to Arrow table."""
    prices_dir = os.path.join(get_data_dir(), "raw", "prices")

    if not os.path.exists(prices_dir):
        print("  No price data found")
        return

    coin_files = [f for f in os.listdir(prices_dir) if f.endswith('.json')]
    print(f"  Processing {len(coin_files)} coins...")

    all_price_data = []

    for coin_file in sorted(coin_files):
        coin_id = coin_file.replace('.json', '')
        data = load_raw_json(f"prices/{coin_id}")

        for price_point, volume_point, mcap_point in zip(
            data.get("prices", []),
            data.get("total_volumes", []),
            data.get("market_caps", [])
        ):
            date = datetime.fromtimestamp(price_point[0] / 1000).date()

            all_price_data.append({
                "date": date.isoformat(),
                "coin_id": coin_id,
                "price_usd": price_point[1],
                "volume_24h": volume_point[1] if volume_point else None,
                "market_cap": mcap_point[1] if mcap_point else None
            })

    schema = pa.schema([
        ("date", pa.string()),
        ("coin_id", pa.string()),
        ("price_usd", pa.float64()),
        ("volume_24h", pa.float64()),
        ("market_cap", pa.float64())
    ])

    print(f"  Transformed {len(all_price_data)} price records from {len(coin_files)} coins")
    table = pa.Table.from_pylist(all_price_data, schema=schema)
    upload_data(table, "coingecko_prices", mode="overwrite")
    publish("coingecko_prices", {
        "name": "CoinGecko Daily Prices",
        "description": "Daily cryptocurrency prices, volumes, and market caps from CoinGecko",
        "source": "CoinGecko API",
        "frequency": "daily",
        "columns": {
            "date": "Date (YYYY-MM-DD)",
            "coin_id": "CoinGecko coin identifier",
            "price_usd": "Price in USD",
            "volume_24h": "24-hour trading volume in USD",
            "market_cap": "Market capitalization in USD"
        }
    })
