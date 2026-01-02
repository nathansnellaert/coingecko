
"""Transform CoinGecko coin data."""

import os
import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish, get_data_dir


def run():
    """Transform coins from daily snapshots to Arrow table with rank history."""
    coins_dir = os.path.join(get_data_dir(), "raw", "coins")

    if not os.path.exists(coins_dir):
        print("  No coin data found")
        return

    # Get all daily snapshot files
    date_files = [f for f in os.listdir(coins_dir) if f.endswith('.json') and '-' in f]
    print(f"  Processing {len(date_files)} daily snapshots...")

    all_coins_data = []

    for date_file in sorted(date_files):
        date = date_file.replace('.json', '')
        data = load_raw_json(f"coins/{date}")
        coins = data["coins"]

        for coin in coins:
            all_coins_data.append({
                "date": date,
                "coin_id": coin["id"],
                "name": coin["name"],
                "symbol": coin["symbol"].upper(),
                "rank": coin.get("market_cap_rank"),
                "market_cap": coin.get("market_cap"),
                "volume_24h": coin.get("total_volume"),
                "price_usd": coin.get("current_price"),
                "price_change_24h_pct": coin.get("price_change_percentage_24h"),
                "circulating_supply": coin.get("circulating_supply"),
                "total_supply": coin.get("total_supply"),
                "max_supply": coin.get("max_supply"),
                "ath": coin.get("ath"),
                "ath_date": coin.get("ath_date"),
                "atl": coin.get("atl"),
                "atl_date": coin.get("atl_date"),
            })

    print(f"  Transformed {len(all_coins_data)} coin records from {len(date_files)} days")
    table = pa.Table.from_pylist(all_coins_data)
    upload_data(table, "coingecko_coins", mode="overwrite")
    publish("coingecko_coins", {
        "name": "CoinGecko Coin Rankings",
        "description": "Daily snapshots of top cryptocurrency rankings, market caps, and metadata from CoinGecko",
        "source": "CoinGecko API",
        "frequency": "daily",
        "columns": {
            "date": "Date of snapshot (YYYY-MM-DD)",
            "coin_id": "CoinGecko coin identifier",
            "name": "Coin name",
            "symbol": "Coin ticker symbol",
            "rank": "Market cap rank on that date",
            "market_cap": "Market capitalization in USD",
            "volume_24h": "24-hour trading volume in USD",
            "price_usd": "Price in USD",
            "price_change_24h_pct": "24-hour price change percentage",
            "circulating_supply": "Circulating supply",
            "total_supply": "Total supply",
            "max_supply": "Maximum supply (if capped)",
            "ath": "All-time high price in USD",
            "ath_date": "Date of all-time high",
            "atl": "All-time low price in USD",
            "atl_date": "Date of all-time low"
        }
    })
