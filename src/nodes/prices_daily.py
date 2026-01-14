"""Transform CoinGecko price data into clean daily prices dataset.

This node transforms per-coin raw price files into a single unified dataset.
"""

import pyarrow as pa
from datetime import datetime
from subsets_utils import load_raw_json, upload_data, load_state, validate
from subsets_utils.testing import assert_valid_date, assert_positive

DATASET_ID = "coingecko_prices_daily"

METADATA = {
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


def test(table: pa.Table) -> None:
    """Validate transform output. Raises AssertionError on failure."""
    # Schema validation - all columns must be listed
    validate(table, {
        "columns": {
            "date": "string",
            "coin_id": "string",
            "price_usd": "double",
            "volume_usd": "double",
            "market_cap_usd": "double",
        },
        "not_null": ["date", "coin_id"],
        "unique": ["date", "coin_id"],
        "min_rows": 1000,
    })

    # Date format validation
    assert_valid_date(table, "date")

    # Price should be positive where not null
    assert_positive(table, "price_usd")

    # Volume should be positive or zero where not null
    assert_positive(table, "volume_usd", allow_zero=True)

    # Market cap should be positive where not null
    assert_positive(table, "market_cap_usd")

    # Check reasonable date range (365 days of history per API limit)
    dates = table.column("date").to_pylist()
    min_date = min(dates)
    max_date = max(dates)
    assert min_date >= "2023-01-01", f"Data too old: {min_date}"
    assert max_date <= "2030-01-01", f"Future dates found: {max_date}"

    # Verify we have multiple coins
    coin_ids = set(table.column("coin_id").to_pylist())
    assert len(coin_ids) >= 100, f"Expected 100+ coins, got {len(coin_ids)}"

    # Check for expected major coins
    expected_coins = {"bitcoin", "ethereum"}
    missing = expected_coins - coin_ids
    assert not missing, f"Missing expected coins: {missing}"

    print(f"  Validated: {len(table):,} rows, {len(coin_ids)} coins, dates {min_date} to {max_date}")


def run():
    """Transform per-coin price files into a single unified dataset."""
    print("Transforming prices to daily dataset...")

    # Get list of coins from state (instead of list_raw_files)
    prices_state = load_state("prices")
    coin_ids = prices_state.get("completed", [])

    if not coin_ids:
        print("  No price data found")
        return

    print(f"  Processing {len(coin_ids)} coins...")

    records = []

    for coin_id in coin_ids:
        try:
            data = load_raw_json(f"prices/{coin_id}")
        except FileNotFoundError:
            continue

        prices = data.get("prices", [])
        volumes = data.get("total_volumes", [])
        market_caps = data.get("market_caps", [])

        # CoinGecko returns multiple points per day (near midnight and end of day)
        # Deduplicate by keeping the last (most recent) price per day
        daily_records = {}
        for i, price_point in enumerate(prices):
            timestamp_ms = price_point[0]
            date = datetime.utcfromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d")

            # Keep last entry per date (overwrites earlier ones)
            daily_records[date] = {
                "date": date,
                "coin_id": coin_id,
                "price_usd": price_point[1] if len(price_point) > 1 else None,
                "volume_usd": volumes[i][1] if i < len(volumes) and len(volumes[i]) > 1 else None,
                "market_cap_usd": market_caps[i][1] if i < len(market_caps) and len(market_caps[i]) > 1 else None,
            }

        records.extend(daily_records.values())

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
    print(f"  Transformed {len(table):,} records from {len(coin_ids)} coins")

    test(table)

    upload_data(table, DATASET_ID, metadata=METADATA, mode="overwrite")
    print("  Done!")


from nodes.prices import run as prices_run

NODES = {
    run: [prices_run],
}


if __name__ == "__main__":
    run()
