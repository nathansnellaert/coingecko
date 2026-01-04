"""Validate coingecko_prices_daily output."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_positive


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

    # Check reasonable date range (Bitcoin started in 2009, but CoinGecko has data from ~2013)
    dates = table.column("date").to_pylist()
    min_date = min(dates)
    max_date = max(dates)
    assert min_date >= "2010-01-01", f"Data impossibly old: {min_date}"
    assert max_date <= "2030-01-01", f"Future dates found: {max_date}"

    # Verify we have multiple coins
    coin_ids = set(table.column("coin_id").to_pylist())
    assert len(coin_ids) >= 100, f"Expected 100+ coins, got {len(coin_ids)}"

    # Check for expected major coins
    expected_coins = {"bitcoin", "ethereum"}
    missing = expected_coins - coin_ids
    assert not missing, f"Missing expected coins: {missing}"

    print(f"  Validated: {len(table):,} rows, {len(coin_ids)} coins, dates {min_date} to {max_date}")
