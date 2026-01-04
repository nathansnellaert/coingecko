
"""Ingest CoinGecko coin list."""

from datetime import datetime, timezone
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state
from utils import rate_limited_get
import os

TARGET_COUNT = 1000


def run():
    """Fetch top coins by market cap and append to historical list."""
    print("  Fetching coins...")
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_timestamp = datetime.now(timezone.utc).isoformat()

    # Check if we already have data for today
    state = load_state("coins")
    last_date = state.get("last_date")
    if last_date == run_date:
        print(f"  Already fetched coins today ({run_date})")
        return

    url = "https://api.coingecko.com/api/v3/coins/markets"
    all_coins = []
    page = 1

    while len(all_coins) < TARGET_COUNT:
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": min(100, TARGET_COUNT - len(all_coins)),
            "page": page,
            "sparkline": False
        }

        response = rate_limited_get(url, params=params)
        data = response.json()

        if not data:
            break

        all_coins.extend(data)
        page += 1
        print(f"    Page {page - 1}: {len(data)} coins")

        if len(data) < params["per_page"]:
            break

    print(f"  Total: {len(all_coins)} coins")

    # Save today's snapshot with date
    save_raw_json({
        "coins": all_coins,
        "timestamp": run_timestamp,
        "date": run_date
    }, f"coins/{run_date}")

    # Also save as "coins" for backward compat with prices ingest
    save_raw_json({"coins": all_coins, "timestamp": run_timestamp}, "coins")

    # Track all unique coin IDs we've ever seen
    all_coin_ids = set(state.get("all_coin_ids", []))
    new_ids = set(c["id"] for c in all_coins)
    all_coin_ids.update(new_ids)

    save_state("coins", {
        "last_date": run_date,
        "last_updated": run_timestamp,
        "all_coin_ids": list(all_coin_ids),
        "total_unique_coins": len(all_coin_ids)
    })

    print(f"  Unique coins tracked: {len(all_coin_ids)}")
