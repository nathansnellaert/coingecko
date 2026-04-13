"""Ingest CoinGecko price history.

This node fetches price history for all tracked coins, saving each coin separately.
"""

from datetime import datetime, timezone
from subsets_utils import save_raw_json, load_raw_json, load_state, save_state
from coingecko_client import rate_limited_get, CoinNotFoundError


def run():
    """Fetch price history for all tracked coins, saving each coin separately."""
    print("Fetching prices...")

    # Get all unique coins we've ever tracked (from coins state)
    coins_state = load_state("coins")
    all_coin_ids = coins_state.get("all_coin_ids", [])

    # If no state yet, fall back to current coins.json
    if not all_coin_ids:
        coins_data = load_raw_json("coins")
        all_coin_ids = [c["id"] for c in coins_data["coins"]]

    state = load_state("prices")
    completed = set(state.get("completed", []))

    pending = [c for c in all_coin_ids if c not in completed]

    if not pending:
        print("  All coins up to date")
        return

    print(f"  Fetching prices for {len(pending)} coins ({len(completed)} already done)...")

    for i, coin_id in enumerate(pending, 1):
        print(f"  [{i}/{len(pending)}] {coin_id}...", end=" ")

        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
        # Free tier limit: 365 days of history per coin.
        # Full historical data (days=max) requires a paid CoinGecko API plan.
        params = {
            "vs_currency": "usd",
            "days": 365,
            "interval": "daily"
        }

        try:
            response = rate_limited_get(url, params=params)
            data = response.json()

            if data.get("prices"):
                save_raw_json(data, f"prices/{coin_id}")
                print(f"({len(data['prices'])} days)")
            else:
                save_raw_json({"prices": [], "market_caps": [], "total_volumes": []}, f"prices/{coin_id}")
                print("(no data)")
        except CoinNotFoundError:
            print("(not found - skipping)")

        completed.add(coin_id)
        save_state("prices", {
            "completed": list(completed),
            "last_updated": datetime.now(timezone.utc).isoformat()
        })

    print(f"  Total: {len(completed)} coins fetched")


from nodes.coins import run as coins_run

NODES = {
    run: [coins_run],
}


if __name__ == "__main__":
    run()
