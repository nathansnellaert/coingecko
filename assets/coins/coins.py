import pyarrow as pa
from utils import get, load_state, save_state
from datetime import datetime, timezone
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import httpx

target_count = 1000

# CoinGecko public API: 5-15 calls/minute, we'll be very conservative with 5 calls/minute
@sleep_and_retry
@limits(calls=5, period=60)
@retry(
    stop=stop_after_attempt(10),  # More retries
    wait=wait_exponential(multiplier=2, min=10, max=120),  # Longer backoff
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError)),
    reraise=True
)
def rate_limited_get(url, params=None):
    response = get(url, params=params)
    if response.status_code == 429:
        raise httpx.HTTPStatusError(f"Rate limited", request=response.request, response=response)
    if response.status_code != 200:
        raise httpx.HTTPStatusError(f"API request failed with status {response.status_code}", request=response.request, response=response)
    return response

def process_coins():
    state = load_state("coins")
    tracked_coins = state.get("tracked_coins", {})
    
    url = "https://api.coingecko.com/api/v3/coins/markets"
    all_coins = []
    page = 1
    
    while len(all_coins) < target_count:
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": min(100, target_count - len(all_coins)),
            "page": page,
            "sparkline": False
        }
        
        response = rate_limited_get(url, params=params)
            
        data = response.json()
        
        if not data:
            break
            
        all_coins.extend(data)
        page += 1
        
        if len(data) < params["per_page"]:
            break
    
    coins_data = []
    updated_tracked = tracked_coins.copy()
    
    for coin in all_coins[:target_count]:
        coin_id = coin["id"]
        
        if coin_id not in tracked_coins:
            updated_tracked[coin_id] = {
                "first_seen": datetime.now(timezone.utc).isoformat(),
                "name": coin["name"],
                "symbol": coin["symbol"].upper()
            }
        
        coins_data.append({
            "coin_id": coin_id,
            "name": coin["name"],
            "symbol": coin["symbol"].upper(),
            "current_rank": coin.get("market_cap_rank"),
            "market_cap": coin.get("market_cap"),
            "volume_24h": coin.get("total_volume"),
            "current_price": coin.get("current_price"),
            "price_change_24h": coin.get("price_change_24h"),
            "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
            "circulating_supply": coin.get("circulating_supply"),
            "total_supply": coin.get("total_supply"),
            "max_supply": coin.get("max_supply"),
            "ath": coin.get("ath"),
            "ath_date": coin.get("ath_date"),
            "atl": coin.get("atl"),
            "atl_date": coin.get("atl_date"),
            "last_updated": coin.get("last_updated"),
            "is_tracked": True
        })
    
    for coin_id, coin_info in tracked_coins.items():
        if coin_id not in [c["coin_id"] for c in coins_data]:
            coins_data.append({
                "coin_id": coin_id,
                "name": coin_info["name"],
                "symbol": coin_info["symbol"],
                "current_rank": None,
                "market_cap": None,
                "volume_24h": None,
                "current_price": None,
                "price_change_24h": None,
                "price_change_percentage_24h": None,
                "circulating_supply": None,
                "total_supply": None,
                "max_supply": None,
                "ath": None,
                "ath_date": None,
                "atl": None,
                "atl_date": None,
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "is_tracked": True
            })
    
    save_state("coins", {
        "tracked_coins": updated_tracked,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_tracked": len(updated_tracked)
    })
    
    return pa.Table.from_pylist(coins_data)