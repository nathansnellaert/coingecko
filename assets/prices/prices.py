import pyarrow as pa
from utils import get, load_state, save_state
from datetime import datetime, timedelta, timezone
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import httpx

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

def process_prices(coins_data):
    state = load_state("prices")
    last_prices = state.get("last_prices", {})
    
    coins_table = coins_data.to_pandas()
    tracked_coins = coins_table[coins_table['is_tracked'] == True]['coin_id'].tolist()
    
    all_price_data = []
    updated_last_prices = {}
    
    for i, coin_id in enumerate(tracked_coins):
        print(f"[{i+1}/{len(tracked_coins)}] Fetching prices for {coin_id}...", end=' ')
        
        last_date = last_prices.get(coin_id)
        if last_date:
            start_date = datetime.fromisoformat(last_date) + timedelta(days=1)
            if start_date >= datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0):
                print("✓ (up to date)")
                updated_last_prices[coin_id] = last_date
                continue
            days = (datetime.now(timezone.utc) - start_date).days + 1
        else:
            days = 365
            start_date = datetime.now(timezone.utc) - timedelta(days=365)
        
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
            params = {
                "vs_currency": "usd",
                "days": min(days, 365),
                "interval": "daily"
            }
            
            response = rate_limited_get(url, params=params)
            data = response.json()
            
            if data.get('prices'):
                for price_point, volume_point, mcap_point in zip(
                    data.get('prices', []),
                    data.get('total_volumes', []),
                    data.get('market_caps', [])
                ):
                    date = datetime.fromtimestamp(price_point[0] / 1000).date()
                    
                    if last_date and date <= datetime.fromisoformat(last_date).date():
                        continue
                    
                    all_price_data.append({
                        "date": date.isoformat(),
                        "coin_id": coin_id,
                        "price_usd": price_point[1],
                        "volume_24h": volume_point[1] if volume_point else None,
                        "market_cap": mcap_point[1] if mcap_point else None
                    })
                
                if all_price_data:
                    latest_date = max(d["date"] for d in all_price_data if d["coin_id"] == coin_id)
                    updated_last_prices[coin_id] = latest_date
                else:
                    updated_last_prices[coin_id] = last_date
                
                print(f"✓ ({len([d for d in all_price_data if d['coin_id'] == coin_id])} new days)")
            else:
                print("✗ (no data)")
                if last_date:
                    updated_last_prices[coin_id] = last_date
                    
        except Exception as e:
            print(f"✗ ({str(e)[:50]})")
            if last_date:
                updated_last_prices[coin_id] = last_date
        
        # Rate limiting is now handled by the decorator
    
    save_state("prices", {
        "last_prices": updated_last_prices,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_coins": len(updated_last_prices)
    })
    
    if all_price_data:
        return pa.Table.from_pylist(all_price_data)
    else:
        return pa.Table.from_pylist([{
            "date": datetime.now(timezone.utc).date().isoformat(),
            "coin_id": "dummy",
            "price_usd": 0.0,
            "volume_24h": 0.0,
            "market_cap": 0.0
        }]).slice(0, 0)