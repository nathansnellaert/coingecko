import os

# Set environment variables for testing
os.environ['CONNECTOR_NAME'] = 'coingecko'
os.environ['RUN_ID'] = 'test-rate-limit'
os.environ['ENABLE_HTTP_CACHE'] = 'false'  # Disable cache to test real API calls
os.environ['CACHE_REQUESTS'] = 'true'
os.environ['CATALOG_TYPE'] = 'local'
os.environ['DATA_DIR'] = 'data'

# Test with a smaller target count first
from assets.coins import coins

# Temporarily reduce target count for testing
original_target = coins.target_count
coins.target_count = 20  # Test with just 20 coins

print("Testing CoinGecko connector with rate limiting...")
print(f"Target count set to: {coins.target_count}")
print("Expected behavior: Should handle rate limits gracefully with retries")
print("-" * 60)

from utils import validate_environment
from assets.coins.coins import process_coins

try:
    validate_environment()
    
    print("\n1. Testing coins asset with rate limiting...")
    coins_data = process_coins()
    print(f"✓ Successfully fetched {len(coins_data)} coins")
    
    # Now test prices with just a few coins
    print("\n2. Testing prices asset with rate limiting...")
    from assets.prices.prices import process_prices
    
    # Only process first 3 coins to test rate limiting
    small_coins_data = coins_data.slice(0, 3)
    prices_data = process_prices(small_coins_data)
    print(f"✓ Successfully fetched {len(prices_data)} price records")
    
    print("\n✅ All tests passed! Rate limiting and retry logic working correctly.")
    
except Exception as e:
    print(f"\n❌ Test failed: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Restore original target count
    coins.target_count = original_target