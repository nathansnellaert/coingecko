# Add parent directory (connector root) to path for utils

import httpx
from subsets_utils import get
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception


class CoinNotFoundError(Exception):
    """Raised when a coin is not found (404) - should not be retried."""
    pass


def should_retry(exception):
    """Only retry on transient errors, not permanent failures like 404."""
    if isinstance(exception, CoinNotFoundError):
        return False
    if isinstance(exception, (httpx.HTTPStatusError, httpx.RequestError)):
        return True
    return False


# CoinGecko public API: 5-15 calls/minute, but free tier is more restricted
# Use 3 calls/minute to be very conservative and avoid 429s
@sleep_and_retry
@limits(calls=3, period=60)
@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=10, max=120),
    retry=retry_if_exception(should_retry),
    reraise=True
)
def rate_limited_get(url, params=None):
    response = get(url, params=params)
    if response.status_code == 404:
        raise CoinNotFoundError(f"Coin not found (404)")
    if response.status_code == 429:
        raise httpx.HTTPStatusError(f"Rate limited", request=response.request, response=response)
    if response.status_code != 200:
        raise httpx.HTTPStatusError(f"API request failed with status {response.status_code}", request=response.request, response=response)
    return response
