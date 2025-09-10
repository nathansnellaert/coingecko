import os
os.environ['CONNECTOR_NAME'] = 'coingecko'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.coins.coins import process_coins
from assets.prices.prices import process_prices

def main():
    validate_environment()
    
    coins_data = process_coins()
    prices_data = process_prices(coins_data)
    
    upload_data(coins_data, "coingecko_coins")
    upload_data(prices_data, "coingecko_prices")

if __name__ == "__main__":
    main()