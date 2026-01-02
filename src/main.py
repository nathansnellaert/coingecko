import argparse
import os


from subsets_utils import validate_environment
from ingest import coins as ingest_coins
from ingest import prices as ingest_prices
from transforms import coins as transform_coins
from transforms import prices as transform_prices


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        print("\n--- Ingesting coins ---")
        ingest_coins.run()
        print("\n--- Ingesting prices ---")
        ingest_prices.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        print("\n--- Transforming coins ---")
        transform_coins.run()
        print("\n--- Transforming prices ---")
        transform_prices.run()


if __name__ == "__main__":
    main()
