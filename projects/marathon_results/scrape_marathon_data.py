#!/usr/bin/env python3
"""
Main script to scrape marathon results based on CSV inputs.

This script:
1. Reads race URLs from data/races_top85.csv
2. Reads missing years from data/missing_race_years_top85.csv
3. Scrapes all missing race-year combinations
4. Saves results incrementally to data/marathon_results.csv
5. Can be restarted - will skip already scraped data

Usage:
    python scrape_marathon_data.py

The script will automatically resume from where it left off if interrupted.
"""

from src.scraper import batch_scrape_races
import sys

def main():
    # File paths
    races_file = 'data/races_top85.csv'
    missing_years_file = 'data/missing_race_years_top85.csv'
    output_file = 'data/marathon_results.csv'

    # Rate limiting configuration
    # Increase request_delay if you're still getting rate limited
    request_delay = 0.5  # seconds between requests
    max_retries = 5      # retries before giving up on rate limit errors
    per_page = 100       # results per page (max 100)

    print("Marathon Results Batch Scraper")
    print("=" * 70)
    print(f"Reading race URLs from: {races_file}")
    print(f"Reading missing years from: {missing_years_file}")
    print(f"Output will be saved to: {output_file}")
    print()
    print("Rate Limiting Configuration:")
    print(f"  - Delay between requests: {request_delay}s")
    print(f"  - Max retries on rate limit: {max_retries}")
    print(f"  - Results per page: {per_page}")
    print()
    print("This script is restartable - you can stop it at any time")
    print("and it will resume from where it left off when restarted.")
    print()
    print("If rate limited (429 errors), the script will:")
    print("  1. Automatically retry with exponential backoff (2s, 4s, 8s, 16s, 32s)")
    print(f"  2. After {max_retries} retries, stop and save progress")
    print("  3. You can then restart to continue")
    print()
    
    # Confirm before starting
    try:
        response = input("Continue? (y/n): ")
        if response.lower() not in ['y', 'yes']:
            print("Cancelled.")
            sys.exit(0)
    except KeyboardInterrupt:
        print("\nCancelled.")
        sys.exit(0)
    
    print()
    
    # Run batch scraper
    try:
        batch_scrape_races(
            races_file,
            missing_years_file,
            output_file,
            per_page=per_page,
            request_delay=request_delay,
            max_retries=max_retries,
            verbose=True
        )
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        print(f"Progress has been saved to {output_file}")
        print("Run this script again to resume.")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nError: {e}")
        print(f"Progress has been saved to {output_file}")
        print("Run this script again to resume.")
        sys.exit(1)

if __name__ == '__main__':
    main()
