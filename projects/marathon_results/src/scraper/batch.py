"""
Batch scraper for processing multiple races and years.

Handles:
- Reading race URLs and missing years from CSV files
- Incremental saving of results
- Restart/resume capability
"""

import csv
import os
import re
from pathlib import Path
from typing import Set, Tuple, Dict, List, Optional
from .scraper import scrape_race_results


def _url_details_to_results(url: str) -> str:
    """
    Convert a marathonguide.com details URL to a results URL.

    Args:
        url: URL ending in /details/

    Returns:
        URL ending in /results/

    Example:
        >>> _url_details_to_results('https://www.marathonguide.com/races/run/boston-marathon-26/2026/details/')
        'https://www.marathonguide.com/races/run/boston-marathon-26/2026/results/'
    """
    return url.replace('/details/', '/results/')


def _extract_race_slug_from_url(url: str) -> Optional[str]:
    """
    Extract race slug from marathonguide.com URL.

    Args:
        url: marathonguide.com URL

    Returns:
        Race slug or None if not found

    Example:
        >>> _extract_race_slug_from_url('https://www.marathonguide.com/races/run/boston-marathon-26/2026/details/')
        'boston-marathon-26'
    """
    pattern = r'/races/run/([^/]+)/'
    match = re.search(pattern, url)
    return match.group(1) if match else None


def _normalize_race_name(race_name: str) -> str:
    """
    Normalize race name for comparison.

    Args:
        race_name: Raw race name

    Returns:
        Normalized race name (lowercase, underscores)
    """
    return race_name.lower().replace(' ', '_').replace("'", '')


def read_race_urls(csv_path: str) -> Dict[str, str]:
    """
    Read race URLs from CSV file.

    Args:
        csv_path: Path to races CSV file (race,url format)

    Returns:
        Dictionary mapping normalized race names to URLs
    """
    race_urls = {}

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            race = row.get('race', '') or ''
            url = row.get('url', '') or ''

            race = race.strip()
            url = url.strip()

            if race and url:
                normalized_race = _normalize_race_name(race)
                race_urls[normalized_race] = _url_details_to_results(url)

    return race_urls


def read_missing_years(csv_path: str) -> Dict[str, List[int]]:
    """
    Read missing years from CSV file.

    Args:
        csv_path: Path to missing years CSV file (race,missing_year,expected_participants format)

    Returns:
        Dictionary mapping normalized race names to lists of missing years
    """
    missing_years = {}

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            race = row.get('race', '') or ''
            year = row.get('missing_year', '') or ''

            race = race.strip()
            year = year.strip()

            if race and year:
                normalized_race = _normalize_race_name(race)
                year_int = int(year)

                if normalized_race not in missing_years:
                    missing_years[normalized_race] = []
                missing_years[normalized_race].append(year_int)

    return missing_years


def read_already_scraped(output_path: str) -> Set[Tuple[str, int]]:
    """
    Read already scraped race/year combinations from output CSV.

    Args:
        output_path: Path to output CSV file

    Returns:
        Set of (race_name, year) tuples that have been scraped
    """
    if not os.path.exists(output_path):
        return set()

    scraped = set()

    with open(output_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            race_name = row.get('race_name', '').strip()
            race_date = row.get('race_date', '').strip()

            if race_name and race_date:
                # Extract year from race_date (YYYY-MM-DD format)
                year = int(race_date[:4]) if len(race_date) >= 4 else None
                if year:
                    scraped.add((race_name, year))

    return scraped


def get_scraping_plan(
    race_urls_path: str,
    missing_years_path: str,
    output_path: str
) -> List[Tuple[str, str, List[int]]]:
    """
    Generate scraping plan based on what needs to be scraped and what's already done.

    Args:
        race_urls_path: Path to races CSV file
        missing_years_path: Path to missing years CSV file
        output_path: Path to output CSV file (to check what's already scraped)

    Returns:
        List of (race_name, url, years_to_scrape) tuples
    """
    # Read input data
    race_urls = read_race_urls(race_urls_path)
    missing_years = read_missing_years(missing_years_path)
    already_scraped = read_already_scraped(output_path)

    # Build scraping plan
    plan = []

    for race_name, url in race_urls.items():
        if race_name in missing_years:
            years_needed = missing_years[race_name]

            # Filter out already scraped years
            # Note: we need to match on the actual race name from the scraped data
            # For now, we'll check if any variation has been scraped
            years_to_scrape = []
            for year in years_needed:
                # Check various forms of the race name
                if not any((scraped_race, year) in already_scraped
                          for scraped_race in [race_name, race_name.replace('_', ' ')]):
                    years_to_scrape.append(year)

            if years_to_scrape:
                plan.append((race_name, url, sorted(years_to_scrape)))

    return plan


def batch_scrape_races(
    race_urls_path: str,
    missing_years_path: str,
    output_path: str,
    per_page: int = 100,
    request_delay: float = 0.5,
    max_retries: int = 5,
    verbose: bool = True
) -> None:
    """
    Batch scrape multiple races and years with restart capability.

    Args:
        race_urls_path: Path to races CSV file (race,url format)
        missing_years_path: Path to missing years CSV file (race,missing_year,expected_participants format)
        output_path: Path to output CSV file (will be created/appended to)
        per_page: Number of results per page (default: 100, max: 100)
        request_delay: Delay in seconds between requests to avoid rate limiting (default: 0.5)
        max_retries: Maximum number of retries for rate limit errors (default: 5)
        verbose: Print progress messages (default: True)

    Example:
        >>> batch_scrape_races(
        ...     'data/races_top85.csv',
        ...     'data/missing_race_years_top85.csv',
        ...     'data/marathon_results.csv'
        ... )
    """
    # Get scraping plan
    plan = get_scraping_plan(race_urls_path, missing_years_path, output_path)

    if not plan:
        if verbose:
            print("No races to scrape. All up to date!")
        return

    # Print plan summary
    if verbose:
        print(f"Scraping Plan:")
        print(f"=" * 70)
        total_years = sum(len(years) for _, _, years in plan)
        print(f"Races to scrape: {len(plan)}")
        print(f"Total race-years: {total_years}\n")

        for i, (race_name, url, years) in enumerate(plan, 1):
            print(f"{i}. {race_name}: {len(years)} year(s) - {years}")
        print()

    # Create output file if it doesn't exist
    file_exists = os.path.exists(output_path)

    # Open output file in append mode
    with open(output_path, 'a', newline='') as f:
        fieldnames = ['name', 'age', 'sex', 'hometown_city', 'hometown_state',
                      'time', 'race_name', 'race_date']
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        # Write header if file is new
        if not file_exists:
            writer.writeheader()

        # Execute scraping plan
        for race_idx, (race_name, url, years) in enumerate(plan, 1):
            if verbose:
                print(f"\n[{race_idx}/{len(plan)}] Scraping {race_name}")
                print(f"  Years: {years}")
                print(f"  URL: {url}")

            for year_idx, year in enumerate(years, 1):
                if verbose:
                    print(f"  [{year_idx}/{len(years)}] Year {year}...", end=' ', flush=True)

                try:
                    count = 0
                    for result in scrape_race_results(
                        url,
                        year=year,
                        per_page=per_page,
                        request_delay=request_delay,
                        max_retries=max_retries
                    ):
                        writer.writerow(result)
                        count += 1

                        # Flush every 100 records to ensure data is saved
                        if count % 100 == 0:
                            f.flush()

                    if verbose:
                        print(f"✓ {count:,} records")

                except Exception as e:
                    if verbose:
                        print(f"\n✗ Error: {e}")
                    # For rate limit errors after max retries, stop completely
                    if "Rate limit exceeded" in str(e):
                        if verbose:
                            print(f"\n{'=' * 70}")
                            print(f"STOPPED: Rate limit exceeded.")
                            print(f"Progress saved to: {output_path}")
                            print(f"Wait a few minutes and run again to resume.")
                        raise  # Stop execution
                    # Continue with next year for other errors

    if verbose:
        print(f"\n{'=' * 70}")
        print(f"Scraping complete! Results saved to: {output_path}")
