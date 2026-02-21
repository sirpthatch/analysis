"""
Marathon results scraper for marathonguide.com

Extracts race results from marathonguide.com race results pages,
handling pagination and parsing racer data into structured records.
"""

import re
import time
from typing import Optional, Iterator, Union, List
from urllib.parse import urlparse
import requests


def _parse_location(location: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Parse location string into city and state.

    Args:
        location: Location string in format "City, State" or "City, ST"

    Returns:
        Tuple of (city, state) both in lowercase, or (None, None) if parsing fails
    """
    if not location:
        return None, None

    # Split on comma
    parts = [p.strip() for p in location.split(',')]

    if len(parts) >= 2:
        city = parts[0].lower() if parts[0] else None
        state = parts[1].lower() if parts[1] else None
        return city, state
    elif len(parts) == 1:
        # Only city provided
        return parts[0].lower() if parts[0] else None, None

    return None, None


def _parse_racer_record(racer: dict, race_name: Optional[str]) -> dict:
    """
    Parse a single racer record into the desired format.

    Args:
        racer: Dictionary containing racer data from API
        race_name: Name of the race

    Returns:
        Dictionary with parsed racer data (all strings in lowercase)
    """
    # Parse location
    city, state = _parse_location(racer.get('location'))

    # Get name (required field, default to empty string)
    name = racer.get('full_name', '').lower() if racer.get('full_name') else None

    # Get time (use chip_time if available, otherwise final_time)
    time = racer.get('chip_time') or racer.get('final_time')
    time = time.lower() if time else None

    # Get optional fields
    age = racer.get('age')
    sex = racer.get('sex', '').lower() if racer.get('sex') else None

    # Get race_date from the racer record itself
    race_date = racer.get('race_date', '').lower() if racer.get('race_date') else None

    return {
        'name': name,
        'age': age,
        'sex': sex,
        'hometown_city': city,
        'hometown_state': state,
        'time': time,
        'race_name': race_name.lower() if race_name else None,
        'race_date': race_date,
    }


def _extract_race_info(url: str) -> tuple[Optional[str], Optional[int]]:
    """
    Extract race slug and year from marathonguide.com URL.

    Args:
        url: marathonguide.com race results URL

    Returns:
        Tuple of (race_slug, year) or (None, None) if parsing fails

    Example:
        >>> _extract_race_info('https://www.marathonguide.com/races/run/boston-marathon-22/2025/results/')
        ('boston-marathon-22', 2025)
    """
    # Pattern: /races/run/{slug}/{year}/results/
    pattern = r'/races/run/([^/]+)/(\d{4})/results'
    match = re.search(pattern, url)

    if match:
        slug = match.group(1)
        year = int(match.group(2))
        return slug, year

    return None, None


def _scrape_single_year(
    race_slug: str,
    year: int,
    per_page: int = 100,
    request_delay: float = 0.5,
    max_retries: int = 5
) -> Iterator[dict]:
    """
    Scrape race results for a single year.

    Args:
        race_slug: Race slug from marathonguide.com URL
        year: Year of the race
        per_page: Number of results to fetch per page (max 100)
        request_delay: Delay in seconds between requests (default: 0.5)
        max_retries: Maximum number of retries for rate limit errors (default: 5)

    Yields:
        Dictionary for each racer

    Raises:
        requests.HTTPError: If API request fails after all retries
    """
    # Build Runzy API URL
    api_url = f"https://back.runzy.com/mg/event-results/{race_slug}/"

    # Headers to avoid 403 Forbidden
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.marathonguide.com/",
        "Origin": "https://www.marathonguide.com",
        "Accept": "application/json",
    }

    # Pagination variables
    page = 1
    total_pages = None
    race_name = None

    while True:
        # Build API params
        params = {
            "subevent": "all",
            "gender": "all",
            "age_group": "all",
            "page": page,
            "limit": per_page,
            "order_by": "over_all_place",
            "order_dir": "asc",
            "year": year
        }

        # Fetch page with retry logic for rate limiting
        retry_count = 0
        while retry_count <= max_retries:
            try:
                response = requests.get(api_url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()
                break  # Success, exit retry loop
            except requests.HTTPError as e:
                if e.response.status_code == 429:  # Rate limited
                    retry_count += 1
                    if retry_count > max_retries:
                        raise Exception(
                            f"Rate limit exceeded after {max_retries} retries. "
                            f"The scraper has been rate limited by the API. "
                            f"Please wait a few minutes and restart to continue. "
                            f"All progress has been saved."
                        ) from e

                    # Exponential backoff: 2, 4, 8, 16, 32 seconds
                    wait_time = 2 ** retry_count
                    print(f"\n  Rate limited (429). Waiting {wait_time}s before retry {retry_count}/{max_retries}...",
                          end='', flush=True)
                    time.sleep(wait_time)
                else:
                    # Other HTTP error, don't retry
                    raise

        # Add delay between requests to avoid rate limiting
        if page > 1:  # Don't delay on first page
            time.sleep(request_delay)

        # Get race name and pagination info from first page
        if page == 1:
            # Race name is in master_event
            master_event = data.get('master_event', {})
            race_name = master_event.get('name')

            pagination = data.get('pagination', {})
            total_pages = pagination.get('last_page', 1)

        # Get results for this page
        results = data.get('results', [])

        if not results:
            break

        # Yield parsed records
        for racer in results:
            record = _parse_racer_record(racer, race_name)
            # Only yield if we have a time (required field)
            if record['time']:
                yield record

        # Check if we've reached the last page
        if total_pages and page >= total_pages:
            break

        page += 1


def scrape_race_results(
    url: str,
    year: Optional[int] = None,
    years: Optional[List[int]] = None,
    per_page: int = 100,
    request_delay: float = 0.5,
    max_retries: int = 5
) -> Iterator[dict]:
    """
    Scrape all race results from a marathonguide.com results page.

    Handles pagination automatically to fetch all results via the Runzy API.
    Can override the year in the URL or scrape multiple years.
    Includes retry logic for rate limiting.

    Args:
        url: URL of the race results page (marathonguide.com)
        year: Optional year to override the year in the URL
        years: Optional list of years to scrape (cannot be used with year parameter)
        per_page: Number of results to fetch per page (max 100)
        request_delay: Delay in seconds between requests to avoid rate limiting (default: 0.5)
        max_retries: Maximum number of retries for rate limit errors (default: 5)

    Yields:
        Dictionary for each racer with fields:
        - name: str (optional)
        - age: int (optional)
        - sex: str (optional)
        - hometown_city: str (optional)
        - hometown_state: str (optional)
        - time: str (required)
        - race_name: str (optional)
        - race_date: str (optional)

    Examples:
        # Use year from URL
        >>> for result in scrape_race_results('https://www.marathonguide.com/races/run/boston-marathon-22/2025/results/'):
        ...     print(result)

        # Override year
        >>> for result in scrape_race_results('https://www.marathonguide.com/races/run/boston-marathon-22/2025/results/', year=2024):
        ...     print(result)

        # Scrape multiple years
        >>> for result in scrape_race_results('https://www.marathonguide.com/races/run/boston-marathon-22/2025/results/', years=[2023, 2024, 2025]):
        ...     print(result)

    Raises:
        ValueError: If URL format is invalid or both year and years are provided
        requests.HTTPError: If API request fails
    """
    # Validate parameters
    if year is not None and years is not None:
        raise ValueError("Cannot specify both 'year' and 'years' parameters")

    # Extract race info from URL
    race_slug, url_year = _extract_race_info(url)

    if not race_slug:
        raise ValueError(f"Invalid marathonguide.com URL format: {url}")

    # Determine which year(s) to scrape
    if years is not None:
        # Scrape multiple years
        years_to_scrape = years
    elif year is not None:
        # Override with specified year
        years_to_scrape = [year]
    else:
        # Use year from URL
        if not url_year:
            raise ValueError(f"Could not extract year from URL: {url}")
        years_to_scrape = [url_year]

    # Scrape each year
    for year_to_scrape in years_to_scrape:
        yield from _scrape_single_year(race_slug, year_to_scrape, per_page, request_delay, max_retries)
