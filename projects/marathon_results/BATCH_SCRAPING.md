# Batch Scraping Marathon Results

This document explains how to use the batch scraper to collect marathon results from multiple races and years.

## Quick Start

Run the main scraping script:

```bash
python scrape_marathon_data.py
```

The script will:
1. Read race URLs from `data/races_top85.csv`
2. Read missing years from `data/missing_race_years_top85.csv`
3. Scrape all race-year combinations that need data
4. Save results incrementally to `data/marathon_results.csv`

## Features

### Incremental Saving
Results are saved continuously as they're scraped. If the scraper is interrupted (network error, rate limiting, manual stop), all progress up to that point is preserved.

### Automatic Resume
The scraper automatically detects what's already been scraped by reading the output file. When restarted, it skips race-year combinations that are already present in the output.

### Rate Limit Handling
The scraper includes built-in rate limit handling:

**Automatic Retry**: When rate limited (HTTP 429 error), the scraper will:
1. Automatically retry with exponential backoff (waits 2s, 4s, 8s, 16s, 32s)
2. Try up to 5 times (configurable) before giving up
3. Save all progress before stopping

**Prevention**: The scraper adds a 0.5 second delay between page requests to avoid hitting rate limits in the first place.

**If Still Rate Limited**:
- The scraper will stop and save all progress
- Wait 5-10 minutes for the rate limit to reset
- Run the script again - it will resume from where it left off

**Adjusting Settings**: Edit `scrape_marathon_data.py` to adjust:
```python
request_delay = 1.0  # Increase delay between requests (default: 0.5)
max_retries = 10     # Increase retry attempts (default: 5)
per_page = 50        # Reduce page size (default: 100)
```

## Input Files

### `data/races_top85.csv`
Contains race names and their marathonguide.com URLs.

Format:
```csv
race,url
boston_marathon,https://www.marathonguide.com/races/run/boston-marathon-26/2026/details/
chicago_marathon,https://www.marathonguide.com/races/run/chicago-marathon-26/2026/details/
```

**Notes:**
- Only races with URLs will be scraped
- URLs can end in either `/details/` or `/results/` - the scraper converts automatically
- Race names should match those in `missing_race_years_top85.csv`

### `data/missing_race_years_top85.csv`
Contains which years are missing for each race.

Format:
```csv
race,missing_year,expected_participants
boston_marathon,2024,5000
boston_marathon,2025,5000
chicago_marathon,2023,9000
```

**Notes:**
- Race names must match those in `races_top85.csv`
- The `expected_participants` column is not used by the scraper (for reference only)

## Output File

### `data/marathon_results.csv`
Contains all scraped race results with the following fields:

- `name`: Runner's full name (lowercase)
- `age`: Runner's age (may be null)
- `sex`: Runner's gender - 'm' or 'f' (may be null)
- `hometown_city`: Runner's hometown city (lowercase, may be null)
- `hometown_state`: Runner's hometown state (lowercase, may be null)
- `time`: Finish time (required - chip time if available, otherwise final time)
- `race_name`: Name of the race (lowercase)
- `race_date`: Date of the race in YYYY-MM-DD format

## How It Works

1. **Planning Phase**: 
   - Reads race URLs and missing years
   - Checks output file to see what's already scraped
   - Creates a list of race-year combinations to scrape

2. **Scraping Phase**:
   - For each race-year combination:
     - Calls the marathonguide.com API (via Runzy backend)
     - Handles pagination automatically
     - Saves results immediately to output file
     - Flushes to disk every 100 records

3. **Resume Logic**:
   - When restarted, reads the output file
   - Identifies unique (race_name, year) combinations already present
   - Skips those combinations in the scraping plan

## Stopping and Resuming

### Stop the Scraper
Press `Ctrl+C` at any time. All progress will be saved.

### Resume Scraping
Simply run the script again:
```bash
python scrape_marathon_data.py
```

The scraper will automatically skip what's already been collected.

## Programmatic Usage

You can also use the batch scraper in your own Python scripts:

```python
from src.scraper import batch_scrape_races

batch_scrape_races(
    race_urls_path='data/races_top85.csv',
    missing_years_path='data/missing_race_years_top85.csv',
    output_path='data/marathon_results.csv',
    per_page=100,  # Max 100
    verbose=True   # Print progress
)
```

## Troubleshooting

### "No races to scrape"
This means all race-year combinations have already been scraped. Check:
- Is the output file complete?
- Do you need to add more years to `missing_race_years_top85.csv`?
- Do you need to add more race URLs to `races_top85.csv`?

### Rate Limiting (HTTP 429 errors)
The API may rate limit after scraping large amounts of data. Solutions:
- Wait a few minutes and restart the script
- Reduce `per_page` parameter to make smaller requests
- The scraper will skip completed race-years and retry failed ones

### Missing race in output
Check that:
- The race has a URL in `races_top85.csv`
- The race name matches exactly between the two CSV files (underscores, special characters, etc.)
- The year is listed in `missing_race_years_top85.csv`

## Performance

Scraping speed depends on:
- Number of participants (larger races take longer)
- API rate limits
- Network connection
- `per_page` setting (higher = fewer requests, but same total data)

Typical performance:
- ~100-200 records per second when not rate limited
- Boston Marathon (~27,000 finishers): ~2-5 minutes
- Smaller race (~3,000 finishers): ~15-30 seconds

## Data Quality

The scraper extracts all available fields, but note:
- `time` is the only required field (all others may be null)
- Some races don't collect hometown information
- Some races don't record ages
- Very old race results may have less data

All text fields are automatically converted to lowercase for consistency.
