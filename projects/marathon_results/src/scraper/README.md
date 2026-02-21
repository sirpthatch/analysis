# Marathon Results Scraper

A Python scraper for extracting marathon race results from marathonguide.com.

## Features

- Extracts racer data: name, age, sex, hometown (city/state), time, race name, and race date
- Automatic pagination handling to fetch all results
- All strings converted to lowercase
- Direct API access for fast, reliable scraping

## Installation

The scraper requires the `requests` library:

```bash
pip install requests
```

## Usage

### Basic Usage

```python
from src.scraper import scrape_race_results

url = 'https://www.marathonguide.com/races/run/boston-marathon-22/2025/results/'

# Use year from URL (2025)
for result in scrape_race_results(url):
    print(result)
```

### Override Year

You can override the year in the URL to scrape a different year of the same race:

```python
from src.scraper import scrape_race_results

url = 'https://www.marathonguide.com/races/run/boston-marathon-22/2025/results/'

# Scrape 2024 results instead
for result in scrape_race_results(url, year=2024):
    print(result)
```

### Scrape Multiple Years

You can scrape multiple years of the same race in a single call:

```python
from src.scraper import scrape_race_results

url = 'https://www.marathonguide.com/races/run/boston-marathon-22/2025/results/'

# Scrape 2023, 2024, and 2025 results
for result in scrape_race_results(url, years=[2023, 2024, 2025]):
    print(f"{result['race_date']}: {result['name']} - {result['time']}")
```

**Note:** When using `years`, results are yielded sequentially by year (all 2023 results, then all 2024 results, etc.).

### Save to CSV

```python
import csv
from src.scraper import scrape_race_results

url = 'https://www.marathonguide.com/races/run/boston-marathon-22/2025/results/'

with open('results.csv', 'w', newline='') as f:
    fieldnames = ['name', 'age', 'sex', 'hometown_city', 'hometown_state', 
                  'time', 'race_name', 'race_date']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    
    for result in scrape_race_results(url, per_page=100):
        writer.writerow(result)
```

### Scrape Multiple Races and Years

```python
from src.scraper import scrape_race_results
import csv

# Define races to scrape
races = [
    {
        'url': 'https://www.marathonguide.com/races/run/boston-marathon-22/2025/results/',
        'years': [2023, 2024, 2025]
    },
    {
        'url': 'https://www.marathonguide.com/races/run/new-york-city-marathon/2024/results/',
        'years': [2022, 2023, 2024]
    },
]

with open('all_results.csv', 'w', newline='') as f:
    fieldnames = ['name', 'age', 'sex', 'hometown_city', 'hometown_state',
                  'time', 'race_name', 'race_date']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    for race in races:
        print(f"Scraping {race['url']} for years {race['years']}...")
        for result in scrape_race_results(race['url'], years=race['years'], per_page=100):
            writer.writerow(result)
```

## Data Fields

Each result dictionary contains:

- `name` (str, optional): Runner's full name in lowercase
- `age` (int, optional): Runner's age
- `sex` (str, optional): Runner's gender ('m' or 'f')
- `hometown_city` (str, optional): Runner's hometown city in lowercase
- `hometown_state` (str, optional): Runner's hometown state in lowercase
- `time` (str, required): Finish time (chip time if available, otherwise final time)
- `race_name` (str, optional): Name of the race in lowercase
- `race_date` (str, optional): Date of the race in YYYY-MM-DD format

**Note:** Only `time` is guaranteed to be present. All other fields are optional and may be `None` if not available in the source data.

## Parameters

The `scrape_race_results()` function accepts the following parameters:

- **url** (str, required): marathonguide.com race results URL
- **year** (int, optional): Override the year in the URL to scrape a different year
- **years** (List[int], optional): Scrape multiple years (cannot be used with `year`)
- **per_page** (int, optional): Number of results per page (default: 100, max: 100)

**Note:** You cannot specify both `year` and `years` parameters simultaneously.

## URL Format

The scraper expects marathonguide.com URLs in this format:

```
https://www.marathonguide.com/races/run/{race-slug}/{year}/results/
```

Example:
```
https://www.marathonguide.com/races/run/boston-marathon-22/2025/results/
```

The year in the URL can be any year (it will be overridden if you use the `year` or `years` parameters).

## Performance

The scraper fetches 100 results per page by default. For a race with 27,000 finishers (like Boston Marathon), this means:
- 270 API requests
- Approximately 30-60 seconds total scraping time

You can adjust the `per_page` parameter (max 100) when calling `scrape_race_results()`.

## Error Handling

The scraper will raise:
- `ValueError`: If the URL format is invalid, or if both `year` and `years` parameters are provided
- `requests.HTTPError`: If the API request fails

## How It Works

1. Extracts the race slug and year from the marathonguide.com URL
2. Calls the Runzy API (back.runzy.com) that powers marathonguide.com
3. Handles pagination automatically until all results are fetched
4. Parses and normalizes each racer's data
5. Returns results as an iterator for memory efficiency
