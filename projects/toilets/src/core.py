import requests

def address_to_fips(street, city, state):
    """
    Retrieves FIPS codes for a given address using the US Census Geocoding API.

    Args:
        street (str): The street address (e.g., "1600 Pennsylvania Ave NW").
        city (str): The city name (e.g., "Washington").
        state (str): The two-letter state abbreviation (e.g., "DC").

    Returns:
        dict or None: A dictionary containing 'state_fips', 'county_fips', and 'tract_fips' if the address is matched,
                      otherwise None.

    Raises:
        requests.RequestException: If the HTTP request fails.
        KeyError: If the expected keys are not found in the API response.
    """
    url = "https://geocoding.geo.census.gov/geocoder/geographies/address"
    params = {
        'street': street,
        'city': city,
        'state': state,
        'benchmark': 'Public_AR_Current',
        'vintage': 'Current_Current',
        'format': 'json'
    }

    response = requests.get(url, params=params)
    data = response.json()

    if data['result']['addressMatches']:
        geographies = data['result']['addressMatches'][0]['geographies']
        return {
            'state_fips': geographies['States'][0]['STATE'],
            'county_fips': geographies['Counties'][0]['COUNTY'],
            'tract_fips': geographies['Census Tracts'][0]['TRACT']
        }
    return None