import requests

def get_target_currencies(base_currency):
    """
    Get a list of target currencies relative to the given base currency.
    
    Parameters:
    - base_currency (str): The base currency for which to retrieve target currencies.
    
    Returns:
    - list of str: A list of target currencies.
    
    Raises:
    - ValueError: If the API request to retrieve currencies fails.
    """
    # API endpoint to retrieve a list of all available currencies
    url = "https://api.frankfurter.app/currencies"

    # Make a GET request to the API
    response = requests.get(url)

    # Check if the API request was successful (status code 200)
    if response.status_code == 200:
        # Extract and return a list of target currencies excluding the base currency
        return [k for k in response.json().keys() if k != base_currency]
    else:
        # If the API request fails, raise a ValueError with an error message
        error_msg = f"Error in currency request: {response.status_code}"
        raise ValueError(error_msg)


def format_exchange_rates(api_response, base_currency, target_currencies):
    """
    Format exchange rates from the API response.

    Parameters:
    - api_response (dict): The API response containing exchange rates.
    - base_currency (str): The base currency for which the exchange rates are provided.
    - target_currencies (list of str): List of target currencies for which to format exchange rates.

    Returns:
    - dict: A dictionary containing formatted exchange rates with date, currency, and exchange rate.

    Example:
    {
        'date': ['2024-03-05', '2024-03-05', ...],
        'currency': ['USD', 'EUR', ...],
        'exchange_rate_to_base_currency': [1.0, 0.85, ...]
    }
    """

    # Construct the name for the exchange rate based on the base currency
    rate_name = f'exchange_rate_to_{base_currency.lower()}'

    # Initialize a dictionary to store formatted exchange rates
    formatted_rates = {'date': [], 'currency': [], rate_name: []}

    # Iterate over dates in the API response
    for date in api_response['rates'].keys():
        # Iterate over target currencies
        for currency in target_currencies:
            # Append date, currency, and corresponding exchange rate to the formatted rates dictionary
            formatted_rates['date'].append(date)
            formatted_rates['currency'].append(currency)
            formatted_rates[rate_name].append(api_response['rates'][date][currency])

    return formatted_rates


def get_currencies_from(base_currency, start_date):
    """
    Get formatted exchange rates for a specific base currency and start date.

    Parameters:
    - base_currency (str): The base currency for which to retrieve exchange rates.
    - start_date (str): The start date from which to retrieve exchange rates.

    Returns:
    - dict: A dictionary containing formatted exchange rates with date, currency, and exchange rate.

    Example:
    {
        'date': ['2024-03-05', '2024-03-05', ...],
        'currency': ['USD', 'EUR', ...],
        'exchange_rate_to_base_currency': [1.0, 0.85, ...]
    }

    Raises:
    - ValueError: If the API request to retrieve exchange rates fails.
    """

    # Get the list of target currencies using the helper function
    target_currencies = get_target_currencies(base_currency)

    # Construct the URL for the API request
    url = f"https://api.frankfurter.app/{start_date}..?from={base_currency}&to={','.join(target_currencies)}"

    # Make a GET request to the API
    response = requests.get(url)

    # Check if the API request was successful (status code 200)
    if response.status_code == 200:
        # Format and return the exchange rates from the API response
        return format_exchange_rates(response.json(), base_currency, target_currencies)
    else:
        # If the API request fails, raise a ValueError with an error message
        error_msg = f"Error in exchange rates request: {response.status_code}"
        raise ValueError(error_msg)