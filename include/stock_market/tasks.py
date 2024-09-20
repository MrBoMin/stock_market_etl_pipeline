import requests
from airflow.hooks.base import BaseHook
import json

def _get_stock_prices(url, symbol):
    print(f"Fetching stock prices from: {url}")

    # Fetch API connection details
    api = BaseHook.get_connection('stock_api')
    
    # Construct the correct URL with parameters
    url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"
    
    try:
        # Make the request with a timeout
        response = requests.get(url, headers=api.extra_dejson['headers'], timeout=10)
        
        # Check for HTTP errors
        response.raise_for_status()
        
        # Parse and return the response
        return json.dumps(response.json()['chart']['result'][0])
    
    except requests.exceptions.Timeout:
        print("Request timed out")
        return None
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
