from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from include.stock_market.tasks import _get_stock_prices
import datetime
import requests

SYMBOL = 'AAPL'

@dag(
    start_date=datetime.datetime(2024, 9, 20),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
    description='ETL Pipeline to extract AAPL Stock Price'
)
def stock_market_etl():

    @task.sensor(poke_interval=60, timeout=3600, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url, headers=api.extra_dejson['headers'], timeout=10)
        print(response.status_code)
        if response.status_code == 404:
            condition = response.json()['finance']['result'] is None
            print(condition)
            print(PokeReturnValue(is_done=condition, xcom_value=url))
            return PokeReturnValue(is_done=condition, xcom_value=url)
        else:
             return PokeReturnValue(is_done=condition, xcom_value=url)
   

    @task
    def get_stock_prices(result: PokeReturnValue, symbol: str):
        print(result)
        print(f"Is Done: {result.is_done}")
        print(f"XCom Value (URL): {result.xcom_value}")
        if result.is_done:
            # Proceed with stock price retrieval
            print(f"Fetching stock prices for {symbol} from {result.xcom_value}")
            _get_stock_prices(result.xcom_value, symbol)
        else:
            print(f"API not available, cannot fetch stock prices for {symbol}")

    # Invoke tasks
    is_api_available_task = is_api_available()
    stock_prices_task = get_stock_prices(is_api_available_task, SYMBOL)

    # Set task dependencies
    is_api_available_task >> stock_prices_task


stock_market_etl()
