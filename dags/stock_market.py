from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from include.stock_market.tasks import _get_stock_prices,_store_stock_prices
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
            return PokeReturnValue(is_done=False, xcom_value=None)


    get_stock_prices = python_task = PythonOperator(
        task_id="get_stock_data",
        python_callable= _get_stock_prices,
        op_kwargs = {'url' : '{{ task_instance.xcom_pull(task_ids="is_api_available") }}', 'symbol' : SYMBOL}
    )


    store_stock_prices = PythonOperator(
        task_id = "store_stock_prices", 
        python_callable = _store_stock_prices,
        op_kwargs = {'stock' : '{{task_instance.xcom_pull(task_ids="get_stock_prices")}}'}
    )
    # Set task dependencies
    is_api_available() >> get_stock_prices
    get_stock_prices >> store_stock_prices


stock_market_etl()
