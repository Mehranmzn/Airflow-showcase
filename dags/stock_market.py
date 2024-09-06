from airflow.decorators import dag, task  
from airflow.hooks.base import BaseHook  
from datetime import datetime
import requests
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python_operator import PythonOperator
from include.stock_market.tasks import _get_stock_prices, _store_prices

SYMBOL = 'AAPL'

@dag(
start_date=datetime(2023, 1, 1),
schedule_interval='@daily',
catchup=False,
tags=['stock_market']
)




def stock_market():
    @task.sensor(
        poke_interval = 30,
        timeout = 300,
        mode = 'poke'
    )
    def is_api_availabe()-> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers = api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None

        return PokeReturnValue(is_done = condition, xcom_value = url)


    get_stock_prices = PythonOperator(
        task_id = 'get_stock_prices',
        python_callable = _get_stock_prices,
        op_kwargs = 
        {'url':'{{task_instance.xcom_pull(task_ids = "is_api_availabe")}}',
          'symbol': SYMBOL}
    )

    store_prices = PythonOperator(
        task_id = 'store_prices',
        python_callable = _store_prices,
        op_kwargs = {'stock': '{{ task_instance.xcom_pull(task_ids = "get_stock_prices")}}'}
    )

    is_api_availabe() >> get_stock_prices >> store_prices

stock_market()