# Astro Docker Image comes with AirFlow Python libraries
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime

from include.stock_market.tasks import _get_stock_prices

# astro dev run tasks test <dag id> <task id> <year-month-day>

SYMBOL = 'NVDA'

@dag(
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    # run DAG for every time it was supposed to run since start date to now
    catchup=False,
    tags=['stock_market']
)

# dag id
def stock_market():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests

        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)

        response = requests.get(url, headers=api.extra_dejson['headers'])
        
        # tells if API is available (when is None)
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    # use traditional operator b/c going to use Docker operator later (want to avoid mixing)
    get_stock_prices = PythonOperator(
        task_id = 'get_stock_prices',
        python_callable = _get_stock_prices,

        # parameters for python_callable (function found in include/stock_market)
        # url is templated with {{<value>}}, this means the value is only evaluated when it is ran
        op_kwargs = {'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL}
    )
    
    is_api_available() >> get_stock_prices

stock_market()