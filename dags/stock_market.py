# Astro Docker Image comes with AirFlow Python libraries
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from datetime import datetime

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
        condition = response.json()['finance']['result'] is None            # tells if API is available (when is None)
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    is_api_available()

stock_market()