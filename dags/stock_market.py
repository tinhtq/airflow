import requests
from airflow.decorators import sensor, dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue
from airflow.providers.docker.operators.docker import DockerOperator
from include.stock_market.tasks import (
    _get_stock_prices,
    _store_prices,
    _get_formatted_csv,
)

SYMBOL = "AAPL"


@dag(
    schedule="@daily",
    start_date=datetime(2024, 11, 21),
    catchup=False,
    tags=["stock_market"],
)
def stock_market():
    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection("stock_api")
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson["headers"])
        condition = response.json()["finance"]["result"] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id="get_stock_prices",
        python_callable=_get_stock_prices,
        op_kwargs={
            "url": '{{ task_instance.xcom_pull(task_ids="is_api_available") }}',
            "symbol": SYMBOL,
        },
    )
    store_prices = PythonOperator(
        task_id="store_prices",
        python_callable=_store_prices,
        op_kwargs={
            "prices": '{{ task_instance.xcom_pull(task_ids="get_stock_prices") }}'
        },
    )
    format_prices = DockerOperator(
        task_id="format_prices",
        image="airflow/spark-app",
        container_name="format_prices",
        api_version="auto",
        auto_remove=True,
        docker_url="tcp://docker-proxy:2375",
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        network_mode="container:spark-master",
        environment={
            "SPARK_APPLICATION_ARGS": '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        },
    )
    get_formatted_csv = PythonOperator(
        task_id="get_formatted_csv",
        python_callable=_get_formatted_csv,
        op_kwargs={"path": '{{ task_instance.xcom_pull(task_ids="format_prices") }}'},
    )
    is_api_available() >> get_stock_prices >> store_prices >> format_prices


stock_market()
