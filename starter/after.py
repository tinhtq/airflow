from datetime import datetime, timedelta
from airflow.decorators import dag, task
import random


@dag(
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["check-odd"]
)
def task_flow():
    @task()
    def generate_random_number(**context):
        ti = context['ti']
        number = random.randint(1, 100)
        ti.xcom_push(key='random_number', value=number)
        print(f"Generated random number: {number}")

    @task()
    def check_even_odd(**context):
        ti = context['ti']
        number = ti.xcom_pull(task_ids='generate_number', key='random_number')
        result = "even" if number % 2 == 0 else "odd"
        print(f"The number {number} is {result}.")
    check_even_odd(generate_random_number())


task_flow()
