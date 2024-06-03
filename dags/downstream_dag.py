from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime


@dag(
    start_date=datetime(2024, 5, 1),
    schedule=None,
    catchup=False,
    params={
        "sleep_time": Param(
            10,
            type="number",
        )
    },
)
def downstream_dag():

    @task
    def wait(**context):
        import time

        sleep_time = context["params"]["sleep_time"]

        time.sleep(sleep_time)
        return f"I slept for {sleep_time} seconds"

    wait()


downstream_dag()
