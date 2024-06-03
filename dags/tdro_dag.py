from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime

@dag(
    start_date=datetime(2024, 5, 1),
    schedule=None,
    catchup=False,
)
def tdro_dag():

    TriggerDagRunOperator(
        task_id="tdro_task",
        trigger_dag_id="downstream_dag",
        wait_for_completion=True,
        poke_interval=5,
        deferrable=True,
        conf={"sleep_time": 5},
    )

tdro_dag()