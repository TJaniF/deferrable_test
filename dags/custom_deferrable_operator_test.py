from airflow.decorators import dag
from pendulum import datetime

from include.deferrable_operator_template import MyOperator

@dag(
    start_date=datetime(2024, 5, 1),
    schedule=None,
    catchup=False,
)
def custom_deferrable_operator_test():

    t1 = MyOperator(
        task_id="t1",
        wait_for_completion=True,
        poke_interval=5,
        deferrable=True,
    )


custom_deferrable_operator_test()
