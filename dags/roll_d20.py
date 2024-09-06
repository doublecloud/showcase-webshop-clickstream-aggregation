import random
from datetime import datetime as dt

from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=dt(2024, 9, 1, 0, 0, 0),
    catchup=False,
    tags=["sample"],
)
def roll_d20():
    @task
    def dice_roll():
        return random.randint(1, 20)

    @task
    def roll_result(roll_value):
        print("Hello from DoubleCloud")
        print("You rolled", roll_value)

    roll_result(roll_value=dice_roll())


my_dag = roll_d20()


if __name__ == '__main__':
    my_dag.test()
