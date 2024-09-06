from datetime import datetime as dt
import pathlib

from airflow.decorators import dag, task
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator


@dag(
    schedule=None,
    start_date=dt(2024, 9, 1, 0, 0, 0),
    catchup=False,
    template_searchpath=[pathlib.Path(__file__).absolute().parent / "sql"],
    tags=["webshop", "clickstream"],
)
def compute_full_perf_metrics():
    @task
    def mic_check():
        print("Hello from DoubleCloud")

    select_stmnt = ClickHouseOperator(
        task_id='select_stmnt',
        sql='compute_full_args.sql',
        clickhouse_conn_id='ch_default',
    )

    mic_check() >> select_stmnt


my_dag = compute_full_perf_metrics()


if __name__ == '__main__':
    my_dag.test()
