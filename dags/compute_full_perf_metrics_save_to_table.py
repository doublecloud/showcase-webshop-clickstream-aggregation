import datetime
import logging
import pathlib

from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag, task

from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.providers.common.sql.operators import sql
from airflow_clickhouse_plugin.operators.clickhouse_dbapi import ClickHouseBaseDbApiOperator


class ClickHouseBranchSQLOperator(
    sql.BranchSQLOperator,
    ClickHouseBaseDbApiOperator,
):
    """
    temporary workaround for Airflow < 2.9.4
    see https://github.com/bryzgaloff/airflow-clickhouse-plugin/issues/87
    """

    pass


log = logging.getLogger(__name__)

sql_dir = pathlib.Path(__file__).absolute().parent / "sql"


@dag(
    dag_id=pathlib.Path(__file__).stem,
    schedule=None,
    start_date=datetime.datetime(2024, 9, 1, 0, 0, 0),
    catchup=False,
    template_searchpath=[sql_dir],
    dag_display_name="Compute Webshop Perf Metrcis and save to agg table",
    tags=["webshop", "clickstream"],
    max_active_runs=1,
)
def compute_metrics_save_to_table_dag():
    check_tbl_exists = ClickHouseBranchSQLOperator(
        task_id='check_if_agg_table_exists',
        sql='check_if_agg_table_exists.sql',
        conn_id='ch_default',
        follow_task_ids_if_true='check_if_agg_table_has_rows',
        follow_task_ids_if_false='create_agg_table',
    )

    check_tbl_has_rows = ClickHouseBranchSQLOperator(
        task_id='check_if_agg_table_has_rows',
        sql='check_if_agg_table_has_rows.sql',
        conn_id='ch_default',
        follow_task_ids_if_true='truncate_agg_table',
        follow_task_ids_if_false='compute_insert_aggs',
    )

    create_tbl = ClickHouseOperator(
        task_id='create_agg_table',
        sql='create_agg_table.sql',
        clickhouse_conn_id='ch_default',
    )

    truncate_tbl = ClickHouseOperator(
        task_id='truncate_agg_table',
        sql='truncate_agg_table.sql',
        clickhouse_conn_id='ch_default',
    )

    compute_insert_aggs = ClickHouseOperator(
        task_id='compute_insert_aggs',
        sql='compute_insert_aggs.sql',
        clickhouse_conn_id='ch_default',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check_tbl_exists >> [create_tbl, check_tbl_has_rows]
    check_tbl_has_rows >> [truncate_tbl, compute_insert_aggs]
    [truncate_tbl, create_tbl] >> compute_insert_aggs


my_dag = compute_metrics_save_to_table_dag()


if __name__ == '__main__':
    my_dag.test()
