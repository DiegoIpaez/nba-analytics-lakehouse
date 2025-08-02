import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from include.fetch_nba_data import fetch_players


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="nba_elt_taskflow_dag",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
    tags=["nba", "elt", "taskflow"],
)
def nba_elt_pipeline():
    """
    DAG to extract, transform, and load NBA player data
    """

    create_tables_op = SQLExecuteQueryOperator(
        task_id="create_nba_raw_tables",
        conn_id="postgres_default",
        sql="sql/create_nba_raw_tables.sql",
        show_return_value_in_logs=True,
    )

    @task()
    def extract_players():
        """Extract players data from API and return as a list of dictionaries"""
        players_df = fetch_players()
        return players_df.to_dict("records")

    @task()
    def insert_players(players_data):
        """Insert players data into raw_players table"""
        players_df = pd.DataFrame(players_data)
        hook = PostgresHook(postgres_conn_id="postgres_default")
        rows = [
            (
                row["id"],
                row["full_name"],
                row.get("first_name", ""),
                row.get("last_name", ""),
                row.get("is_active", False),
            )
            for _, row in players_df.iterrows()
        ]
        hook.insert_rows(
            table="raw_players",
            rows=rows,
            target_fields=["id", "full_name", "first_name", "last_name", "is_active"],
            commit_every=1000,
            replace=False,
        )

    players_data = extract_players()
    insert_task = insert_players(players_data)

    create_tables_op >> insert_task


nba_elt_pipeline()
