import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    RenderConfig,
    ExecutionConfig,
)

from include.fetch_nba_data import fetch_players, fetch_player_stats
from include.load_raw_nba_data import insert_players, insert_player_game_stats
from include.constants import (
    POSTGRES_CONN_ID,
    DBT_EXECUTABLE_PATH,
    DBT_NPS_PROJECT_PATH,
)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={"schema": "public"},
    ),
)

render_config = RenderConfig(
    emit_datasets=True,
)


@dag(
    dag_id="nba_elt_taskflow_dag",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
    tags=["nba", "elt", "taskflow", "grouped"],
    description="ETL pipeline with grouped tasks for extracting and loading NBA data",
)
def nba_elt_pipeline():
    create_tables = SQLExecuteQueryOperator(
        task_id="create_nba_raw_tables",
        conn_id=POSTGRES_CONN_ID,
        sql="sql/create_nba_raw_tables.sql",
        show_return_value_in_logs=True,
    )

    @task_group(group_id="extract_group")
    def extract_tasks():
        """
        Extract tasks for players and player game stats
        Returns:
            dict: A dictionary containing the extracted players and player game stats
        """

        @task()
        def extract_players():
            players_df = fetch_players()
            return players_df.to_dict("records")

        @task()
        def extract_player_stats(players):
            stats_df = fetch_player_stats(players)
            return stats_df.to_dict("records")

        players = extract_players()
        stats = extract_player_stats(players)

        return {"players": players, "stats": stats}

    @task_group(group_id="load_group")
    def load_tasks(players, stats):
        """
        Load tasks for players and player game stats
        Args:
            players (list): A list of dictionaries containing player metadata.
            stats (list): A list of dictionaries containing player game stats.
        """

        @task()
        def load_players(players):
            insert_players(players)

        @task()
        def load_player_stats(stats):
            insert_player_game_stats(stats)

        load_players(players)
        load_player_stats(stats)

    transform_group = DbtTaskGroup(
        group_id="transform_group",
        project_config=ProjectConfig(DBT_NPS_PROJECT_PATH),
        profile_config=profile_config,
        render_config=render_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
    )

    extracted = extract_tasks()
    (
        create_tables
        >> load_tasks(players=extracted["players"], stats=extracted["stats"])
        >> transform_group
    )


nba_elt_pipeline()
