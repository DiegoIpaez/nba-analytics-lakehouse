import os

# CONNECTIONS
POSTGRES_CONN_ID = "postgres_default"

# REDIS
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_HOST = os.getenv("REDIS_HOST")

# DBT
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
DBT_NPS_PROJECT_PATH = "/usr/local/airflow/dags/dbt/nba_player_stats"
