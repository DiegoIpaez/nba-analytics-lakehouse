import os

# CONNECTIONS
POSTGRES_CONN_ID = "postgres_default"

# REDIS
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_HOST = os.getenv("REDIS_HOST")
