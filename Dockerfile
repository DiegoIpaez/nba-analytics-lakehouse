FROM quay.io/astronomer/astro-runtime:13.1.0
USER root
RUN apt-get update && apt-get install libpq-dev python-dev-is-python3 gcc -y
# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && pip install --no-cache-dir dbt-postgres astronomer-cosmos && deactivate