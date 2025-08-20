# NBA Analytics Lakehouse 🏀

A complete data pipeline for NBA analytics using Apache Airflow, dbt, PostgreSQL, and Apache Superset, all orchestrated with Astronomer Astro.

## 🏗️ Project Architecture

This project implements a modern lakehouse for NBA data analytics with the following layers:

- **Ingestion**: Data extraction using `nba_api`
- **Storage**: PostgreSQL as data warehouse
- **Transformation**: dbt for data modeling
- **Orchestration**: Apache Airflow with Astronomer Astro
- **Visualization**: Apache Superset for dashboards
- **Caching**: Redis for performance optimization

## 📋 Prerequisites

- Docker & Docker Compose
- Astro CLI installed
- Python 3.9+
- At least 4GB of available RAM

## 🚀 Setup & Installation

### 1. Clone the repository
```bash
git clone https://github.com/DiegoIpaez/nba-analytics-lakehouse.git
cd nba-analytics-lakehouse
```

### 2. Set up environment variables
```bash
# Create .env file (optional)
cp .env.example .env
```

### 3. Start services
```bash
# Start support services (PostgreSQL, Redis, Superset)
docker-compose up -d

# Start Airflow with Astro
astro dev start
```

### 4. Verify services
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Superset**: http://localhost:3001 (admin/admin)
- **PostgreSQL**: localhost:5433 (postgres/postgres)
- **Redis**: localhost:6379

## 📊 Project Structure

```
├── dags/                          # Airflow DAGs
│   ├── __pycache__/              # Python cache
│   └── models/                   # dbt models
│       ├── intermediate/         # Intermediate models
│       │   ├── int_player_career_stats.sql
│       │   ├── int_team_games.sql
│       │   └── schema.yml
│       ├── marts/               # Final models (data marts)
│       │   ├── player_ranking.sql
│       │   ├── team_season_stats.sql
│       │   └── schema.yml
│       └── staging/             # Staging models
│           ├── stg_player_stats.sql
│           └── schema.yml
├── include/                     # Custom Python modules
│   ├── __pycache__/
│   ├── constants.py             # Project constants
│   ├── fetch_nba_data.py        # NBA API extraction functions
│   ├── load_raw_nba_data.py     # Raw data loading
│   └── redis_client.py          # Redis client
├── plugins/                     # Custom Airflow plugins
├── tests/                       # Project tests
├── sql/                        # Additional SQL scripts
│   ├── create_nba_raw_tables.sql
│   ├── cosmos_dag.py
│   ├── exampledag.py
│   └── nba_elt_dag.py
├── docker-compose.yml          # Complementary services
├── Dockerfile                  # Custom Astro image
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## 🔄 Data Pipeline

### 1. **Extract**
- Uses `nba_api` to obtain data from:
  - Player statistics
  - Team information
  - Game results
  - Rankings and standings

### 2. **Load**
- Raw data is loaded into PostgreSQL
- Redis acts as cache to optimize frequent queries

### 3. **Transform**
The dbt pipeline includes three layers:

#### **Staging** (`staging/`)
- `stg_player_stats.sql`: Player statistics cleaning and normalization

#### **Intermediate** (`intermediate/`)
- `int_player_career_stats.sql`: Career aggregations per player
- `int_team_games.sql`: Team game data processing

#### **Marts** (`marts/`)
- `player_ranking.sql`: Player rankings by different metrics
- `team_season_stats.sql`: Aggregated statistics by season and team

## 📈 Data Models

### Staging Layer
- **stg_player_stats**: Normalized base player statistics

### Intermediate Layer
- **int_player_career_stats**: Calculated career metrics
- **int_team_games**: Processed game data

### Marts Layer
- **player_ranking**: Player rankings and classifications
- **team_season_stats**: Season and team analysis

## 🛠️ Useful Commands

### Astro Development
```bash
# Start development environment
astro dev start

# Stop environment
astro dev stop

# View logs
astro dev logs

# Execute commands in container
astro dev bash
```

### dbt Commands
```bash
# Install dbt dependencies
astro dev bash
source dbt_venv/bin/activate

# Run models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Docker Compose
```bash
# Start complementary services
docker-compose up -d

# View service logs
docker-compose logs -f [service]

# Stop services
docker-compose down
```

## 🔧 Superset Configuration

1. Access http://localhost:3001
2. Login: admin/admin
3. Configure PostgreSQL connection:
   ```
   Host: postgres_dbt
   Port: 5432
   Database: postgres
   Username: postgres
   Password: postgres
   ```

## 📝 Available DAGs

- **nba_elt_dag**: Main NBA data ELT pipeline
- **cosmos_dag**: dbt model execution with Cosmos
- **example_dag**: Example DAG for testing

## 🚨 Troubleshooting

### Common Issues

1. **Port 8080 occupied**
   ```bash
   astro dev stop
   # Change port in airflow_settings.yaml
   astro dev start
   ```

2. **Memory issues**
   - Verify Docker has at least 4GB allocated
   - Close unnecessary applications

3. **PostgreSQL connection issues**
   - Verify service is running: `docker-compose ps`
   - Check logs: `docker-compose logs postgres_dbt`

### Important Logs
```bash
# Airflow logs
astro dev logs scheduler
astro dev logs webserver

# Complementary services logs
docker-compose logs postgres_dbt
docker-compose logs superset
docker-compose logs redis
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
