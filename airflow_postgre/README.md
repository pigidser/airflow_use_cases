# Ingesting NY Taxi data to local PostgreSQL database

This repository shows how to use Docker Compose to set up Airflow environment and create a simple pipeline to download data file, convert it to csv format and then load to local database.

# How to run the project

First compose up Airflow:

```bash
docker compose build
docker compose up airflow-init
docker compose up
```

Now compose up PostgreeSQL:

```bash
cd postgresql
docker compose up -d
```

# Lessons Learned:

## Communication between multiple docker-compose projects
(https://stackoverflow.com/questions/38088279/communication-between-multiple-docker-compose-projects)

## See docker networks
```bash
docker network ls
```
Airflow composer creates airflow_default network that we use in PostgreSQL docker-compose as

```yaml
networks:
  airflow:
    external:
      name: airflow_default
```

## How to check if Airflow can connect to PostgreSQL

```bash
# Get airflow-scheduler container id
docker ps
# Connect to container
docker exec -it [container id] bash
# Run python
python
```

```python
from sqlalchemy import create_engine
engine = create_engine("postgresql://root:root@pgdatabase:5432/ny_taxi")
engine.connect()
```

## Using `pgcli` to connect to Postgres

```bash
pgcli -h localhost -p 5432 -u root -d ny_taxi
```