# Airflow + PySpark com Docker

Ambiente de desenvolvimento com Apache Airflow 2.10 (LocalExecutor + Postgres) e PySpark 3.5 em modo local.

## Pré-requisitos
- Docker e Docker Compose

## Como subir

1. Crie o arquivo `.env`:
```bash
   cat > .env << EOT
   AIRFLOW_UID=$(id -u)
   _AIRFLOW_WWW_USER_USERNAME=admin
   _AIRFLOW_WWW_USER_PASSWORD=admin
   EOT
```
2. Construa e inicialize:
```bash
   docker compose build
   docker compose up airflow-init
   docker compose up -d
```
3. Acesse http://localhost:8080 (usuário e senha definidos no `.env`).

## Estrutura
- `dags/`: pipelines do Airflow
- `docker/`: Dockerfile (Airflow + Java + PySpark)
- `plugins/`, `config/`, `data/`: extensões, configurações e dados locais

## DAGs de exemplo
- `hello_airflow`: teste básico
- `pyspark_teste`: job PySpark em modo local
