"""
### Pipeline modelo: vendas (LND -> RAW -> TRS)

- **LND**: extrai o dado como veio da fonte (CSV), com hash e metadados.
- **RAW**: preserva o dado original (tudo string), normaliza nomes das colunas
  e grava Parquet particionado por `dt_ref`.
- **TRS**: aplica nomes finais e tipos definidos em `schema_tables.py`;
  registros inválidos vão para `trs_rejeitados/`.

Reprocessar uma data sobrescreve apenas a partição `dt_ref` (idempotente).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from modelo_pipeline.flow.finish_group_flow import build_finish_group
from modelo_pipeline.flow.landing_group_flow import build_landing_group
from modelo_pipeline.flow.prepare_group_flow import build_prepare_group
from modelo_pipeline.flow.raw_group_flow import build_raw_group
from modelo_pipeline.flow.trusted_group_flow import build_trusted_group
from modelo_pipeline.tools.const import pipeline as P
from modelo_pipeline.tools.const import tasks as T

default_args = {
    "owner": "dados",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id=P.DAG_ID,
    description="Pipeline modelo LND -> RAW -> TRS",
    start_date=datetime(2024, 1, 1),
    schedule=P.SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["modelo", "vendas", "spark", f"v{P.PIPELINE_VERSAO}"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id=T.START.id)
    end = EmptyOperator(task_id=T.END.id)

    start >> build_prepare_group() >> build_landing_group() >> build_raw_group() \
        >> build_trusted_group() >> build_finish_group() >> end
