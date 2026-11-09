"""
Fábrica de DAGs de produto (BIZ): cria UMA DAG por pasta de dags/produto/ com dag_params.yaml.
(A ingestão tem a fábrica dela: dags/ingestao/_dag_factory_ingestao.py.)

Tasks (a DAG só orquestra; o cabeçalho de dataflow_commons/config_produto.py documenta o YAML):
  transform_biz  obrigatória: notebook_biz.py lê a TRS, aplica as regras de negócio e grava na BIZ;
                 publica o Dataset da saída (biz://...)
  load_dw        opcional:    recarrega a tabela do DW (YAML) com a saída da transform_biz;
                 credenciais da Connection 'conexao' (padrão mssql_dw), DDL em ddl_dw.sql

Agendamento pelos Datasets da TRS (schedule.datasets): condicao 'todos' (padrão) ou 'qualquer',
e, com schedule.cron, também no horário (DatasetOrTimeSchedule).
dag_id = <dominio>__<produto> (gerado de params_dag; a pasta tem esse nome).
"""
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.datasets import Dataset, DatasetAll, DatasetAny
from airflow.operators.python import PythonOperator
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

from dataflow_commons.config_produto import (NOTEBOOK_BIZ, TASK_DW, TASK_TRANSFORM, carregar_config_produto,
                                             derivar_dag_id_produto)
from dataflow_commons.runner import run_biz, run_load_dw

log = logging.getLogger(__name__)


def montar_schedule(schedule: dict):
    datasets = [Dataset(uri) for uri in schedule["datasets"]]
    # sempre DatasetAll/DatasetAny, mesmo com um só: o gráfico mostra o Dataset de entrada com a condição
    condicao = DatasetAll(*datasets) if schedule["condicao"] == "todos" else DatasetAny(*datasets)
    if schedule["cron"]:
        return DatasetOrTimeSchedule(timetable=CronTriggerTimetable(schedule["cron"], timezone="UTC"),
                                     datasets=condicao)
    return condicao


def construir_dag(cfg: dict, pkg_dir: Path) -> DAG:
    p, tasks = cfg["params_dag"], cfg["tasks"]
    base_kwargs = {"dt_ref": "{{ ds }}", "run_id": "{{ run_id }}"}
    with DAG(
        dag_id=derivar_dag_id_produto(p),
        description=f"Produto {p['dominio']}/{p['produto']}",
        schedule=montar_schedule(cfg["schedule"]),
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": p.get("owner", "dados"),
            "retries": p.get("retries", 2),
            "retry_delay": timedelta(minutes=5),
            "execution_timeout": timedelta(minutes=p.get("execution_timeout_min", 60)),
        },
        tags=["produto", *p.get("tags", [])],
        doc_md=f"Gerada de `{pkg_dir.name}/dag_params.yaml`. Veja o README da pasta.",
    ) as dag:
        transform = tasks[TASK_TRANSFORM]
        saida = transform["saida"]
        transformar = PythonOperator(
            task_id=TASK_TRANSFORM,
            python_callable=run_biz,
            op_kwargs={"pkg_dir": str(pkg_dir), "notebook": NOTEBOOK_BIZ, "saida": saida, **base_kwargs},
            outlets=[Dataset(saida)],
            **({"retries": transform["retries"]} if transform["retries"] is not None else {}),
        )
        if TASK_DW in tasks:
            dw = tasks[TASK_DW]
            gravar_dw = PythonOperator(
                task_id=TASK_DW,
                python_callable=run_load_dw,
                op_kwargs={"origem": saida, "tabela": dw["tabela"], "conexao": dw["conexao"],
                           "pkg_dir": str(pkg_dir), **base_kwargs},
                **({"retries": dw["retries"]} if dw["retries"] is not None else {}),
            )
            transformar >> gravar_dw
    return dag


for _cfg_path in sorted(Path(__file__).parent.glob("*/dag_params.yaml")):
    if _cfg_path.parent.name.startswith("_"):
        continue
    try:
        _cfg = carregar_config_produto(_cfg_path)    # valida chaves, URIs, tasks e arquivos
        globals()[derivar_dag_id_produto(_cfg["params_dag"])] = construir_dag(_cfg, _cfg_path.parent)
    except Exception:                               # um config quebrado não derruba as outras DAGs
        log.exception("Falha ao criar DAG de produto a partir de %s", _cfg_path)
