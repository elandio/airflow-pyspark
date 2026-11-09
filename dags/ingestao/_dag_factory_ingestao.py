"""
Fábrica de DAGs de ingestão: cria UMA DAG por pasta de dags/ingestao/ com dag_params.yaml.
(Os produtos têm a fábrica deles: dags/produto/_dag_factory_produto.py.)

Sem 'params_dag.tasks' no YAML:  lnd >> raw >> trs >> finish
Com 'params_dag.tasks': as tasks, dependências e grupos declarados (ver cabeçalho do dag_params.yaml),
e o grupo finish depois das tasks finais.

Cada task executa um notebook_<camada>[_<subbase>].py da pasta da ingestão.
A LND decide o que atualizar e devolve a lista pelo XCom; RAW e TRS a recebem.
finish (só com tudo em sucesso): save_last_update >> clean_lnd.
dag_id = dominio__orgao_origem__fonte_dados__dataset (gerado de params_dag).
Para recarregar tudo: Trigger DAG w/ config e marque "reset".
"""
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from dataflow_commons.config import GRUPO_FINISH, carregar_config, derivar_dag_id, resolver_tasks
from dataflow_commons.runner import run_clean_lnd, run_layer, run_save_last_update

log = logging.getLogger(__name__)


def construir_dag(cfg: dict, pkg_dir: Path) -> DAG:
    p = cfg["params_dag"]
    tasks = resolver_tasks(cfg)
    with DAG(
        dag_id=derivar_dag_id(p),
        description=f"Ingestão {p['dominio']}/{p['orgao_origem']}/{p['fonte_dados']}/{p['dataset']}",
        start_date=datetime(2024, 1, 1),
        schedule=p["schedule"],
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": p.get("owner", "dados"),
            "retries": p.get("retries", 2),
            "retry_delay": timedelta(minutes=5),
            "execution_timeout": timedelta(minutes=p.get("execution_timeout_min", 60)),
        },
        params={"reset": Param(False, type="boolean",
                               description="Ignora a última atualização gravada e recarrega tudo")},
        tags=["ingestao", *p.get("tags", [])],
        doc_md=f"Gerada de `{pkg_dir.name}/dag_params.yaml`. Veja o README da pasta.",
    ) as dag:
        grupos = {nome: TaskGroup(group_id=nome, tooltip=(conf or {}).get("tooltip", ""))
                  for nome, conf in (p.get("task_groups") or {}).items()}
        operadores = {}
        lnd = next(t for t in tasks.values() if t["camada"] == "lnd")
        lnd_id = f"{lnd['task_group']}.{lnd['task_id']}" if lnd["task_group"] else lnd["task_id"]
        update_xcom = "{{ ti.xcom_pull(task_ids='%s') | tojson }}" % lnd_id   # id completo, com o grupo
        base_kwargs = {"pkg_dir": str(pkg_dir), "dt_ref": "{{ ds }}", "run_id": "{{ run_id }}"}
        for tid, t in tasks.items():
            extra = {}
            if t["task_group"]:
                extra["task_group"] = grupos[t["task_group"]]
            if t["retries"] is not None:
                extra["retries"] = t["retries"]
            if t["trigger_rule"]:
                extra["trigger_rule"] = t["trigger_rule"]
            if t["camada"] == "trs":   # sinaliza para as DAGs de produto que esta TRS foi atualizada
                uri = f"trs://{p['dominio']}/{p['orgao_origem']}/{p['fonte_dados']}/{p['dataset']}"
                extra["outlets"] = [Dataset(uri + (f"/{t['subbase']}" if t["subbase"] else ""))]
            if t["camada"] == "lnd":
                camada_kwargs = {"reset": "{{ params.reset }}"}
            else:   # o que a LND devolveu (period_ids + last_update)
                camada_kwargs = {"update": update_xcom}
            operadores[tid] = PythonOperator(
                task_id=tid,
                python_callable=run_layer,
                op_kwargs={"camada": t["camada"], "subbase": t["subbase"], **base_kwargs, **camada_kwargs},
                **extra,
            )
        for tid, t in tasks.items():
            for dependencia in t["dependencies"]:
                operadores[dependencia] >> operadores[tid]

        # finish: só com todas as tasks finais em sucesso; nada novo na LND -> tudo skipped
        finais = [operadores[tid] for tid in tasks
                  if not any(tid in t["dependencies"] for t in tasks.values())]
        with TaskGroup(group_id=GRUPO_FINISH, tooltip="Grava a última atualização e limpa a LND"):
            salvar = PythonOperator(task_id="save_last_update", python_callable=run_save_last_update,
                                    op_kwargs={**base_kwargs, "update": update_xcom})
            limpar = PythonOperator(task_id="clean_lnd", python_callable=run_clean_lnd,
                                    op_kwargs={**base_kwargs, "update": update_xcom})
        finais >> salvar >> limpar
    return dag


for _cfg_path in sorted(Path(__file__).parent.glob("*/dag_params.yaml")):
    if _cfg_path.parent.name.startswith("_"):
        continue
    try:
        _cfg = carregar_config(_cfg_path)           # valida chaves, nomes, tasks e arquivos
        globals()[derivar_dag_id(_cfg["params_dag"])] = construir_dag(_cfg, _cfg_path.parent)
    except Exception:                               # um config quebrado não derruba as outras DAGs
        log.exception("Falha ao criar DAG a partir de %s", _cfg_path)
