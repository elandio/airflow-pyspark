"""Contexto da execução. Ao carregar, traz TUDO da camada (e da sub-base) em questão.

Ingestão: get_context(camada=...). Produto (BIZ): get_context_biz().

No Airflow vem do runner (PIPELINE_CONTEXT). No notebook, dos argumentos (ou valores locais).
A lista do que processar NÃO é calculada aqui: a LND decide (state.push_update_list) e
RAW/TRS a recebem em ctx.update_list.
"""
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import List, Optional

from .config import CAMADAS, carregar_config, config_da_camada, derivar_dag_id
from .schema import carregar_schema
from .state import validar_update_list

_DATA = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SUBBASE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")


@dataclass
class Context:
    """Contexto de uma task de ingestão (lnd, raw ou trs), criado por get_context()."""

    camada: str                    # lnd | raw | trs
    dt_ref: str                    # data lógica da execução (AAAA-MM-DD)
    run_id: str
    pkg_dir: Path                  # pasta da ingestão
    params_dag: dict               # seção params_dag
    cfg: dict                      # seção da camada (com a sub-base mesclada, se houver)
    schema: Optional[list] = field(default=None)   # colunas da TRS (só na camada trs)
    subbase: Optional[str] = field(default=None)   # sub-base desta task (raw/trs), se houver
    update_list: Optional[List[Optional[str]]] = field(default=None)  # raw/trs: period_ids vindos da LND
    reset: bool = False                            # lnd: ignorar o last_update e recarregar tudo

    dominio = property(lambda s: s.params_dag["dominio"])
    orgao_origem = property(lambda s: s.params_dag["orgao_origem"])
    fonte_dados = property(lambda s: s.params_dag["fonte_dados"])   # sistema/portal (texto)
    dataset = property(lambda s: s.params_dag["dataset"])
    dag_id = property(lambda s: derivar_dag_id(s.params_dag))
    app_name = property(lambda s: f"{s.camada}_{s.dataset}" + (f"_{s.subbase}" if s.subbase else ""))


def configurar_logs() -> None:
    """Garante que logging.info dos módulos apareça no stdout (se o root ainda não tem handler).

    Notebook chamado pelo runner (PIPELINE_CONTEXT definido): sem data, porque o Airflow já põe
    a dele; o runner lê o nível no início da linha e o repassa ao log da task.
    No venv: com data.
    """
    if not logging.getLogger().handlers:
        formato = ("%(levelname)s %(name)s: %(message)s" if os.environ.get("PIPELINE_CONTEXT")
                   else "%(asctime)s %(levelname)s %(name)s: %(message)s")
        logging.basicConfig(level=logging.INFO, stream=sys.stdout, format=formato)
        logging.getLogger("py4j").setLevel(logging.WARNING)


def get_context(camada: Optional[str] = None, dt_ref: Optional[str] = None,
                run_id: Optional[str] = None, pkg_dir=None, subbase: Optional[str] = None,
                update_list: Optional[list] = None, reset: Optional[bool] = None) -> Context:
    """Cria o contexto da camada.

    Notebook: ctx = get_context(camada="raw", subbase="mensal", update_list=["202610"])
    RAW e TRS exigem update_list (no Airflow vem do XCom da LND). Base única: update_list=[None].
    Rode a partir da pasta da ingestão (onde está o dag_params.yaml) ou informe pkg_dir.
    """
    configurar_logs()
    env = json.loads(os.getenv("PIPELINE_CONTEXT") or "{}")
    camada = camada or env.get("camada")
    dt_ref = dt_ref or env.get("dt_ref") or date.today().isoformat()
    run_id = run_id or env.get("run_id") or "manual"
    subbase = subbase or env.get("subbase") or None
    update_list = update_list if update_list is not None else env.get("update_list")
    reset = bool(reset if reset is not None else env.get("reset", False))
    pkg = Path(pkg_dir or env.get("pkg_dir") or Path.cwd())

    if camada not in CAMADAS:
        raise ValueError(f"Informe a camada: get_context(camada={' | '.join(CAMADAS)!r})")
    if not _DATA.match(dt_ref):
        raise ValueError(f"dt_ref inválido (use AAAA-MM-DD): {dt_ref!r}")
    if subbase and (camada == "lnd" or not _SUBBASE.match(subbase)):
        raise ValueError(f"Sub-base inválida: {subbase!r} (snake_case; a LND não tem sub-base)")

    if camada == "lnd":
        if update_list is not None:
            raise ValueError("A LND decide a lista de atualização; não informe update_list")
    elif update_list is None:
        raise ValueError(f"Camada {camada}: informe update_list (lista de period_id vinda da LND), "
                         f"ex.: get_context(camada={camada!r}, update_list=['202601'])")
    else:
        update_list = validar_update_list(list(update_list))

    cfg = carregar_config(pkg / "dag_params.yaml")
    cfg_camada = config_da_camada(cfg, camada, subbase)
    schema = None
    if camada == "trs":
        if not cfg_camada.get("schema"):
            raise ValueError("TRS sem schema: defina trs.schema (ou trs.subbases.<subbase>.schema)")
        schema = carregar_schema(pkg / cfg_camada["schema"])

    return Context(camada=camada, dt_ref=dt_ref, run_id=run_id, pkg_dir=pkg,
                   params_dag=cfg["params_dag"], cfg=cfg_camada, schema=schema, subbase=subbase,
                   update_list=update_list, reset=reset)


# --- produto (BIZ) ----------------------------------------------------------------------------

@dataclass
class ContextBiz:
    """Contexto do notebook de produto (BIZ), criado por get_context_biz()."""

    dt_ref: str                    # data lógica da execução (AAAA-MM-DD); num run por Dataset, a data do disparo
    run_id: str
    pkg_dir: Path                  # pasta do produto
    params_dag: dict               # seção params_dag
    entradas: List[str]            # schedule.datasets (URIs trs://...)
    saida: str                     # tasks.transform_biz.saida (URI biz://...)
    parametros: dict               # tasks.transform_biz.parametros (livre)

    dominio = property(lambda s: s.params_dag["dominio"])
    produto = property(lambda s: s.params_dag["produto"])
    dag_id = property(lambda s: f"{s.dominio}__{s.produto}")
    app_name = property(lambda s: f"biz_{s.produto}")


def get_context_biz(pkg_dir=None, dt_ref: Optional[str] = None, run_id: Optional[str] = None) -> ContextBiz:
    """Contexto do notebook_biz.py: lê o dag_params.yaml do produto (no venv e no Airflow) e liga o logging.

    No Airflow, dt_ref, run_id e pkg_dir vêm do PIPELINE_CONTEXT (runner.run_biz). No venv, rode a partir
    da pasta do produto ou informe pkg_dir.
    """
    from .config_produto import TASK_TRANSFORM, carregar_config_produto

    configurar_logs()
    env = json.loads(os.getenv("PIPELINE_CONTEXT") or "{}")
    pkg = Path(pkg_dir or env.get("pkg_dir") or Path.cwd())
    dt_ref = dt_ref or env.get("dt_ref") or date.today().isoformat()
    if not _DATA.match(dt_ref):
        raise ValueError(f"dt_ref inválido (use AAAA-MM-DD): {dt_ref!r}")
    cfg = carregar_config_produto(pkg / "dag_params.yaml")
    transform = cfg["tasks"][TASK_TRANSFORM]
    if env.get("saida") and env["saida"] != transform["saida"]:      # o runner usa o mesmo YAML
        raise ValueError(f"saida do Airflow ({env['saida']}) diferente da do dag_params.yaml ({transform['saida']})")
    return ContextBiz(dt_ref=dt_ref, run_id=run_id or env.get("run_id") or "manual", pkg_dir=pkg,
                      params_dag=cfg["params_dag"], entradas=list(cfg["schedule"]["datasets"]),
                      saida=transform["saida"], parametros=dict(transform["parametros"]))
