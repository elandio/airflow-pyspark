"""Caminhos padronizados.

    <camada>/<dominio>/<orgao_origem>/<fonte_dados>/<dataset>[/<subbase>][/<period_id>]

- LND: um download por dataset (nunca tem sub-base); subpasta por período, se houver period_id.
- RAW: por sub-base (se houver) e por período.
- TRS: dado final em um único schema; por período (pasta), por partição (partition_by) ou base
       única; uma TRS por sub-base, quando houver.
- BIZ: biz/<dominio>/<produto>/<tabela> (get_path_uri).
- controle: last_update/<dag_id>.json e <dominio>/.../<dataset>/execucoes.jsonl.
"""
import os
import re
from pathlib import Path
from typing import Optional

CAMADAS_DADOS = ("lnd", "raw", "trs", "biz", "controle")
PERIOD_ID = re.compile(r"^[A-Za-z0-9]+([_-][A-Za-z0-9]+)*$")


# <projeto>/data_lake: src/dataflow_commons/paths.py -> parents[2] é a raiz do projeto
# (venv: ~/projetos/airflow-pyspark/data_lake; container: /opt/airflow/data_lake)
_DATA_LAKE_PADRAO = Path(__file__).resolve().parents[2] / "data_lake"


def base_dir() -> Path:
    """Raiz dos dados (data lake). PIPELINE_BASE_DIR sobrescreve; sem ela, <projeto>/data_lake."""
    return Path(os.getenv("PIPELINE_BASE_DIR") or _DATA_LAKE_PADRAO)


def layer_path(camada: str, dominio: str, orgao_origem: str, fonte_dados: str, dataset: str,
               period_id: Optional[str] = None, subbase: Optional[str] = None) -> Path:
    """<base_dir>/<camada>/<dominio>/<orgao_origem>/<fonte_dados>/<dataset>[/<subbase>][/<period_id>]."""
    if camada not in CAMADAS_DADOS:
        raise ValueError(f"Camada inválida: {camada}. Use uma de {CAMADAS_DADOS}")
    p = base_dir() / camada / dominio / orgao_origem / fonte_dados / dataset
    if subbase:
        p = p / subbase
    return p / str(period_id) if period_id else p


def _caminho(ctx, camada: str, period_id: Optional[str], subbase: Optional[str] = None) -> Path:
    """period_id informado -> subpasta do período; None -> base única (sem subpasta)."""
    if period_id is not None and not PERIOD_ID.match(str(period_id)):
        raise ValueError(f"period_id inválido: {period_id!r} (letras, dígitos, '_' ou '-')")
    return layer_path(camada, ctx.dominio, ctx.orgao_origem, ctx.fonte_dados, ctx.dataset,
                      period_id, subbase)


def get_path_lnd(ctx, period_id: Optional[str] = None) -> Path:
    """Diretório da LND (com subpasta do período, se informado). Não tem sub-base."""
    return _caminho(ctx, "lnd", period_id)


def get_path_raw(ctx, period_id: Optional[str] = None) -> Path:
    """Diretório da RAW (com a sub-base do ctx, se houver, e a subpasta do período)."""
    return _caminho(ctx, "raw", period_id, ctx.subbase)


def get_path_trs(ctx, period_id: Optional[str] = None) -> Path:
    """Diretório da TRS (schema único; com a sub-base do ctx, se houver, e a subpasta do período).

    Sem period_id: a raiz do dataset (todos os períodos; leia com read_trs).
    """
    return _caminho(ctx, "trs", period_id, ctx.subbase)


def get_path_controle(ctx) -> Path:
    """Pasta de controle do dataset (e da sub-base): onde fica o execucoes.jsonl."""
    return layer_path("controle", ctx.dominio, ctx.orgao_origem, ctx.fonte_dados, ctx.dataset,
                      subbase=ctx.subbase)


# --- URI de Dataset (o mesmo texto do Dataset do Airflow) -> pasta no data lake ------------------
_PARTES_URI = {"trs": (4, 5), "biz": (3, 3)}     # trs://dom/orgao/fonte/dataset[/subbase] | biz://dom/produto/tabela
_PARTE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")


def get_path_uri(uri: str) -> Path:
    """'trs://<dominio>/<orgao_origem>/<fonte_dados>/<dataset>[/<subbase>]' ou
    'biz://<dominio>/<produto>/<tabela>' -> <base_dir>/<camada>/<partes...>.

    É o mesmo URI do Dataset do Airflow: quem agenda pelo Dataset lê e grava pelo mesmo texto.
    """
    camada, sep, resto = str(uri).partition("://")
    partes = resto.split("/") if sep else []
    if camada not in _PARTES_URI:
        raise ValueError(f"URI inválido: {uri!r} (use trs://... ou biz://...)")
    minimo, maximo = _PARTES_URI[camada]
    if not minimo <= len(partes) <= maximo or not all(_PARTE.match(p) for p in partes):
        formato = ("trs://<dominio>/<orgao_origem>/<fonte_dados>/<dataset>[/<subbase>]" if camada == "trs"
                   else "biz://<dominio>/<produto>/<tabela>")
        raise ValueError(f"URI inválido: {uri!r} (formato {formato}, partes em snake_case)")
    return base_dir().joinpath(camada, *partes)
