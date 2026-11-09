"""Leitura e validação do dag_params.yaml de PRODUTO (BIZ). Sem Airflow: testável no venv.

params_dag:
  dominio, produto          identidade; dag_id = <dominio>__<produto> = nome da pasta
  schedule:
    datasets: [trs://...]   TRS que disparam o produto (a BIZ lê só da TRS)
    condicao: todos         todos (padrão: só depois de todas atualizarem) | qualquer (a primeira que atualizar)
    cron: "0 6 * * 1"       opcional: roda também nesse horário (DatasetOrTimeSchedule, UTC)
  owner, tags, retries, execution_timeout_min   opcionais
tasks:
  transform_biz:            obrigatória: notebook_biz.py lê a TRS, aplica as regras e grava na BIZ
    saida: biz://<dominio>/<produto>/<tabela>
    parametros: {...}       opcional: mapa livre de parâmetros do recorte (ano, códigos...),
                            lido no notebook em get_context_biz().parametros
  load_dw:                  opcional: recarrega uma tabela do DW com a saída da transform_biz
    tabela: <schema>.<tabela>
    conexao: mssql_dw       opcional: conn_id da Connection do Airflow (padrão mssql_dw)
  ddl_dw.sql (na pasta, opcional): DDL idempotente executado antes da carga (IF OBJECT_ID ... IS NULL)
"""
import re
from pathlib import Path

import yaml

from .paths import get_path_uri

NOTEBOOK_BIZ = "notebook_biz.py"
TASK_TRANSFORM = "transform_biz"
TASK_DW = "load_dw"
_NOME = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
_TABELA_DW = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")     # [schema.]tabela
_CHAVES_PARAMS = {"dominio", "produto", "schedule", "owner", "tags", "retries", "execution_timeout_min"}
_CHAVES_SCHEDULE = {"datasets", "condicao", "cron"}
_CHAVES_TASK = {TASK_TRANSFORM: {"saida", "parametros", "retries"}, TASK_DW: {"tabela", "conexao", "retries"}}
CONEXAO_DW_PADRAO = "mssql_dw"
_CONN_ID = re.compile(r"^[A-Za-z0-9_]+$")
CONDICOES = ("todos", "qualquer")


def derivar_dag_id_produto(params_dag: dict) -> str:
    """dag_id = <dominio>__<produto> (nunca digitado à mão). Duas partes: não colide com a ingestão (quatro)."""
    return f"{params_dag['dominio']}__{params_dag['produto']}"


def _desconhecidas(onde: str, mapa: dict, permitidas: set) -> None:
    """Falha se o mapa tiver chaves fora das permitidas (erro de digitação não passa em silêncio)."""
    extras = set(mapa) - permitidas
    if extras:
        raise ValueError(f"{onde}: chaves desconhecidas {sorted(extras)} (permitidas: {sorted(permitidas)})")


def _uri(valor, camada: str, onde: str) -> str:
    """Valida um URI <camada>://... (formato e partes em snake_case) e o devolve."""
    if not isinstance(valor, str) or not valor.startswith(f"{camada}://"):
        raise ValueError(f"{onde}: esperado um URI {camada}://..., recebi {valor!r}")
    get_path_uri(valor)                                  # valida o formato (partes e snake_case)
    return valor


def _retries(valor, onde: str):
    """Valida retries: None ou inteiro >= 0."""
    if valor is not None and (isinstance(valor, bool) or not isinstance(valor, int) or valor < 0):
        raise ValueError(f"{onde}: 'retries' deve ser um inteiro >= 0")
    return valor


def validar_config_produto(cfg: dict, nome_pasta: str) -> dict:
    """Valida e devolve a config normalizada: {'params_dag', 'schedule', 'tasks'}."""
    if not isinstance(cfg, dict):
        raise ValueError("dag_params.yaml de produto vazio ou inválido")
    _desconhecidas("dag_params.yaml", cfg, {"params_dag", "tasks"})
    p = cfg.get("params_dag")
    if not isinstance(p, dict):
        raise ValueError("dag_params.yaml sem a seção 'params_dag'")
    _desconhecidas("params_dag", p, _CHAVES_PARAMS)
    for k in ("dominio", "produto"):
        if not _NOME.match(str(p.get(k) or "")):
            raise ValueError(f"params_dag.{k} obrigatório, em snake_case minúsculo, sem '__': {p.get(k)!r}")
    dag_id = derivar_dag_id_produto(p)
    if nome_pasta != dag_id:
        raise ValueError(f"A pasta '{nome_pasta}' deve se chamar '{dag_id}'")
    _retries(p.get("retries"), "params_dag")

    s = p.get("schedule")
    if not isinstance(s, dict):
        raise ValueError("params_dag.schedule deve ser um mapa com 'datasets' (e opcionais 'condicao', 'cron')")
    _desconhecidas("params_dag.schedule", s, _CHAVES_SCHEDULE)
    datasets = s.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise ValueError("params_dag.schedule.datasets: informe ao menos um trs://... (a BIZ lê só da TRS)")
    datasets = [_uri(d, "trs", "params_dag.schedule.datasets") for d in datasets]
    if len(set(datasets)) != len(datasets):
        raise ValueError(f"params_dag.schedule.datasets com repetição: {datasets}")
    condicao = s.get("condicao") or "todos"
    if condicao not in CONDICOES:
        raise ValueError(f"params_dag.schedule.condicao inválida: {condicao!r} (use {' | '.join(CONDICOES)})")
    cron = s.get("cron")
    if cron is not None and (not isinstance(cron, str) or len(cron.split()) != 5):
        raise ValueError(f"params_dag.schedule.cron deve ter 5 campos (ex.: '0 6 * * 1'): {cron!r}")

    tasks = cfg.get("tasks")
    if not isinstance(tasks, dict) or TASK_TRANSFORM not in tasks:
        raise ValueError(f"tasks: a task '{TASK_TRANSFORM}' é obrigatória")
    _desconhecidas("tasks", tasks, set(_CHAVES_TASK))
    normalizadas = {}
    for tid, t in tasks.items():
        t = t or {}
        if not isinstance(t, dict):
            raise ValueError(f"tasks.{tid} deve ser um mapa")
        _desconhecidas(f"tasks.{tid}", t, _CHAVES_TASK[tid])
        normalizadas[tid] = {"retries": _retries(t.get("retries"), f"tasks.{tid}")}

    prefixo = f"biz://{p['dominio']}/{p['produto']}/"
    saida = _uri(tasks[TASK_TRANSFORM].get("saida"), "biz", f"tasks.{TASK_TRANSFORM}.saida")
    if not saida.startswith(prefixo):
        raise ValueError(f"tasks.{TASK_TRANSFORM}.saida deve ficar no produto: {prefixo}<tabela> (recebi {saida})")
    normalizadas[TASK_TRANSFORM]["saida"] = saida
    parametros = tasks[TASK_TRANSFORM].get("parametros")
    if parametros is None:
        parametros = {}
    if not isinstance(parametros, dict) or not all(isinstance(k, str) and _NOME.match(k) for k in parametros):
        raise ValueError(f"tasks.{TASK_TRANSFORM}.parametros deve ser um mapa com chaves em snake_case: {parametros!r}")
    normalizadas[TASK_TRANSFORM]["parametros"] = parametros
    if TASK_DW in tasks:
        tabela = (tasks[TASK_DW] or {}).get("tabela")
        if not isinstance(tabela, str) or not _TABELA_DW.match(tabela):
            raise ValueError(f"tasks.{TASK_DW}.tabela obrigatória, no formato [schema.]tabela: {tabela!r}")
        normalizadas[TASK_DW]["tabela"] = tabela
        conexao = (tasks[TASK_DW] or {}).get("conexao") or CONEXAO_DW_PADRAO
        if not isinstance(conexao, str) or not _CONN_ID.match(conexao):
            raise ValueError(f"tasks.{TASK_DW}.conexao inválida (conn_id do Airflow): {conexao!r}")
        normalizadas[TASK_DW]["conexao"] = conexao

    return {"params_dag": p, "schedule": {"datasets": datasets, "condicao": condicao, "cron": cron},
            "tasks": normalizadas}


def carregar_config_produto(caminho) -> dict:
    """Lê, valida e confere os arquivos do produto (notebook_biz.py)."""
    caminho = Path(caminho)
    if not caminho.exists():
        raise FileNotFoundError(f"dag_params.yaml não encontrado em {caminho.parent}")
    cfg = validar_config_produto(yaml.safe_load(caminho.read_text(encoding="utf-8")), caminho.parent.name)
    if not (caminho.parent / NOTEBOOK_BIZ).exists():
        raise FileNotFoundError(f"task '{TASK_TRANSFORM}': notebook não encontrado: {caminho.parent / NOTEBOOK_BIZ}")
    return cfg
