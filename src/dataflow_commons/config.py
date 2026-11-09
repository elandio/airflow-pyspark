"""Leitura e validação do dag_params.yaml (identidade, tasks, camadas e sub-bases)."""
import re
from pathlib import Path

import yaml

_NOME = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
_NOTEBOOK = re.compile(r"^notebook_(lnd|raw|trs)(?:_([a-z0-9]+(?:_[a-z0-9]+)*))?\.py$")
_IDENTIDADE = ("dominio", "orgao_origem", "fonte_dados", "dataset")
CAMADAS = ("lnd", "raw", "trs")
GRUPO_FINISH = "finish"   # grupo criado pela fábrica no fim da DAG (save_last_update >> clean_lnd)
_CHAVES_TASK = {"notebook", "dependencies", "task_group", "retries", "trigger_rule"}
_TRIGGER_RULES = {"all_success", "all_done", "all_failed", "one_success", "one_failed",
                  "none_failed", "none_failed_min_one_success", "none_skipped", "always"}


def derivar_dag_id(params_dag: dict) -> str:
    """dag_id = dominio__orgao_origem__fonte_dados__dataset (nunca digitado à mão)."""
    return "__".join(params_dag[k] for k in _IDENTIDADE)


def nome_notebook(camada: str, subbase=None) -> str:
    """notebook_<camada>[_<subbase>].py"""
    return f"notebook_{camada}" + (f"_{subbase}" if subbase else "") + ".py"


def parse_notebook(nome: str):
    """'notebook_raw_mensal.py' -> ('raw', 'mensal'). A camada e a sub-base vêm do nome."""
    m = _NOTEBOOK.match(str(nome))
    if not m:
        raise ValueError(f"Nome de notebook inválido: {nome!r}. Use notebook_<camada>[_<subbase>].py "
                         f"com camada lnd, raw ou trs (ex.: notebook_raw_mensal.py)")
    camada, subbase = m.group(1), m.group(2)
    if camada == "lnd" and subbase:
        raise ValueError(f"{nome}: a LND é um download único e não tem sub-base")
    return camada, subbase


def _merge(base: dict, extra: dict) -> dict:
    """Mescla extra sobre base, recursivamente nos mapas (usado para as sub-bases)."""
    saida = dict(base)
    for k, v in (extra or {}).items():
        saida[k] = _merge(saida[k], v) if isinstance(v, dict) and isinstance(saida.get(k), dict) else v
    return saida


def config_da_camada(cfg: dict, camada: str, subbase=None) -> dict:
    """Seção da camada; se houver sub-base, mescla <camada>.subbases.<subbase> sobre a base."""
    secao = dict(cfg.get(camada) or {})
    subs = secao.pop("subbases", None) or {}
    if not isinstance(subs, dict):
        raise ValueError(f"{camada}.subbases deve ser um mapa")
    return _merge(secao, subs.get(subbase) or {}) if subbase else secao


def _validar_sem_ciclos(tasks: dict) -> None:
    """Falha se as dependências entre tasks formarem um ciclo (ordenação topológica)."""
    pendentes = {t: set(v["dependencies"]) for t, v in tasks.items()}
    while pendentes:
        livres = [t for t, deps in pendentes.items() if not deps]
        if not livres:
            raise ValueError(f"Dependência circular entre as tasks: {sorted(pendentes)}")
        for t in livres:
            del pendentes[t]
        for deps in pendentes.values():
            deps.difference_update(livres)


def resolver_tasks(cfg: dict) -> dict:
    """Tasks da DAG (params_dag.tasks) normalizadas e validadas. Sem 'tasks': lnd >> raw >> trs."""
    p = cfg["params_dag"]
    brutas = p.get("tasks") or {
        "lnd": {"notebook": nome_notebook("lnd")},
        "raw": {"notebook": nome_notebook("raw"), "dependencies": ["lnd"]},
        "trs": {"notebook": nome_notebook("trs"), "dependencies": ["raw"]},
    }
    if not isinstance(brutas, dict):
        raise ValueError("params_dag.tasks deve ser um mapa {task_id: {...}}")
    grupos = p.get("task_groups") or {}
    if not isinstance(grupos, dict) or not all(_NOME.match(str(g)) for g in grupos):
        raise ValueError("params_dag.task_groups deve ser um mapa com nomes em snake_case")
    if GRUPO_FINISH in grupos or GRUPO_FINISH in brutas:
        raise ValueError(f"'{GRUPO_FINISH}' é reservado para o grupo final criado pela fábrica")

    tasks, notebooks = {}, set()
    for tid, t in brutas.items():
        if not _NOME.match(str(tid)):
            raise ValueError(f"task_id inválido (snake_case minúsculo): {tid!r}")
        t = t or {}
        extras = set(t) - _CHAVES_TASK
        if extras:
            raise ValueError(f"task '{tid}': chaves desconhecidas {sorted(extras)} "
                             f"(permitidas: {sorted(_CHAVES_TASK)})")
        if not t.get("notebook"):
            raise ValueError(f"task '{tid}': 'notebook' é obrigatório")
        camada, subbase = parse_notebook(t["notebook"])
        if t["notebook"] in notebooks:
            raise ValueError(f"task '{tid}': notebook {t['notebook']} já usado por outra task")
        notebooks.add(t["notebook"])

        deps = t.get("dependencies") or []
        if not isinstance(deps, list):
            raise ValueError(f"task '{tid}': 'dependencies' deve ser uma lista")
        grupo = t.get("task_group")
        if grupo and grupo not in grupos:
            raise ValueError(f"task '{tid}': task_group '{grupo}' não está em params_dag.task_groups")
        regra = t.get("trigger_rule")
        if regra and regra not in _TRIGGER_RULES:
            raise ValueError(f"task '{tid}': trigger_rule inválida {regra!r} ({sorted(_TRIGGER_RULES)})")
        retries = t.get("retries")
        if retries is not None and (isinstance(retries, bool) or not isinstance(retries, int) or retries < 0):
            raise ValueError(f"task '{tid}': 'retries' deve ser um inteiro >= 0")
        tasks[tid] = {"task_id": tid, "notebook": t["notebook"], "camada": camada, "subbase": subbase,
                      "dependencies": list(deps), "task_group": grupo, "retries": retries,
                      "trigger_rule": regra}

    for tid, t in tasks.items():
        for d in t["dependencies"]:
            if d not in tasks:
                raise ValueError(f"task '{tid}': dependência '{d}' não existe")
            if d == tid:
                raise ValueError(f"task '{tid}': depende de si mesma")
    _validar_sem_ciclos(tasks)
    if sum(t["camada"] == "lnd" for t in tasks.values()) != 1:
        raise ValueError("A DAG precisa de exatamente uma task de LND (notebook_lnd.py)")
    return tasks


def carregar_config(caminho) -> dict:
    """Lê e valida o dag_params.yaml da ingestão e confere se os notebooks e schemas existem."""
    caminho = Path(caminho)
    if not caminho.exists():
        raise FileNotFoundError(
            f"dag_params.yaml não encontrado em {caminho.parent}. "
            "Rode o notebook a partir da pasta da ingestão ou informe pkg_dir.")
    cfg = yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}
    validar_config(cfg, caminho.parent.name)
    for t in resolver_tasks(cfg).values():            # arquivos que as tasks vão executar
        if not (caminho.parent / t["notebook"]).exists():
            raise FileNotFoundError(f"task '{t['task_id']}': notebook não encontrado: "
                                    f"{caminho.parent / t['notebook']}")
        if t["camada"] == "trs":
            schema = config_da_camada(cfg, "trs", t["subbase"])["schema"]
            if not (caminho.parent / schema).exists():
                raise FileNotFoundError(f"task '{t['task_id']}': schema não encontrado: "
                                        f"{caminho.parent / schema}")
    return cfg


def validar_config(cfg: dict, nome_pasta: str) -> None:
    """Valida identidade, nome da pasta (= dag_id), seções, sub-bases e tasks. Falha com ValueError."""
    params = cfg.get("params_dag")
    if not isinstance(params, dict):
        raise ValueError("dag_params.yaml sem a seção 'params_dag'")
    faltando = [k for k in (*_IDENTIDADE, "schedule") if not params.get(k)]
    if faltando:
        raise ValueError(f"params_dag sem as chaves obrigatórias: {faltando}")
    for k in _IDENTIDADE:
        if not _NOME.match(str(params[k])):
            raise ValueError(f"params_dag.{k} deve ser snake_case minúsculo, sem '__': {params[k]!r}")

    dag_id = derivar_dag_id(params)
    if params.get("dag_id") not in (None, "", dag_id):
        raise ValueError(f"params_dag.dag_id é gerado automaticamente ('{dag_id}'). "
                         f"Remova a chave ou use exatamente esse valor.")
    if nome_pasta != dag_id:
        raise ValueError(f"A pasta '{nome_pasta}' deve se chamar '{dag_id}'")

    for camada in CAMADAS:
        if not isinstance(cfg.get(camada) or {}, dict):
            raise ValueError(f"Seção '{camada}' deve ser um mapa")
    if not isinstance(cfg.get("lnd"), dict):
        raise ValueError("dag_params.yaml sem a seção 'lnd'")
    for camada in ("raw", "trs"):
        subs = (cfg.get(camada) or {}).get("subbases") or {}
        if not isinstance(subs, dict):
            raise ValueError(f"{camada}.subbases deve ser um mapa {{subbase: {{...}}}}")
        for nome, override in subs.items():
            if not _NOME.match(str(nome)):
                raise ValueError(f"subbase deve ser snake_case minúsculo, sem '__': {nome!r}")
            if override is not None and not isinstance(override, dict):
                raise ValueError(f"{camada}.subbases.{nome} deve ser um mapa")
    for t in resolver_tasks(cfg).values():
        if t["camada"] == "trs" and not config_da_camada(cfg, "trs", t["subbase"]).get("schema"):
            onde = f"trs.subbases.{t['subbase']}.schema" if t["subbase"] else "trs.schema"
            raise ValueError(f"task '{t['task_id']}': defina {onde} (arquivo de schema da TRS)")
