"""Executa o notebook (.py) de uma camada em processo separado e valida o contrato.

LND:  o notebook chama state.push_update_list; run_layer devolve {"period_ids", "last_update"},
      que o Airflow publica no XCom.
RAW/TRS: recebem esse retorno (update) e processam ctx.update_list. Lista vazia -> task pulada.
finish:  run_save_last_update (grava o last_update e registra) e run_clean_lnd (apaga a LND).
BIZ:     run_biz executa o notebook do produto e exige Parquet novo no URI de saída (biz://...);
         run_load_dw recarrega uma tabela do DW com essa saída (dataflow_commons.dw).
"""
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from . import control
from .config import nome_notebook
from .context import Context, get_context
from .paths import get_path_lnd, get_path_raw, get_path_trs, get_path_uri
from .state import decode_update, get_last_update, save_last_update, validar_update_list

log = logging.getLogger(__name__)
# linha de log do notebook (formato do context.configurar_logs no subprocesso): "<NIVEL> <resto>"
_NIVEL_LINHA = re.compile(r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL) (.*)$")


def _repassar_linha(linha: str) -> None:
    """Leva uma linha do notebook ao log da task com o nível real (print e o resto vão como INFO)."""
    casou = _NIVEL_LINHA.match(linha)
    if casou:
        log.log(getattr(logging, casou.group(1)), casou.group(2))
    else:
        log.info(linha)


def _executar_notebook(script: Path, contexto: dict) -> int:
    """Roda o notebook em subprocesso (cwd = pasta do notebook) com PIPELINE_CONTEXT e repassa a saída."""
    return _executar([str(script)], script.parent, contexto)


def _executar(args: list, cwd: Path, contexto: dict, env_extra: dict = None) -> int:
    """python <args> em subprocesso com PIPELINE_CONTEXT (+ env_extra, ex.: segredos) e repassa a saída."""
    # o subprocess sempre enxerga o dataflow_commons, mesmo sem PYTHONPATH configurado
    src = str(Path(__file__).resolve().parents[1])
    pythonpath = os.pathsep.join(p for p in (src, os.environ.get("PYTHONPATH", "")) if p)
    env = {**os.environ, "PYTHONPATH": pythonpath, "PYTHONUNBUFFERED": "1",
           "PIPELINE_CONTEXT": json.dumps(contexto), **(env_extra or {})}
    proc = subprocess.Popen([sys.executable, *args], cwd=cwd, env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for linha in proc.stdout:               # leva a saída do notebook para o log da task
        _repassar_linha(linha.rstrip())
    return proc.wait()


def _recentes(pasta: Path, padrao: str, desde: float) -> bool:
    """Há arquivo que casa com o padrão e foi gravado nesta execução (mtime >= desde)?"""
    if not pasta.exists():
        return False
    return any(f.is_file() and not f.name.startswith("_") and f.stat().st_mtime >= desde - 1
               for f in pasta.rglob(padrao))


def validar_saida(ctx: Context, period_ids: list, desde: float = 0.0) -> None:
    """Contrato mínimo de cada camada, validado mesmo se o notebook for customizado.

    Exige arquivos gravados NESTA execução (mtime >= desde) em cada período da lista:
    um notebook que não grava nada não passa por causa de dados antigos no diretório.
    """
    if ctx.camada == "lnd":
        for pid in period_ids:
            if not _recentes(get_path_lnd(ctx, pid), "*", desde):
                raise FileNotFoundError(f"LND sem arquivos gravados nesta execução em {get_path_lnd(ctx, pid)}")
    elif ctx.camada == "raw":
        for pid in period_ids:
            if not _recentes(get_path_raw(ctx, pid), "*.parquet", desde):
                raise FileNotFoundError(f"RAW sem Parquet gravado nesta execução em {get_path_raw(ctx, pid)}")
    elif ctx.camada == "trs":
        # o notebook escolhe gravar por pasta de período ou particionado pela raiz (partition_by):
        # exige a pasta de cada período quando ela existe e, em qualquer caso, Parquet novo na TRS
        base = get_path_trs(ctx)
        for pid in period_ids:
            pasta = get_path_trs(ctx, pid)
            if pid and pasta.exists() and not _recentes(pasta, "*.parquet", desde):
                raise FileNotFoundError(f"TRS sem Parquet gravado nesta execução em {pasta}")
        if not _recentes(base, "*.parquet", desde):
            raise FileNotFoundError(f"TRS sem Parquet gravado nesta execução em {base}")


def _ler_update(update) -> dict:
    """Retorno da LND vindo do XCom (JSON pelo template '| tojson') ou já como dict."""
    if isinstance(update, str):
        update = json.loads(update) if update.strip() else None
    if not isinstance(update, dict) or "period_ids" not in update:
        raise ValueError(f"Retorno da LND ausente ou inválido (XCom da task lnd): {update!r}")
    update["period_ids"] = validar_update_list(update["period_ids"])
    return update


def _pular(motivo: str) -> None:
    """Marca a task como skipped no Airflow; fora dele (venv) só registra."""
    log.info("Task pulada: %s", motivo)
    try:
        from airflow.exceptions import AirflowSkipException
    except ImportError:
        return
    raise AirflowSkipException(motivo)


def _bool(valor) -> bool:
    """Converte o valor do template do Airflow ("True", "1", "sim"...) em bool."""
    return valor if isinstance(valor, bool) else str(valor).strip().lower() in ("true", "1", "sim")


def run_layer(camada: str, pkg_dir: str, dt_ref: str, run_id: str, subbase: str = None,
              update=None, reset=False):
    """Executa notebook_<camada>[_<subbase>].py da pasta da ingestão e valida o contrato da camada.

    LND: devolve {"period_ids": [...], "last_update": {...}} (vira o XCom da task).
    RAW/TRS: 'update' é esse retorno; com lista vazia a task é pulada sem rodar o notebook.
    """
    pkg = Path(pkg_dir).resolve()           # o subprocess roda com cwd=pkg: caminho relativo quebraria
    subbase = subbase or None
    reset = _bool(reset)
    update_list = None
    if camada != "lnd":
        update_list = _ler_update(update)["period_ids"]
    ctx = get_context(camada=camada, dt_ref=dt_ref, run_id=run_id, pkg_dir=pkg, subbase=subbase,
                      update_list=update_list, reset=reset if camada == "lnd" else None)

    if camada != "lnd" and not update_list:
        control.registrar(ctx, "pulado", {"motivo": "LND sem atualização"})
        _pular("A LND não encontrou nada novo para atualizar")
        return None

    notebook = nome_notebook(camada, subbase)
    script = pkg / notebook
    if not script.exists():
        raise FileNotFoundError(f"Notebook não encontrado: {script}")

    with tempfile.TemporaryDirectory(prefix="pipeline_") as tmp:
        retorno_path = Path(tmp) / "update_list.json"
        contexto = {"camada": camada, "dt_ref": dt_ref, "run_id": run_id, "pkg_dir": str(pkg),
                    "subbase": subbase, "update_list": update_list, "reset": ctx.reset,
                    "update_list_path": str(retorno_path) if camada == "lnd" else None}

        control.registrar(ctx, "inicio")
        t0 = time.time()
        codigo = _executar_notebook(script, contexto)

        if codigo != 0:
            control.registrar(ctx, "falha", {"codigo": codigo})
            raise RuntimeError(f"Notebook '{notebook}' falhou (código {codigo}). Veja o log acima.")
        try:
            if camada == "lnd":
                if not retorno_path.exists():
                    raise RuntimeError(f"'{notebook}' não chamou push_update_list(ctx, period_ids, last_update): "
                                       "a LND precisa informar o que RAW e TRS vão processar")
                retorno = _ler_update(json.loads(retorno_path.read_text(encoding="utf-8")))
                validar_saida(ctx, retorno["period_ids"], desde=t0)
            else:
                retorno = None
                validar_saida(ctx, update_list, desde=t0)
        except Exception as exc:
            control.registrar(ctx, "falha", {"erro": str(exc)})
            raise
    extra = {"segundos": round(time.time() - t0, 1)}
    if retorno is not None:
        extra["retorno_lnd"] = retorno
    control.registrar(ctx, "fim", extra)
    return retorno


def run_save_last_update(pkg_dir: str, dt_ref: str, run_id: str, update=None) -> None:
    """finish: grava o last_update informado pela LND (só roda depois de todas as TRS) e registra."""
    update = _ler_update(update)
    ctx = get_context(camada="lnd", dt_ref=dt_ref, run_id=run_id, pkg_dir=Path(pkg_dir))
    anterior = get_last_update(ctx.dag_id)
    novo = decode_update(update.get("last_update"))
    if novo is None:
        log.info("%s: a LND não informou last_update; nada a gravar", ctx.dag_id)
    else:
        save_last_update(ctx.dag_id, novo, run_id=run_id)
    log.info("Atualização de %s concluída | períodos: %s | last_update: %s -> %s",
             ctx.dag_id, update["period_ids"], anterior, novo)
    control.registrar(ctx, "atualizacao", {
        "period_ids": update["period_ids"], "last_update_anterior": None if anterior is None else str(anterior),
        "last_update": None if novo is None else str(novo)})


def run_clean_lnd(pkg_dir: str, dt_ref: str, run_id: str, update=None) -> None:
    """finish: apaga os arquivos da LND dos períodos desta execução ([None] = a LND inteira)."""
    update = _ler_update(update)
    ctx = get_context(camada="lnd", dt_ref=dt_ref, run_id=run_id, pkg_dir=Path(pkg_dir))
    removidos = []
    for pid in update["period_ids"]:
        pasta = get_path_lnd(ctx, pid)
        if pasta.exists():
            shutil.rmtree(pasta)
            removidos.append(str(pasta))
    log.info("LND limpa: %d pasta(s) removida(s) %s", len(removidos), removidos)
    control.registrar(ctx, "limpeza_lnd", {"removidos": removidos})


def run_biz(pkg_dir: str, notebook: str, saida: str, dt_ref: str, run_id: str) -> str:
    """Executa o notebook de um produto (BIZ) e valida o contrato: Parquet gravado nesta execução
    no caminho do URI de saída ('biz://<dominio>/<produto>/<tabela>'). Devolve o URI.
    """
    script = Path(pkg_dir).resolve() / notebook
    if not script.exists():
        raise FileNotFoundError(f"Notebook não encontrado: {script}")
    destino = get_path_uri(saida)
    if not saida.startswith("biz://"):
        raise ValueError(f"saida deve ser um URI da BIZ (biz://...): {saida!r}")

    t0 = time.time()
    codigo = _executar_notebook(script, {"camada": "biz", "dt_ref": dt_ref, "run_id": run_id,
                                         "pkg_dir": str(script.parent), "saida": saida})
    if codigo != 0:
        raise RuntimeError(f"Notebook '{notebook}' falhou (código {codigo}). Veja o log acima.")
    if not _recentes(destino, "*.parquet", t0):
        raise FileNotFoundError(f"BIZ sem Parquet gravado nesta execução em {destino} ({saida})")
    log.info("BIZ OK: %s (%.1f s)", saida, time.time() - t0)
    return saida


def run_load_dw(origem: str, tabela: str, dt_ref: str, run_id: str, pkg_dir: str = None,
                conexao: str = None) -> str:
    """Recarga completa da 'tabela' do DW ([schema.]tabela) com a BIZ de 'origem' (biz://...).

    Credenciais da Connection 'conexao' (padrão mssql_dw). Se a pasta do produto tem ddl_dw.sql, ele é
    executado antes (idempotente). A carga roda em subprocesso com Spark (dataflow_commons.dw): staging,
    DELETE + INSERT numa transação e conferência da contagem. Devolve a tabela.
    """
    from . import dw

    if not origem.startswith("biz://"):
        raise ValueError(f"origem deve ser um URI da BIZ (biz://...): {origem!r}")
    if not get_path_uri(origem).exists():
        raise FileNotFoundError(f"BIZ não encontrada para {origem}: {get_path_uri(origem)}")
    dw.separar_tabela(tabela)                                    # valida o nome antes de subir o Spark
    con = dw.obter_conexao(conexao or dw.CONEXAO_PADRAO)
    ddl = Path(pkg_dir).resolve() / "ddl_dw.sql" if pkg_dir else None
    contexto = {"camada": "dw", "dt_ref": dt_ref, "run_id": run_id, "origem": origem, "tabela": tabela,
                "url": dw.jdbc_url(con), "usuario": con["usuario"],
                "ddl": str(ddl) if ddl and ddl.exists() else None}
    log.info("Carga no DW: %s -> %s (%s)", origem, tabela, contexto["url"])

    t0 = time.time()
    codigo = _executar(["-m", "dataflow_commons.dw"], Path(pkg_dir or "."), contexto,
                       env_extra={"PIPELINE_DW_SENHA": con["senha"] or ""})
    if codigo != 0:
        raise RuntimeError(f"Carga no DW de {origem} -> {tabela} falhou (código {codigo}). Veja o log acima.")
    log.info("DW OK: %s (%.1f s)", tabela, time.time() - t0)
    return tabela
