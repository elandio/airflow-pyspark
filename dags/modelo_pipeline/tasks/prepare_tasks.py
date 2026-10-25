import json
import logging
from datetime import datetime, timezone

from modelo_pipeline.tools.const import pipeline as P
from modelo_pipeline.tools.const.schema_tables import SCHEMA_TRS

log = logging.getLogger(__name__)


def validar_metadados():
    destinos = [c.destino for c in SCHEMA_TRS]
    origens = [c.origem for c in SCHEMA_TRS]
    if len(destinos) != len(set(destinos)):
        raise ValueError("Nomes de destino duplicados em SCHEMA_TRS")
    if len(origens) != len(set(origens)):
        raise ValueError("Nomes de origem duplicados em SCHEMA_TRS")
    log.info("Pipeline %s v%s: %d colunas na TRS", P.PIPELINE_NOME, P.PIPELINE_VERSAO, len(destinos))


def registrar_execucao(evento: str, dt_ref: str, run_id: str):
    """Grava uma linha JSON por evento em controle/execucoes.jsonl."""
    P.CONTROLE_DIR.mkdir(parents=True, exist_ok=True)
    registro = {
        "evento": evento,
        "pipeline": P.PIPELINE_NOME,
        "versao": P.PIPELINE_VERSAO,
        "dt_ref": dt_ref,
        "run_id": run_id,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    with open(P.CONTROLE_DIR / "execucoes.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False) + "\n")
    log.info("Execução registrada: %s", registro)
