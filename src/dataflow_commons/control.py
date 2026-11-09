"""Registro de execuções por camada (controle/.../execucoes.jsonl)."""
import json
from datetime import datetime, timezone

from .paths import get_path_controle


def registrar(ctx, evento: str, extra: dict = None) -> None:
    """Acrescenta uma linha JSON em controle/.../execucoes.jsonl (inicio, fim, falha, pulado, atualizacao...)."""
    pasta = get_path_controle(ctx)
    pasta.mkdir(parents=True, exist_ok=True)
    registro = {
        "evento": evento, "camada": ctx.camada, "subbase": ctx.subbase, "dag_id": ctx.dag_id, "dt_ref": ctx.dt_ref,
        "update_list": ctx.update_list, "run_id": ctx.run_id,
        "ts": datetime.now(timezone.utc).isoformat(), **(extra or {}),
    }
    with open(pasta / "execucoes.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False) + "\n")
