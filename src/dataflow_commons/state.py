"""Última atualização carregada (last_update) por dag_id e lista do que a LND atualizou.

Fluxo:
  LND (notebook)   check_update -> baixa só o novo -> push_update_list(ctx, period_ids, last_update)
  RAW / TRS        recebem a lista da LND (XCom) em ctx.update_list
  finish           save_last_update grava o last_update só depois que todas as TRS terminam

O armazenamento fica atrás de UpdateStore (arquivo JSON hoje; Postgres depois, sem mexer nos notebooks).
Sem Airflow: funciona no venv e no container.
"""
import json
import logging
import operator
import os
import re
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional, Protocol

from .paths import PERIOD_ID, base_dir

log = logging.getLogger(__name__)

_DAG_ID = re.compile(r"^[a-z0-9]+(_{1,2}[a-z0-9]+)*$")
_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?)?$")


# --- tipos aceitos para o last_update ----------------------------------------------------------

def _tipo(valor) -> str:
    """Tipo do valor de atualização: date | datetime | int | str (ISO). Outros são recusados."""
    if isinstance(valor, datetime):            # antes de date: datetime é subclasse de date
        return "datetime"
    if isinstance(valor, date):
        return "date"
    if isinstance(valor, int) and not isinstance(valor, bool):
        return "int"
    if isinstance(valor, str):
        if not _ISO.match(valor):
            raise ValueError(f"Data em texto deve estar em ISO (AAAA-MM-DD[THH:MM[:SS]]): {valor!r}. "
                             "Converta no notebook (ex.: datetime.strptime(v, '%d/%m/%Y').date()).")
        datetime.fromisoformat(valor)          # recusa datas inexistentes (ex.: 2026-02-30)
        return "str"
    raise TypeError(f"Tipo não suportado para atualização: {type(valor).__name__} ({valor!r}). "
                    "Use date, datetime, int ou texto ISO.")


def encode_update(valor) -> Optional[dict]:
    """{'value': ..., 'type': ...}: formato do arquivo e do XCom (JSON preserva o tipo)."""
    if valor is None:
        return None
    tipo = _tipo(valor)
    return {"value": valor.isoformat() if tipo in ("date", "datetime") else valor, "type": tipo}


def decode_update(registro: Optional[dict]):
    """Inverso do encode_update: {'value', 'type'} -> date, datetime, int ou str (None se vazio)."""
    if not registro:
        return None
    valor, tipo = registro["value"], registro["type"]
    if tipo == "date":
        return date.fromisoformat(valor)
    if tipo == "datetime":
        return datetime.fromisoformat(valor)
    if tipo in ("int", "str"):
        return valor
    raise ValueError(f"Tipo de atualização desconhecido no registro: {tipo!r}")


# --- armazenamento -----------------------------------------------------------------------------

class UpdateStore(Protocol):
    """Interface do armazenamento do last_update (implementações: FileUpdateStore; Postgres planejado)."""

    def get(self, dag_id: str) -> Optional[dict]:
        """Registro gravado ({'value', 'type', ...}) ou None."""

    def save(self, dag_id: str, registro: dict) -> None:
        """Grava (substitui) o registro do dag_id."""

    def delete(self, dag_id: str) -> bool:
        """Apaga o registro; True se existia."""


class FileUpdateStore:
    """Um JSON por dag_id em <PIPELINE_BASE_DIR>/controle/last_update/<dag_id>.json.

    Um arquivo por DAG: DAGs rodando ao mesmo tempo nunca disputam o mesmo arquivo.
    """

    def __init__(self, pasta=None):
        self._pasta = Path(pasta) if pasta else None

    @property
    def pasta(self) -> Path:
        """Pasta dos arquivos: a informada no construtor ou <base_dir>/controle/last_update."""
        return self._pasta or base_dir() / "controle" / "last_update"

    def _arquivo(self, dag_id: str) -> Path:
        if not _DAG_ID.match(str(dag_id)):
            raise ValueError(f"dag_id inválido: {dag_id!r}")
        return self.pasta / f"{dag_id}.json"

    def get(self, dag_id: str) -> Optional[dict]:
        """Conteúdo do <dag_id>.json, ou None se não existe."""
        arquivo = self._arquivo(dag_id)
        if not arquivo.exists():
            return None
        return json.loads(arquivo.read_text(encoding="utf-8"))

    def save(self, dag_id: str, registro: dict) -> None:
        """Gravação atômica: escreve num temporário da mesma pasta e renomeia (os.replace)."""
        arquivo = self._arquivo(dag_id)
        arquivo.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=arquivo.parent, prefix=f".{dag_id}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(registro, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, arquivo)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def delete(self, dag_id: str) -> bool:
        """Apaga o <dag_id>.json; True se existia."""
        arquivo = self._arquivo(dag_id)
        if not arquivo.exists():
            return False
        arquivo.unlink()
        return True


_STORES = {"file": FileUpdateStore}


def get_update_store() -> UpdateStore:
    """Store configurado em PIPELINE_UPDATE_STORE (padrão: file)."""
    nome = os.getenv("PIPELINE_UPDATE_STORE", "file")
    if nome not in _STORES:
        raise ValueError(f"PIPELINE_UPDATE_STORE inválido: {nome!r} (use {' | '.join(_STORES)})")
    return _STORES[nome]()


# --- CONSULTAR / GRAVAR / DELETAR ---------------------------------------------------------------

def get_last_update(dag_id: str):
    """CONSULTAR: última atualização gravada para o dag_id (no tipo original), ou None."""
    return decode_update(get_update_store().get(dag_id))


def save_last_update(dag_id: str, value, run_id: Optional[str] = None) -> None:
    """GRAVAR: grava ou substitui a última atualização do dag_id."""
    if value is None:
        raise ValueError("save_last_update: value não pode ser None (use delete_last_update)")
    registro = {"dag_id": dag_id, **encode_update(value), "run_id": run_id,
                "gravado_em": datetime.now(timezone.utc).isoformat()}
    get_update_store().save(dag_id, registro)
    log.info("last_update gravado para %s: %s", dag_id, value)


def delete_last_update(dag_id: str) -> bool:
    """DELETAR: remove a última atualização (a próxima execução recarrega tudo). True se existia."""
    removido = get_update_store().delete(dag_id)
    log.info("last_update de %s %s", dag_id, "removido" if removido else "não existia")
    return removido


# --- seleção do que atualizar --------------------------------------------------------------------

def _comparavel(a, b) -> None:
    """Falha se a e b não puderem ser comparados (tipos diferentes, datetime com e sem fuso)."""
    ta, tb = _tipo(a), _tipo(b)
    if ta != tb:
        raise TypeError(f"Tipos diferentes na comparação de atualização: {ta} ({a!r}) x {tb} ({b!r})")
    if ta == "datetime" and (a.tzinfo is None) != (b.tzinfo is None):
        raise TypeError(f"Não misture datetime com e sem fuso horário: {a!r} x {b!r}")


_COMPARACOES = {">": operator.gt, ">=": operator.ge, "==": operator.eq,
                "!=": operator.ne, "<": operator.lt, "<=": operator.le}


def check_update(list_update: list, dag_id: str, campo: str = "update_file", comparacao: str = ">") -> list:
    """Devolve só os itens de list_update com item[campo] <comparacao> última atualização gravada.

    - Sem última atualização gravada: devolve a lista inteira.
    - comparacao: ">" (padrão), ">=", "==", "!=", "<" ou "<=".
      Com ">", o item com a mesma data da última carga NÃO volta. Se a fonte informa data sem
      hora e pode publicar de novo no mesmo dia, use ">=" (o reprocesso é idempotente).
    - Item sem o campo, tipos misturados ou texto fora do ISO: erro (nada é descartado em silêncio).
    """
    if comparacao not in _COMPARACOES:
        raise ValueError(f"comparacao inválida: {comparacao!r}. Use uma de {list(_COMPARACOES)}")
    if not isinstance(list_update, list):
        raise TypeError(f"list_update deve ser uma lista de dicts, recebi {type(list_update).__name__}")
    for item in list_update:
        if not isinstance(item, dict) or item.get(campo) is None:
            raise ValueError(f"Item sem '{campo}': {item!r}")
    if list_update:
        primeiro = list_update[0][campo]
        for item in list_update:            # valida todos, mesmo sem marca (falharia na próxima execução)
            _comparavel(primeiro, item[campo])

    ultima = get_last_update(dag_id)
    if ultima is None:
        log.info("%s: sem última atualização gravada; %d itens para carregar", dag_id, len(list_update))
        return list(list_update)
    if list_update:
        _comparavel(ultima, list_update[0][campo])
    comparar = _COMPARACOES[comparacao]
    novos = [item for item in list_update if comparar(item[campo], ultima)]
    log.info("%s: última atualização %s (%s); %d de %d itens para carregar",
             dag_id, ultima, comparacao, len(novos), len(list_update))
    return novos


# --- lista do que a LND atualizou (vai para o XCom pelo runner) ---------------------------------

def validar_update_list(period_ids) -> list:
    """Lista de period_id (texto) sem repetição; [None] = base única; [] = nada novo."""
    if not isinstance(period_ids, list):
        raise TypeError(f"A lista de atualização deve ser uma list, recebi {type(period_ids).__name__}")
    if period_ids == [None]:
        return period_ids
    for p in period_ids:
        if not isinstance(p, str) or not PERIOD_ID.match(p):
            raise ValueError(f"period_id inválido na lista de atualização: {p!r} "
                             "(texto com letras, dígitos, '_' ou '-'; use [None] para base única)")
    if len(set(period_ids)) != len(period_ids):
        raise ValueError(f"period_id repetido na lista de atualização: {period_ids}")
    return period_ids


def push_update_list(ctx, period_ids: list, last_update=None) -> dict:
    """LND: declara o que RAW e TRS vão processar e o last_update a gravar no finish.

    No Airflow, o runner lê este retorno, valida o contrato da LND e publica no XCom.
    No notebook (sem Airflow), só mostra o que seria enviado.
    """
    if ctx.camada != "lnd":
        raise ValueError("push_update_list só pode ser chamado na LND")
    period_ids = validar_update_list(list(period_ids))
    retorno = {"period_ids": period_ids, "last_update": encode_update(last_update)}
    destino = json.loads(os.getenv("PIPELINE_CONTEXT") or "{}").get("update_list_path")
    if destino:
        Path(destino).write_text(json.dumps(retorno), encoding="utf-8")
    log.info("Lista de atualização: %s | last_update: %s", period_ids or "(nada novo)", last_update)
    return retorno
