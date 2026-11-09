"""Schema da TRS (arquivo referenciado em trs.schema): leitura, validação e aplicação.

Cada coluna do YAML (colunas:) tem origem, destino, tipo e, opcional, descricao. O apply_schema só
seleciona, renomeia e converte (cast): conversões especiais (vírgula decimal, datas, símbolos da fonte)
são feitas no TRATAR do notebook, antes.
"""
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

_TIPOS = re.compile(
    r"^(string|int|bigint|double|float|boolean|date|timestamp|decimal\(\d+,\d+\))$")


@dataclass(frozen=True)
class Coluna:
    """Uma coluna da TRS: de onde vem (origem), como se chama (destino) e o tipo do Spark."""
    origem: str                      # nome da coluna no DataFrame de entrada (nomes da RAW, já tratados)
    destino: str                     # nome final na TRS, MAIÚSCULO com prefixo (CD_, NM_, VL_, DT_...)
    tipo: str                        # string | int | bigint | double | float | boolean | date | timestamp | decimal(p,s)
    descricao: str = ""


_CHAVES_COLUNA = {"origem", "destino", "tipo", "descricao"}


def carregar_schema(caminho) -> list:
    """Lê o schema_trs.yaml (lista em 'colunas'), valida e devolve a lista de Coluna."""
    dados = yaml.safe_load(Path(caminho).read_text(encoding="utf-8")) or {}
    colunas = []
    for i, c in enumerate(dados.get("colunas") or [], 1):
        extras = set(c) - _CHAVES_COLUNA
        if extras:
            raise ValueError(f"{Path(caminho).name}, coluna {i} ({c.get('destino')}): chaves desconhecidas "
                             f"{sorted(extras)} (permitidas: {sorted(_CHAVES_COLUNA)})")
        colunas.append(Coluna(**c))
    validar_schema(colunas)
    return colunas


def validar_schema(colunas) -> None:
    """Exige ao menos uma coluna, origem e destino sem repetição e tipos válidos."""
    if not colunas:
        raise ValueError("schema da TRS sem colunas")
    for nome, valores in (("origem", [c.origem for c in colunas]),
                          ("destino", [c.destino for c in colunas])):
        if len(valores) != len(set(valores)):
            raise ValueError(f"Valores duplicados em '{nome}' no schema")
    for c in colunas:
        if not _TIPOS.match(c.tipo):
            raise ValueError(f"Tipo inválido '{c.tipo}' em {c.destino}")


def apply_schema(df, schema):
    """Aplica o schema da TRS: 'origem' vira 'destino', convertida com cast para 'tipo'.
    Preserva _DH_INGESTAO_TRS (criada pelo read_raw), se existir.

    Falha com ValueError (com a coluna e o motivo) se a 'origem' não existe no df ou o
    'tipo' não é aceito pelo Spark. Valor que não converte vira NULL: o cast não gera erro.
    """
    from pyspark.sql import functions as F

    colunas = []
    for col in schema:
        try:
            convertida = F.col(col.origem).cast(col.tipo).alias(col.destino)
            df.select(convertida)                  # só análise (lazy): valida coluna e tipo
        except Exception as erro:
            raise ValueError(
                f"Erro ao aplicar o schema na coluna {col.origem!r} -> {col.destino!r} "
                f"({col.tipo}): {str(erro).strip()}") from erro
        colunas.append(convertida)

    extras = [c for c in ("_DH_INGESTAO_TRS",) if c in df.columns]
    return df.select(*colunas, *extras)
