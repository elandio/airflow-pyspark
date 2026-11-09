"""Gravações padrão das camadas: write_lnd, write_raw, write_trs e write_biz. Só gravam os dados."""
import logging
from pathlib import Path
from typing import Optional

from .paths import get_path_lnd, get_path_raw, get_path_trs, get_path_uri
from .texto import normalize_replace, normalizar_colunas

log = logging.getLogger(__name__)


def write_lnd(ctx, sub_path: Optional[str], conteudo, tipo_arquivo: str, nome: Optional[str] = None) -> Path:
    """Grava o arquivo como veio da fonte na LND. Só grava os dados.

    sub_path      último diretório, dentro da LND (ex.: o period_id). None -> raiz da LND.
    tipo_arquivo  extensão do arquivo, sem ponto: "json", "csv", "parquet", "xlsx", "csv.gz"...
    nome          nome sem extensão; padrão <dataset>_<sub_path> (ou <dataset> na base única)
    conteudo      bytes (texto é gravado em UTF-8)

    Ex.: write_lnd(ctx, "202601", response.content, "json")  ->  lnd/.../202601/pimpf_202601.json
    """
    tipo = str(tipo_arquivo).strip().lower().lstrip(".")
    base = nome or (f"{ctx.dataset}_{sub_path}" if sub_path else ctx.dataset)
    if base.lower().endswith(f".{tipo}"):         # aceita nome já com a extensão, sem duplicar
        base = base[:-(len(tipo) + 1)]
    if isinstance(conteudo, str):
        conteudo = conteudo.encode("utf-8")

    pasta = get_path_lnd(ctx, sub_path)
    pasta.mkdir(parents=True, exist_ok=True)
    destino = pasta / f"{base}.{tipo}"
    tmp = destino.with_name(destino.name + ".part")   # grava em .part e renomeia: sem arquivo pela metade
    tmp.write_bytes(conteudo)
    tmp.replace(destino)
    log.info("LND gravada em %s", destino)
    return destino


def write_raw(ctx, df, sub_path: Optional[str] = None, normalizar: bool = False) -> Path:
    """Grava a RAW em Parquet: colunas de dados como string e as colunas de controle.

    sub_path: último diretório, dentro da RAW (ex.: o period_id). None -> raiz da RAW.
    Nomes das colunas: como vierem (normalizar=False) ou padronizados por texto.normalize_replace
    (normalizar=True: 'Preço Unitário (R$)' -> 'PRECO_UNITARIO_R$'). Nomes repetidos ganham sufixo _1, _2...
    Controle: _NM_ARQUIVO_LND (nulo se não vier) e _DH_INGESTAO_RAW (momento da gravação se não vier).
    Sobrescreve o diretório de destino.
    """
    from pyspark.sql import functions as F

    nm, dh = "_NM_ARQUIVO_LND", "_DH_INGESTAO_RAW"     # criadas pelos leitores da LND (readers)
    dados = [c for c in df.columns if c not in (nm, dh)]
    novos = normalizar_colunas(dados, normalize_replace if normalizar else str)
    colunas = [F.col(f"`{o}`").cast("string").alias(n) for o, n in zip(dados, novos)]
    colunas.append((F.col(nm) if nm in df.columns else F.lit(None)).cast("string").alias(nm))
    colunas.append((F.col(dh) if dh in df.columns else F.current_timestamp()).cast("timestamp").alias(dh))

    destino = get_path_raw(ctx, sub_path)
    df.select(*colunas).write.mode("overwrite").option("compression", "snappy").parquet(str(destino))
    log.info("RAW gravada em %s", destino)
    return destino


def write_trs(ctx, df, sub_path: Optional[str] = None, partition_by=None, mode: str = "overwrite",
              options: Optional[dict] = None) -> Path:
    """Grava o DataFrame na TRS em Parquet, com o writer do Spark. Só grava: não valida nem conta.

    sub_path      último diretório, dentro da TRS (ex.: o period_id). None -> raiz da TRS.
    partition_by  coluna(s) para o partitionBy do Spark.
    mode          modo do Spark: overwrite (padrão), append, ignore, error.
    options       options do writer do Spark; somam-se a / sobrescrevem as padrão:
                  compression=snappy e, com partition_by, partitionOverwriteMode=dynamic
                  (sobrescreve só as partições presentes no DataFrame).

    Ex.: write_trs(ctx, df, "202601")
         write_trs(ctx, df, partition_by=["SG_UF"], options={"compression": "zstd"})
    """
    destino = get_path_trs(ctx, sub_path)
    _gravar_parquet(df, destino, partition_by, mode, options)
    log.info("TRS gravada em %s", destino)
    return destino


def write_biz(df, uri: str, partition_by=None, mode: str = "overwrite", options: Optional[dict] = None) -> Path:
    """Grava o DataFrame na BIZ em Parquet, no caminho do URI 'biz://<dominio>/<produto>/<tabela>'.

    Mesmos partition_by, mode e options do write_trs (com partition_by: dynamic overwrite,
    só as partições presentes no DataFrame são regravadas).
    """
    if not str(uri).startswith("biz://"):
        raise ValueError(f"write_biz grava só na BIZ: {uri!r} (use biz://<dominio>/<produto>/<tabela>)")
    destino = get_path_uri(uri)
    _gravar_parquet(df, destino, partition_by, mode, options)
    log.info("BIZ gravada em %s", destino)
    return destino


def _gravar_parquet(df, destino: Path, partition_by, mode: str, options: Optional[dict]) -> None:
    """Grava Parquet com o writer do Spark: snappy e, com partition_by, dynamic overwrite (options sobrescrevem)."""
    padrao = {"compression": "snappy"}
    if partition_by:
        padrao["partitionOverwriteMode"] = "dynamic"
    writer = df.write.mode(mode).options(**{**padrao, **(options or {})})
    if partition_by:
        writer = writer.partitionBy(*([partition_by] if isinstance(partition_by, str) else partition_by))
    writer.parquet(str(destino))
