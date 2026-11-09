"""Leituras padrão. Todas devolvem DataFrame Spark e aceitam o ctx.

LND -> DataFrame:  read_lnd_csv, read_lnd_excel, df_from_pandas (tudo string), read_lnd_parquet (tipos do arquivo)
RAW -> DataFrame:  read_raw
TRS -> DataFrame:  read_trs (todos os períodos ou um); read_trs_uri (pelo URI do Dataset, para a BIZ)

Colunas de controle (data-hora = momento da leitura, fixado na chamada da função):
  read_lnd_* / df_from_pandas   _NM_ARQUIVO_LND (arquivo lido) e _DH_INGESTAO_RAW
  read_raw                      remove as duas acima e acrescenta _DH_INGESTAO_TRS
"""
import logging
from datetime import datetime
from typing import Optional

from .paths import get_path_lnd, get_path_raw, get_path_trs, get_path_uri

log = logging.getLogger(__name__)


CONTROLE_RAW = ("_NM_ARQUIVO_LND", "_DH_INGESTAO_RAW")
CONTROLE_TRS = "_DH_INGESTAO_TRS"


def _agora():
    """Momento da leitura como literal: não muda se o DataFrame for recalculado (ao contrário de current_timestamp)."""
    from pyspark.sql import functions as F

    return F.lit(datetime.now()).cast("timestamp")


def _com_controle_raw(df, nm_arquivo):
    """Acrescenta _NM_ARQUIVO_LND (coluna ou valor) e _DH_INGESTAO_RAW (momento da leitura)."""
    from pyspark.sql import Column
    from pyspark.sql import functions as F

    nome = nm_arquivo if isinstance(nm_arquivo, Column) else F.lit(nm_arquivo)
    return df.select("*", nome.cast("string").alias(CONTROLE_RAW[0]), _agora().alias(CONTROLE_RAW[1]))


def read_lnd_csv(ctx, spark, period_id: Optional[str] = None, padrao: str = "*.csv", **opcoes):
    """Lê todos os *.csv do diretório da LND, tudo como string.

    Usa raw.leitura do dag_params.yaml (separador, encoding, cabecalho); 'opcoes' sobrescrevem.
    'padrao' escolhe os arquivos (ex.: "mensal_*.csv" para a sub-base mensal de um download único).
    """
    from pyspark.sql import functions as F

    leitura = ctx.cfg.get("leitura", {})
    reader = (spark.read.option("header", leitura.get("cabecalho", True))
              .option("inferSchema", False)                 # preserva o original
              .option("sep", leitura.get("separador", ","))
              .option("encoding", leitura.get("encoding", "utf-8"))
              .option("pathGlobFilter", padrao))
    for k, v in opcoes.items():
        reader = reader.option(k, v)
    df = reader.csv(str(get_path_lnd(ctx, period_id)))
    return _com_controle_raw(df, F.col("_metadata.file_name"))


def read_lnd_parquet(ctx, spark, period_id: Optional[str] = None, padrao: str = "*.parquet", **opcoes):
    """Lê todos os *.parquet do diretório da LND (tipos do arquivo; o write_raw converte para string).

    'padrao' escolhe os arquivos (ex.: "mensal_*.parquet"); 'opcoes' vão para o reader do Spark
    (ex.: mergeSchema=True quando os arquivos do período têm colunas diferentes).
    """
    from pyspark.sql import functions as F

    reader = spark.read.option("pathGlobFilter", padrao)
    for k, v in opcoes.items():
        reader = reader.option(k, v)
    df = reader.parquet(str(get_path_lnd(ctx, period_id)))
    return _com_controle_raw(df, F.col("_metadata.file_name"))


def df_from_pandas(spark, pdf, nm_arquivo: Optional[str] = None):
    """Converte qualquer DataFrame pandas (XLSX, API, tabela web, PDF) em DataFrame Spark de strings.

    Acrescenta as colunas de controle da RAW; nm_arquivo preenche _NM_ARQUIVO_LND (None se não houver arquivo).
    """
    from pyspark.sql.types import StringType, StructField, StructType

    def _str(v):
        return None if v is None or v != v else str(v)    # v != v detecta NaN

    linhas = [[_str(v) for v in linha] for linha in pdf.itertuples(index=False, name=None)]
    colunas = [str(c) for c in pdf.columns]
    df = spark.createDataFrame(linhas, StructType([StructField(c, StringType(), True) for c in colunas]))
    return _com_controle_raw(df, nm_arquivo)


def read_lnd_excel(ctx, spark, period_id: Optional[str] = None, arquivo: Optional[str] = None,
                   sheet_name=0, header: int = 0):
    """Lê um XLSX/XLS da LND (via pandas) como DataFrame de strings. Requer pandas e openpyxl."""
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("Instale pandas e openpyxl para ler Excel: pip install pandas openpyxl") from exc

    pasta = get_path_lnd(ctx, period_id)
    if arquivo:
        alvo = pasta / arquivo
    else:
        candidatos = sorted([*pasta.glob("*.xlsx"), *pasta.glob("*.xls")])
        if len(candidatos) != 1:
            raise ValueError(f"Esperado 1 arquivo Excel em {pasta}, achei {len(candidatos)}; informe 'arquivo'")
        alvo = candidatos[0]
    pdf = pd.read_excel(alvo, sheet_name=sheet_name, header=header, dtype=str)
    return df_from_pandas(spark, pdf, nm_arquivo=alvo.name)


def read_raw(ctx, spark, period_id: Optional[str] = None):
    """Lê a RAW do período (ou do dataset todo, no modo 'unico').

    Remove as colunas de controle da RAW e acrescenta _DH_INGESTAO_TRS.
    """
    df = spark.read.parquet(str(get_path_raw(ctx, period_id))).drop(*CONTROLE_RAW)
    df = df.select("*", _agora().alias(CONTROLE_TRS))
    return df


def read_trs(ctx, spark, period_id: Optional[str] = None):
    """Lê a TRS de um período ou, sem period_id, a TRS inteira, com as colunas de partição (partition_by).

    Pastas de período (trs/.../<period_id>) são lidas uma a uma e empilhadas; base única ou
    particionada direto na raiz (trs/.../<COL>=<valor>) é lida de uma vez.
    """
    base = get_path_trs(ctx, period_id)
    if period_id is not None:
        return spark.read.parquet(str(base))
    return _ler_trs(spark, base)


def read_trs_uri(spark, uri: str):
    """Lê a TRS inteira pelo URI do Dataset (ex.: 'trs://producao_fisica_industrial/ibge/sidra/pimpf').

    Para a BIZ, que não tem o ctx da ingestão. Mesmo comportamento de read_trs(ctx, spark).
    """
    base = get_path_uri(uri)
    if not uri.startswith("trs://"):
        raise ValueError(f"read_trs_uri lê só a TRS: {uri!r}")
    if not base.exists():
        raise FileNotFoundError(f"TRS não encontrada para {uri}: {base}")
    return _ler_trs(spark, base)


def _ler_trs(spark, base):
    """Lê a TRS da pasta base: pastas de período empilhadas, ou de uma vez (base única ou partições)."""
    if not base.is_dir():
        return spark.read.parquet(str(base))
    periodos = sorted(p for p in base.iterdir()
                      if p.is_dir() and "=" not in p.name and not p.name.startswith(("_", ".")))
    if not periodos:
        return spark.read.parquet(str(base))
    return union_by_name(spark.read.parquet(str(p)) for p in periodos)


def union_by_name(frames):
    """Empilha DataFrames pelo nome da coluna; colunas ausentes em algum viram NULL."""
    frames = list(frames)
    if not frames:
        raise ValueError("Nenhum DataFrame para empilhar")
    resultado = frames[0]
    for df in frames[1:]:
        resultado = resultado.unionByName(df, allowMissingColumns=True)
    return resultado
