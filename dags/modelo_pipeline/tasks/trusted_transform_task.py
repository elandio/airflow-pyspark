"""TRS: aplica nomes finais e tipos do SCHEMA_TRS; separa registros inválidos."""
import logging
import shutil

from modelo_pipeline.tools.const import pipeline as P
from modelo_pipeline.tools.const.schema_tables import SCHEMA_TRS

log = logging.getLogger(__name__)


def _limpar(coluna):
    """trim e string vazia -> NULL."""
    from pyspark.sql import functions as F
    return F.when(F.trim(F.col(coluna)) != "", F.trim(F.col(coluna)))


def _converter(limpo, col):
    from pyspark.sql import functions as F

    tipo = col.tipo.lower()
    if tipo.startswith(("decimal", "double", "float")):
        if col.decimal_virgula:  # "1.250,50" -> "1250.50"
            limpo = F.regexp_replace(F.regexp_replace(limpo, r"\.", ""), ",", ".")
        return limpo.cast(col.tipo)
    if tipo == "date":
        return F.to_date(limpo, col.formato or "yyyy-MM-dd")
    if tipo == "timestamp":
        return F.to_timestamp(limpo, col.formato or "yyyy-MM-dd HH:mm:ss")
    return limpo.cast(col.tipo)


def aplicar_schema(raw_df, schema=None):
    """Retorna (validos, rejeitados). Função pura de DataFrame: testável sem Airflow."""
    from pyspark.sql import functions as F

    schema = schema or SCHEMA_TRS
    casts, motivos = [], []
    for col in schema:
        limpo = _limpar(col.origem)
        convertido = _converter(limpo, col)
        casts.append(convertido.alias(col.destino))
        # falha = não converteu um valor presente, ou é obrigatória e ficou nula
        falhou = convertido.isNull() & (limpo.isNotNull() | F.lit(col.obrigatoria))
        motivos.append(F.when(falhou, F.lit(col.destino)))

    marcado = raw_df.withColumn("_motivo_rejeicao", F.concat_ws(",", *motivos))
    validos = (
        marcado.filter(F.col("_motivo_rejeicao") == "")
        .select(*casts, F.current_timestamp().alias("ts_carga"), P.CAMPO_PARTICAO)
    )
    rejeitados = marcado.filter(F.col("_motivo_rejeicao") != "")
    return validos, rejeitados


def gerar_trusted(dt_ref: str):
    from pyspark.sql import functions as F

    from modelo_pipeline.tools.spark import criar_spark

    spark = criar_spark("trs_" + P.PIPELINE_NOME)
    try:
        raw = (
            spark.read.parquet(str(P.RAW_DIR))
            .filter(F.col(P.CAMPO_PARTICAO) == dt_ref)
        )
        validos, rejeitados = aplicar_schema(raw)
        validos, rejeitados = validos.cache(), rejeitados.cache()

        total_raw, n_validos, n_rej = raw.count(), validos.count(), rejeitados.count()
        if total_raw != n_validos + n_rej:
            raise ValueError(f"Registros perdidos: RAW={total_raw} TRS={n_validos} REJ={n_rej}")
        if n_validos == 0:
            raise ValueError("Nenhum registro válido para gravar na TRS")
        taxa = n_rej / total_raw
        if taxa > P.MAX_TAXA_REJEICAO:
            raise ValueError(f"Taxa de rejeição {taxa:.1%} acima do limite {P.MAX_TAXA_REJEICAO:.0%}")

        (
            validos.write.mode("overwrite")
            .option("compression", "snappy")
            .partitionBy(P.CAMPO_PARTICAO)
            .parquet(str(P.TRS_DIR))
        )

        # rejeitados: limpa a partição anterior para o reprocesso não deixar sobra
        shutil.rmtree(P.REJEITADOS_DIR / f"{P.CAMPO_PARTICAO}={dt_ref}", ignore_errors=True)
        if n_rej:
            (
                rejeitados.write.mode("overwrite")
                .partitionBy(P.CAMPO_PARTICAO)
                .parquet(str(P.REJEITADOS_DIR))
            )
            log.warning("%d registros rejeitados em %s", n_rej, P.REJEITADOS_DIR)

        final = spark.read.parquet(str(P.TRS_DIR / f"{P.CAMPO_PARTICAO}={dt_ref}"))
        esperadas = {c.destino for c in SCHEMA_TRS}
        if not esperadas.issubset(set(final.columns)):
            raise ValueError(f"Schema final inesperado: {final.columns}")
        log.info("TRS OK: validos=%d rejeitados=%d schema=%s", n_validos, n_rej, final.dtypes)
    finally:
        spark.stop()
