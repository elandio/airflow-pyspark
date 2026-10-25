"""RAW: preserva o dado original (tudo string), normaliza nomes e grava Parquet."""
import logging

from modelo_pipeline.tools.const import pipeline as P

log = logging.getLogger(__name__)


def gerar_raw(dt_ref: str):
    from pyspark.sql import functions as F

    from modelo_pipeline.tools.spark import criar_spark
    from modelo_pipeline.tools.texto import normalizar_colunas

    spark = criar_spark("raw_" + P.PIPELINE_NOME)
    try:
        origem = P.LND_DIR / f"{P.CAMPO_PARTICAO}={dt_ref}" / P.ARQUIVO_LND

        df = (
            spark.read.option("header", True)
            .option("inferSchema", False)          # tudo string: sem perda de informação
            .option("sep", P.CSV_SEP)
            .option("encoding", P.CSV_ENCODING)
            .csv(str(origem))
        )
        df = df.toDF(*normalizar_colunas(df.columns))
        df = (
            df.withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_source_file", F.lit(P.ARQUIVO_LND))
            .withColumn(P.CAMPO_PARTICAO, F.lit(dt_ref))
        )

        total_origem = df.count()
        (
            df.write.mode("overwrite")             # dynamic: só a partição dt_ref
            .option("compression", "snappy")
            .partitionBy(P.CAMPO_PARTICAO)
            .parquet(str(P.RAW_DIR))
        )

        total_raw = spark.read.parquet(str(P.RAW_DIR / f"{P.CAMPO_PARTICAO}={dt_ref}")).count()
        if total_raw != total_origem:
            raise ValueError(f"Contagem divergente LND={total_origem} RAW={total_raw}")
        log.info("RAW OK: %d linhas, colunas=%s", total_raw, df.columns)
    finally:
        spark.stop()
