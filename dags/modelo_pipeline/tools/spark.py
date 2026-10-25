"""Criação padronizada da SparkSession (modo local)."""


def criar_spark(app_name: str):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        # só sobrescreve as partições que estão sendo gravadas (reprocesso idempotente)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # dt_ref=2026-10-25 continua string, sem virar date
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
