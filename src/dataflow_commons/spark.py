"""SparkSession padronizada (modo local)."""
import os

# Driver JDBC do SQL Server. A mesma versão é baixada no docker/Dockerfile (mude nos dois lugares).
MSSQL_JDBC_MAVEN = "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"


def criar_spark(app_name: str, jdbc: bool = False):
    """jdbc=True carrega o driver do SQL Server: o jar de PIPELINE_JDBC_JARS (imagem Docker) ou,
    sem a variável (venv), o pacote do Maven (baixado uma vez para ~/.ivy2)."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")  # partição lida como string
    )
    if jdbc:
        jars = os.environ.get("PIPELINE_JDBC_JARS")
        builder = (builder.config("spark.jars", jars).config("spark.driver.extraClassPath", jars) if jars
                   else builder.config("spark.jars.packages", MSSQL_JDBC_MAVEN))
    return builder.getOrCreate()
