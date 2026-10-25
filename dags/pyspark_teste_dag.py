from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def rodar_spark():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("teste_airflow_pyspark")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    dados = [("Ana", 30), ("Bruno", 25), ("Carla", 35)]
    df = spark.createDataFrame(dados, ["nome", "idade"])

    df.show()
    print("Media de idade:", df.agg({"idade": "avg"}).collect()[0][0])

    spark.stop()


with DAG(
    dag_id="pyspark_teste",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "spark"],
) as dag:
    PythonOperator(
        task_id="roda_spark",
        python_callable=rodar_spark,
    )
