from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste"],
) as dag:
    BashOperator(
        task_id="diz_ola",
        bash_command="echo 'Airflow funcionando!'",
    )
