from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from modelo_pipeline.tasks.raw_transform_task import gerar_raw
from modelo_pipeline.tools.const import tasks as T


def build_raw_group() -> TaskGroup:
    with TaskGroup(group_id=T.GRP_RAW.id, tooltip=T.GRP_RAW.nome) as grupo:
        PythonOperator(
            task_id=T.RAW_TRANSFORM.id, python_callable=gerar_raw,
            op_kwargs={"dt_ref": "{{ ds }}"})
    return grupo
