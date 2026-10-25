from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from modelo_pipeline.tasks.trusted_transform_task import gerar_trusted
from modelo_pipeline.tools.const import tasks as T


def build_trusted_group() -> TaskGroup:
    with TaskGroup(group_id=T.GRP_TRUSTED.id, tooltip=T.GRP_TRUSTED.nome) as grupo:
        PythonOperator(
            task_id=T.TRUSTED_TRANSFORM.id, python_callable=gerar_trusted,
            op_kwargs={"dt_ref": "{{ ds }}"})
    return grupo
