from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from modelo_pipeline.tasks.landing_download_task import baixar_para_landing
from modelo_pipeline.tools.const import tasks as T


def build_landing_group() -> TaskGroup:
    with TaskGroup(group_id=T.GRP_LANDING.id, tooltip=T.GRP_LANDING.nome) as grupo:
        PythonOperator(
            task_id=T.LANDING_DOWNLOAD.id, python_callable=baixar_para_landing,
            op_kwargs={"dt_ref": "{{ ds }}"})
    return grupo
