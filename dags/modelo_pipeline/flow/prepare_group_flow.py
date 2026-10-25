from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from modelo_pipeline.tasks.prepare_tasks import registrar_execucao, validar_metadados
from modelo_pipeline.tools.const import tasks as T


def build_prepare_group() -> TaskGroup:
    with TaskGroup(group_id=T.GRP_PREPARE.id, tooltip=T.GRP_PREPARE.nome) as grupo:
        validar = PythonOperator(
            task_id=T.PREPARE_VALIDAR.id, python_callable=validar_metadados)
        registrar = PythonOperator(
            task_id=T.PREPARE_REGISTRAR.id, python_callable=registrar_execucao,
            op_kwargs={"evento": "inicio", "dt_ref": "{{ ds }}", "run_id": "{{ run_id }}"})
        validar >> registrar
    return grupo
