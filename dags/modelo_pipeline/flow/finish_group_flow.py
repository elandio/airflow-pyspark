from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from modelo_pipeline.tasks.finish_tasks import limpar_landing
from modelo_pipeline.tasks.prepare_tasks import registrar_execucao
from modelo_pipeline.tools.const import tasks as T


def build_finish_group() -> TaskGroup:
    with TaskGroup(group_id=T.GRP_FINISH.id, tooltip=T.GRP_FINISH.nome) as grupo:
        limpar = PythonOperator(
            task_id=T.FINISH_LIMPAR.id, python_callable=limpar_landing,
            op_kwargs={"dt_ref": "{{ ds }}"})
        registrar = PythonOperator(
            task_id=T.FINISH_REGISTRAR.id, python_callable=registrar_execucao,
            op_kwargs={"evento": "fim", "dt_ref": "{{ ds }}", "run_id": "{{ run_id }}"})
        limpar >> registrar
    return grupo
