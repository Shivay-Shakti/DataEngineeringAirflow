from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from random import choice

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_complex = DAG(
    'complex_dag',
    default_args=default_args,
    description='A complex DAG with branching',
    #schedule_interval=timedelta(days=1),
)

def decide_branch(**kwargs):
    if choice([True, False]):
        print("branch_task_1")
        return 'branch_task_1'
    else:
        print("branch_task_2")
        return 'branch_task_2'

branching_task = BranchPythonOperator(
    task_id='branching_task',
    python_callable=decide_branch,
    provide_context=True,
    dag=dag_complex,
)

dummy_task_1 = DummyOperator(
    task_id='branch_task_1',
    dag=dag_complex,
)

dummy_task_2 = DummyOperator(
    task_id='branch_task_2',
    dag=dag_complex,
)

dummy_task_3 = DummyOperator(
    task_id='final_task',
    dag=dag_complex,
)

trigger_next_dag = TriggerDagRunOperator(
    task_id='trigger_next_dag',
    trigger_dag_id='next_dag_to_execute',
    dag=dag_complex,
)

branching_task >> [dummy_task_1, dummy_task_2]
dummy_task_1 >> dummy_task_3
dummy_task_2 >> trigger_next_dag

