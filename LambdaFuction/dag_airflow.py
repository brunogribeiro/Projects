from sys import path
path.insert(0, 'PATH')
from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 3, 17, 00), # format -> YYYY-M-D HH:MM
    'retries': 0
}

dag = DAG(
    'ExemploDAG',
    default_args=default_args,
    catchup=False,
    schedule_interval='*/15 14-17 * * *',)

def verify_s3_func(*args, **kwargs):
    s3_updated = ons_files.verify_db('TESTE')
    if s3_updated == False:
        return 'waiting_file'
    return 'create_ec2_acomph'

def waiting_file_func(*args, **kwargs):
    print("Waiting file!")

# Verify if the file from the webhook is already updated to s3
verify_s3 = BranchPythonOperator(
    task_id='verify_s3',
    provide_context=True,
    python_callable=verify_s3_func,
    dag=dag)

# If the file is not updated, the dag call this task
waiting_file = PythonOperator(
    task_id="waiting_file",
    provide_context=True,
    python_callable=waiting_file_func,
    dag=dag
)

command = "python3.6 /home/{}.py {}"

create_ec2 = BashOperator(
    task_id='create_ec2',
    bash_command=command.format("runner", "createec2"),
    dag=dag)

execute = BashOperator(
    task_id='execute',
    bash_command=command.format("runner", "acomph"),
    retries=2,
    dag=dag)

terminate_ec2= BashOperator(
    task_id='terminate_ec2',
    bash_command=command.format("runner, "terminateec2"),
    trigger_rule="all_done",
    dag=dag)

verify_s3 >> waiting_file
verify_s3 >> create_ec2 >> execute >> terminate_ec2

