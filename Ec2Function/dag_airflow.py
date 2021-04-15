from sys import path
path.insert(0, '/git/master/Ferramentas/AWS')
import create_ec2
import terminate_ec2
import execute_command
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import configuracao as conf

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 3, 17, 00), # format -> YYYY-M-D HH:MM
    'retries': 0
}

dag = DAG(
    'DAGExample',
    default_args=default_args,
    catchup=False,
    schedule_interval='*/15 14-17 * * *',)
	
tagname = 'ETL_Example'

def verifyS3Func(*args, **kwargs):
    s3_updated = ons_files.verify_db('TESTE')
    if s3_updated == False:
        return 'waitingFile'
    return 'createEc2_acomph'

def waitingFileFunc(*args, **kwargs):
    print("Waiting file!")

def create():
    create_ec2.CreateEc2().CreateInstance(conf.aws['amiPDI'],conf.aws['ec2typeMed'],tagname)
def terminate():
    terminate_ec2.TerminateEc2().Terminate(tagname,'running')
def execute():
    exec_command.execute_command.exec().run_command(tagname, 'date')
    

# Verify if the file from the webhook is already updated to s3
verifyS3 = BranchPythonOperator(
    task_id='verifyS3',
    provide_context=True,
    python_callable=verifyS3Func,
    dag=dag)

# If the file is not updated, the dag call this task
waitingFile = PythonOperator(
    task_id="waitingFile",
    provide_context=True,
    python_callable=waitingFileFunc,
    dag=dag
)

createEc2 = PythonOperator(
    task_id='createEc2',
    python_callable=create,
    dag=dag)

ExecCommand = PythonOperator(
    task_id='Executa_GIT',
    python_callable=exec_git,
    dag=dag)

terminateEc2 = PythonOperator(
    task_id='terminateEc2',
    python_callable=terminate,
    trigger_rule="all_done",
    dag=dag)
	
verifyS3 >> waitingFile
verifyS3 >> createEc2 >> ExecCommand >> terminateEc2

