# coding: utf-8
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import boto3


s3BucketParquet = Variable.get('dag_bucket', default_var='test')
JsonConfig = 'config/config.json'
s3 = boto3.client('s3')

content_object = s3.get_object(Bucket="{}".format(s3BucketParquet), Key=JsonConfigBBCE)
filej = content_object['Body']
jsondata = json.load(filej)

s3BucketParquet=jsondata['s3_bucket']
schema=jsondata['schema_dms']
schema_dw=jsondata['schema_dw']
arn_task_dms=jsondata['arn_task_dms']
region_name=jsondata['region_aws']

start_date = datetime(2020, 05, 1)
schedule = Variable.get('dag_schedule', default_var="0 13 * * *")

default_args = {
    'owner': 'DAS',
    'depends_on_past': False,
    'start_date': start_date,
}

dag = DAG(
    'TesteDAG',
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
    schedule_interval=schedule)

default_spark_conf = {
    'spark.executor.memory': '2g'
    , 'spark.dynamicAllocation.initialExecutors': '1'
    , 'spark.dynamicAllocation.minExecutors': '1'
    , 'spark.dynamicAllocation.maxExecutors': '2'
    , 'spark.sql.shuffle.partitions': '24'
    , 'spark.default.parallelism': '4'
    , 'spark.cores.max':  '4'
    , 'spark.executor.cores': '1'
}
##START DMS TASK

load_dms_tables = []
load_dms_tables.append("--arnTaskDMS={}".format(arn_task_dms))
load_dms_tables.append("--regionName={}".format(region_name))

load_dms_tables = SparkSubmitOperator(
    task_id='load_dms_tables'
    , name='load_dms_tables'
    , application='/pipeline/dag_data/load_dms_table.py'
    , application_args=load_dms_tables
    , verbose=False
    , conf=default_spark_conf
    , conn_id='spark_default'
    , dag=dag
)


## START TRANSFORMATION

app_test = []
app_test.append("--bucket={}".format(s3BucketParquet))
app_test.append("--schema={}".format(schema))
app_test.append("--tableOrigem={}".format('Table'))
app_test.append("--tableDestino={}".format('Table1'))

transform_dim_unidade_valor = SparkSubmitOperator(
    task_id='transform_Table1'
    , name='Table1'
    , application='/pipeline/dag_data/bbce/dim_unidade_valor.py'
    , application_args=app_test
    , verbose=False
    , conf=default_spark_conf
    , conn_id='spark_default'
    , dag=dag
)

app_load_test = []
app_load_test.append("--dimtable=Table1")
app_load_test.append("--bucket={}".format(s3BucketParquet))
app_load_test.append("--schema={}".format(schema_dw))
app_load_test.append("--deduplica={}".format('N'))
app_load_test.append("--pks={}".format('SK_TABLE'))
app_load_test.append("--lconfig={}".format(JsonConfig))

load_test = SparkSubmitOperator(
    task_id='app_load_test',
    name='load_test',
    application='/pipeline/dag_data/to_redshift.py',
    application_args=app_load_test,
    packages="com.databricks:spark-redshift_2.11:3.0.0-preview1,com.amazonaws:aws-java-sdk-s3:1.11.679,com.amazonaws:aws-java-sdk-core:1.11.679",
    verbose=False,
    conf=default_spark_conf,
    conn_id='spark_default',
    dag=dag,
)


app_test >> app_load_test