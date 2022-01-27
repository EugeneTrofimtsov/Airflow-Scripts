import os
import sys
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

os.environ['JAVA_HOME'] = 'path'
os.environ['HADOOP_HOME'] = 'path'
os.environ['HADOOP_CONF_DIR'] = 'path'
os.environ['YARN_HOME'] = 'path'
os.environ['SPARK_HOME'] = 'path'
os.environ['SPARK_CONF_DIR'] = 'path'
os.environ['HIVE_HOME'] = 'path'
os.environ['HIVE_CONF_DIR'] = 'path'
os.environ['HADOOP_OPTS'] = 'opts'

sys.path.append(os.path.join(os.environ['JAVA_HOME'], 'bin'))
sys.path.append(os.path.join(os.environ['HADOOP_HOME'], 'bin'))
sys.path.append(os.path.join(os.environ['YARN_HOME'], 'bin'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))
sys.path.append(os.path.join(os.environ['HIVE_HOME'], 'bin'))

default_args = {
    'owner': 'user',
    'description': 'few words',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'max_active_runs': 1,
    'catchup': False,
    'email': ['user@mail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0
}

connection = BaseHook.get_connection('connection_name')
config = Variable.get('variable_name', deserialize_json=True)
home_dir = config['home_dir']
tuz = config['tuz']
realm = config['realm']
hive_db = config['hive_db']
dag_id = 'dag_name'
task_id = 'task_name'
schedule_interval = '[cron | preset]'

with DAG(dag_id=dag_id, default_args=default_args, schedule_interval=schedule_interval) as dag:

    bash_command = f'spark-submit \
    --class path.to.Main \
    --master yarn \
    --deploy-mode [client|cluster] \
    --keytab {home_dir}{tuz}.keytab \
    --principal {tuz}@{realm} \
    --queue name \
    --jars {home_dir}jar \
    --driver-cores X \
    --driver-memory XG \
    --num-executors X \
    --executor-cors X \
    --executor-memory XG \
    --conf spark.app.name={dag_id}.{task_id} \
    --conf spark.driver.memoryOverhead=XG \
    --conf spark.executor.memoryOverhead=XG \
    --conf spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8 \
    --conf spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8 \
    --conf spark.hadoop.hive.exec.dynamic.partition=true \
    --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \
    --conf spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation=true \
    {home_dir}project_jar \
    PARAM1=={connection.host}:{connection.port}:{connection.schema} \
    PARAM2==value2 \
    PARAM3=={hive_db}'

    # First option - local spark-submit command with bash
    bash_spark_task = BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        dag=dag
    )

    # Second option - airflow ssh operator remote spark submit command with bash
    ssh_spark_task = SSHOperator(
        task_id=task_id,
        ssh_conn_id='ssh_connection_name',
        bash_command=bash_command,
        dag=dag
    )

    spark_conf = {
        'spark.master': 'yarn',
        'spark.submit.deployMode': 'cluster',
        'spark.driver.memory': 'Xg',
        'spark.executor.memory': 'Xg',
        'spark.executor.cores': 'X',
        'spark.num.executors': 'X',
        'spark.driver.memoryOverhead': 'Xg',
        'spark.executor.memoryOverhead': 'Xg',
        'spark.hadoop.hive.exec.dynamic.partition': 'true',
        'spark.hadoop.hive.exec.dynamic.partition.mode': 'nonstrict',
        'spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation': 'true',
        'spark.executor.extraJavaOptions': '-Dfile.encoding=UTF-8',
        'spark.driver.extraJavaOptions': '-Dfile.encoding=UTF-8'
    }

    # Third option - airflow spark submit operator
    spark_submit_task = SparkSubmitOperator(
        task_id=task_id,
        name=f'{dag_id}.{task_id}',
        application=os.path.join(home_dir, 'project_jar'),
        java_class='path.to.Main',
        spark_home='path',
        spark_binary='path',
        principal=f'{tuz}@{realm}',
        keytab=os.path.join(home_dir, f'{tuz}.keytab'),
        jars=os.path.join(home_dir, 'jar'),
        application_args=[
            f'PARAM1=={connection.host}:{connection.port}:{connection.schema}',
            'PARAM2==value2',
            f'PARAM3=={hive_db}'
        ],
        conf=spark_conf,
        dag=dag
    )
