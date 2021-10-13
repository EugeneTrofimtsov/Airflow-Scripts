import os
import psutil
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

CLEAR_DIRS = [
    '/app/airflow/logs'
]
DAYS = 31  # срок хранения логов в днях

default_args = {
    'owner': 'user',
    'description': 'clear airflow logs',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'max_active_runs': 1,
    'catchup': False,
    'email': ['user@mail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def get_folder_size(folder):
    if not os.path.exists(folder):
        return 0
    total_size = os.path.getsize(folder)
    for item in os.listdir(folder):
        item_path = os.path.join(folder, item)
        if os.path.isfile(item_path):
            total_size += os.path.getsize(item_path)
        elif os.path.isdir(item_path):
            total_size += get_folder_size(item_path)
    return total_size


def get_folder_size_in_units(folder, unit):
    units_dict = {'Bytes': 0, 'KB': 1, 'MB': 2, 'GB': 3}
    return get_folder_size(folder) / (1024 ** units_dict[unit])


def clear_logs():
    now = datetime.now()
    for cl_dir in CLEAR_DIRS:
        print(f'Каталог: {cl_dir}')
        free = psutil.disk_usage(cl_dir).free / (1024 ** 3)
        print(f'Свободно: {free:.4} ГБ в разделе, где находится каталог')
        catalog_size = get_folder_size_in_units(cl_dir, 'GB')
        print(f'Размер каталога: {catalog_size:.4} ГБ')
        for root, dirs, files in os.walk(cl_dir):
            for file in files:
                full_file = os.path.join(root, file)
                file_size = os.path.getsize(full_file) / 1024 ** 2
                file_date = datetime.fromtimestamp(os.path.getatime(full_file))
                if file.endswith('.log') and now - file_date > timedelta(days=DAYS):
                    print('Удаления файла:', file)
                    print('Полный путь:', full_file)
                    print(f'Размер: {file_size:.4} MB')
                    print('Дата последнего обращения:', file_date)
                    os.remove(full_file)
            for catalog in dirs:
                full_dir = os.path.join(root, catalog)
                dir_date = datetime.fromtimestamp(os.path.getatime(full_dir))
                if not os.listdir(full_dir) and now - dir_date > timedelta(days=DAYS):
                    print('Удаление пустого каталога:', dir_date)
                    print('Дата последнего обращения:', full_dir)
                    os.rmdir(full_dir)


with DAG(dag_id='clear_airflow_logs', default_args=default_args, schedule_interval=[cron | preset]) as dag:

    clear_logs_task = PythonOperator(
        task_id='clear_logs_task',
        python_callable=clear_logs(),
        dag=dag
    )
