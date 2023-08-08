from datetime import datetime
from airflow import DAG
from utils import common
import os
from airflow.operators.python import PythonOperator

dag = DAG('trial', description='Pipeline to test refernce files in DAG', schedule_interval=None, start_date=datetime(2023, 8, 3), catchup=False)

DAGS_FOLDER = os.getenv("DAGS_FOLDER")

task_1 = PythonOperator(
    task_id="task_1",
    python_callable=common.yamlGetter,
    op_kwargs={"config_file": f"{DAGS_FOLDER}/config/config.yml"},
    dag=dag
)