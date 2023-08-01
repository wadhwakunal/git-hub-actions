from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow.contrib.operators.dataproc_operator import DataprocCreateClusterOperator, DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
    },
    "worker_config": {
        "num_instances": 0,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": "burner-kunwadhw2"},
    "placement": {"cluster_name": "test"},
    "pyspark_job": {"main_python_file_uri": f"gs://kunal-bucket/code/dataproc.py",
                    "jar_file_uris": [f"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]},
}

dag = DAG('demo_pipeline', description='Pipeline to read csv file from GCS and insert data ino BigQuery using Dataproc', schedule_interval=None, start_date=datetime(2023, 7, 13), catchup=False)

create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dag=dag,
    dataset_id="poc",
    table_id="employees_composer",
    schema_fields=[
        {"name":"EMPLOYEE_ID","type":"INTEGER","mode":"NULLABLE"},
        {"name":"FIRST_NAME","type":"STRING","mode":"NULLABLE"},
        {"name":"LAST_NAME","type":"STRING","mode":"NULLABLE"},
        {"name":"EMAIL","type":"STRING","mode":"NULLABLE"},
        {"name":"PHONE_NUMBER","type":"STRING","mode":"NULLABLE"},
        {"name":"HIRE_DATE","type":"STRING","mode":"NULLABLE"},
        {"name":"JOB_ID","type":"STRING","mode":"NULLABLE"},
        {"name":"SALARY","type":"INTEGER","mode":"NULLABLE"},
        {"name":"COMMISSION_PCT","type":"STRING","mode":"NULLABLE"},
        {"name":"MANAGER_ID","type":"STRING","mode":"NULLABLE"},
        {"name":"DEPARTMENT_ID","type":"INTEGER","mode":"NULLABLE"}
    ],
)

create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id="burner-kunwadhw2",
    cluster_config=CLUSTER_CONFIG,
    region="us-central1",
    cluster_name="test",
    dag=dag
)

pyspark_task = DataprocSubmitJobOperator(
    task_id="pyspark_task", 
    job=PYSPARK_JOB, 
    region="us-central1", 
    project_id="burner-kunwadhw2",
    dag=dag
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id="burner-kunwadhw2",
    cluster_name="test",
    region="us-central1",
    dag=dag
)

create_table >> create_cluster >> pyspark_task >> delete_cluster