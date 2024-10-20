import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Define cluster config
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
}

# Define Spark job
SPARK_JOB = {
    "reference": {"project_id": "{{ var.value.project_id }}"},
    "placement": {"cluster_name": "{{ task_instance.xcom_pull('create_cluster')['cluster_name'] }}"},
    "pyspark_job": {
        "main_python_file_uri": "gs://fanisko-bucket/code/app.py",
        "args": [
            # Add any arguments your PySpark job needs
        ]
    },
}

# Create the DAG
with models.DAG(
    'dataproc_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline using Dataproc',
    schedule_interval=datetime.timedelta(days=1),
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id="{{ var.value.project_id }}",
        cluster_config=CLUSTER_CONFIG,
        region="{{ var.value.region }}",
        cluster_name='etl-cluster-{{ ds_nodash }}',
    )

    submit_job = DataprocSubmitJobOperator(
        task_id='submit_job',
        project_id="{{ var.value.project_id }}",
        region="{{ var.value.region }}",
        job=SPARK_JOB,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id="{{ var.value.project_id }}",
        cluster_name='etl-cluster-{{ ds_nodash }}',
        region="{{ var.value.region }}",
    )

    create_cluster >> submit_job >> delete_cluster