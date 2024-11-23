from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor, EmrJobFlowSensor
from airflow.utils.helpers import exactly_one


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 11, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),  # Correct placement of retry_delay
}

# DAG definition
dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='DAG to run PySpark job daily on EMR from 23rd November',
    schedule_interval='@daily', # Removed invalid 'retry_delay' here
    #template_searchpath=['s3a://etlspark/monthly/22-11-2024/configurations/config.json '],
)

# Rest of the code remains the same
JOB_FLOW_OVERRIDES = {
    'Name': 'Spark Cluster',
    'LogUri': 's3://etlspark/monthly/22-11-2024/logs/',
    'ReleaseLabel': 'emr-7.5.0',
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Slave',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            }
        ],
        'Ec2KeyName': 'emr-cluster',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    "BootstrapActions": [
        {
            "Name": "Custom Bootstrap Action",
            "ScriptBootstrapAction": {
                "Path": "s3://etlspark/bootstrap.sh",
            }
        }
    ],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

create_job_flow_task = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag,
    do_xcom_push=True,
)

# Add a sensor to wait until the cluster is in "WAITING" state
cluster_wait_sensor = EmrJobFlowSensor(
    task_id='cluster_wait_sensor',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    poke_interval=60,  # Check every minute
    timeout=600,  # Timeout after 10 minutes if the cluster is not in WAITING state
    target_states ='WAITING',  # We want to wait until the cluster reaches 'WAITING' state
    dag=dag,
)
spark_step = {
    'Name': 'ETL Transform',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        "Args": [
            "spark-submit",
            "--deploy-mode", "cluster",
            "--py-files", "s3://etlspark/monthly/22-11-2024/py_files_22-11-2024.zip",
            "s3://etlspark/monthly/22-11-2024/main.py",
            "--json_file_path", "{{ 's3a://etlspark/monthly/22-11-2024/configurations/config.json' }}"
        ]
    }
}

add_step_task = EmrAddStepsOperator(
    task_id='add_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=[spark_step],
    dag=dag,
)

step_sensor_task = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)

create_job_flow_task >> cluster_wait_sensor >> add_step_task >> step_sensor_task
