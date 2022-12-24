from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 12, 30),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('zeppelin_job_dag', default_args=default_args, schedule_interval=timedelta(hours=1))

# Define the HTTP request parameters
url = 'http://localhost:8085/api/notebook/job/<notebookID>'
method = 'POST'
headers = {'Content-Type': 'application/json'}

# Create the HTTP Operator
zeppelin_job = SimpleHttpOperator(
    task_id='zeppelin_job',
    method=method,
    http_conn_id='zeppelin_conn',
    endpoint=url,
    headers=headers,
    dag=dag
)

# Set the task dependencies
zeppelin_job
