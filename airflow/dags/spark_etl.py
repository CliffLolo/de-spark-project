"""
This DAG will trigger a job in a Zeppelin notebook with using an HTTP POST request every hour.
You'll need to create a connection to the Zeppelin server in the Airflow UI using the zeppelin_conn connection ID.
You can do this by going to the Admin > Connections page in the Airflow UI and adding a new connection with the following details:
* Conn Id: zeppelin_conn
* Conn Type: HTTP
* Host: http://localhost:8085
You'll also need to make sure that the Airflow webserver has access to the Zeppelin server.
You can do this by adding the IP address of the Airflow webserver to the zeppelin.server.allowed.origins property in the Zeppelin configuration.
Once you've set up the connection and the DAG, you can trigger the DAG using the Airflow UI or the airflow command-line tool.
The DAG will then run on the schedule you defined, triggering the Zeppelin job at the specified intervals.
"""

from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 12, 10),
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
