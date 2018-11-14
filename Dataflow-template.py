import datetime

from airflow import models
from airflow.contrib.operators import dataflow_operator


yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'dataflow_default_options': {
            'project': 'my-test-project-218908',
            'zone': 'europe-west1-d',
            'tempLocation': 'gs://staging-bucket-tes/staging'
            }
}


# [START bigquery_extracton_test]
with models.DAG(
        'dataflow_test',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    # [END bigquery_extracton_test]

    first_dataflow = dataflow_operator.DataflowTemplateOperator(
            task_id='dataflow_test',
            template='gs://staging-bucket-tes/templates/PublisherDemo',
            gcp_conn_id='google_cloud_default',
            dag=dag
            )

first_dataflow