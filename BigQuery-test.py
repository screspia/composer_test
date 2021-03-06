import datetime

from airflow import models
from airflow.contrib.operators import bigquery_get_data
from airflow.contrib.operators import bigquery_operator


yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

bq_dataset_name = 'test.users_data_last'

default_dag_args = {
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}


# [START bigquery_extracton_test]
with models.DAG(
        'users_data_bigquery_load_test',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    # [END bigquery_extracton_test]

    bq_extraction_test = bigquery_operator.BigQueryOperator(
            task_id='bq_extraction_test_query',
            bql="""
            SELECT *
            FROM `test.users_data` 
            """,
            use_legacy_sql=False,
            destination_dataset_table=bq_dataset_name)

bq_extraction_test 