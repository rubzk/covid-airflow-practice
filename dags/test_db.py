from datetime import datetime, timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}



def connect_db():

	query = """SELECT 1,2,3 """

	pg_hook = PostgresHook(postgre_conn_id='postgres_default', schema='airflow')

	connection = pg_hook.get_conn()

	cursor = connection.cursor()

	cursor.execute(query)

	connection.close()





dag = DAG(
    'test_db', default_args=default_args,
    schedule_interval='@once')


connect_postgres = PythonOperator(
	task_id='get_data',
	python_callable=connect_db,
	dag=dag,
	provide_context=True)