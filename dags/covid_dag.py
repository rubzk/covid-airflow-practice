from datetime import datetime, timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago

import requests
import pandas as pd 
import json





def request_data(**context):

	date = datetime.today() - timedelta(1)
	date = date.strftime('%Y-%m-%d') 

	columns =  ['date', 'confirmed', 'deaths', 'recovered', 'confirmed_diff','deaths_diff', 'recovered_diff', 'last_update', 'active', 'active_diff','fatality_rate', 'region']
	regions = ['ARG','BRA','COL','MEX','CHL','PER','PRY','URY']

	df = pd.DataFrame(columns=columns)

	for region in regions:

		url = 'https://covid-api.com/api/reports?date={}&iso={}'.format(date,region)
		data = requests.get(url)
		data = json.loads(data.text)
		df = df.append(data['data'])

	return df





def transform_data(**context):

	ti = context['ti']

	df = ti.xcom_pull(task_ids='get_data')

	df = pd.concat([df.drop(columns='region'), df['region'].apply(pd.Series)], axis=1)
	df.drop(columns=['lat','long','cities'], inplace=True)

	return df


def inject_data(**context):

	ti = context['ti']

	df= ti.xcom_pull(task_ids='inject_data')

	query = """INSERT INTO covid-data (date, confirmed, deaths, recovered, confirmed_diff,
	deaths_diff, recovered_diff, last_update, active, last_active, active_diff, fatality_rate,
	region)
	VALUES %s"""

	pg_hook = PostgresHook(postgre_conn_id='covid')
	connection= pg_hook.get_conn()
	cursor = connection.cursor()
	cursor.execute(query, df.values.tolist())

	connection.commit()

	connection.close()








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


dag = DAG(
    'covid_example_dag', default_args=default_args,
    schedule_interval='@once')

get_data = PythonOperator(
	task_id='get_data',
	python_callable=request_data,
	dag=dag,
	provide_context=True)

transform_data = PythonOperator(
	task_id='transform_data',
	python_callable=transform_data,
	dag=dag,
	provide_context=True)

inject_data= PythonOperator(
	task_id='inject_data',
	python_callable=inject_data,
	dag=dag,
	provide_context=True)

get_data >> transform_data >> inject_data