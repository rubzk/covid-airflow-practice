from airflow.models import Connection, Variable, Session

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow import models, settings

default_args = {
    'owner': 'manasi',
    'depends_on_past': False,
    'start_date': datetime(2016, 4, 15),
    'email': ['manasidalvi14@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('reboot-conf', default_args=default_args)

def set_connection(**config):
    for k,v in config.items():
        conn = Connection()
        setattr(conn, k, v)
    session = settings.Session()
    session.add(conn)
    session.commit()
    session.close()

task1 = PythonOperator(
    dag = dag,
    task_id = 'set-connections',
    python_callable = set_connection,
)

