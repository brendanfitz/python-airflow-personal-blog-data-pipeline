import sys
from os import remove
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

sys.path.insert(0, "/home/brendan/Github/python-airflow-personal-blog-data-pipeline/utils/treasury_yield_curve")
from yield_curve_scraper import YieldCurveScraper

default_args = dict(
    owner='airflow',
    depends_on_past=False,
    catchup=False,
    start_date='2020-10-25',
    retries=0,
)

dag = DAG(
    "treasury_yield_curve",
    default_args=default_args,
    description="scrapes treasury yield curve",
    schedule_interval="@daily",
)

def fetch_treasury_data():
    yesterday = dt.date.today() - dt.timedelta(days=1)
    year = yesterday.strftime('%Y')
    month = yesterday.strftime('%m')
    scraper = YieldCurveScraper(year, month)
    filepath = scraper.write_to_csv()
    return filepath

def cleanup(**context):
    ti = context['ti']

    filename = ti.xcom_pull(key='return_value', task_ids='fetch_treasury_data')

    remove(filename)

fetch_treasury_data_task = PythonOperator(
    task_id='fetch_treasury_data',
    python_callable=fetch_treasury_data,
    dag=dag,
)

xcom_macro = '{{ task_instance.xcom_pull(task_ids="fetch_treasury_data", key="return_value") }}'

sql_filename = 'utils/treasury_yield_curve/load_treasury_yield_curve_data.sql'
sql = open(sql_filename).read().format(xcom_macro)
load_data_task = PostgresOperator(
    task_id='load_data',
    sql=sql,
    postgres_conn_id='postgres_db',
    dag=dag,
)

vaccum_table_task = PostgresOperator(
    task_id='vaccum_table',
    sql="VACUUM visuals.treasury_yield_curve;",
    postgres_conn_id='postgres_db',
    autocommit=True,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup,
    provide_context=True,
    dag=dag,
)

fetch_treasury_data_task >> load_data_task >> [vaccum_table_task, cleanup_task]
