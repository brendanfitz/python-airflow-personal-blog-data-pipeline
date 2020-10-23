from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2020-10-22',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('first_dag',
    default_args=default_args,
    description="My first dag WOO WOO!",
    schedule_interval="@once",
)

greeting_command = """\
echo "Hi, I'm {{params.name}}'s Airflow"\
"""

greeting_task = BashOperator(
    task_id='greeting',
    bash_command=greeting_command,
    params={'name': 'Brendan'},
    dag=dag,
)

date_task = BashOperator(
    task_id="print_date",
    bash_command="date",
    dag=dag,
)

sendoff_task = BashOperator(
    task_id='sendoff',
    bash_command='echo "Goodbye, Adios and Farewell!"',
    dag=dag,
)

greeting_task >> date_task >> sendoff_task
