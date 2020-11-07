from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from first_dag_utils import greeting_command

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2020-10-22',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

greeting_command = """\
echo "Hi, I'm {{params.name}}'s Airflow"\
"""

dag = DAG('first_dag',
    default_args=default_args,
    description="My first dag WOO WOO!",
    schedule_interval="@once",
)

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

pwd = BashOperator(
    task_id="pwd",
    bash_command="pwd",
    dag=dag,
)


greeting_task >> date_task >> sendoff_task
