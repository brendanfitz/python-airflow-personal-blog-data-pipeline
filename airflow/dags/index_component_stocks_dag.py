import time
from os import path, mkdir
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import sys
sys.path.insert(0, "/home/brendan/Github/python-airflow-personal-blog-data-pipeline/index_component_stocks")
from stock_index_scraper import StockIndexScraper

default_args = dict(
    owner='airflow',
    depends_on_past=False,
    catchup=False,
    start_date='2020-10-25',
    retries=0,
    # retry_delay=timedelta(minutes=5)
)

dag = DAG(
    'index_component_stocks',
    default_args=default_args,
    description='loads and cleans data for index stock component blog visuals',
    schedule_interval=None,
)

def fetch_stock_industries(stock_index_name):
    scraper = StockIndexScraper(stock_index_name, from_s3=True, load_all=False)
    filepath = scraper.scrape_stock_industries(save_to_file=True)
    return {'filepath': filepath}

def fetch_stock_prices(stock_index_name):
    scraper = StockIndexScraper(stock_index_name, from_s3=True, load_all=False)
    filepath = scraper.scrape_index_component_stocks(save_to_file=True)
    return {'filepath': filepath}

def clean_and_merge_industries(context):
    pass

def load_data(context):
    pass

def cleanup():
    return

fetch_stock_industries_task = PythonOperator(
    task_id='fetch_stock_industries',
    python_callable=fetch_stock_industries,
    op_kwargs={'stock_index_name': 'dowjones'},
    dag=dag,
)

fetch_stock_prices_task = PythonOperator(
    task_id='fetch_stock_prices',
    python_callable=fetch_stock_prices,
    op_kwargs={'stock_index_name': 'dowjones'},
    dag=dag,
)

clean_and_merge_industries_task = PythonOperator(
    task_id="clean_and_merge_industries",
    python_callable=clean_and_merge_industries,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id="cleanup_task",
    python_callable=cleanup,
    provide_context=True,
    dag=dag,
)

[fetch_stock_industries_task, fetch_stock_prices_task] >> clean_and_merge_industries_task >> cleanup_task
