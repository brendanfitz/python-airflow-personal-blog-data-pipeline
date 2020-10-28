import time
from os import path, mkdir, remove
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

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
    return filepath

def fetch_stock_prices(stock_index_name):
    scraper = StockIndexScraper(stock_index_name, from_s3=True, load_all=False)
    filepath = scraper.scrape_index_component_stocks(save_to_file=True)
    return filepath

def clean_and_merge_industries(stock_index_name, **kwargs):
    ti = kwargs['ti']

    prices_filename = ti.xcom_pull(
        key='return_value',
        task_ids="fetch_stock_prices"
    )
    df_prices = pd.read_csv(prices_filename, index_col='Symbol')

    industries_filename = ti.xcom_pull(
        key='return_value',
        task_ids="fetch_stock_industries"
    )
    df_industries = pd.read_csv(industries_filename, index_col='Symbol')

    scraper = StockIndexScraper(stock_index_name, from_s3=True, load_all=False)
    df = scraper.clean_df_scraped_and_merge_industries(df_prices, df_industries)
    filepath = scraper.save_df_to_file(df, f"{stock_index_name}__clean")

    return filepath

def load_data(stock_index_name, **kwargs):
    ti = kwargs['ti']

    filename = ti.xcom_pull(
        key='return_value',
        task_ids="clean_and_merge_industries"
    )

    scraper = StockIndexScraper(stock_index_name, from_s3=True, load_all=False)
    scraper.df = pd.read_csv(filename, index_col='Symbol')
    scraper.data = scraper.create_data()

    pghook = PostgresHook('postgres_db')
    cur = pghook.get_cursor()

    # delete old data
    delete_stmt = ("DELETE FROM visuals.index_component_stocks "
                   "WHERE stock_index_name = %s")
    cur.execute(delete_stmt, (stock_index_name, ))

    # insert new data
    row_count = 0
    for row in scraper.data_to_tuples():
        insert_stmt = ("INSERT INTO visuals.index_component_stocks "
                       "VALUES""(%s,%s,%s,%s,%s,%s,%s)")
        cur.execute(insert_stmt, row)
        row_count += 1

    pghook.conn.commit()

    return {'row_count': row_count}


def cleanup(**kwargs):
    ti = kwargs['ti']

    prices_filename = ti.xcom_pull(
        key='return_value',
        task_ids="fetch_stock_prices"
    )

    industries_filename = ti.xcom_pull(
        key='return_value',
        task_ids="fetch_stock_industries"
    )

    cleaned_filename = ti.xcom_pull(
        key='return_value',
        task_ids="clean_and_merge_industries"
    )

    for filename in [prices_filename, industries_filename, cleaned_filename]:
        remove(filename)


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
    provide_context=True,
    op_kwargs={'stock_index_name': 'dowjones'},
    dag=dag,
)

load_data_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    provide_context=True,
    op_kwargs={'stock_index_name': 'dowjones'},
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id="cleanup_task",
    python_callable=cleanup,
    provide_context=True,
    dag=dag,
)

[fetch_stock_industries_task, fetch_stock_prices_task] >> clean_and_merge_industries_task >> load_data_task >> cleanup_task
