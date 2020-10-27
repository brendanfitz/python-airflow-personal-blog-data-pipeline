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

dummy = DummyOperator(task_id='dummy_op', dag=dag)

"""
DATADIR = 'data'
if not path.isdir(DATADIR):
    mkdir(DATADIR)

def save_df_to_file(df, file_suffix):
    ts = time.strftime("%Y%m%d-%H%M%S")
    filename = f"{file_suffix}___{ts}.csv"
    filepath = path.join(DATADIR, filename)

    df.to_csv(filepath)

    return filepath

def scrape_stock_industries(stock_index_name):
    if stock_index_name == 'sp500':
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        table_num = 0
    elif stock_index_name == 'dowjones':
        url = 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average'
        table_num = 1
    else:
        raise ValueError('Index Must Be "sp500" or "dowjones"')

    df = (pd.read_html(url)[table_num]
        .assign(Symbol=lambda x: x.Symbol.str.replace('NYSE:\xa0', ''))
        .set_index('Symbol')
        .rename(columns={'GICS Sector': 'Industry'})
        .loc[:, ['Industry']]
    )

    filepath = save_df_to_file(df, 'stock_industries')
    return filepath
"""
def scrape_stock_industries(stock_index_name):
    try:
        scraper = StockIndexScraper(stock_index_name, from_s3=True, load_all=False)
        filepath = scraper.scrape_stock_industries(save_to_file=True)
        return {'filepath': filepath}
    except:
        print("error")

fetch_stock_industries = PythonOperator(
    task_id='fetch_stock_industries',
    python_callable=scrape_stock_industries,
    op_kwargs={'stock_index_name': 'dowjones'},
    dag=dag,
)
