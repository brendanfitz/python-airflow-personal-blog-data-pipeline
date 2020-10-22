import requests
import json
from os import environ, mkdir, path
import pandas as pd
import boto3
import time

class StockIndexScraper(object):

    uris = ['sp500', 'dowjones']

    def __init__(self, stock_index_name, from_s3):
        self.stock_index_name = stock_index_name
        self.from_s3 = from_s3
        self.data = self.scrape_index_component_stocks()

    def scrape_index_component_stocks(self):
        if self.stock_index_name not in StockIndexScraper.uris:
            msg = f'stock_index_name must be in {StockIndexScraper.uris}'
            raise ValueError(msg)

        df_scraped_func = self.df_scraped_from_s3 if self.from_s3 else self.df_scraped_from_web
        df_scraped = df_scraped_func()

        industries = self.scrape_stock_industries()

        df = self.clean_df_scraped_and_merge_industries(df_scraped, industries)

        data = (df
            .reset_index()
            .to_dict(orient='records')
        )
        return data

    def df_scraped_from_web(self):
        url = r'https://www.slickcharts.com/{}'.format(self.stock_index_name)
        response = requests.get(url)
        df = (pd.read_html(response.text)[0]
            .set_index('Symbol')
        )
        return df

    def df_scraped_from_s3(self):
        aws_config = {
            'aws_access_key_id': environ.get('METIS_APP_AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': environ.get('METIS_APP_AWS_SECRET_KEY'),
        }
        client = boto3.client('s3', **aws_config)

        response = client.get_object(
            Bucket='metis-projects',
            Key=f"stock_index_data/{self.stock_index_name}.json",
        )
        data = json.load(response.get('Body'))
        df = (pd.DataFrame(data['data'])
            .set_index('Symbol')
        )

        return df

    def scrape_stock_industries(self):
        if self.stock_index_name == 'sp500':
            url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            table_num = 0
        elif self.stock_index_name == 'dowjones':
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
        return df

    @staticmethod
    def all_stocks_have_industries(df):
        mask = df.loc[:, 'Industry'].isna()
        return df.loc[mask, ].empty

    @staticmethod
    def clean_df_scraped_and_merge_industries(df_scraped, industries):
        columns = ['Company', 'Weight', 'Price']
        df_without_weights = (df_scraped
            .loc[:, columns]
            .join(industries)
            .sort_values(['Industry', 'Weight'], ascending=[True, False])
        )

        industry_weights = (df_without_weights
            .groupby(['Industry'], as_index=False)['Weight'].sum()
            .set_index('Industry')
            .rename(columns={'Weight': 'Industry Weight'})
        )

        df = (df_without_weights
            .fillna("Unknown")
            .join(industry_weights, on=['Industry'])
            .fillna(0)
            .sort_values(['Industry Weight', 'Weight'], ascending=[False, False])
        )

        return df

    def write_csv(self, verbose=False):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        filename = f"{self.stock_index_name}_{timestr}.csv"

        if not path.isdir('data'):
            mkdir('data')
        filepath = path.join('data', filename)

        self.data_to_df().to_csv(filepath, index=False)

        if verbose:
            print("="*80)
            print(f"See file located at {filepath}")
            print("="*80)

        return filepath

    def data_to_df(self):
        column_order = [
            "Stock Index Name",
            "Symbol",
            "Company",
            "Weight",
            "Price",
            "Industry",
            "Industry Weight"
        ]

        df = (pd.DataFrame(self.data)
            .assign(stock_index_name=self.stock_index_name)
            .rename(columns={"stock_index_name": "Stock Index Name"})
            .reindex(columns=column_order)
        )

        return df

    def data_to_tuples(self):
        df = self.data_to_df()

        records = list(df.to_records(index=False))

        records_tuple = tuple([tuple(row) for row in records])

        return records_tuple


