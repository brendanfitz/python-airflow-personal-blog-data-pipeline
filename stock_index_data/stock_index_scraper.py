import requests
import json
from os import environ
import pandas as pd
import boto3

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
            .join(industry_weights, on=['Industry'])
            .fillna("Unknown")
            .sort_values(['Industry Weight', 'Weight'], ascending=[False, False])
        )

        return df
