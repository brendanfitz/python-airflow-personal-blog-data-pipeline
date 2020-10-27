import requests
import json
from os import environ, mkdir, path
import pandas as pd
import boto3
import time

class StockIndexScraper(object):

    uris = ['sp500', 'dowjones']
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9", 
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Dnt": "1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36", 
    }
    DATADIR = 'data'

    def __init__(self, stock_index_name, from_s3, load_all=True):
        if not path.isdir(self.DATADIR):
            mkdir(self.DATADIR)
        self.stock_index_name = stock_index_name
        self.from_s3 = from_s3

        if load_all:
            self.df_scraped = self.scrape_index_component_stocks()

            self.industries = self.scrape_stock_industries()

            self.df = self.clean_df_scraped_and_merge_industries(
                self.df_scraped,
                self.industries
            )

            self.data = (self.df
                .reset_index()
                .to_dict(orient='records')
            )

    def scrape_index_component_stocks(self):
        if self.stock_index_name not in StockIndexScraper.uris:
            msg = f'stock_index_name must be in {StockIndexScraper.uris}'
            raise ValueError(msg)

        data_loader = self.s3_load if self.from_s3 else self.web_load
        df_scraped = data_loader()
        return df_scraped


    def web_load(self):
        url = r'https://www.slickcharts.com/{}'.format(self.stock_index_name)
        response = requests.get(url, headers=self.headers)
        try:
            df = (pd.read_html(response.text)[0]
                .set_index('Symbol')
            )
        except ValueError as e:
            filename = f"{self.stock_index_name}_error_page.html"
            with open(filename, 'w') as f:
                f.write(response.text)

            print("\nRequest Headers")
            print("*"*80)
            print(response.request.headers)
            print("*"*80)

            raise e
        return df

    def s3_load(self):
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

    def scrape_stock_industries(self, save_to_file=False):
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

        if save_to_file:
            filepath = self.save_df_to_file(df, 'stock_industries')
            return filepath

        return df

    def save_df_to_file(self, df, file_suffix):
        ts = time.strftime("%Y%m%d-%H%M%S")
        filename = f"{file_suffix}___{ts}.csv"
        filepath = path.join(self.DATADIR, filename)

        df.to_csv(filepath)

        return filepath

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

        filepath = path.join(self.DATADIR, filename)

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


