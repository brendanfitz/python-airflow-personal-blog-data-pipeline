#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from os import path, mkdir
import requests
from urllib.parse import urlencode, quote
import xml.etree.ElementTree as ET
import pandas as pd


class YieldCurveScraper(object):
    tmap = {
        'Id': 'ID',
        'NEW_DATE': 'Date',
        'BC_1MONTH': '1 Month',
        'BC_2MONTH': '2 Month',
        'BC_3MONTH': '3 Month',
        'BC_6MONTH': '6 Month',
        'BC_1YEAR': '1 Year',
        'BC_2YEAR': '2 Year',
        'BC_3YEAR': '3 Year',
        'BC_5YEAR': '5 Year',
        'BC_7YEAR': '7 Year',
        'BC_10YEAR': '10 Year',
        'BC_20YEAR': '20 Year',
        'BC_30YEAR': '30 Year',
    }
    BASE = 'https://data.treasury.gov/feed.svc/DailyTreasuryYieldCurveRateData'
    DATADIR = 'data'

    def __init__(self, year, month):
        if not path.isdir(self.DATADIR):
            mkdir(self.DATADIR)
        self.year = year
        self.month = month
        self.url = self.create_url()
        self.data = self.fetch_yield_curve()

    def create_url(self):
        query = {'$filter': f"month(NEW_DATE) eq {self.month} and year(NEW_DATE) eq {self.year}"}
        url = self.BASE + '?' + urlencode(query, safe="$()", quote_via=quote)
        return url

    def fetch_yield_curve(self):
        response = requests.get(self.url)
        xml = response.content
        root = ET.fromstring(xml)

        ns = {'ns': 'http://www.w3.org/2005/Atom',
              'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
              'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices'}

        data = list()
        for entry in root.findall('ns:entry', ns):
            content = (entry.find('ns:content', ns)
             .find('m:properties', ns)
            )
            row = dict()
            for child in content:
                treasury_name = self.treasury_map(child.tag, ns['d'])
                if treasury_name != 'BC_30YEARDISPLAY':
                    percent_yield = child.text
                    row[treasury_name] = percent_yield
            data.append(row)

        return data

    @staticmethod
    def treasury_map(scraped_name, ns):
        clean_name = scraped_name.replace('{' + ns + '}', '')
        return YieldCurveScraper.tmap.get(clean_name, clean_name)

    def write_to_csv(self):
        ts = time.strftime("%Y%m%d-%H%M%S")
        filename = f"treasury_yield_curve___{ts}.csv"
        filepath = path.join(self.DATADIR, filename)

        df = pd.DataFrame(self.data)

        df.Date = pd.to_datetime(df.Date)

        df.to_csv(filepath, index=False, date_format='%Y-%m-%d')

        return filepath
