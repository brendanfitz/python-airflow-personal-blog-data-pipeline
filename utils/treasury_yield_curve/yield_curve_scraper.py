#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import urllib.parse as p
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

    def __init__(self, year):
        self.year = year
        self.url = self.create_url()
        self.data = self.get_yield_curve()
    
    def create_url(self):
        base = 'https://data.treasury.gov/feed.svc/DailyTreasuryYieldCurveRateData'
        query = {'$filter': 'month(NEW_DATE) eq 10 and year(NEW_DATE) eq 2020'}
        url = base + '?' + urlencode(query, safe="$()", quote_via=quote)
        return url
    
    def get_yield_curve(self):
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
