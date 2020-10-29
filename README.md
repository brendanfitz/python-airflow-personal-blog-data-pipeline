# python-airflow-personal-blog-data-pipeline

### Background

This Airflow repository is used as an ETL data pipeline to power the data behind [my blog](https://www.brendan-fitzpatrick.com/) visuals. Each dag corresponds to a data source for a specific visualization. The data is sourced from various APIs or scraped from public sites and then loaded into a PostgreSQL server on the same EC2 instance.

An instance of this currently running live an EC2 server. As a bonus, included are various bash scripts used for server maintenance (each specially written by yours truly!).

Enjoy!
