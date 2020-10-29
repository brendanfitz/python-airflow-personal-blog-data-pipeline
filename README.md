# python-airflow-personal-blog-data-pipeline

### Background

This Airflow repository is used as an ETL data pipeline to power the data behind [my blog](https://www.brendan-fitzpatrick.com/) visuals. Each dag corresponds to a data source for a specific visualization and is load into a PostgreSQL server. The data is sourced from various APIs or scrapes public sites. 

An instance of this currently running live an EC2 server. I have also included various bash scripts used for server maintenance.

Enjoy!
