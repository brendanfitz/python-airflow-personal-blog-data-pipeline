import argparse
import configparser
import psycopg2
from stock_index_scraper import StockIndexScraper

def main(args):
    scraper = StockIndexScraper(args.index, from_s3=True)


    config = configparser.ConfigParser()
    config.read('config.ini')
    db_kwargs = dict(config['postgres'])

    try:
        conn = psycopg2.connect(**db_kwargs)
        cur = conn.cursor()

        with open('create_stock_index_table.sql') as f:
            create_query = f.read()
        cur.execute(create_query)

        tup = scraper.data_to_tuples()
        import pdb; pdb.set_trace();
        # args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", x) for x in tup)
        cur.execute("INSERT INTO stock_index_components VALUES (%s,%s,%s,%s,%s,%s,%s)", tup[0]) 

        conn.commit()
    finally:
        cur.close()
        conn.close()

def parse_args():
    description = "download stock index data from s3"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-i", "--index")
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parse_args()
    main(args)
