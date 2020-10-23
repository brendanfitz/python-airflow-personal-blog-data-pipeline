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

        with open('create_index_component_stocks_table.sql') as f:
            create_query = f.read()
        cur.execute(create_query)

        if args.cleartbl:
            cur.execute("DELETE FROM visual.index_component_stocks")

        for row in scraper.data_to_tuples():
            insert_stmt = ("INSERT INTO visuals.index_component_stocks "
                           "VALUES""(%s,%s,%s,%s,%s,%s,%s)")
            cur.execute(insert_stmt, row)

        conn.commit()
    finally:
        conn.close()

    print("Data load complete. {:,.0f} rows loaded".format(len(scraper.data)))

def parse_args():
    description = "download stock index data from s3"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("index", help="dowjones or sp500")
    parser.add_argument("--cleartbl",
        action="store_true",
        help="clear table before loading"
    )
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parse_args()
    main(args)
