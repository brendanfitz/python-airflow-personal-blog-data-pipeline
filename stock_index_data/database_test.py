import configparser
import psycopg2

config = configparser.ConfigParser()
config.read('config.ini')
db_kwargs = dict(config['postgres'])

with psycopg2.connect(**db_kwargs) as conn:
    cur = conn.cursor()
    query = "SELECT * FROM test;"
    cur.execute(query)
    print(cur.fetchone())
    cur.close()




