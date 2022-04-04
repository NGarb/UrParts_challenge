from db import Creator
from scrape_and_store import Scraper
import sys
import logging
from datetime import datetime

# TODO read from configs
logging.basicConfig(
    filename='./logs/HISTORYlistener_scraper.log',
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

if __name__ == '__main__':
    creator = Creator()
    print('... Creating tables')
    if sys.argv[1:] == 'drop':
        creator.drop_tables_and_functions()
        logging.info('dropped and recreated schema')
        creator.create_tables()
    elif sys.argv[1:] == 'truncate':
        creator.truncate_tables()
        logging.info('truncated all tables')
    creator.create_table_insert_functions()
    creator.db_instance.close_connection()
    logging.info('created all tables and functions for UrParts db')

    scraper = Scraper()
    logging.info('initialized scraper')
    start_time = datetime.now()
    print('... Starting scraper')
    scraper.scrape_into_db
    end_time = datetime.now()
    logging.info('scraping complete!')
    logging.info(f'duration: {end_time - start_time}')
