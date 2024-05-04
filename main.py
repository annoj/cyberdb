import argparse
import asyncio
import sys
import yaml

from rssscraper.rssscraper import RssScraper
from rssscraper.db import SqlLiteDB

RSS_SCRAPER_CONFIG_FILE_DEFAULT = './db.sqlite3'
RSS_SCRAPER_WAIT_INTERVAL_DEFAULT = 60
RSS_SCRAPER_DB_PATH_DEFAULT = 'db.sqlite3'


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config-file', required=False,
                        default='./rss-scraper.yaml')
    return parser.parse_args()


def main():
    args = parse_args()

    config_path = args.config_file or RSS_SCRAPER_CONFIG_FILE_DEFAULT
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except Exception:
        print(f'Could not load config file at {config_path}', file=sys.stderr)
        return 1

    wait = config.get('wait_interval') or RSS_SCRAPER_WAIT_INTERVAL_DEFAULT
    db_path = config.get('db_path') or RSS_SCRAPER_DB_PATH_DEFAULT
    rss_urls = config.get('urls') or []
    patterns = config.get('patterns') or []

    loop = asyncio.new_event_loop()

    sqlite_db = SqlLiteDB(db_path)
    sqlite_db_task = loop.create_task(sqlite_db.start_listen_for_results())

    scraper_tasks = []
    for url in rss_urls:
        scraper = RssScraper(url, sqlite_db.result_queue, wait_interval=wait)
        for pattern in patterns:
            scraper.add_pattern(pattern)
        scraper_tasks.append(loop.create_task(scraper.run()))

    try:
        asyncio.set_event_loop(loop)
        loop.run_forever()
    except KeyboardInterrupt:
        print('Received SIGING, shutting down gracefully')
    finally:
        for task in scraper_tasks:
            task.cancel()
        sqlite_db_task.cancel()
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        asyncio.set_event_loop(None)


if __name__ == '__main__':
    main()
