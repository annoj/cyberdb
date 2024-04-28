import asyncio
import hashlib
import json
import re
import requests_xml
import sqlite3

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List


class DB(ABC):
    @abstractmethod
    def commit_rss_item_and_matches(item, matches):
        ...


class MockDB(DB):
    def __init__(self):
        pass

    def commit_rss_item_and_matches(self, item, matches):
        pass


class SqlLiteDB(DB):
    def __init__(self, path):
        self.__path = path
        self.__connection = sqlite3.connect(self.__path)
        self.__create_rss_item_table_if_not_exists()
        self.__create_pattern_table_if_not_exists()
        self.__create_matches_table_if_not_exists()
        self.__create_evidences_table_if_not_exists()

    def __del__(self):
        self.__connection.commit()
        self.__connection.close()

    def __create_rss_item_table_if_not_exists(self):
        query = '''
            CREATE TABLE IF NOT EXISTS
            rss_items (
                sha256 VARCHAR PRIMARY KEY,
                title VARCHAR,
                link VARCHAR,
                description VARCHAR,
                author VARCHAR,
                category VARCHAR,
                comments VARCHAR,
                enclosure VARCHAR,
                guid VARCHAR,
                pub_date VARCHAR,
                source VARCHAR
            )
        '''
        self.__connection.execute(query)
        self.__connection.commit()

    def __create_pattern_table_if_not_exists(self):
        query = '''
            CREATE TABLE IF NOT EXISTS
            patterns (
                regex VARCHAR PRIMARY KEY
            )
        '''
        self.__connection.execute(query)
        self.__connection.commit()

    def __create_matches_table_if_not_exists(self):
        query = '''
            CREATE TABLE IF NOT EXISTS
            matches (
                match VARCHAR PRIMARY KEY
            )
        '''
        self.__connection.execute(query)
        self.__connection.commit()

    def __create_evidences_table_if_not_exists(self):
        query = '''
            CREATE TABLE IF NOT EXISTS
            evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rss_item_sha256 VARCHAR,
                match VARCHAR,
                pattern VARCHAR
            )
        '''
        self.__connection.execute(query)
        self.__connection.commit()

    def commit_rss_item_and_matches(self, item, matches):
        insert_rss_item_if_not_exists_query = '''
            INSERT OR IGNORE INTO rss_items
            VALUES (
                :sha256,
                :title,
                :link,
                :description,
                :author,
                :category,
                :comments,
                :enclosure,
                :guid,
                :pub_date,
                :source
            )
        '''
        self.__connection.execute(
            insert_rss_item_if_not_exists_query,
            vars(item)
        )

        for match in matches:
            insert_pattern_if_not_exists_query = '''
                INSERT OR IGNORE INTO patterns
                VALUES (
                    :pattern
                )
            '''
            self.__connection.execute(
                insert_pattern_if_not_exists_query,
                (match.pattern.pattern, )
            )

            insert_match_if_not_exists_query = '''
                INSERT OR IGNORE INTO matches
                VALUES (
                    :match
                )
            '''
            self.__connection.executemany(
                insert_match_if_not_exists_query,
                [(m, ) for m in match.matches]
            )

            insert_evidence_if_not_exists_query = '''
                INSERT OR IGNORE INTO evidence
                (
                    rss_item_sha256,
                    match,
                    pattern
                )
                VALUES (
                    :rss_item_sha256,
                    :match,
                    :pattern
                )
            '''
            self.__connection.executemany(
                insert_evidence_if_not_exists_query,
                [
                    (item.sha256, m, match.pattern.pattern)
                    for m in match.matches
                ]
            )

        self.__connection.commit()


@dataclass
class RssItem():
    sha256: str
    title: str
    link: str
    description: str
    author: str
    category: str
    comments: str
    enclosure: str
    guid: str
    pub_date: str
    source: str


class RssScraper():

    @dataclass
    class patternMatch():
        pattern: str
        matches: List[str]

    def __init__(self, url: str, db: DB, patterns=[], wait_interval=10):
        self.__url = url
        self.__db = db
        self.__patterns = patterns
        self.__wait = wait_interval
        self.__running = False
        self.__xml_session = requests_xml.XMLSession()
        self.__processed_rss_item_sha256s = set()

    def __del__(self):
        self.__running = False

    def add_pattern(self, pattern):
        self.__patterns.append(re.compile(pattern))

    @staticmethod
    def __parse_item_category(category):
        if not category:
            return None

        if isinstance(category, list):
            return list(map(lambda c: c.get('$'), category))

        return category.get('$') or None

    def __process_item(self, item):
        # Only process item if it's sha256 has not been encountered before
        sha256 = hashlib.sha256(item.text.encode('utf-8')).hexdigest()
        if sha256 in self.__processed_rss_item_sha256s:
            return

        # Add item sha256 to list of processed sha256s
        self.__processed_rss_item_sha256s.add(sha256)

        # Check xml text against patterns
        pattern_matches = []
        for pattern in self.__patterns:
            m = pattern.findall(item.text)
            if m:
                pattern_matches.append(self.patternMatch(pattern, m))

        # If item does not any patterns, return
        if not pattern_matches:
            return

        # Create RssItem
        parsed_item = json.loads(item.json())['item']
        rss_item = RssItem(
            sha256,
            (parsed_item.get('title', {}).get('$')) or None,
            (parsed_item.get('link', {}).get('$')) or None,
            (parsed_item.get('description', {}).get('$')) or None,
            (parsed_item.get('author', {}).get('$')) or None,
            str(self.__parse_item_category(parsed_item.get('category'))),
            (parsed_item.get('comments', {}).get('$')) or None,
            (parsed_item.get('enclosure', {}).get('$')) or None,
            (parsed_item.get('guid', {}).get('$')) or None,
            (parsed_item.get('pubDate', {}).get('$')) or None,
            (parsed_item.get('source', {}).get('$')) or None,
        )

        # Commit RssItem to db
        # try:
        #     self.__db.commit_rss_item(rss_item, pattern_matches)
        # except Exception:
        #     # TODO: Properly handle failure to commit item to db
        #     raise Exception
        self.__db.commit_rss_item_and_matches(rss_item, pattern_matches)

    async def run(self):
        self.__running = True

        while self.__running:
            # Fetch new batch from rss feed
            res = self.__xml_session.get(self.__url)
            res.raise_for_status()

            # Get items list from batch
            items = res.xml.find('item', first=False)

            # Process items
            for item in items:
                self.__process_item(item)

            # Wait for specified delay
            await asyncio.sleep(self.__wait)

        # Close XMLSession if stop running
        self.__xml_session.close()

    def stop(self):
        self.__running = False
