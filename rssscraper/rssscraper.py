import asyncio
import hashlib
import json
import re
import requests_xml

from dataclasses import dataclass
from typing import List


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

    def __init__(self, url: str, result_queue, patterns=[], wait_interval=10):
        self.__url = url
        self.__result_queue = result_queue
        self.__patterns = patterns
        self.__wait = wait_interval
        self.__running = False
        self.__xml_session = requests_xml.XMLSession()
        self.__processed_rss_item_sha256s = set()

        # print(f'RssScraper {self.__url=} {self.__result_queue=}')

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

        self.__result_queue.put_nowait(
            {'item': rss_item, 'matches': pattern_matches}
        )

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
