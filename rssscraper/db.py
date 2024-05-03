import sqlite3

from abc import ABC, abstractmethod


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
                INSERT INTO evidence (
                    rss_item_sha256,
                    match,
                    pattern
                )
                SELECT
                    :rss_item_sha256,
                    :match,
                    :pattern
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM evidence
                    WHERE
                        rss_item_sha256 = :rss_item_sha256
                        AND match = :match
                        AND pattern = :pattern
                );

            '''
            self.__connection.executemany(
                insert_evidence_if_not_exists_query,
                [
                    (item.sha256, m, match.pattern.pattern)
                    for m in match.matches
                ]
            )

        self.__connection.commit()