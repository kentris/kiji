import pandas as pd
import datetime
import logging
import os
import sqlite3
from typing import List


class KijiUploader:
    """  """
    def __init__(self):
        self.CREATE_TABLES = [
            """CREATE TABLE IF NOT EXISTS "articles" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "title" TEXT NOT NULL UNIQUE, "body" TEXT NOT NULL, "pub_date" TEXT, "source" INTEGER, "genre" INTEGER, "status" INTEGER)""",
            """CREATE TABLE IF NOT EXISTS "genre" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" TEXT, "description" TEXT)""",
            """CREATE TABLE IF NOT EXISTS "source" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" TEXT, "description" TEXT)"""
        ]
        self.CHECK_FOR_ARTICLE = """SELECT * FROM articles WHERE TITLE = ? AND BODY = ? AND PUB_DATE = ? AND SOURCE = ? AND GENRE = ?"""
        self.INSERT_ARTICLE = """INSERT INTO articles ('title', 'body', 'pub_date', 'source', 'genre', 0) VALUES (?,?,?,?,?, ?)"""
        self.db = None
        self.conn = None
        self.dir_path = os.path.dirname(os.path.realpath(__file__))

    def upload(self, db: str, upload_dir: str, uploaded_dir: str):
        """

        :param db:
        :param upload_dir:
        :param uploaded_dir:
        :return:
        """
        self.start_logger()
        self.open_connection(db)

        # Iterate over all files to be processed
        logging.info(f"Processing {len(os.listdir(upload_dir))} article files in {upload_dir}.")
        for article_file in os.listdir(upload_dir):
            try:
                logging.info(f"Processing {os.path.join(self.dir_path, upload_dir, article_file)}.")
                article_df = pd.read_csv(os.path.join(self.dir_path, upload_dir, article_file))
                article_tuples = [tuple(row) for row in article_df.values]
                self.process_articles(article_tuples)
                logging.info(f"Finished processing {article_file}")
                # Move file to completed directory
                os.rename(
                    os.path.join(self.dir_path, upload_dir, article_file),
                    os.path.join(self.dir_path, uploaded_dir, article_file)
                )
            except Exception as e:
                logging.warning("Error processing file.", e)
        logging.info("Finished processing article files.")

        self.close_db_connection()

    def start_logger(self):
        """Initiate the logger.

        :return N/A:
        """
        current_ts = datetime.datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
        if not os.path.exists(os.path.join(self.dir_path, "logs")):
            os.makedirs(os.path.join(self.dir_path, "logs"))
        file_name = os.path.join(self.dir_path, "logs", f"KijiUploader{current_ts}.log")

        logging.basicConfig(
            filename=file_name,
            filemode='a',
            format='%(asctime)s - %(message)s',
            level=logging.DEBUG
        )
        logging.info("Logging initiated for KijiUploader.")

    def open_connection(self, db: str):
        """Establishes a database connection to our Japanese News Database.

        :param db: The database we're connecting to
        :return N/A:
        """
        logging.info(f"Establishing database connection: {db}.")
        conn = sqlite3.connect(db)
        db = conn.cursor()
        for ct in self.CREATE_TABLES:
            db.execute(ct)
        logging.info("Successfully created database connection.")
        self.db = db
        self.conn = conn

    def is_in_database(self, article: tuple):
        """Check if an article is in the database already.

        :param article: A tuple containing the field values for the article
        :return in_database: boolean: Boolean indicating presence of article
        """
        self.db.execute(self.CHECK_FOR_ARTICLE, article)
        matches = self.db.fetchall()
        in_database = len(matches) > 0
        return in_database

    def process_articles(self, articles: List[tuple]):
        """Check if articles already exist in the database, and insert those
        which aren't in the database.

        :param articles: List containing the article tuples.
        :return N/A:
        """
        processed = {'total': 0, 'success': 0, 'failure': 0}

        # Only insert new articles
        filtered_articles = [
            article for article in articles
            if not self.is_in_database(article)
        ]
        num_filtered_articles = len(articles) - len(filtered_articles)
        logging.info(f"Inserting {len(filtered_articles)} into the database.")
        logging.info(f"\tFiltered out {num_filtered_articles} from downloaded file.")

        # Attempt to insert articles into database
        for article in filtered_articles:
            processed['total'] += 1
            try:
                self.db.execute(self.INSERT_ARTICLE, article)
                self.conn.commit()
                processed['success'] += 1
            except sqlite3.IntegrityError:
                processed['failure'] += 1
                logging.info(f"Failed to insert article: {article}")
            except Exception as e:
                processed['failure'] += 1
                logging.info(f"Unhandled exception {e}: {article}")
        logging.info(
            f"Finished processing articles. "
            f"Total={processed['total']}, "
            f"Success={processed['success']}, "
            f"Failure={processed['failure']}"
        )

    def close_db_connection(self):
        """Close the database connection.

        :return N/A:
        """
        self.db.close()
        logging.info("Closed DB connection.")


def main():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    data_dir = "data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    db = os.path.join(dir_path, data_dir, "kiji.db")

    upload_dir = os.path.join(dir_path, data_dir, "incoming")
    if not os.path.exists(upload_dir):
        os.makedirs(upload_dir)

    uploaded_dir = os.path.join(dir_path, data_dir, "processed")
    if not os.path.exists(uploaded_dir):
        os.makedirs(uploaded_dir)

    ku = KijiUploader()
    ku.upload(db, upload_dir, uploaded_dir)


if __name__ == "__main__":
    main()
