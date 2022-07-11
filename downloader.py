from bs4 import BeautifulSoup
from collections import namedtuple
import datetime
import enum
import logging
import os
import pandas as pd
import re
import urllib
from urllib.request import Request, urlopen


Article = namedtuple("Article", ["title", "body", "pub_date", "source", "genre"])
Source = namedtuple("Source", ["url", "genre", "datasource"])


class Genre(enum.Enum):
    Society = 1
    Culture_Entertainment = 2
    Science_Medicine = 3
    Politics = 4
    Economics = 5
    International = 6
    Sports = 7


class DataSource(enum.Enum):
    NHK = 1
    Asahi = 2


class KijiDownloader:
    """A downloader designed to iterate over the provided URLs, Genres, and Data Sources
    predefined for the KijiDownloader. This process will iterate over the RSS feeds
    specified in the URLs, grab the title, body, and publication date of the articles,
    and then save the output in a CSV file to be

    """
    def __init__(self):
        self.sources = [
            # NHK URLs
            Source("http://www3.nhk.or.jp/rss/news/cat1.xml", Genre.Society, DataSource.NHK),
            Source("http://www3.nhk.or.jp/rss/news/cat2.xml", Genre.Culture_Entertainment, DataSource.NHK),
            Source("http://www3.nhk.or.jp/rss/news/cat3.xml", Genre.Science_Medicine, DataSource.NHK),
            Source("http://www3.nhk.or.jp/rss/news/cat4.xml", Genre.Politics, DataSource.NHK),
            Source("http://www3.nhk.or.jp/rss/news/cat5.xml", Genre.Economics, DataSource.NHK),
            Source("http://www3.nhk.or.jp/rss/news/cat6.xml", Genre.International, DataSource.NHK),
            Source("http://www3.nhk.or.jp/rss/news/cat7.xml", Genre.Sports, DataSource.NHK),
            # Asahi URLs
            Source("http://www3.asahi.com/rss/national.rdf", Genre.Society, DataSource.Asahi),
            Source("http://www3.asahi.com/rss/politics.rdf", Genre.Politics, DataSource.Asahi),
            Source("http://www3.asahi.com/rss/sports.rdf", Genre.Sports, DataSource.Asahi),
            Source("http://www3.asahi.com/rss/business.rdf", Genre.Economics, DataSource.Asahi),
            Source("http://www3.asahi.com/rss/international.rdf", Genre.International, DataSource.Asahi),
            Source("http://www3.asahi.com/rss/culture.rdf", Genre.Culture_Entertainment, DataSource.Asahi),
        ]
        # Header to be specified to allow the bot to query the websites
        self.hdr = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Referer': 'https://cssspritegenerator.com',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive'}
        # The dictionary function mappings for the various data sources
        self.download_rss = {
            DataSource.NHK: self.download_rss_nhk,
            DataSource.Asahi: self.download_rss_asahi
        }
        self.download_articles = {
            DataSource.NHK: self.download_articles_nhk,
            DataSource.Asahi: self.download_articles_asahi
        }

    def download(self, output_dir=os.getcwd()):
        """Download and save the articles from sources.

        :return N/A:
        """
        self.start_logger()

        # Download and parse the articles
        articles = []
        for source in self.sources:
            new_articles = self.download_source(source)
            articles.extend(new_articles)

        # Create a dataframe containing the parsed articles, and save to CSV
        dt = datetime.datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
        filename = os.path.join(output_dir, f"japan_articles_{dt}.csv")
        article_df = pd.DataFrame(articles, columns=Article._fields)
        article_df.to_csv(filename, index=False)
        logging.info("Finished downloading")

    def start_logger(self):
        """Initiate the logger.

        :return N/A:
        """
        current_ts = datetime.datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
        if not os.path.exists("logs"):
            os.makedirs("logs")
        file_name = os.path.join("logs", f"KijiDownloader{current_ts}.log")

        logging.basicConfig(
            filename=file_name,
            filemode='a',
            format='%(asctime)s - %(message)s',
            level=logging.DEBUG
        )
        logging.info("Logging initiated for KijiDownloader.")

    def download_source(self, source: Source):
        """Retrieves all News Articles for the specified News Source URLs. Using
        the provided URL, Genre, and Source, a list of new News Articles is retrieved.

        :param source: A namedtuple that contains the URL, genre, and data source.
        :return articles:list[Articles]: A list containing the parsed articles.
        """
        # Get information from the provided Source
        url, genre, datasource = source.url, source.genre, source.datasource
        message = f"Downloading {genre.name} from {datasource.name}"
        logging.info(message)

        # Get the article URLs for the specified URL of the datasource
        download_rss = self.download_rss[datasource]
        article_urls = download_rss(url)
        message = f"\tDownloading {len(article_urls)} {genre.name} articles from {datasource.name}"
        logging.info(message)

        # Parse the article html and create an Article object for saving
        download_articles = self.download_articles[datasource]
        articles = []
        for au in article_urls:
            try:
                title, date_time, body = download_articles(au)
                article = Article(
                    title,
                    body,
                    date_time,
                    datasource.value,
                    genre.value
                )
                articles.append(article)
            except urllib.request.URLError as e:
                message = f"URLError with url={au}. {e}"
                logging.warning(message)

        message = f"\tDownloaded {len(articles)} {genre.name} articles from {datasource.name}"
        logging.info(message)

        return articles

    def download_rss_nhk(self, url: str):
        """Parse one of the RSS feeds for NHK News, returning article URLs.

        :param url: The RSS feed URL
        :return article_urls:list[str]: A list containing the URLs for all
            of the articles listed on the RSS feed.
        """
        article_urls = []
        bad_urls = ["http://www3.nhk.or.jp/news/"]
        # Grab html of the RSS feed, extracting the article URLs
        try:
            request = Request(url, None, headers=self.hdr)
            page_text = str(urlopen(request).read(), 'UTF-8')
            pattern = r'<link>(.*)</link>'
            article_urls = re.findall(pattern, page_text)
            # Seems to have a few links we don't want to include
            for bu in bad_urls:
                if bu in article_urls:
                    article_urls.remove(bu)
        except urllib.error.HTTPError as e:
            logging.warning(e)
        except Exception as e:
            message = f"Unhandled exception with url={url}: {e}"
            logging.warning(message)
        return article_urls

    def download_rss_asahi(self, url: str):
        """Parse one of the RSS feeds for Asahi News, returning article URLs.

        :param url: The RSS feed URL
        :return article_urls:list[str]: A list containing the URLs for all
            of the articles listed on the RSS feed.
        """
        article_urls = []
        bad_urls = ["https://www.asahi.com/"]
        # Grab html of the RSS feed, extracting the article URLs
        try:
            request = Request(url, None, headers=self.hdr)
            page_text = str(urlopen(request).read(), 'UTF-8')
            pattern = r'<link>(.*)</link>'
            article_urls = re.findall(pattern, page_text)
            # Seems to have a few links we don't want to include
            for bu in bad_urls:
                if bu in article_urls:
                    article_urls.remove(bu)
        except urllib.error.HTTPError as e:
            logging.warning(e)
        except Exception as e:
            message = f"Unhandled exception with url={url}: {e}"
            logging.warning(message)
        return article_urls

    def download_articles_asahi(self, url: str):
        """Download and parse the Asahi article URL into database format.

        :param url: The article URL that is being downloaded and parsed.
        :return title_text:str: The article title
        :return date_text:str: The article date
        :return body_text:str: The article body
        """
        request = Request(url, None, headers=self.hdr)
        page_text = str(urlopen(request).read(), 'UTF-8')
        page = BeautifulSoup(page_text, "lxml")

        # Get Title - There could be multiple h1 tags, but title should be the last h1 tag
        title_text = ""
        try:
            h1_tags = page.find_all('h1')
            h1_tag = h1_tags[-1]
            # Check if there is a span in this tag, and remove it if it exists
            if h1_tag.find('span'):
                span_tag = h1_tag.find('span')
                span_tag.decompose()
            # The remaining text in the h1 tag should be the title
            title_text = h1_tag.text
        except AttributeError as e:
            message = f"Unable to parse title for URL={url}"
            logging.warning(message)

        # Get Date - The date is within a time tag
        date_text = ""
        try:
            date_text = page.find('time').text
            date_text = self.jp_date_to_yyyymmdd(date_text)
        except AttributeError as e:
            message = f"Unable to parse date for URL={url}"
            logging.warning(message)

        # Body Text - Appears to be in <p> tags, inside <div class="nfyQp">
        body_text = ""
        try:
            div_tag = page.find('div', {'class': 'nfyQp'})
            p_tags = div_tag.find_all('p')
            for p_tag in p_tags:
                body_text += p_tag.text
        except AttributeError as e:
            message = f"Unable to parse body for URL={url}"
            logging.warning(message)

        return title_text, date_text, body_text

    def download_articles_nhk(self, url: str):
        """Download and parse the NHK article URL into database format.

        :param url: The article URL that is being downloaded and parsed.
        :return title_text:str: The article title
        :return date_text:str: The article date
        :return body_text:str: The article body
        """
        request = Request(url, None, headers=self.hdr)
        page_text = str(urlopen(request).read(), 'UTF-8')
        page = BeautifulSoup(page_text, "lxml")

        # Get Title - The title is within a span tag, inside <h1 class="content--title">
        title_text = ""
        try:
            h1_tag = page.find('h1', {'class': 'content--title'})
            title_text = h1_tag.find('span').text
        except AttributeError as e:
            message = f"Unable to parse title for URL={url}"
            logging.warning(message)

        # Get Date - The date is within a time tag, inside <p clas="content--date">
        date_text = ""
        try:
            date_tag = page.find('p', {'class': 'content--date'})
            date_text = date_tag.find('time').text
            date_text = self.jp_date_to_yyyymmdd(date_text)
        except AttributeError as e:
            message = f"Unable to parse date for URL={url}"
            logging.warning(message)

        # Content can be stored in a few different ways
        body_text = ""
        try:
            # <p class="content--sumary" style>
            if page.find('p', {'class': 'content--summary'}):
                body_tag = page.find('p', {'class': 'content--summary'})
                body_text += body_tag.text
            # <div class="maincontent_body text"><p></div>
            else:
                body_tag = page.find('div', {'class': 'maincontent_body text'})
                p_tags = body_tag.find_all('p')
                for p_tag in p_tags:
                    body_text += p_tag.text
        except AttributeError as e:
            message = f"Unable to parse body for URL={url}"
            logging.warning(message)

        return title_text, date_text, body_text

    def jp_date_to_yyyymmdd(self, dt: str):
        """Parse the provided Japanese date into a numeric format.

        :param dt: The datetime string to be parsed
        :return dt_text:str: The parsed datetime string
        """
        # Each of the regex used to grab each portion of the datetime
        year_pattern = r'(\d*)年'
        month_pattern = r'(\d*)月'
        day_pattern = r'(\d*)日'
        hour_pattern = r'(\d*)時'
        minute_pattern = r'(\d*)分'

        # Default datetime string to now (should be within 1 day of actual publication)
        dt_text = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Try to extract the datetime elements from string
        try:
            yyyy = int(re.findall(year_pattern, dt)[0])
            mm = int(re.findall(month_pattern, dt)[0])
            dd = int(re.findall(day_pattern, dt)[0])
            hour = int(re.findall(hour_pattern, dt)[0])
            minute = int(re.findall(minute_pattern, dt)[0])

            formatted_dt = datetime.datetime(
                yyyy,
                mm,
                dd,
                hour,
                minute
            )
            dt_text = formatted_dt.strftime("%Y-%m-%d %H:%M:%S")
        except TypeError as e:
            message = f"Unable to parse dt={dt}"
            logging.warning(message)
        except IndexError as e:
            message = f"Unable to parse dt={dt}"
            logging.warning(message)

        return dt_text


def main():
    output_dir = os.path.join("data", "incoming")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    kd = KijiDownloader()
    kd.download(output_dir)


if __name__ == "__main__":
    main()
