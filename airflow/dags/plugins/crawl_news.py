import os
import xml.etree.ElementTree as ET

import requests

from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class CrawlRSSNews:
    # '''Retrieve and parse RSS feed News'''
    # def __init__(self):
    #     self.rss_url = os.getenv('RSS_URL').split(";;")
        
    # def parse_data(self):
    #     '''Parse the RSS feed data and extract articles'''

    #     headers = {
    #         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    #         'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    #     }
        
    #     articles = []
    #     feed_data = None
    #     for url in self.rss_url:
    #         # fetch data
    #         try:
    #             response = requests.get(url, headers=headers)
    #             response.raise_for_status()  # Raise an error for bad responses
    #             feed_data = response.text
    #             print("Data fetched successfully.")
    #         except requests.RequestException as e:
    #             feed_data = None
    #             print(f"Error fetching data: {e}")
            
    #         # parse data
    #         if feed_data:
    #             root = ET.fromstring(feed_data)
    #             for item in root.findall('.//item'):
    #                 title = item.find('title').text
    #                 link = item.find('link').text
    #                 pub_date = item.find('pubDate').text
    #                 articles.append({'title': title, 'link': link, 'pub_date': pub_date})
    #         else:
    #             print("No data to parse.")
            
    #     return articles
    def __init__(self):
        self.rss_url = os.getenv('RSS_URL').split(";;")

    def fetch_data(self, url):
        '''Fetch RSS feed data from a URL'''
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an error for bad responses
            print(f"Data fetched successfully from {url}.")
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching data from {url}: {e}")
            return None

    def parse_feed(self, feed_data):
        '''Parse the RSS feed data and extract articles'''
        articles = []
        if feed_data:
            root = ET.fromstring(feed_data)
            for item in root.findall('.//item'):
                title = item.find('title').text
                link = item.find('link').text
                pub_date = item.find('pubDate').text
                articles.append({'title': title, 'link': link, 'pub_date': pub_date})
        return articles

    def parse_data(self):
        '''Fetch and parse RSS feed data concurrently'''
        all_articles = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Fetch data concurrently
            future_to_url = {executor.submit(self.fetch_data, url): url for url in self.rss_url}
            for future in as_completed(future_to_url):
                feed_data = future.result()
                # Parse data concurrently
                articles = self.parse_feed(feed_data)
                all_articles.extend(articles)

        return all_articles
    
    def display_articles(self):
        articles = self.parse_data()
        print("Latest Articles:")
        for article in articles:
            print(
                f"Title: {article['title']}\nLink: {article['link']}\n"
                f"Published: {article['pub_date']}\n"
            )
            

def crawl_api_news():
    newsapi = os.getenv('NEWSAPI')
    newsapi_url=f'https://newsapi.org/v2/top-headlines?country=us&apiKey={newsapi}'

    response = requests.get(newsapi_url)
    data = []
    if response.status_code == 200:
        for article in response.json()['articles']:
            data.append({
                'title': article['title'], 
                'link': article['url'], 
                'pub_date': article['publishedAt']
            })
    return data
