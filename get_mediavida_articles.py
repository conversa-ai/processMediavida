from requests_html import HTMLSession
import time
from bs4 import BeautifulSoup
import pandas as pd
import argparse
import os

from util import create_ifnotexists_directory

def get_articles(base_url, num_pages=1500, output_folder='output'):
    # Create an HTMLSession object
    session = HTMLSession()

    num_pages = num_pages  # Replace with the number of pages you want to scrape
    articles_dict = []
    for page_num in range(1, num_pages + 1):
        url = base_url + '/p' + str(page_num)
        r = session.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        articles = soup.find_all("div", {"class": "thread"})
        for article in articles:
            article_dict = {}
            article_id = article.find('a')['id']
            print(article_id)
            article_link = 'https://www.mediavida.com' + article.find('a')['href']
            article_dict['article_id'] = article_id
            article_dict['article_link'] = article_link
            articles_dict.append(article_dict)
        time.sleep(1)

    df = pd.DataFrame(articles_dict)
    df.to_csv(os.path.join(output_folder, 'articles.csv'), index=False, sep=',')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_link', type=str, required=True)
    parser.add_argument('--output_folder', type=str, required=True)
    parser.add_argument('--num_pages', type=int, required=True)
    args = parser.parse_args()

    output_folder = args.output_folder
    create_ifnotexists_directory(output_folder)

    get_articles(args.input_link, args.num_pages, output_folder)