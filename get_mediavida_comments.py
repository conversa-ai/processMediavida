import re
import requests
import os
import pandas as pd
import json
from bs4 import BeautifulSoup
import argparse

from util import create_ifnotexists_directory

def get_comments(link):
    page = requests.get(link)
    soup = BeautifulSoup(page.content, 'html.parser')
    comments = soup.find_all('div', {'id': re.compile('^post-\d+')})
    next_link = soup.find('a', {'class': 'btn btn-primary'})
    if next_link:
        next_link = 'https://www.mediavida.com' + next_link['href']
        comments += get_comments(next_link)
    return comments

def process_comment(comment):
    text = comment.find('div', {'class': 'post-contents'}).text.strip()
    comment_id = comment['id'].split('-')[1]
    user = comment.find('a', {'class': re.compile('^autor user-card')})
    if user is None:
        user = '[deleted]'
    else:
        user = user.text.strip()
    return {
        'order': comment_id,
        'user': user,
        'content': text
    }

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--articles_metadata_folder', type=str, required=True)
    parser.add_argument('--output_folder', type=str, required=True)
    args = parser.parse_args()

    output_folder = args.output_folder
    create_ifnotexists_directory(output_folder)

    metadata = pd.read_csv(os.path.join(args.articles_metadata_folder, 'articles.csv'))
    existing_ids = [file.split('.')[0] for file in os.listdir(output_folder)]
    for index, row in metadata.iterrows():
        if row['article_id'] in existing_ids:
            print('Article ' + row['article_id'] + ' already exists. Skipping...')
            continue

        if "hilo" in row['article_link'] or "referendum" in row['article_link'] or 'manana' in row['article_link']  \
                or 'coronachat' in row['article_link'] or 'tinder' in row['article_link'] or 'sorteamos' in row['article_link']:
            print('Article ' + row['article_id'] + ' is a general thread. Skipping...')
            continue

        print('Getting comments for article ' + row['article_id'] + '...')
        print('Link: ' + row['article_link'])

        article_dict = {}
        try:
            article_dict['url'] = row['article_link']
            comments = get_comments(row['article_link'])
            comment_dicts = []
            for comment in comments:
                comment_dicts.append(process_comment(comment))
            article_dict['objects'] = comment_dicts
        except requests.exceptions.RequestException as e:
            print(f'Request failed with error: {e}')
            continue
        except RecursionError:
            print('Recursion error. Skipping...')
            continue

        with open(os.path.join(output_folder, row['article_id'] + '.json'), 'w') as f:
            json.dump(article_dict, f)