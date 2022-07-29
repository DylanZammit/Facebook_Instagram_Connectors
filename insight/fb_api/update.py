from fb_api import MyGraphAPI
import matplotlib.pyplot as plt
import pandas as pd
from connector import MySQLConnector
from IPython import embed
import os
from insight.entities import *
from insight.storage import Storage


class PageExtractor:

    def __init__(self, page):
        self.api = MyGraphAPI(page=page)
        self.page = Page(page)
        self.storage = Storage()

    def store(self):
        self.storage.store(self.page)

    def comment_replies(self):
        pass
    
    def pages(self, is_competitor):
        self.page.is_competitor = is_competitor

    def page_engagement(self):
        fields = 'id,fan_count'
        res = self.api.get_object(self.page.page_id, fields=fields)

        self.page.num_likes = res['fan_count']

    def post_comments(self):
        pass

    def post_fixed(self):
        pass

    def post_reacts(self):
        pass

    def post_shares(self):
        pass

    def react_types(self):
        pass

    def users(self):
        pass

competitors = []

def main(pages):
    """
    only specify pages we have page_access_token for now
    """

    conn = MySQLConnector(os.environ.get('SOCIAL_CONN'))

    insert_query = 'INSERT INTO page VALUES (%s, %s, %s, %s, %s, %s, %s)';

    if not isinstance(pages, list):
        pages = [pages]

    params = []
    df = {}
    for page in pages:
        api = MyGraphAPI(page=page)

        fields = 'id,fan_count,link,overall_star_rating,rating_count,name'
        res = api.get_object(page_name, fields=fields)
        today = pd.Timestamp.now().date()
        res['scrape_date'] = str(today)
        num_photos = len(api.get_object(f'{page_name}/photos')['data'])
        num_posts = len(api.get_object(f'{page_name}/posts')['data'])

        res['num_photos'] = num_photos
        res['num_posts'] = num_posts

        params.append((
            res['id'],
            res['scrape_date'],
            res['fan_count'],
            res['overall_star_rating'],
            res['rating_count'],
            res['num_posts'],
            res['num_photos']
        ))
    print(params)
    conn.insert(insert_query, params)

if __name__ == '__main__':
    page_name = 'levelupmalta'
    main(page_name)
