from facebook_scraper import get_profile
import os
from connector import PGConnector as Connector
import pandas as pd
from datetime import timedelta, datetime
from mysql.connector.errors import IntegrityError
from insight.utils import *
from credentials import POSTGRES

def save_history(f):
    def wrapper(self, *args, **kwargs):
        rows = f(self, *args, **kwargs)
        n_rows = len(rows)
        m = len('_store_')
        key = f.__name__[m:]
        self.history[key] = n_rows
        return rows
    return wrapper


class Storage:

    def __init__(self, schema='competitors'):
        credentials = POSTGRES.copy()
        credentials['schema'] = schema
        self.conn = Connector(**credentials)
        self.scrape_date = pd.Timestamp(datetime.now())
        self.last_profile_call = pd.Timestamp('2000-01-01')
        self.schema = schema
        self.history = {}

    def __str__(self):
        return '\n'.join([f'{k} rows stored = {v}' for k, v in self.history.items()])

    def store(self, obj):
        '''
        Can pass facebook objects.
        If a list is passed, all items must be of the same object type
        '''
        if isinstance(obj, list):
            if len(obj) == 0: 
                return
            rep = obj[0]
        else:
            rep = obj
            obj = [obj]

        otype = type(rep).__name__.lower()
        f = getattr(self, '_store_{}'.format(otype))
        rows = f(obj)

    @save_history
    def _store_page_dim(self, pages):
        df = self.conn.execute(f'SELECT page_id FROM {self.schema}.dim_fb_page')
        rows = []
        for page in pages:
            page_id = int(page.page_id)
            exists = page_id in df.page_id.values
            if not exists:
                rows.append((page.username, page.is_competitor, page_id, page.name))

        self.conn.insert(f'{self.schema}.dim_fb_page', rows)
        return rows

    @save_history
    def _store_page_fact(self, pages):
        df = self.conn.execute(f'SELECT page_id, pull_date FROM {self.schema}.fact_fb_page')

        rows = []
        for page in pages:
            page_id = page.page_id

            exists = len(df[(df.pull_date==self.scrape_date)&(df.page_id==page_id)])

            if not exists:
                rows.append((page_id, self.scrape_date, 
                             getattr(page, 'num_followers', None),
                             getattr(page, 'num_likes', None),
                             getattr(page, 'page_views_total', None),
                             getattr(page, 'page_engaged_users', None),
                             getattr(page, 'page_impressions', None),
                             getattr(page, 'page_impressions_unique', None),
                             page.for_date,
                            ))
            else:
                print(f'fact page {page_id}@{self.scrape_date} already exists')

        rows = list(set(rows))
        self.conn.insert(f'{self.schema}.fact_fb_page', rows)
        return rows

    def _store_page(self, pages):
        rows_dim = self._store_page_dim(pages)
        rows_fact = self._store_page_fact(pages)

        for page in pages:
            self.store(page.posts)

        return rows_dim + rows_fact

    @save_history
    def _store_posts_dim(self, posts):
        df = self.conn.execute(f'SELECT post_id FROM {self.schema}.dim_fb_post')
        rows = []
        comments = []
        for post in posts:
            post_id = post.post_id
            exists = post_id in df.post_id.values
            if post.was_live is not None:
                was_live = int(post.was_live)
            else:
                was_live = None

            if not exists:
                rows.append((
                        post.page_id,
                        post_id,
                        post.post_time,
                        int(post.has_text),
                        int(post.has_video),
                        int(post.has_image),
                        was_live,
                        post.caption,
                        getattr(post, 'sent_label', None),
                        getattr(post, 'sent_score', None),
                        int(post.post_type),
                    ))

            if hasattr(post, 'comments'):
                comments += post.comments

        if len(comments):
            self.store(comments)

        rows = list(set(rows))
        self.conn.insert(f'{self.schema}.dim_fb_post', rows)
        return rows

    @save_history
    def _store_posts_fact(self, posts):
        
        df = self.conn.execute(f'SELECT post_id, pull_date FROM {self.schema}.fact_fb_post')
        rows = []
        for post in posts:
            exists = len(df[(df.post_id==post.post_id)&(df.pull_date==self.scrape_date)])
            if not exists:
                rows.append((
                    post.page_id,
                    post.post_id,
                    self.scrape_date,
                    post.num_shares,
                    post.num_comments,
                    post.num_reacts,
                    post.num_like,
                    post.num_haha,
                    post.num_love,
                    post.num_wow,
                    post.num_sad,
                    post.num_angry,
                    getattr(post, 'post_impressions', None),
                    getattr(post, 'post_impressions_unique', None),
                    getattr(post, 'post_impressions_paid', None),
                    getattr(post, 'post_impressions_paid_unique', None),
                    getattr(post, 'post_impressions_fan', None),
                    getattr(post, 'post_impressions_fan_unique', None),
                    getattr(post, 'post_impressions_fan_paid', None),
                    getattr(post, 'post_impressions_fan_paid_unique', None),
                    getattr(post, 'post_impressions_organic', None),
                    getattr(post, 'post_impressions_organic_unique', None),
                ))

        rows = list(set(rows))
        self.conn.insert(f'{self.schema}.fact_fb_post', rows)
        return rows

    def _store_post(self, posts):
        rows_dim = self._store_posts_dim(posts)
        rows_fact = self._store_posts_fact(posts)
        return rows_dim + rows_fact

    def _store_comment(self, comments):
        rows_dim = self._store_comment_dim(comments)
        rows_fact = self._store_comment_fact(comments)
        return rows_dim + rows_fact
    
    @save_history
    def _store_comment_dim(self, comments):
        df = self.conn.execute(f'SELECT comment_id FROM {self.schema}.dim_fb_comment')
        rows = []
        for comment in comments:
            exists = comment.comment_id in df.comment_id.values
            if not exists:
                rows.append((
                    comment.post_id,
                    comment.comment_id,
                    comment.parent_id,
                    comment.create_time,
                    comment.reply_level,
                    comment.message,
                    getattr(comment, 'sent_label', None),
                    getattr(comment, 'sent_score', None),
                ))

        rows = list(set(rows))
        self.conn.insert(f'{self.schema}.dim_fb_comment', rows)
        return rows

    @save_history
    def _store_comment_fact(self, comments):
        df = self.conn.execute(f'SELECT comment_id, pull_date FROM {self.schema}.fact_fb_comment')
        rows = []
        for comment in comments:
            exists = len(df[(df.pull_date==self.scrape_date)&(df.comment_id==comment.comment_id)])
            if not exists:
                rows.append((
                    comment.post_id,
                    self.scrape_date,
                    comment.comment_id,
                    comment.num_replies,
                    comment.num_likes
                ))

        rows = list(set(rows))
        self.conn.insert(f'{self.schema}.fact_fb_comment', rows)
        return rows

