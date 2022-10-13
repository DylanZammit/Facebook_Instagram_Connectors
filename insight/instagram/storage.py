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

    def __init__(self):
        self.conn = Connector(**POSTGRES)
        self.scrape_date = pd.Timestamp(datetime.now().date())
        self.last_profile_call = pd.Timestamp('2000-01-01')
        self.history = {}

    def __str__(self):
        return '\n'.join([f'{k} rows stored = {v}' for k, v in self.history.items()])

    def store(self, obj):
        '''
        Can pass insta objects.
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
        df = self.conn.execute('SELECT page_id FROM dim_insta_page')
        rows = []
        for page in pages:
            page_id = page.page_id
            exists = page_id in df.page_id.values
            if not exists:
                rows.append((page_id, page.username, page.is_competitor, page.name))

        self.conn.insert('dim_insta_page', rows)
        return rows

    @save_history
    def _store_page_fact(self, pages):
        df = self.conn.execute('SELECT page_id, pull_date FROM fact_insta_page')

        rows = []
        for page in pages:
            page_id = page.page_id
            exists = len(df[(df.pull_date==self.scrape_date)&(df.page_id==page_id)])

            if not exists:
                rows.append((
                    page_id, 
                    self.scrape_date, 
                    page.num_followers, 
                    getattr(page, 'num_following', None),
                    page.num_media,
                    getattr(page, 'new_followers', None),
                    getattr(page, 'impressions', None),
                    getattr(page, 'reach', None),
                    getattr(page, 'profile_views', None),
                ))
            else:
                print(f'fact page {page_id}@{self.scrape_date} already exists')

        rows = list(set(rows))
        self.conn.insert('fact_insta_page', rows)
        return rows

    def _store_page(self, pages):
        for page in pages:
            rows_dim = self._store_page_dim(pages)
            rows_fact = self._store_page_fact(pages)

            if hasattr(page, 'medias'):
                self.store(page.medias)

        return rows_dim + rows_fact

    @save_history
    def _store_media_dim(self, medias):
        df = self.conn.execute('SELECT media_id FROM dim_insta_media')
        rows = []
        for media in medias:
            media_id = int(media.pk)
            page_id = int(media.id.split('_')[1])
            exists = media_id in df.media_id.values
            if not exists:
                rows.append((
                        media_id,
                        media.code,
                        page_id,
                        media.taken_at,
                        media.media_type
                    ))
            if hasattr(media, 'comments'):
                self.store(media.comments)
        rows = list(set(rows))
        self.conn.insert('dim_insta_media', rows)
        return rows

    @save_history
    def _store_media_fact(self, medias):
        
        df = self.conn.execute('SELECT media_id, pull_date FROM fact_insta_media')
        rows = []
        for media in medias:
            #page_id = int(media.id.split('_')[0])
            #media_id = int(media.id.split('_')[1])

            page_id = media.page_id
            media_id = media.pk
            
            exists = len(df[(df.media_id==media_id)&(df.pull_date==self.scrape_date)])
            if not exists:
                rows.append((
                    page_id,
                    media_id,
                    self.scrape_date,
                    media.comment_count,
                    media.like_count,
                    getattr(media, 'view_count', None),
                    getattr(media, 'impressions', None),
                    getattr(media, 'reach', None),
                    getattr(media, 'engagement', None),
                    getattr(media, 'saved', None),
                ))

        rows = list(set(rows))
        self.conn.insert('fact_insta_media', rows)
        return rows

    def _store_media(self, medias):
        rows_dim = self._store_media_dim(medias)
        rows_fact = self._store_media_fact(medias)
        return rows_dim + rows_fact

    def _store_comment(self, comments):
        rows_dim = self._store_comment_dim(comments)
        rows_fact = self._store_comment_fact(comments)
        return rows_dim + rows_fact
    
    @save_history
    def _store_comment_dim(self, comments):
        df = self.conn.execute('SELECT comment_id FROM dim_insta_comment')
        rows = []
        for comment in comments:
            exists = comment.comment_id in df.comment_id.values
            if not exists:
                rows.append((
                    comment.media_id,
                    comment.comment_id,
                    comment.parent_id,
                    comment.create_time,
                    comment.reply_level,
                    comment.message,
                    comment.username,
                    getattr(comment, 'sent_label', None),
                    getattr(comment, 'sent_score', None),
                ))

        rows = list(set(rows))
        self.conn.insert('dim_insta_comment', rows)
        return rows

    @save_history
    def _store_comment_fact(self, comments):
        df = self.conn.execute('SELECT comment_id, pull_date FROM fact_insta_comment')
        rows = []
        for comment in comments:
            exists = len(df[(df.pull_date==self.scrape_date)&(df.comment_id==comment.comment_id)])
            if not exists:
                rows.append((
                    comment.media_id,
                    self.scrape_date,
                    comment.comment_id,
                    comment.num_replies,
                    comment.num_likes
                ))

        rows = list(set(rows))

        self.conn.insert('fact_insta_comment', rows)
        return rows
