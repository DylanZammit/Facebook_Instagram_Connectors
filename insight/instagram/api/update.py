#!/bin/bash -l

from insight import MyGraphAPI
import pandas as pd
from datetime import datetime, timedelta
import os
from insight.instagram.entities import *
from insight.instagram.storage import Storage
import requests
import json
from logger import mylogger, pb
from insight.utils import *

media_metrics = """engagement,impressions,reach,saved"""
page_metrics = """follower_count,impressions,profile_views,reach"""


class InstaExtractor:

    def __init__(self, page, is_competitor, do_sentiment=True, do_translate=True):
        self.api = MyGraphAPI(page=page)
        self.username = page
        self.is_competitor = is_competitor
        self.page_kwargs = {
            'username': self.username,
            'is_competitor': self.is_competitor,
            'page_id': self.api.ig_user
        }
        self.storage = Storage()
        if do_sentiment:
            self.sent = Sentiment(do_translate)
        else:
            self.sent = object()
            self.sent.get_sentiment = dummy


    def get_page(self):
        # +1 day for API for some reason
        today = str(datetime.now().date())
        yesterday = str(datetime.now().date() - timedelta(days=1))
        params = {
            'access_token': self.api.access_token,
            'fields': f'followers_count,insights.metric({page_metrics}).period(day).since({yesterday}).until({today})'
        }
        
        url =f'https://graph.facebook.com/{self.api.ig_user}'

        res = requests.get(url, params=params)
        res = json.loads(res.text)
        metrics = res['insights']['data']
        for metric in metrics:
            assert len(metric['values'])==1
            metric_name = metric['name']
            kwargs = {}
            if metric_name == 'follower_count':
                metric_name = 'new_followers'
            kwargs[metric_name] = metric['values'][0]['value']

        kwargs['num_followers'] = res['followers_count']
        
        self.page_kwargs.update(kwargs)

        return self

    def format_comments(self, data, media_id):
        comments = []
        for comment_data in data:
            message = comment_data['text']
            replies = comment_data.get('replies', [])
            num_replies = len(replies)
            comment_id = int(comment_data['id'])
            sent_label, sent_score = self.sent.get_sentiment(message)
            comment = Comment(
                comment_id=comment_id,
                media_id=media_id,
                parent_id=None,
                num_likes=comment_data['like_count'],
                num_replies=num_replies,
                create_time=comment_data['timestamp'],
                message=message,
                reply_level=0,
                username=comment_data['username'],
                sent_label=sent_label,
                sent_score=sent_score
            )
            comments.append(comment)

            # TEST THIS
            if isinstance(replies, list):
                #if len(replies) > 0: breakpoint()
                replies = {}

            for reply in replies.get('data', []):
                #breakpoint()
                message = reply['text']
                sent_label, sent_score = self.sent.get_sentiment(message)
                comment = Comment(
                    comment_id=reply['id'],
                    media_id=media_id,
                    parent_id=comment_id,
                    num_likes=reply['like_count'],
                    num_replies=0,
                    create_time=reply['timestamp'],
                    message=message,
                    reply_level=1,
                    username=reply['username'],
                    sent_label=sent_label,
                    sent_score=sent_score
                )
                comments.append(comment)

        return comments

    def get_post(self, n_posts=None, since=None, until=None, limit=100):
        if n_posts is None: n_posts = np.inf
        medias = []
        basic_fields = 'caption,comments_count,id,like_count,media_product_type,media_type,shortcode,timestamp'
        insights = f'insights.metric({media_metrics})'
        url = f'https://graph.facebook.com/{self.api.ig_user}/media'
        fields = ','.join([basic_fields, insights])

        comment_fields = 'id,like_count,parent_id,text,timestamp,username'
        comments_field = f'comments.fields({comment_fields},replies.fields({comment_fields}))'

        fields = ','.join([fields, comments_field])

        params = {
            'fields': fields,
            'since': since,
            'until': until,
            'limit': limit,
            'access_token': self.api.access_token
        }

        next_url = url

        posts_loaded = 0
        while next_url is not None and posts_loaded < n_posts:
            res = requests.get(next_url, params=params)
            res = json.loads(res.text)
            api_posts = res['data']
            next_url = res.get('paging', {}).get('next', None)
            params = {}

            posts_loaded += len(api_posts)
            for api_post in api_posts:

                media_id = int(api_post['id'])
                logger.info(f'reading post {media_id}')

                media_type = MEDIA_TYPE_MAP[api_post['media_type']]
                media_product_type = MEDIA_PRODUCT_TYPE_MAP[api_post['media_product_type']]
            
                media_type = get_media_content(media_type, media_product_type)

                num_comments = api_post['comments_count']
                create_time = api_post['timestamp']

                num_like = api_post.get('like_count', 0)
                caption = api_post.get('caption', '')
                code_id = api_post['shortcode']
                has_text = caption!=''

                sent_label, sent_score = self.sent.get_sentiment(caption)
                comments = self.format_comments(api_post['comments']['data'], media_id) if 'comments' in api_post else []

                metrics = api_post['insights']['data']

                kwargs = {}
                for metric in metrics:
                    name = metric['name']
                    value = metric['values'][0]['value']
                    kwargs[name] = value

                media = Media(
                    pk=media_id,
                    id='{}_{}'.format(media_id, self.api.ig_user),
                    media_type=media_type,
                    comment_count=num_comments,
                    like_count=num_like,
                    page_id=self.page_kwargs['page_id'],
                    caption=caption,
                    taken_at=create_time,
                    code=code_id,
                    comments=comments,
                    **kwargs
                )
                medias.append(media)

        self.page_kwargs['medias'] = medias
        self.page_kwargs['num_media'] = len(medias)

        return self


@time_it
@bullet_notify
def main(page_name, is_competitor, store, schema, nopage, noposts, n_posts, limit, since, until, **kwargs):
    extractor = InstaExtractor(page_name, is_competitor=int(is_competitor))
    extractor = extractor.get_page().get_post(n_posts=n_posts, since=since, until=until, limit=n_posts)
    if store:
        logger.info('storing...')
        store = Storage(schema=schema)
        page = Page(**extractor.page_kwargs)
        store.store(page)
        #if not noposts: store.store(extractor.posts)
        logger.info('stored')

        return store.history

    return {}


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--page_name', help='page name or id [default=levelupmalta]', type=str, default='levelupmalta')
    parser.add_argument('--is_competitor', help='is competitor', action='store_true') 
    parser.add_argument('--schema', help='db schema name', type=str, default='competitors')
    parser.add_argument('--store', help='store data', action='store_true') 
    parser.add_argument('--nopage', help='do not store page data', action='store_true') 
    parser.add_argument('--noposts', help='do not store posts data', action='store_true') 
    parser.add_argument('--n_posts', help='number of posts to load, default=all', type=int)
    parser.add_argument('--limit', help='number of posts per api call', type=int, default=100)
    parser.add_argument('--since', help='YYYY-MM-DD. Also accepts "tdy" and "ydy"', type=str)
    parser.add_argument('--until', help='YYYY-MM-DD', type=str)
    args = parser.parse_args()

    if args.since == 'tdy':
        since = str(datetime.now().date())
    elif args.since == 'ydy':
        since = str(datetime.now().date()-timedelta(days=1))
    else:
        since = args.since

    page_name = args.page_name

    fn_log = 'update_IG_{}'.format(page_name)
    notif_name = f'{page_name} INSTA API'
    logger = mylogger(fn_log)

    main(
        page_name, 
        args.is_competitor, 
        args.store, 
        args.schema, 
        args.nopage, 
        args.noposts, 
        args.n_posts,
        args.limit,
        since, 
        args.until, 
        logger=logger, 
        title=notif_name
    )
