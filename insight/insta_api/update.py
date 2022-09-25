#!/bin/bash -l

from insight.fb_api import MyGraphAPI
import pandas as pd
from datetime import datetime, timedelta
from IPython import embed
import os
from insight.entities import *
from insight.storage import InstaStorage as Storage
import requests
import json
from logger import mylogger, pb
from insight.utils import *

media_metrics = """engagement,impressions,reach,saved"""
page_metrics = """follower_count,impressions,profile_views,reach"""


class PageExtractor:

    def __init__(self, page):
        self.api = MyGraphAPI(page=page)
        self.page = Page(username=page)
        self.storage = Storage()

    def store(self):
        self.storage.store(self.page)

    def comment_replies(self):
        pass
    
    def pages(self, is_competitor):
        self.page.is_competitor = is_competitor
        self.page.page_id = self.api.ig_user
        return self

    def page_engagement(self):
        # +1 day for API for some reason
        today = str(datetime.now().date() + timedelta(days=1))
        yesterday = str(datetime.now().date())
        params = {
            'period': 'day',
            'access_token': self.api.access_token,
            'since': yesterday,
            'until': today,
            'metric': page_metrics
        }
        url =f'https://graph.facebook.com/{self.api.ig_user}/insights'

        res = requests.get(url, params=params)
        metrics = json.loads(res.text)['data']
        for metric in metrics:
            assert len(metric['values'])==1
            metric_name = metric['name']
            if metric_name == 'follower_count':
                metric_name = 'new_followers'
            setattr(self.page, metric_name, metric['values'][0]['value'])

        fields = 'followers_count'
        params = {
            'access_token': self.api.access_token,
            'fields': fields
        }
        url = f'https://graph.facebook.com/{self.api.ig_user}'

        res = requests.get(url, params=params)
        data = json.loads(res.text)

        self.page.num_followers = data['followers_count']

        return self

    def post_comments(self):
        pass

    def post_fixed(self):
        medias = []
        params = {
            'access_token': self.api.access_token,
            'fields': 'caption,comments_count,id,like_count,media_product_type,media_type,shortcode,timestamp'
        }
        params['fields'] += f',insights.metric({media_metrics})'
        url = f'https://graph.facebook.com/{self.api.ig_user}/media'

        res = requests.get(url, params=params)
        api_posts = json.loads(res.text)['data']

        num_media = len(api_posts)
        self.page.num_media = num_media
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

            media = Media(
                pk=media_id,
                id='{}_{}'.format(self.api.ig_user, media_id),
                media_type=media_type,
                comment_count=num_comments,
                like_count=num_like,
                page_id=self.page.page_id,
                caption=caption,
                taken_at=create_time,
                code=code_id
            )

            metrics = api_post['insights']['data']

            for metric in metrics:
                name = metric['name']
                value = metric['values'][0]['value']
                setattr(media, name, value)

            medias.append(media)

        if not hasattr(self.page, 'medias'):
            self.page.medias = medias
        else:
            self.page.medias += medias
        return self

    def post_reacts(self):
        pass

    def post_shares(self):
        pass

    def users(self):
        pass


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--page_name', help='page name or id [default=levelupmalta]', type=str, default='levelupmalta')
    parser.add_argument('--store', help='store data', action='store_true') 
    args = parser.parse_args()

    page_name = args.page_name

    fn_log = 'update_{}'.format(page_name)
    notif_name = f'{page_name} INSTA API'
    logger = mylogger(fn_log, notif_name)

    try:
        extractor = PageExtractor(page_name)
        extractor = extractor.pages(0).page_engagement().post_fixed()
        if args.store:
            logger.info('storing')
            extractor.store()
    except Exception as e:
        logger.critical(e)
        pb.push_note(notif_name, str(e))
    else:
        pb.push_note(notif_name, 'Success!')
        logger.info('success')

