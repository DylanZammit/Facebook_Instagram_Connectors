#!/bin/bash -l

from insight.fb_api import MyGraphAPI
#import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta
from IPython import embed
import os
from insight.entities import *
from insight.storage import Storage
import requests
import json
from logger import mylogger, pb

post_metrics = """post_impressions,post_impressions_unique,post_impressions_paid,post_impressions_paid_unique,post_impressions_fan,post_impressions_fan_unique,post_impressions_fan_paid,post_impressions_fan_paid_unique,post_impressions_organic,post_impressions_organic_unique"""

class PageExtractor:

    def __init__(self, page):
        self.api = MyGraphAPI(page=page)
        self.page = Page(page)
        self.storage = Storage()
        self.react_types = ['LIKE', 'LOVE', 'WOW', 'HAHA', 'SAD', 'ANGRY', 'NONE']

    def store(self):
        self.storage.store(self.page)

    def comment_replies(self):
        pass
    
    def pages(self, is_competitor):
        self.page.is_competitor = is_competitor
        return self

    def page_engagement(self):
        # +1 day for API for some reason
        today = str(datetime.now().date() + timedelta(days=1))
        params = {
            'period': 'day',
            'access_token': self.api.access_token,
            'since': today,
            'until': today
        }
        url = 'https://graph.facebook.com/levelupmalta/insights?metric=page_views_total,page_engaged_users,page_impressions,page_impressions_unique'

        res = requests.get(url, params=params)
        metrics = json.loads(res.text)['data']
        for metric in metrics:
            assert len(metric['values'])==1
            setattr(self.page, metric['name'], metric['values'][0]['value'])

        fields = 'id,fan_count'
        res = self.api.get_object(self.page.page_id, fields=fields)

        self.page.num_likes = res['fan_count']

        return self

    def post_comments(self):
        pass

    def post_fixed(self):
        api_posts = self.api.get_object(self.page.page_id, fields='posts')['posts']['data']
        for api_post in api_posts:
            page_post_id = api_post['id']
            logger.info(f'reading post {page_post_id}')
            post_id = int(page_post_id.split('_')[1])

            react_field = ','.join(['reactions.type({}).limit(0).summary(1).as({})'.format(react, react) for react in self.react_types])
            fields = 'attachments,reactions.limit(0).summary(1).as(TOT),comments.summary(1),shares,created_time,message,permalink_url,' + react_field
            data = self.api.get_object(page_post_id, fields=fields)
            
            attachments = data['attachments']['data']
            media = next((att['media'] for att in attachments if 'media' in att), {})
            has_image = 'image' in media
            has_video = 'video' in media
            num_shares = data.get('shares', {'count': 0})['count']
            num_comments = data['comments']['summary']['total_count']
            create_time = data['created_time']
            num_reacts = data['TOT']['summary']['total_count']
            num_like = data['LIKE']['summary']['total_count']
            num_love = data['LOVE']['summary']['total_count']
            num_wow = data['WOW']['summary']['total_count']
            num_haha = data['HAHA']['summary']['total_count']
            num_angry = data['ANGRY']['summary']['total_count']
            num_sad = data['SAD']['summary']['total_count']
            message = data.get('message', '')
            has_text = message != ''

            post = Post(
                num_shares=num_shares,
                num_comments=num_comments,
                num_reacts=num_reacts,
                num_like=num_like,
                num_love=num_love,
                num_wow=num_wow,
                num_haha=num_haha,
                num_angry=num_angry,
                num_sad=num_sad,
                has_video=has_video,
                has_image=has_image,
                has_text=has_text,
                was_live=None,
                post_id=post_id,
                post_time=create_time,
                page_id=self.page.page_id
            )

            data = self.api.get_object(f'{page_post_id}/insights', metric=post_metrics)['data']
            for metric in data:
                name = metric['name']
                value = metric['values'][0]['value']
                setattr(post, name, value)

            self.page.posts.append(post)

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
    notif_name = f'{page_name} API'
    logger = mylogger(fn_log, notif_name)

    try:
        extractor = PageExtractor(page_name)
        extractor = extractor.pages(0).page_engagement().post_fixed()
        if args.store:
            logger.info('storing')
            extractor.store()
    except Exception as e:
        logger.critical(e)
        pb.push_note(notif_name, e)
    else:
        pb.push_note(notif_name, 'Success!')
        logger.info('success')

