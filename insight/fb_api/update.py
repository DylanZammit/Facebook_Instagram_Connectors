#!/bin/bash -l

from insight.fb_api import MyGraphAPI
#import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta
from IPython import embed
import os
from insight.entities import *
from insight.storage import FacebookStorage as Storage
import requests
import json
from logger import mylogger, pb
from traceback import format_exc

post_metrics = """post_impressions,post_impressions_unique,post_impressions_paid,post_impressions_paid_unique,post_impressions_fan,post_impressions_fan_unique,post_impressions_fan_paid,post_impressions_fan_paid_unique,post_impressions_organic,post_impressions_organic_unique"""

class PageExtractor:

    def __init__(self, username, is_competitor):
        self.api = MyGraphAPI(page=username)
        self.page = Page(username=username, is_competitor=is_competitor)
        self.storage = Storage()
        self.react_types = ['LIKE', 'LOVE', 'WOW', 'HAHA', 'SAD', 'ANGRY', 'NONE']

    def store(self):
        self.storage.store(self.page)
    
    def get_page(self):
        # +1 day for API for some reason
        today = str(datetime.now().date() + timedelta(days=1))
        params = {
            'period': 'day',
            'access_token': self.api.access_token,
            'since': today,
            'until': today,
            'metric': 'page_views_total,page_engaged_users,page_impressions,page_impressions_unique'
        }
        url = f'https://graph.facebook.com/{self.page.username}/insights'

        res = requests.get(url, params=params)
        metrics = json.loads(res.text)['data']
        for metric in metrics:
            assert len(metric['values'])==1
            setattr(self.page, metric['name'], metric['values'][0]['value'])

        fields = 'id,fan_count,followers_count,name'
        res = self.api.get_object(self.page.username, fields=fields)

        self.page.num_likes = res['fan_count']
        self.page.num_followers = res['followers_count']
        self.page.name = res['name']
        self.page.page_id = int(res['id'])

        return self

    def get_obj_comments(self, post_id, comment_id=None, get_replies=True):
        '''
        If comment_id is passed, it is assumed that it is a reply.
        Otherwise it is assumed that it is a post comment
        '''
        return self._get_obj_comments(post_id, comment_id, 0, get_replies)
    
    def _get_obj_comments(self, post_id, parent_id=None, level=0, get_replies=True):
        obj_id = post_id if not parent_id else parent_id
        url = f'https://graph.facebook.com/{obj_id}/comments'
        params = {
            'fields': 'comment_count,created_time,like_count,message,parent',
            'access_token': self.api.access_token
        }
        res = requests.get(url, params=params)
        data = json.loads(res.text)['data']

        parent_id = None if parent_id is None else int(parent_id.split('_')[1])
        comments = []
        for comment_data in data:
            num_replies = comment_data['comment_count']
            comment_id = comment_data['id']
            comment = Comment(
                comment_id=int(comment_id.split('_')[1]),
                post_id=int(post_id.split('_')[1]),
                parent_id=parent_id,
                num_likes=comment_data['like_count'],
                num_replies=num_replies,
                create_time=comment_data['created_time'],
                message=comment_data['message'],
                reply_level=level
            )
            comments.append(comment)

            if get_replies and num_replies:
                replies = self._get_obj_comments(post_id, comment_id, level=level+1, get_replies=get_replies)
                comments += replies

        return comments

    def format_comments(self, data, post_id, parent_id=None, level=0):
        comments = []
        for comment_data in data:
            num_replies = comment_data['comment_count']
            comment_id = int(comment_data['id'].split('_')[1])
            comment = Comment(
                comment_id=comment_id,
                post_id=post_id,
                parent_id=parent_id,
                num_likes=comment_data['like_count'],
                num_replies=num_replies,
                create_time=comment_data['created_time'],
                message=comment_data['message'],
                reply_level=level
            )
            comments.append(comment)
            if 'comments' in comment_data:
                replies = self.format_comments(comment_data['comments']['data'], post_id, comment_id, level=level+1)
                comments += replies
        
        return comments

    def get_post(self, reply_level=None):
        url = f'https://graph.facebook.com/{self.page.page_id}/posts'
        react_field = ','.join(['reactions.type({}).limit(0).summary(1).as({})'.format(react, react) for react in self.react_types])
        insight_field = f'insights.metric({post_metrics})'

        fields = ','.join([
            'attachments,reactions.limit(0).summary(1).as(TOT),shares,created_time,message,permalink_url',
            react_field,
            insight_field
        ])

        if reply_level is not None:
            comments_field = 'comments.fields(comment_count,created_time,like_count,message,parent{})'
            for i in range(reply_level):
                comments_field = comments_field.format(',' + comments_field) if i < reply_level-1 else comments_field.format('')
            fields = ','.join([fields, comments_field])

        params = {
            'fields': fields,
            'access_token': self.api.access_token
        }
        res = requests.get(url, params=params)
        api_posts = json.loads(res.text)['data']

        for api_post in api_posts:
            page_post_id = api_post['id']
            logger.info(f'reading post {page_post_id}')
            post_id = int(page_post_id.split('_')[1])
            
            attachments = api_post['attachments']['data']
            media = next((att['media'] for att in attachments if 'media' in att), {})
            has_image = 'image' in media
            has_video = 'video' in media
            num_shares = api_post.get('shares', {'count': 0})['count']
            create_time = api_post['created_time']
            num_reacts = api_post['TOT']['summary']['total_count']
            num_like = api_post['LIKE']['summary']['total_count']
            num_love = api_post['LOVE']['summary']['total_count']
            num_wow = api_post['WOW']['summary']['total_count']
            num_haha = api_post['HAHA']['summary']['total_count']
            num_angry = api_post['ANGRY']['summary']['total_count']
            num_sad = api_post['SAD']['summary']['total_count']
            caption = api_post.get('message', '')
            has_text = caption!= ''

            if reply_level is None:
                comments = self.get_obj_comments(page_post_id)
            else:
                comments = self.format_comments(api_post['comments']['data'], post_id) if 'comments' in api_post else []

            num_comments = len(comments)

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
                page_id=self.page.page_id,
                caption=caption,
                comments=comments
            )

            for metric in api_post['insights']['data']:
                name = metric['name']
                value = metric['values'][0]['value']
                setattr(post, name, value)

            self.page.posts.append(post)

        return self


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--page_name', help='page name or id [default=levelupmalta]', type=str, default='levelupmalta')
    parser.add_argument('--is_competitor', help='is_competitor', action='store_true')
    parser.add_argument('--store', help='store data', action='store_true') 
    parser.add_argument('--reply_level', help='reply levels to read [def=all]', type=int)
    args = parser.parse_args()

    page_name = args.page_name

    fn_log = 'update_{}'.format(page_name)
    notif_name = f'{page_name} API'
    logger = mylogger(fn_log, notif_name)

    try:
        extractor = PageExtractor(page_name, is_competitor=int(args.is_competitor))
        extractor = extractor.get_page().get_post(reply_level=args.reply_level)
        if args.store:
            logger.info('storing')
            extractor.store()
    except:
        err = format_exc()
        logger.critical(err)
        pb.push_note(notif_name, err)
    else:
        #pb.push_note(notif_name, 'Success!')
        logger.info('success')

