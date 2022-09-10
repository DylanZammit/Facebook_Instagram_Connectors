from facebook_scraper import *
from facebook_scraper.exceptions import TemporarilyBanned, NotFound
import gender_guesser.detector as getgender
import numpy as np
import time
import re
import os
import pandas as pd
from IPython import embed
from insight.fb_api import MyGraphAPI
from datetime import timedelta, datetime
from mysql.connector.errors import IntegrityError
from insight.utils import *


class Page:

    def __init__(self, page_id, **kwargs):
        self.posts = []
        self.users = []
        self.page_id = page_id
        self.is_competitor = 1
        self.num_likes = None
        for k, v in kwargs.items():
            setattr(self, k, v)


    def parse_replies(self, replies, parent_id):
        users = []
        comment_replies = []
        for reply in replies:
            comment_id = reply['comment_id']
            comment_time = reply['comment_time']
            commenter_id = reply['commenter_id']
            num_reacts = reply.get('comment_reaction_count', 0)
            if num_reacts is None: num_reacts = 0
            name, surname = extract_namesurname(reply.get('commenter_name'))
            user_id = extract_id(reply['commenter_url'])
            if not user_id.isdecimal() and commenter_id is not None:
                text_id = user_id
                user_id = commenter_id
            else:
                text_id = None

            user = User(
                user_id=user_id, 
                text_id=text_id,
                name=name, 
                surname=surname
            )
            
            comment_reply = Reply(
                reply_id=comment_id,
                parent_id=parent_id,
                create_time=comment_time,
                user_id=user_id,
                num_reacts=num_reacts,
            )

            users.append(user)
            comment_replies.append(comment_reply)

        return users, comment_replies

    def get_page_posts(self, num_posts=None, latest_date=None, get_commenters=False, get_sharers=False, get_reactors=False):
        if latest_date is None: latest_date = pd.Timestamp('2000-01-01')
        if num_posts is None: num_posts = np.inf

        options = {
            'comments': get_commenters,
            'sharers': get_sharers, 
            'reactors': get_reactors,
            'posts_per_page': 20
        }

        posts = get_posts(self.page_id, options=options)

        page_posts = []
        users = []
        for i, post in enumerate(posts):
            post_reacts = []
            post_shares = []
            post_comments = []

            if i > 0: rsleep(10, 10, q=False)
            post_details = {}

            try:
                post_time = post['time']
                if i >= num_posts: break
                if pd.Timestamp(post_time) < latest_date: break

                post_id = post['post_id']
                print(f'{i+1}) Post={post_id} on {post_time}')
                has_text = True if post['text'] is not None else False
                num_comments = post['comments']
                num_shares = post['shares']
                has_image = post['image'] is not None
                has_video = post['video'] is not None
                video_watches = post['video_watches']
                was_live = post['was_live']

                reactions = post['reactions']
                if reactions is None: reactions = {}
                num_like = reactions.get('like', 0)
                num_love = reactions.get('love', 0)
                num_haha = reactions.get('haha', 0)
                num_wow = reactions.get('wow', 0)
                num_angry = reactions.get('angry', 0)
                num_sad = reactions.get('sad', 0)
                num_reacts = post.get('reaction_count', 0)
                if num_reacts is None: num_reacts = 0

                if get_sharers:
                    for sharer in post['sharers']:
                        name, surname = extract_namesurname(sharer['name'])
                        user_id = extract_id(sharer['link'])

                        share = Share(
                            user_id=user_id, 
                            post_id=post_id
                        )

                        user = User(
                            user_id=user_id, 
                            name=name, 
                            surname=surname
                        )

                        users.append(user)
                        post_shares.append(share)

                if get_reactors:
                    reactors = post['reactors']
                    if reactors == None: reactors = []
                    for reactor in reactors:
                        name, surname = extract_namesurname(reactor['name'])
                        react_str = reactor.get('type')

                        if react_str is None:
                            print(f'None type react for {name, surname}')
                            react_type = None
                        else:
                            react_str = react_str.upper()
                            react_type = react2id.get(react_str)

                            if react_type is None:
                                react_type = react2id.get('LIKE')
                                print(f'Treating special react {react_str} by {name} {surname} as LIKE')

                        user_id = extract_id(reactor['link'])

                        user = User(
                            user_id=user_id, 
                            name=name, 
                            surname=surname
                        )

                        react = React(
                            user_id=user_id,
                            post_id=post_id,
                            react_id=react_type
                        )

                        users.append(user)
                        post_reacts.append(react)

                if get_commenters:
                    for comment in post['comments_full']:
                        comment_time = comment['comment_time']
                        comment_id = comment['comment_id']
                        commenter_id = comment.get('commenter_id')
                        name, surname = extract_namesurname(comment.get('commenter_name'))
                        user_id = extract_id(comment['commenter_url'])
                        if not user_id.isdecimal() and commenter_id is not None:
                            text_id = user_id
                            user_id = commenter_id
                        else:
                            text_id = None
                        num_reacts = comment.get('comment_reaction_count', 0)
                        if num_reacts is None: num_reacts = 0
                        replies = comment.get('replies', [])
                        num_replies = len(replies)

                        reply_users, comment_replies = self.parse_replies(replies, comment_id)

                        users += reply_users
                        user = User(
                            user_id=user_id, 
                            text_id=text_id,
                            name=name, 
                            surname=surname
                        )
                        
                        post_comment = Comment(
                            comment_id=comment_id,
                            replies=comment_replies,
                            post_id=post_id,
                            create_time=comment_time,
                            user_id=user_id,
                            num_reacts=num_reacts,
                            num_replies=num_replies
                        )
                        users.append(user)
                        post_comments.append(post_comment)

                self.posts.append(
                    Post(
                        post_id=post_id,
                        page_id=self.page_id,
                        has_text=has_text,
                        has_video=has_video,
                        has_image=has_image,
                        post_time=post_time,
                        was_live=was_live,
                        num_shares=num_shares,
                        num_comments=num_comments,
                        num_reacts=num_reacts,
                        num_like=num_like,
                        num_haha=num_haha,
                        num_love=num_love,
                        num_wow=num_wow,
                        num_sad=num_sad,
                        num_angry=num_angry,
                        reacts=post_reacts,
                        shares=post_shares,
                        comments=post_comments
                    )
                )

            except TemporarilyBanned as e:
                print(f'Temporarily banned')
                break

        self.users = users

    def get_details(self):
        page_details = get_page_info(self.page_id)
        num_likes = page_details.get('likes')
        if num_likes is not None:
            setattr(self, 'num_likes', num_likes)
            return
        print(f'Could not get page likes for {self.page_id}')


class Post:

    def __init__(self, post_id, **kwargs):
        self.shares = []
        self.comments = []
        self.reacts = []
        self.post_id = int(post_id)

        for k, v in kwargs.items():
            setattr(self, k, v)

    def get_info(self, comments=True, shares=True, reacts=True):
        raise NotImplementedError


class User:

    d = getgender.Detector(case_sensitive=False)
    
    def __init__(self, user_id, **kwargs):
        self.user_id = user_id
        for k, v in kwargs.items():
            setattr(self, k, v)

        if hasattr(self, 'name'):
            gender = User.d.get_gender(self.name)
            if 'female' in gender:
                self.gender = 'F'
            elif 'male' in gender:
                self.gender = 'M'
            else:
                self.gender = None
    
    def get_info(self):
        raise NotImplementedError


class Comment:
    
    def __init__(self, comment_id, **kwargs):
        self.comment_id = int(comment_id)
        self.replies = []
        for k, v in kwargs.items():
            setattr(self, k, v)

class Reply:
    
    def __init__(self, reply_id, parent_id, **kwargs):
        self.reply_id = int(reply_id)
        self.parent_id = int(parent_id)
        for k, v in kwargs.items():
            setattr(self, k, v)


class Share:
    
    def __init__(self, user_id, post_id, **kwargs):
        self.post_id = post_id
        self.user_id = user_id
        for k, v in kwargs.items():
            setattr(self, k, v)


class React:
    
    def __init__(self, post_id, user_id, react_id):
        self.post_id = post_id
        self.user_id = user_id
        self.react_id = react_id
