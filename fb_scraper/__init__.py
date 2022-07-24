from facebook_scraper import *
import gender_guesser.detector as getgender
import numpy as np
import time
import re
import os
from connector import MySQLConnector
import pandas as pd
from facebook_scraper.exceptions import TemporarilyBanned, NotFound
from selenium.webdriver.common.proxy import Proxy, ProxyType
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from IPython import embed
from fb_api import MyGraphAPI

d = getgender.Detector(case_sensitive=False)
reg_identifier = r'^(?:.*)\/(?:pages\/[A-Za-z0-9-]+\/)?(?:profile\.php\?id=)?([A-Za-z0-9.]+)'

id2react = {
    1: 'LIKE',
    2: 'LOVE',
    3: 'HAHA',
    4: 'ANGRY',
    5: 'SAD',
    6: 'WOW'
}

react2id = {v: k for k, v in id2react.items()}


def extract_id(link):
    reg_identifier = r'^(?:.*)\/(?:pages\/[A-Za-z0-9-]+\/)?(?:profile\.php\?id=)?([A-Za-z0-9.]+)'
    user_id = re.match(reg_identifier, link).group(1)
    return user_id


def extract_namesurname(name_surname):
    name, surname = name_surname.split()[0], ' '.join(name_surname.split()[1:])
    return name, surname


def rsleep(t, cap=10, q=True):
    sleep = t+np.random.randint(0, cap)
    if not q: 
        print(f'Sleep for {sleep} seconds...', end='', flush=True)
    time.sleep(sleep)
    if not q: print('done', flush=True, end='\r')

    
class Storage:

    def __init__(self):
        self.conn = MySQLConnector(os.environ.get('SOCIAL_CONN'))
        self.scrape_date = datetime.now().date()

    def store(self, obj):
        '''
        Can pass facebook objects.
        If a list is passed, all items must be of the same object type
        '''
        if isinstance(obj, list):
            if len(obj) == 0: 
                print('Empty list passed')
                return
            rep = obj[0]
        else:
            rep = obj
            obj = [obj]

        otype = type(rep).__name__.lower()
        f = getattr(self, '_store_{}'.format(otype))
        rows = f(obj)
        print(f'{len(rows)} {otype}s inserted')

    def _store_page_fixed(self, pages):
        df = self.conn.execute('SELECT page_id FROM pages')
        rows = []
        for page in pages:
            page_id = page.page_id
            exists = page_id in df.page_id.values
            if not exists:
                rows.append((page_id, 1))

        self.conn.insert('pages', rows)
        return rows

    def _store_page_engagement(self, pages):
        df = self.conn.execute('SELECT page_id, scrape_date FROM page_engagement')

        rows = []
        for page in pages:
            page_id = page.page_id
            exists = len(df[(df.scrape_date==self.scrape_date)&(df.page_id==page_id)])

            if not exists and page.num_likes is not None:
                rows.append((page_id, self.scrape_date, page.num_likes))
            elif page.num_likes is None:
                print(f'{page_id} number of likes is None')
            else:
                print(f'{page_id}@{self.scrape_date} already exists')
        rows = list(set(rows))
        self.conn.insert('page_engagement', rows)
        return rows

    def _store_page(self, pages):
        rows_fixed = self._store_page_fixed(pages)
        rows_eng = self._store_page_engagement(pages)

        for page in pages:
            self.store(page.users)

        for page in pages:
            self.store(page.posts)

        return rows_fixed + rows_eng

    def _store_posts_fixed(self, posts):
        df = self.conn.execute('SELECT post_id FROM post_fixed')
        rows = []
        for post in posts:
            post_id = post.post_id
            exists = post_id in df.post_id.values
            if not exists:
                rows.append((
                        post_id,
                        post.page_id,
                        post.post_time,
                        post.has_text,
                        post.has_video,
                        post.has_image,
                        post.was_live
                    ))
        rows = list(set(rows))
        self.conn.insert('post_fixed', rows)
        return rows

    def _store_posts_engagement(self, posts):
        
        df = self.conn.execute('SELECT post_id, scrape_date FROM post_engagement')
        rows = []
        for post in posts:
            
            exists = len(df[(df.post_id==post.post_id)&(df.scrape_date==self.scrape_date)])
            if not exists:
                rows.append((
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
                ))

            self.store(post.comments)
            self.store(post.shares)
            self.store(post.reacts)
        rows = list(set(rows))
        self.conn.insert('post_engagement', rows)
        return rows

    def _store_post(self, posts):
        rows_fixed = self._store_posts_fixed(posts)
        rows_eng = self._store_posts_engagement(posts)
        return rows_fixed + rows_eng

    def _store_user(self, users):
        df = self.conn.execute('SELECT * FROM users')
        rows = []
        for user in users:
            user_id = user.user_id
            name = user.name
            surname = user.surname
            gender = user.gender
            
            if user_id.isdecimal():
                text_id = None
                exists = user_id in df.user_id.values
                column = 'user_id'
                new_id = user_id
            else:
                text_id = user_id
                user_id = None
                exists = text_id in df.text_id.values
                column = 'text_id'
                new_id = text_id

            if not exists:
                if name not in df.name.values and surname not in df.surname.values:
                    rows.append((
                        user_id, 
                        text_id,
                        name, 
                        surname, 
                        gender
                    ))
                else:
                    update_row = f"UPDATE users SET {column}='{new_id}' WHERE name='{name}' AND surname='{surname}';"
                    self.conn.run_query(update_row)

        rows = list(set(rows))
        self.conn.insert('users', rows)
        return rows

    def _store_react(self, reacts):
        df = self.conn.execute('SELECT post_id, user_id FROM post_reacts')
        user_ids = self.conn.execute('SELECT user_id, text_id FROM users')
        rows = []
        for react in reacts:
            if react.user_id.isdecimal():
                other_id = user_ids[user_ids.user_id==react.user_id].text_id
            else:
                other_id = user_ids[user_ids.text_id==react.user_id].user_id
            if len(other_id) == 0:
                cond_id = (df.user_id==react.user_id)
            else:
                cond_id = ((df.user_id==react.user_id)|(df.user_id==other_id.iloc[0]))

            exists = len(df[(df.post_id==react.post_id)&cond_id])
            if not exists:
                rows.append((
                    self.scrape_date,
                    react.post_id,
                    react.user_id,
                    react.react_id
                ))

        rows = list(set(rows))
        self.conn.insert('post_reacts', rows)
        return rows

    def _store_share(self, shares):
        df = self.conn.execute('SELECT post_id, user_id FROM post_shares')
        rows = []
        for share in shares:
            exists = len(df[(df.post_id==share.post_id)&(df.user_id==share.user_id)])
            if not exists:
                rows.append((
                    self.scrape_date,
                    share.post_id,
                    share.user_id,
                ))

        rows = list(set(rows))
        self.conn.insert('post_shares', rows)
        return rows

    def _store_comment(self, comments):
        df = self.conn.execute('SELECT comment_id FROM post_comments')
        rows = []
        for comment in comments:
            exists = comment.comment_id in df.comment_id.values
            if not exists:
                rows.append((
                    comment.comment_id,
                    self.scrape_date,
                    comment.post_id,
                    comment.create_time,
                    comment.user_id,
                    comment.num_reacts,
                    comment.num_replies
                ))

        rows = list(set(rows))
        self.conn.insert('post_comments', rows)
        return rows


class Page:

    def __init__(self, page_id, **kwargs):
        self.posts = []
        self.users = []
        self.page_id = page_id
        self.num_likes = None
        for k, v in kwargs.items():
            setattr(self, k, v)

    def get_page_posts(self, num_posts=np.inf, start_date=None, get_commenters=True, get_sharers=True, get_reactors=True):
        if start_date is None: start_date = pd.Timestamp('2000-01-01')

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
                if pd.Timestamp(post_time) < start_date: break

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
                            print(f'None type react for {name_surname}')
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
                        name, surname = extract_namesurname(comment.get('commenter_name'))
                        user_id = extract_id(comment['commenter_url'])
                        num_reacts = comment.get('comment_reaction_count', 0)
                        if num_reacts is None: num_reacts = 0
                        num_replies = len(comment.get('replies', []))
                        #ALSO STORE REPLIES
                        user = User(
                            user_id=user_id, 
                            name=name, 
                            surname=surname
                        )
                        
                        post_comment = Comment(
                            comment_id=comment_id,
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
    
    def __init__(self, user_id, **kwargs):
        self.user_id = user_id
        for k, v in kwargs.items():
            setattr(self, k, v)

        if hasattr(self, 'name'):
            gender = d.get_gender(self.name)
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


def main():
    cookies = 'cookies.txt'
    page_name = 'timesofmalta'

    set_cookies(cookies)

    software_names = [SoftwareName.CHROME.value]
    operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
    user_agent_rotator = UserAgent(software_names=software_names, opearting_systems=operating_systems, limit=100)
    user_agent = user_agent_rotator.get_random_user_agent()

    set_user_agent(user_agent)

    start_date = datetime.now().date()
    start_date = pd.Timestamp('2022-07-24 20:00:00')
    page = Page(page_name)
    page.get_details()
    page.get_page_posts(start_date=start_date)

    store = Storage()
    store.store(page)
    embed()

if __name__ == '__main__':
    main()
