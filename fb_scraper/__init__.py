from facebook_scraper import *
import numpy as np
import time
import re
import os
from connector import MySQLConnector
import pandas as pd
from facebook_scraper.exceptions import TemporarilyBanned

def rsleep(t, cap=5, q=True):
    sleep = t+np.random.randint(0, cap)
    if not q: 
        print(f'Sleep for {sleep} seconds...', end='\r')
    time.sleep(sleep)

id2react = {
    1: 'LIKE',
    2: 'LOVE',
    3: 'HAHA',
    4: 'ANGRY',
    5: 'SAD',
    6: 'WOW'
}

react2id = {v: k for k, v in id2react.items()}

class FacebookStorer:

    def __init__(self, cookies=None, throttle=5):
        if cookies: set_cookies(cookies)
        self.conn = MySQLConnector(os.environ.get('SOCIAL_CONN'))
        self.throttle = throttle

    # TODO: throttle on ban
    # TODO: switch proxy/cookie on ban
    def store_reactors(self, post_id, store_user=True):
        print(f'storing reacts for post_id={post_id}')
        reg = r'^(?:.*)\/(?:pages\/[A-Za-z0-9-]+\/)?(?:profile\.php\?id=)?([A-Za-z0-9.]+)'
        reactors = get_reactors(post_id)

        for reactor in reactors:
            react_type = reactor['type']
            link = reactor['link']
            identifier = re.match(reg, link).group(1)
            user_id = None
            user_info = None

            if identifier == '': 
                print(f'User {link} identifer not recognized')
                continue

            if not identifier.isdecimal():
                print(identifier, react_type)
                stored_reactors_query = f"SELECT COUNT(*) FROM users WHERE text_id='{identifier}'"
                user_exists = self.conn.execute(stored_reactors_query).iloc[0][0]

                if not user_exists and store_user:
                    try:
                        user_info = get_profile(identifier)
                        user_info['text_id'] = identifier
                    except AttributeError:
                        print(f'User {link} not found')
                        continue
                    except TemporarilyBanned as e:
                        print(f'Account temporarily banned: {str(e)}')
                        raise Exception
                    user_id = user_info.get('id')
                else:
                    user_id_query = f"SELECT user_id FROM users WHERE text_id='{identifier}'"
                    user_id = self.conn.execute(user_id_query).user_id[0]
                    print(f'User {identifier}:{user_id} already exists')
            else:
                user_id = int(identifier)

            if not user_id:
                print(f'User {link} has no id')
                continue

            user_id = int(user_id)

            if user_exists:
                print(f'User {identifier} already stored for this post')
            elif store_user:
                self.store_user(user_id, user_info)
                rsleep(self.throttle, q=False)
            self.store_react(user_id, post_id, react_type)


    def store_react(self, user_id: int, post_id: int, react_type: int, force=False):
        react_exists_query = f'SELECT COUNT(*) FROM post_reacts WHERE user_id={user_id} AND post_id={post_id}'
        exists = self.conn.execute(react_exists_query).iloc[0][0]
        if exists and not force: return
        react_type = react_type.upper()

        if isinstance(react_type, int) or react_type.isdecimal():
            react_id = int(react_type)
            if react_id not in id2react:
                print(f'React {react_type} not recognized for post={post_id} and user = {user_id}')
                return
        else:
            if react_type not in react2id:
                print(f'React {react_type} not recognized for post={post_id} and user = {user_id}')
            react_id = react2id[react_type]

        insert_query = 'INSERT INTO post_reacts VALUES (%s, %s, %s)'
        row = (post_id, user_id, react_id)
        print(row)
        self.conn.insert(insert_query, row)


    # TODO: store user string identifier. Allows for easy lookup
    def store_user(self, user_id: int, info=None, force=False):
        user_exists_query = f'SELECT COUNT(*) FROM users WHERE user_id={user_id}'
        exists = self.conn.execute(user_exists_query).iloc[0][0]
        if exists and not force: return

        print(f'storing user id = {user_id}')
        if not info:
            info = get_profile(str(user_id))

        name_surname = info.get('Name')
        name, surname = name_surname.split()[0], ' '.join(name_surname.split()[1:])
        localities = info.get('Places lived', [])
        locality = None
        if len(localities):
            locality = next((loc['text'] for loc in localities if loc['type'] == 'Current City'), None)
            if not locality:
                locality = localities[0]['text']
        num_friends = info.get('Friend_count')

        contact_info = info.get('Contact info \nEdit', '').split('\n')

        mobile, email = None, None
        if 'Mobile \nEdit' in contact_info:
            mobile = contact_info[contact_info.index('Mobile')-1]
        if 'Email \nEdit' in contact_info:
            email = contact_info[contact_info.index('Email')-1]

        basic_info = info.get('Basic info', '').split('\n')

        dob, gender = None, None
        if 'Birthday' in basic_info:
            dob = basic_info[basic_info.index('Birthday')-1]
            try:
                dob = pd.Timestamp(dob).strftime('%Y-%m-%d')
            except:
                print(f'Invalid DOB {dob} for {name_surname}:{user_id}')
                dob = None

        if 'Gender' in basic_info:
            gender = basic_info[basic_info.index('Gender')-1][0]

        text_id = info.get('text_id')

        insert_query = """
            INSERT INTO users VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        row = (user_id, text_id, name, surname, locality, num_friends, mobile, email, dob, gender)
        print(row)
        self.conn.insert(insert_query, row)

    # TODO: finish this method
    def store_posts(self, page_id, num_posts=None, 
                    get_comments=False, get_sharers=False, 
                    get_reactors=False, **kwargs):
        """
        can use api for this
        """

        raise NotImplementedError

        options = {
            'comments': get_comments, 
            'sharers': get_sharers, 
            'reactors': get_reactors,
            'posts_per_page': 20
        }

        posts = get_posts(page_id, options=options, **kwargs)
        if not num_posts: num_posts = np.inf
        for i, post in enumerate(posts):
            if i >= num_posts: break
            post_id = post['post_id']
            content = post['text']

            breakpoint()

if __name__ == '__main__':
    page_name = 'levelupmalta'
    cookies = 'cookies.txt'
    post_id = '104704395524093'
    
    fb = FacebookStorer(cookies)

    fb.store_reactors(post_id)

