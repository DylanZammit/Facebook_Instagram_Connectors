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

reg_identifier = r'^(?:.*)\/(?:pages\/[A-Za-z0-9-]+\/)?(?:profile\.php\?id=)?([A-Za-z0-9.]+)'


def rsleep(t, cap=10, q=True):
    sleep = t+np.random.randint(0, cap)
    if not q: 
        print(f'Sleep for {sleep} seconds...', end='')
    time.sleep(sleep)
    if not q: print('done')

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

    def __init__(self, cookies=None, throttle=10):
        if cookies: set_cookies(cookies)
        self.conn = MySQLConnector(os.environ.get('SOCIAL_CONN'))
        self.throttle = throttle

        software_names = [SoftwareName.CHROME.value]
        operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
        user_agent_rotator = UserAgent(software_names=software_names, opearting_systems=operating_systems, limit=100)
        user_agent = user_agent_rotator.get_random_user_agent()

        set_user_agent(user_agent)


    # TODO: throttle on ban
    # TODO: switch proxy/cookie on ban
    def store_reactors(self, post_id):
        print(f'storing reacts for post_id={post_id}')
        try:
            reactors = get_reactors(post_id)
        except NotFound as e:
            print(f'Content nor found: {e}')
            return

        for reactor in reactors:
            react_type = reactor['type']
            link = reactor['link']
            identifier = re.match(reg_identifier, link).group(1)
            user_id = None
            user_info = None

            if identifier == '': 
                print(f'User {link} identifer not recognized')
                continue

            user_exists = False
            if not identifier.isdecimal():
                print(identifier, react_type)
                stored_reactors_query = f"SELECT COUNT(*) FROM users WHERE text_id='{identifier}'"
                user_exists = self.conn.execute(stored_reactors_query).iloc[0][0]

                if not user_exists:
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
                    user_id = self.conn.execute(user_id_query)
                    user_id = user_id.user_id[0]
                    print(f'User {identifier}:{user_id} already exists')
            else:
                user_id = str(identifier)

            if not user_id:
                print(f'User {link} has no id')
                continue

            if user_exists:
                print(f'User {identifier} already stored for this post')
            else:
                self.store_full_user(user_id, user_info)
                rsleep(self.throttle, q=False)
            self.store_react(user_id, post_id, react_type)

    def store_comment(self, comment_id, user_id, text):
        raise NotImplementedError

    def store_post_fixed(self):
        raise NotImplementedError

    def store_post_engagements(self):
        raise NotImplementedError

    def store_react(self, reacts: list = None, user_id: str = None, post_id: int = None, react_type: int = None, force: bool = False):
        scrape_date = datetime.now().date()
        if reacts is None:
            reacts = [(user_id, post_id, react_type)]

        rows = []
        for user_id, post_id, react_type in reacts:
            react_exists_query = f"""
                SELECT COUNT(*) 
                FROM post_reacts 
                WHERE 
                    user_id='{user_id}' AND
                    post_id={post_id} AND
                    scrape_date='{scrape_date.strftime('%Y-%m-%d')}'
            """
            exists = self.conn.execute(react_exists_query).iloc[0][0]
            if exists and not force: return
            #react_type = react_type.upper()

            if isinstance(react_type, int) or react_type.isdecimal():
                react_id = int(react_type)
                if react_id not in id2react:
                    print(f'React {react_type} not recognized for post={post_id} and user = {user_id}')
                    return
            else:
                if react_type not in react2id:
                    print(f'React {react_type} not recognized for post={post_id} and user = {user_id}')
                react_id = react2id[react_type]

            insert_query = 'INSERT INTO post_reacts VALUES (%s, %s, %s, %s)'
            row = (scrape_date, post_id, user_id, react_id)
            rows.append(row)
        print(rows)
        self.conn.insert(insert_query, rows)
        return rows

    def store_simple_user(self, users = None, user_id = None, name = None, surname = None, force=False):

        if users is None:
            users = [(user_id, name, surname)]

        rows = []
        for user_id, name, surname in users:
            user_exists_query = f"SELECT COUNT(*) FROM users_simple WHERE user_id='{user_id}'"
            exists = self.conn.execute(user_exists_query).iloc[0][0]
            if exists and not force: return
            print(f'storing user id = {user_id}')
            d = getgender.Detector(case_sensitive=False)
            gender = d.get_gender(name)
            if 'female' in gender:
                gender = 'F'
            elif 'male' in gender:
                gender = 'M'
            else:
                gender = None
            rows.append((user_id, name, surname, gender))

        insert_query = """
            INSERT INTO users_simple VALUES
            (%s, %s, %s, %s)
        """
        print(rows)
        self.conn.insert(insert_query, rows)
        return rows


    # TODO: store user string identifier. Allows for easy lookup
    def store_full_user(self, user_id: int, info=None, force=False):
        user_exists_query = f'SELECT COUNT(*) FROM users WHERE user_id={user_id}'
        exists = self.conn.execute(user_exists_query).iloc[0][0]
        if exists and not force: return

        print(f'storing user id = {user_id}')
        if not info:
            try:
                info = get_profile(str(user_id))
            except AttributeError as e:
                print(f'Attribute error: {e}')
                return

        name_surname = info.get('Name')
        name, surname = name_surname.split()[0], ' '.join(name_surname.split()[1:])
        localities = info.get('Places lived', [])
        locality = None
        if len(localities):
            locality = next((loc['text'] for loc in localities if loc['type'] == 'Current City'), None)
            if not locality:
                locality = localities[0]['text']
        num_friends = info.get('Friend_count')

        contact_info = info.get('Contact info', '').split('\n')

        mobile, email = None, None
        if 'Mobile' in contact_info:
            mobile = contact_info[contact_info.index('Mobile')-1]
        if 'Email' in contact_info:
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
    def store_posts(self, page_id, num_posts=None,  **kwargs):
        """
        can use api for this
        """
        scrape_date = datetime.now().date().strftime('%Y-%m-%d')

        options = {
            'comments': True, 
            'sharers': True, 
            'reactors': True,
            'progress': True,
            'posts_per_page': 20
        }

        posts = get_posts(page_id, options=options, **kwargs)
        if not num_posts: num_posts = np.inf
        params_eng = []
        post_reacts = []
        new_users = []
        for i, post in enumerate(posts):
            if i >= num_posts: break
            post_id = post['post_id']
            print(post_id)
            post_time = post['time']
            content = post['text']
            num_comments = post['comments']
            num_shares = post['shares']
            has_image = post['image'] is not None
            has_video = post['video'] is not None
            video_watches = post['video_watches']
            was_live = post['was_live']

            for sharer in post['sharers']:
                name_surname = sharer['name']
                name, surname = name_surname.split()[0], ' '.join(name_surname.split()[1:])
                link = sharer['link']
                sharer_id = re.match(reg_identifier, link).group(1)

            reactions = post['reactions']
            if reactions is None: reactions = {}
            num_like = reactions.get('like', 0)
            num_love = reactions.get('love', 0)
            num_haha = reactions.get('haha', 0)
            num_wow = reactions.get('wow', 0)
            num_angry = reactions.get('angry', 0)
            num_sad = reactions.get('sad', 0)
            num_reacts = post.get('reaction_count', 0)

            params_eng.append((
                post_id,
                scrape_date,
                num_shares,
                num_comments,
                num_reacts,
                num_like,
                num_haha,
                num_love,
                num_wow,
                num_sad,
                num_angry,
            ))

            reactors = post['reactors']
            if reactors == None: reactors = []
            for reactor in reactors:
                name_surname = reactor['name']
                name, surname = name_surname.split()[0], ' '.join(name_surname.split()[1:])
                react_str = reactor['type'].upper()
                react_type = react2id.get(react_str)
                if react_type is None:
                    print(f'Special react {react_str} for {name_surname}')
                    continue
                link = reactor['link']
                reactor_id = re.match(reg_identifier, link).group(1)
                post_reacts.append((reactor_id, post_id, react_type))
                new_users.append((reactor_id, name, surname))


            for comment in post['comments_full']:
                comment_id = comment['comment_id']
                name_surname = comment.get('commenter_name')
                name, surname = name_surname.split()[0], ' '.join(name_surname.split()[1:])
                link = comment['commenter_url']
                comment_time = comment['comment_time']
                commentor_id = re.match(reg_identifier, link).group(1)
                comment_reactions = comment.get('comment_reaction_count', 0)
                text = comment['comment_text']
                #ALSO STORE REPLIES
                new_users.append((commentor_id, name, surname))

            rsleep(10, 10, q=False)

        insert_eng = """
            INSERT INTO post_engagement VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        print('storing data...')
        if len(new_users):
            rows = self.store_simple_user(new_users)
            print(f'{len(rows)} rows of new users inserted')
        else:
            print('No new users to insert')

        if len(post_reacts):
            rows = self.store_react(post_reacts)
            print(f'{len(params_eng)} rows of engagement data inserted')
        else:
            print('No post reacts to insert')

        # TODO: check for duplicates first!!
        if len(params_eng):
            self.conn.insert(insert_eng, params_eng)
            print(f'{len(params_eng)} rows of engagement data inserted')
        else:
            print('No engagameent data to insert')


if __name__ == '__main__':

    cookies = 'cookies.txt'
    page_name = 'levelupmalta'
    page_name = 'timesofmalta'
    fb = FacebookStorer(cookies)

    print('storing posts')
    fb.store_posts(page_name, 10)
    breakpoint()

    from fb_api import MyGraphAPI
    api = MyGraphAPI() 
    posts = api.get_object(f'{page_name}/posts').get('data', [])
    post_ids = [post['id'].split('_')[1] for post in posts]
    post_id = '104704395524093'
    
    
    for post_id in post_ids:
        fb.store_reactors(post_id)
        rsleep(60, cap=10, q=False)

