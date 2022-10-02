from insight.entities import Page
from insight.storage import FacebookStorage as Storage
from selenium.webdriver.common.proxy import Proxy, ProxyType
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from facebook_scraper.exceptions import TemporarilyBanned, NotFound
from facebook_scraper import *
from IPython import embed
from traceback import format_exc
import pandas as pd
import numpy as np
import argparse
import os
from logger import mylogger, pb
from insight.entities import *
from insight.utils import *

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

class FacebookScraper:

    def __init__(self, login=False, cookies=None, posts_per_page=50):
        """
        doc here
        """

        if cookies:
            cookies = os.path.join(BASE_PATH, cookies)
            set_cookies(cookies)

        software_names = [SoftwareName.CHROME.value]
        operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
        user_agent_rotator = UserAgent(software_names=software_names, opearting_systems=operating_systems, limit=100)
        user_agent = user_agent_rotator.get_random_user_agent()

        set_user_agent(user_agent)

        self.posts_per_page = posts_per_page


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

    def scrape_posts(self, username, num_posts=None, latest_date=None, get_commenters=False, get_sharers=False, get_reactors=False):
        if latest_date is None: latest_date = pd.Timestamp('2000-01-01')
        if num_posts is None: num_posts = np.inf

        if not hasattr(self, 'page_id'):
            self.page_id = username2id[username]

        ppp = self.posts_per_page
        options = {
            'comments': get_commenters,
            'sharers': get_sharers, 
            'reactors': get_reactors,
            'posts_per_page': ppp,
        }

        posts = get_posts(username, options=options, extra_info=True)

        page_posts = []
        users = []
        for i, post in enumerate(posts):
            post_reacts = []
            post_shares = []
            post_comments = []

            if i > 0 and (i+1)%ppp==0: rsleep(10, 10, q=False)
            post_details = {}

            try:
                post_time = post['time']
                if i >= num_posts: break
                if pd.Timestamp(post_time) < latest_date: break

                post_id = post['post_id']
                logger.info(f'{i+1}) Post={post_id} on {post_time}')
                has_text = True if post['text'] is not None else False
                caption = post.get('text', None)
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
                            logger.info(f'None type react for {name, surname}')
                            react_type = None
                        else:
                            react_str = react_str.upper()
                            react_type = react2id.get(react_str)

                            if react_type is None:
                                react_type = react2id.get('LIKE')
                                logger.info(f'Treating special react {react_str} by {name} {surname} as LIKE')

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

                page_posts.append(
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
                        comments=post_comments,
                        caption=caption
                    )
                )

            except TemporarilyBanned as e:
                logger.critical(f'Temporarily banned')
                break

        self.users = users
        return page_posts

    def scrape_page(self, username):

        page_details = get_page_info(username)
        num_likes = page_details.get('likes', None)
        num_followers = page_details.get('Follower_count', None)
        page_name = page_details.get('Name', None)
        page_id = page_details.get('id', None)
        if page_id is None:
            page_id = username2id[username]
        self.page_id = page_id
        if num_likes is None:
            logger.warning(f'Could not get page likes for {username}')
        if num_followers is None:
            logger.warning(f'Could not get page followers for {username}')

        page = Page(
            num_likes=num_likes,
            is_competitor=1,
            num_followers=num_followers,
            page_id=page_id,
            username=username,
            name=page_name
        )

        return page


@time_it
@bullet_notify
def main(page_name, num_posts, posts_since, posts_lookback, store, logger, title):
    if posts_lookback:
        latest_date = datetime.now() - timedelta(hours=posts_lookback)
    elif posts_since:
        latest_date = datetime.strptime(posts_since, '%Y-%m-%d %H:%M:%S')
    else:
        latest_date = None

    extractor = FacebookScraper(cookies='cookies.txt')

    page = extractor.scrape_page(page_name)

    posts = []
    if num_posts:
        i = 0
        while len(posts) == 0:
            logger.info(f'reading {num_posts} posts attempt #{i+1}')
            if i > 0: 
                rsleep(60, q=False)
                extractor = FacebookScraper(cookies='cookies.txt')

            posts = extractor.scrape_posts(
                username=page_name,
                num_posts=num_posts,
                latest_date=latest_date
            )
            i += 1

            if len(posts) == 0 and i == 5:
                logger.critical('Failed to read posts after 5 attempts')
                break
        logger.info(f'{len(posts)} posts loaded')
        
    if store:
        store = Storage()
        store.store(page)
        store.store(posts)
        return store.history
    return {}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--page_name', help='page name or id [default=timesofmalta]', type=str, default='timesofmalta')
    parser.add_argument('--n_posts', help='Number of posts to read [default=0]', type=int)
    parser.add_argument('--posts_since', help='datetime to read posts since: "%%Y-%%m-%%d %%H:%%M:%%S', type=str)
    parser.add_argument('--posts_lookback', help='num of hours since now', type=int)
    parser.add_argument('--store', help='store data to postgres', action='store_true') 
    args = parser.parse_args()
    
    page_name = args.page_name
    notif_name = f'{page_name} FB Scrape'
    fn_log = 'scrape_FB_{}'.format(page_name)
    logger = mylogger(fn_log)

    main(page_name, args.n_posts, args.posts_since, args.posts_lookback, args.store, logger=logger, title=notif_name)
