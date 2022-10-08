from insight.facebook.storage import Storage
from selenium.webdriver.common.proxy import Proxy, ProxyType
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from facebook_scraper.exceptions import TemporarilyBanned, NotFound
from facebook_scraper import *
from traceback import format_exc
import pandas as pd
import numpy as np
import argparse
import os
from logger import mylogger, pb
from insight.facebook.entities import *
from insight.utils import *

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

class FacebookScraper:

    def __init__(self, login=False, cookies=None, posts_per_page=50, do_sentiment=True, do_translate=True):
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
        if do_sentiment:
            self.sent = Sentiment(do_translate)
        else:
            self.sent = object()
            self.sent.get_sentiment = dummy

    def scrape_posts(self, username, num_posts=None, latest_date=None):
        if latest_date is None: latest_date = pd.Timestamp('2000-01-01')
        if num_posts is None: num_posts = np.inf

        if not hasattr(self, 'page_id'):
            self.page_id = username2id[username]

        ppp = self.posts_per_page
        options = {
            'posts_per_page': ppp,
        }

        posts = get_posts(username, options=options, extra_info=True)

        page_posts = []
        users = []
        for i, post in enumerate(posts):

            if i > 0 and (i+1)%ppp==0: rsleep(10, 10, q=False)
            post_details = {}

            try:
                post_time = str(post['time'])
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

                if has_image:
                    post_type = 2
                elif has_video:
                    post_type = 3
                elif has_text:
                    post_type = 1
                else:
                    post_type = None

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

                sent_label, sent_score = self.sent.get_sentiment(caption)

                page_posts.append(
                    Post(
                        post_id=post_id,
                        page_id=self.page_id,
                        has_text=has_text,
                        has_video=has_video,
                        has_image=has_image,
                        post_time=post_time,
                        was_live=was_live,
                        post_type=post_type,
                        num_shares=num_shares,
                        num_comments=num_comments,
                        num_reacts=num_reacts,
                        num_like=num_like,
                        num_haha=num_haha,
                        num_love=num_love,
                        num_wow=num_wow,
                        num_sad=num_sad,
                        num_angry=num_angry,
                        caption=caption,
                        sent_label=sent_label,
                        sent_score=sent_score
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
        self.page_id = int(page_id)
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
                rsleep(30, q=False)
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
