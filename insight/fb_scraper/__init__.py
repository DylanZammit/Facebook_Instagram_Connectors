from insight.entities import Page
from insight.storage import Storage
from selenium.webdriver.common.proxy import Proxy, ProxyType
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from facebook_scraper import *
from IPython import embed
import pandas as pd
import argparse
import os
from logger import mylogger

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MCMC setup args.')
    parser.add_argument('--page_name', help='page name or id [default=timesofmalta]', type=str, default='timesofmalta')
    parser.add_argument('--n_posts', help='Number of posts to read [default=inf]', type=int)
    parser.add_argument('--posts_since', help='datetime to read posts since: "%%Y-%%m-%%d %%H:%%M:%%S', type=str)
    parser.add_argument('--posts_lookback', help='num of hours since now', type=int)
    parser.add_argument('--commenters', help='read and store commentors', action='store_true') 
    parser.add_argument('--sharers', help='read and store sharers', action='store_true') 
    parser.add_argument('--reactors', help='read and store reactors', action='store_true') 
    parser.add_argument('--store', help='store data to postgres', action='store_true') 
    args = parser.parse_args()
    

    page_name = args.page_name
    fn_log = 'scrape_{}'.format(page_name)
    logger = mylogger(fn_log, f'{page_name} Scrape')

    try:

        cookies = os.path.join(BASE_PATH, 'cookies.txt')

        software_names = [SoftwareName.CHROME.value]
        operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
        user_agent_rotator = UserAgent(software_names=software_names, opearting_systems=operating_systems, limit=100)
        user_agent = user_agent_rotator.get_random_user_agent()

        set_cookies(cookies)
        set_user_agent(user_agent)
        
        if args.posts_lookback:
            latest_date = datetime.now() - timedelta(hours=args.posts_lookback)
        elif args.posts_since:
            latest_date = datetime.strptime(args.posts_since, '%Y-%m-%d %H:%M:%S')
        else:
            latest_date = None

        num_posts = args.n_posts

        page = Page(page_name)
        page.get_details()

        page.get_page_posts(
            num_posts=num_posts, 
            latest_date=latest_date, 
            get_commenters=args.commenters,
            get_sharers=args.sharers, 
            get_reactors=args.reactors
        )

        if args.store:
            store = Storage()
            store.store(page)

    except Exception as e:
        logger.critical(e)
