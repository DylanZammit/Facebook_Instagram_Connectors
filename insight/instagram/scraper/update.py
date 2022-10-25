from instagrapi import Client
from instagrapi.exceptions import ClientError
from credentials import INSTA_USERNAME, INSTA_PASSWORD
from traceback import format_exc
from insight.instagram.storage import Storage
import os
from logger import mylogger, pb
from insight.instagram.entities import Page, Media
from insight.utils import *
from credentials import POSTGRES
from fp.fp import FreeProxy
# https://github.com/adw0rd/instagrapi/discussions/220


BASE_PATH = os.path.dirname(os.path.abspath(__file__))


class InstaScraper(Client):

    def __init__(self, username, password, login=False, posts_per_page=10, new_user=False, proxy=True):
        """
        load: path to saved session settings
        username/password: credentials of instagram account
        """
        super().__init__()
        self.posts_per_page = posts_per_page

        self.logged_in = login
        if login:
            if not new_user:
                try:
                    fn = os.path.join(BASE_PATH, 'user_settings.json')
                    self.load_settings(fn)
                    logger.info(f'Loaded user settings {fn}')
                except Exception as e:
                    logger.warning('user settings not found. Logging in')

            logger.info('logging in...')
            self.login(username, password)

            if proxy:
                try:
                    proxy = FreeProxy(anonym=True, timeout=0.3).get()
                    logger.info(f'Using proxy={proxy}')
                    self.set_proxy(proxy)
                except:
                    logger.warning('Error when setting proxy: {}'.format(format_exc()))

    def save_session(self, path):
        if self.logged_in:
            self.dump_settings(path)
            return 1
        else:
            logger.warning('Not logged in, can\'t save session')
            return 0

    def scrape_page(self, page_name):
        page_info = self.user_info_by_username(page_name)
        #page_info = self.user_info_by_username_gql(page_name)
        
        page_id = int(page_info.pk)
        username = page_info.username
        num_followers = page_info.follower_count
        num_following = page_info.following_count
        num_media = page_info.media_count
        self.page_id = page_id

        page = Page(
            username=username, 
            page_id=page_id, 
            num_followers=num_followers, 
            num_following=num_following, 
            num_media=num_media
        )

        return page

    def scrape_medias(self, page_id, n):
        logger.info('loading medias')
        medias = []
        scraped_medias = []
        next_page = None
        while n > 0:
            logger.info(f'{n} posts remaining to load')
            n_read = n if n < self.posts_per_page else self.posts_per_page
            res = self.user_medias_paginated(page_id, n_read, next_page)
            scraped_medias += res[0]
            next_page = res[1]
            n -= n_read
            rsleep(2)

        logger.info('Loaded {} medias'.format(len(scraped_medias)))
        for media in scraped_medias:
            logger.info(f'Reading {media.pk}')

            media_type = media.media_type
            media_ptype = media.product_type
            media_type = get_media_content(media_type, media_ptype.upper())

            pk, page_id = media.id.split('_')

            # I want to use my dataclass
            mymedia = Media(
                pk=int(pk),
                page_id=int(page_id),
                id=media.id,
                media_type=media_type,
                comment_count=media.comment_count,
                like_count=media.like_count,
                caption=media.caption_text,
                taken_at=str(media.taken_at),
                code=media.code,
            )

            medias.append(mymedia)

        return medias


@time_it
@bullet_notify
def main(page_name, num_media, proxy, login, new_user, no_page, store, posts_per_page, logger, title, **kwargs):

    try:
        client = InstaScraper(
            login=login, 
            new_user=new_user,
            username=INSTA_USERNAME, 
            password=INSTA_PASSWORD,
            posts_per_page=posts_per_page,
            proxy=proxy
        )
    except Exception as e:
    #except ClientError as e:
        logger.warning(format_exc())
        logger.warning('Logging in from scratch')
        rsleep(10, q=False)
        client = InstaScraper(
            login=login, 
            new_user=True,
            username=INSTA_USERNAME, 
            password=INSTA_PASSWORD,
            posts_per_page=posts_per_page,
            proxy=proxy
        )

    
    page_info_loaded = True
    if no_page:
        try:
            page = []
            from connector import PGConnector as Connector
            query =f"SELECT page_id FROM insta_pages WHERE username='{page_name}'"
            conn = Connector(**POSTGRES)
            page_id = conn.execute(query).page_id[0]
        except Exception as e:
            # handle proper error
            logger.critical('Did not update insta_page')
            page_info_loaded = False

    if not no_page or not page_info_loaded:
        page = client.scrape_page(page_name)
        page_id = page.page_id


    medias = []
    if num_media:
        i = 0
        while len(medias) == 0:
            logger.info(f'reading {num_media} medias attempt #{i+1}')
            if i > 0: 
                rsleep(30)
                client = InstaScraper(
                    login=login, 
                    new_user=False,
                    username=INSTA_USERNAME, 
                    password=INSTA_PASSWORD,
                    posts_per_page=posts_per_page
                )

            medias = client.scrape_medias(page_id, num_media)
            i += 1

            if len(medias) == 0 and i == 3:
                logger.critical('Failed to read posts after 3 attempts')
                break
        logger.info(f'{len(medias)} medias loaded')

    client.save_session(os.path.join(BASE_PATH, 'user_settings.json'))

    if store:
        logger.info('storing')
        storage = Storage()
        storage.store(page)
        storage.store(medias)

        return storage.history
    return {}

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--login', action='store_true')
    parser.add_argument('--new_user', action='store_true')
    parser.add_argument('--page_name', help='[def=timesofmalta]', default='timesofmalta', type=str)
    parser.add_argument('--store', action='store_true')
    parser.add_argument('--no_page', action='store_true')
    parser.add_argument('--proxy', action='store_true')
    parser.add_argument('--num_media', help='number of media posts to scrape [def=50]', type=int, default=30)
    parser.add_argument('--posts_per_page', help='number of posts per page [def=10]', type=int, default=15)
    args = parser.parse_args()

    page_name = args.page_name

    fn_log = f'{page_name}/scrape_IG'
    notif_name = f'{page_name} INSTA Scrape'
    logger = mylogger(fn_log)

    main(page_name, args.num_media, args.proxy, args.login, args.new_user, args.no_page, args.store, args.posts_per_page, logger=logger, title=notif_name)
