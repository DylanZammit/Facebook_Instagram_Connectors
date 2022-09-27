from instagrapi import Client
from credentials import INSTA_USERNAME, INSTA_PASSWORD
from traceback import format_exc
from insight.storage import InstaStorage as Storage
import os
from logger import mylogger, pb
from insight.entities import Page
from insight.utils import *
# https://github.com/adw0rd/instagrapi/discussions/220


BASE_PATH = os.path.dirname(os.path.abspath(__file__))


# TODO: USE SUGGESTED PROXIES
class InstaScraper(Client):

    def __init__(self, username, password, login=False, posts_per_page=10):
        """
        load: path to saved session settings
        username/password: credentials of instagram account
        """
        super().__init__()
        self.posts_per_page = posts_per_page

        self.logged_in = login
        if login:
            try:
                fn = os.path.join(BASE_PATH, 'user_settings.json')
                self.load_settings(fn)
            except Exception as e:
                logger.warning('user settings not found. Logging in')

            logger.info('logging in...')
            self.login(username, password)


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
        next_page = None
        while n > 0:
            logger.info(f'{n} posts remaining to load')
            n_read = n if n < self.posts_per_page else self.posts_per_page
            res = self.user_medias_paginated(page_id, n_read, next_page)
            medias += res[0]
            next_page = res[1]
            n -= n_read
            rsleep(2)

        logger.info('Loaded {} medias'.format(len(medias)))

        for media in medias:
            logger.info(f'Reading {media.pk}')
            logger.info(f'Reading {media.pk}')
            media_type = media.media_type
            media_ptype = media.product_type
            media_type = get_media_content(media_type, media_ptype.upper())
            media.media_type = media_type

        return medias


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--login', action='store_true')
    parser.add_argument('--page_name', default='timesofmalta', type=str)
    parser.add_argument('--store', action='store_true')
    parser.add_argument('--num_media', help='number of media posts to scrape', type=int, default=50)
    parser.add_argument('--posts_per_page', help='number of posts per page', type=int, default=10)
    args = parser.parse_args()

    page_name = args.page_name
    num_media = args.num_media

    fn_log = 'scrape_insta_{}'.format(page_name)
    notif_name = f'{page_name} INSTA Scrape'
    logger = mylogger(fn_log, notif_name)
    try:
        client = InstaScraper(
            login=args.login, 
            username=INSTA_USERNAME, 
            password=INSTA_PASSWORD,
            posts_per_page=args.posts_per_page
        )

        page = client.scrape_page(page_name)


        medias = []
        if num_media:
            i = 0
            while len(medias) == 0:
                logger.info(f'reading {num_media} medias attempt #{i+1}')
                if i > 0: 
                    rsleep(60)
                    extractor = FacebookScraper(cookies='cookies.txt')

                medias = client.scrape_medias(page.page_id, num_media)
                i += 1

                if i == 3:
                    logger.critical('Failed to read posts after 3 attempts')
                    break
            logger.info(f'{len(medias)} medias loaded')


        client.save_session(os.path.join(BASE_PATH, 'user_settings.json'))
        #medias = client.scrape_page(client.user_id_from_username(page_name), 20)

        if args.store:
            logger.info('storing')
            storage = Storage()
            storage.store(page)
            storage.store(medias)

    except:
        err = format_exc()
        logger.critical(err)
        pb.push_note(notif_name, err)
    else:
        pb.push_note(notif_name, 'Success!')
        logger.info('success')
