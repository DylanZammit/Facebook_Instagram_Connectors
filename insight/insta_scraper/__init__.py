from instagrapi import Client
from credentials import INSTA_USERNAME, INSTA_PASSWORD
from insight.storage import InstaStorage as Storage
import os
from logger import mylogger
# save session
# https://github.com/adw0rd/instagrapi/discussions/220


BASE_PATH = os.path.dirname(os.path.abspath(__file__))


class Page:

    def __init__(self, username, **kwargs):
        self.username = username
        for k, v in kwargs.items():
            setattr(self, k, v)


# TODO: USE SUGGESTED PROXIES
class InstaScraper(Client):

    def __init__(self, username, password, load=None):
        """
        load: path to saved session settings
        username/password: credentials of instagram account
        """
        super().__init__()

        if load:
            fn = os.path.join(BASE_PATH, 'user_settings.json')
            self.load_settings(fn)

        print('logging in...', flush=True, end='')
        self.login(username, password)
        print('done')


    def save_session(self, path):
        self.dump_settings(path)

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
        medias = self.user_medias(page_id, n)
        #medias = self.user_medias_gql(page_id, n)
        return medias


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--noload', action='store_true')
    parser.add_argument('--page_name', default='timesofmalta', type=str)
    parser.add_argument('--store', action='store_true')
    args = parser.parse_args()

    page_name = args.page_name

    fn_log = 'scrape_insta_{}'.format(page_name)
    logger = mylogger(fn_log, f'{page_name} INSTA Scrape')
    try:
        client = InstaScraper(
            load=not args.noload, 
            username=INSTA_USERNAME, 
            password=INSTA_PASSWORD
        )

        page = client.scrape_page(page_name)
        medias = client.scrape_medias(page.page_id, 20)
        client.save_session('user_settings.json')
        #medias = client.scrape_page(client.user_id_from_username(page_name), 20)

        if args.store:
            storage = Storage()
            storage.store(page)
            storage.store(medias)
    except Exception as e:
        logger.critical(e)
