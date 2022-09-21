import numpy as np
import regex as re
import time

id2react = {
    1: 'LIKE',
    2: 'LOVE',
    3: 'HAHA',
    4: 'ANGRY',
    5: 'SAD',
    6: 'WOW'
}

react2id = {v: k for k, v in id2react.items()}
MEDIA_TYPE_MAP = {'CAROUSEL_ALBUM': 8, 'IMAGE': 1, 'VIDEO': 2}
MEDIA_PRODUCT_TYPE_MAP = {'FEED': 1, 'IGTV': 2, 'REELS': 3, 'CLIPS': 3} # scraper and api might return different keys
MEDIA_CONTENT = {'PHOTO': 1, 'VIDEO': 2, 'IGTV': 3, 'REEL': 4, 'ALBUM': 5}


def get_media_content(media_type, media_product):
    """
    	REFER TO https://adw0rd.github.io/instagrapi/usage-guide/media.html
    """
    if media_type == 1: return 1
    if media_type == 8: return 5
    if media_product == 1: return 2
    if media_product == 2: return 3
    if media_product == 3: return 4


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

    
