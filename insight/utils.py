import numpy as np
import regex as re
import time
import functools
from traceback import format_exc
from logger import pb
from transformers import pipeline                  
from googletrans import Translator

username2id = {
    'timesofmalta': 160227208174,
    'maltatoday': 21535456940,
    'levelupmalta': 102912759036590,
    'HamrunSpartansFCOfficial': 100048270135588 
}

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

class Sentiment():

    def __init__(self, do_translate=True):
        self.sent_pl = pipeline(
                model='cardiffnlp/twitter-roberta-base-sentiment-latest',
                max_length=512,
                truncation=True
            )
        self.translate = Translator().translate if do_translate else dummy(1)

    def get_sentiment(self, msg):
        if msg == '':
            return None, None
        msg = self.translate(msg).text
        sentiment = self.sent_pl(msg)[0]
        sent_label = sentiment['label']
        if sent_label == 'Negative':
            sent_label = 0
        elif sent_label == 'Neutral':
            sent_label = 1
        elif sent_label == 'Positive':
            sent_label = 2
        sent_score = sentiment['score']
        return sent_label, sent_score


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


def bullet_notify(f):
    def wrapper(*args, **kwargs):
        logger = kwargs.get('logger', None)
        title = kwargs.get('title', '')
        err_msg = f'Error: {title}'
        success_msg = f'Success: {title}'
        try:
            res = f(*args, **kwargs)
        except:
            err = format_exc()
            if logger:
                logger.critical(err)
                pb.push_note(err_msg, err)
            return -1
        else:
            hist = '\n'.join([f'{k} rows stored = {v}' for k, v in res.items()])
            if logger:
                logger.info(hist)
            pb.push_note(success_msg, hist)
            return res

    return wrapper
    

def time_it(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        logger = kwargs.get('logger', None)
        start = time.perf_counter()
        result = f(*args, **kwargs)
        end = time.perf_counter()
        if logger is not None:
            logger.info(f"Function {f.__name__} ran in {end - start:0.2f} seconds")
        return result
    return wrapper

def dummy(num_nones=2, *args, **kwargs):
    return (None,)*num_nones
