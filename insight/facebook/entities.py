#from facebook_scraper import *
import gender_guesser.detector as getgender
from insight.utils import *


class Page:

    def __init__(self, **kwargs):
        self.posts = []
        self.users = []
        for k, v in kwargs.items():
            setattr(self, k, v)


class Media:

    def __init__(self, **kwargs):
        self.shares = []
        self.comments = []
        self.reacts = []
        for k, v in kwargs.items():
            setattr(self, k, v)


class Post:

    def __init__(self, post_id, **kwargs):
        self.shares = []
        self.comments = []
        self.reacts = []
        self.post_id = int(post_id)

        for k, v in kwargs.items():
            setattr(self, k, v)

    def get_info(self, comments=True, shares=True, reacts=True):
        raise NotImplementedError


class User:

    d = getgender.Detector(case_sensitive=False)
    
    def __init__(self, user_id, **kwargs):
        self.user_id = user_id
        for k, v in kwargs.items():
            setattr(self, k, v)

        if hasattr(self, 'name'):
            gender = User.d.get_gender(self.name)
            if 'female' in gender:
                self.gender = 'F'
            elif 'male' in gender:
                self.gender = 'M'
            else:
                self.gender = None
    
    def get_info(self):
        raise NotImplementedError


class Comment:
    
    def __init__(self, comment_id, **kwargs):
        self.comment_id = int(comment_id)
        for k, v in kwargs.items():
            setattr(self, k, v)

class Reply:
    
    def __init__(self, reply_id, parent_id, **kwargs):
        self.reply_id = int(reply_id)
        self.parent_id = int(parent_id)
        for k, v in kwargs.items():
            setattr(self, k, v)


class Share:
    
    def __init__(self, user_id, post_id, **kwargs):
        self.post_id = post_id
        self.user_id = user_id
        for k, v in kwargs.items():
            setattr(self, k, v)


class React:
    
    def __init__(self, post_id, user_id, react_id):
        self.post_id = post_id
        self.user_id = user_id
        self.react_id = react_id
