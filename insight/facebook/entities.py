#from facebook_scraper import *
import gender_guesser.detector as getgender
from insight.utils import *
from pydantic import validate_arguments
from dataclasses import dataclass, field
from typing import List


@validate_arguments
@dataclass(frozen=True, order=False)
class Comment:
    post_id: int
    comment_id: int
    create_time: str
    reply_level: int
    parent_id: int = None
    message: str = ''
    sent_label: int = None
    sent_score: float = None
    num_likes: int = None
    num_replies: int = None
    

@validate_arguments
@dataclass(frozen=True, order=False)
class Post:
    page_id: int
    post_id: int
    post_time: str
    has_text: int = None
    has_image: int = None
    has_video: int = None
    post_type: int = None
    num_shares: int = None
    num_comments: int = None
    num_reacts: int = None
    num_like: int = None
    num_haha: int = None
    num_love: int = None
    num_wow: int = None
    num_sad: int = None
    num_angry: int = None
    post_impressions: int = None
    post_impressions_unique: int = None
    post_impressions_paid: int = None
    post_impressions_paid_unique: int = None
    post_impressions_fan: int = None
    post_impressions_fan_unique: int = None
    post_impressions_fan_paid: int = None
    post_impressions_fan_paid_unique: int = None
    post_impressions_organic: int = None
    post_impressions_organic_unique: int = None
    was_live: int = None
    caption: str = ''
    sent_label: int = None
    sent_score: float = None
    comments: list = field(default_factory=list) # should be list of comments
    #comments: list[Comment] = field(default_factory=list)


@validate_arguments
@dataclass(frozen=True, order=False)
class Page:
    for_date: str
    is_competitor: int
    username: str
    page_id: int = None # should not be None in future
    name: str = None
    num_followers: int = None
    num_likes: int = None
    page_views_total: int = None
    page_engaged_users: int = None
    page_impressions: int = None
    page_impressions_unique: int = None
    #posts: list[Post] = field(default_factory=list)
    posts: list = field(default_factory=list)

