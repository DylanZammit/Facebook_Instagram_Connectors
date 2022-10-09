#from facebook_scraper import *
import gender_guesser.detector as getgender
from insight.utils import *
from pydantic import validate_arguments
from dataclasses import dataclass, field
from typing import List


@validate_arguments
@dataclass(frozen=True, order=False)
class Comment:
    media_id: int
    comment_id: int
    create_time: str
    reply_level: int
    parent_id: int = None
    message: str = ''
    username: str = None
    sent_label: int = None
    sent_score: float = None
    num_likes: int = None
    num_replies: int = None
    

@validate_arguments
@dataclass(frozen=True, order=False)
class Media:
    pk: int
    id: str
    page_id: int
    code: str
    taken_at: str = None
    media_type: int = None
    caption: str = ''
    comment_count: int = None
    like_count: int = None
    view_count: int = None
    impressions: int = None
    reach: int = None
    engagement: int = None
    saved: int = None
    comments: list = field(default_factory=list)
    #comments: List[Comment] = field(default_factor=list)


@validate_arguments
@dataclass(frozen=True, order=False)
class Page:
    page_id: int
    is_competitor: int = None
    username: str = None
    name: str = 'DEFAULT PAGE NAME'
    num_followers: int = None
    num_following: int = None
    num_media: int = None
    new_followers: int = None
    impressions: int = None
    reach: int = None
    profile_views: int = None
    medias: list = field(default_factory=list)
    #medias: List[Media] = field(default_factory=list)

