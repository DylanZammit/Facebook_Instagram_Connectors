from facebook_scraper import get_profile
import os
from connector import PGConnector as Connector
import pandas as pd
from datetime import timedelta, datetime
from mysql.connector.errors import IntegrityError
from insight.utils import *
from credentials import POSTGRES

class Storage:

    def __init__(self):
        self.conn = Connector(**POSTGRES)
        self.scrape_date = datetime.now().date()
        self.last_profile_call = pd.Timestamp('2000-01-01')

    def store(self, obj):
        '''
        Can pass facebook objects.
        If a list is passed, all items must be of the same object type
        '''
        if isinstance(obj, list):
            if len(obj) == 0: 
                print('Empty list passed')
                return
            rep = obj[0]
        else:
            rep = obj
            obj = [obj]

        otype = type(rep).__name__.lower()
        f = getattr(self, '_store_{}'.format(otype))
        rows = f(obj)
        print(f'{len(rows)} {otype}s inserted')

    def _store_page_fixed(self, pages):
        df = self.conn.execute('SELECT page_id FROM pages')
        rows = []
        for page in pages:
            page_id = page.page_id
            exists = page_id in df.page_id.values
            if not exists:
                rows.append((page_id, 1))

        self.conn.insert('pages', rows)
        return rows

    def _store_page_engagement(self, pages):
        df = self.conn.execute('SELECT page_id, scrape_date FROM page_engagement')

        rows = []
        for page in pages:
            page_id = page.page_id

            exists = len(df[(df.scrape_date==self.scrape_date)&(df.page_id==page_id)])

            if not exists and page.num_likes is not None:
                rows.append((page_id, self.scrape_date, page.num_likes, 
                             getattr(page, 'page_views_total', None),
                             getattr(page, 'page_engaged_users', None),
                             getattr(page, 'page_impressions', None),
                             getattr(page, 'page_impressions_unique', None),
                            ))
            elif page.num_likes is None:
                print(f'{page_id} number of likes is None')
            else:
                print(f'{page_id}@{self.scrape_date} already exists')

        rows = list(set(rows))
        self.conn.insert('page_engagement', rows)
        return rows

    def _store_page(self, pages):
        rows_fixed = self._store_page_fixed(pages)
        rows_eng = self._store_page_engagement(pages)

        for page in pages:
            self.store(page.users)

        for page in pages:
            self.store(page.posts)

        return rows_fixed + rows_eng

    def _store_posts_fixed(self, posts):
        df = self.conn.execute('SELECT post_id FROM post_fixed')
        rows = []
        for post in posts:
            post_id = post.post_id
            exists = post_id in df.post_id.values
            if not exists:
                rows.append((
                        post_id,
                        post.page_id,
                        post.post_time,
                        int(post.has_text),
                        int(post.has_video),
                        int(post.has_image),
                        int(post.was_live)
                    ))
        rows = list(set(rows))
        self.conn.insert('post_fixed', rows)
        return rows

    def _store_posts_engagement(self, posts):
        
        df = self.conn.execute('SELECT post_id, scrape_date FROM post_engagement')
        rows = []
        for post in posts:
            
            exists = len(df[(df.post_id==post.post_id)&(df.scrape_date==self.scrape_date)])
            if not exists:
                rows.append((
                    post.post_id,
                    self.scrape_date,
                    post.num_shares,
                    post.num_comments,
                    post.num_reacts,
                    post.num_like,
                    post.num_haha,
                    post.num_love,
                    post.num_wow,
                    post.num_sad,
                    post.num_angry,
                    getattr(post, 'post_impressions', None),
                    getattr(post, 'post_impressions_unique', None),
                    getattr(post, 'post_impressions_paid', None),
                    getattr(post, 'post_impressions_paid_unique', None),
                    getattr(post, 'post_impressions_fan', None),
                    getattr(post, 'post_impressions_fan_unique', None),
                    getattr(post, 'post_impressions_fan_paid', None),
                    getattr(post, 'post_impressions_fan_paid_unique', None),
                    getattr(post, 'post_impressions_organic', None),
                    getattr(post, 'post_impressions_organic_unique', None),
                ))

            self.store(post.comments)
            self.store(post.shares)
            self.store(post.reacts)
        rows = list(set(rows))
        self.conn.insert('post_engagement', rows)
        return rows

    def _store_post(self, posts):
        rows_fixed = self._store_posts_fixed(posts)
        rows_eng = self._store_posts_engagement(posts)
        return rows_fixed + rows_eng

    def _store_user(self, users):
        df = self.conn.execute('SELECT * FROM users')
        rows = []
        for user in users:

            user_id = user.user_id
            name = user.name.replace("'", "").replace("\"", "")
            surname = user.surname.replace("'", "").replace("\"", "")
            gender = user.gender

            if user_id.isdecimal():
                text_id = None
                exists = user_id in df.user_id.values
                column = 'user_id'
                new_id = user_id
            else:
                text_id = user_id
                user_id = None
                exists = text_id in df.text_id.values
                column = 'text_id'
                new_id = text_id

            if not exists:
                # in same row
                #if name not in df.name.values and surname not in df.surname.values:
                if len(df[(df.name==name)&(df.surname==surname)])==0:
                    rows.append((
                        user_id, 
                        text_id,
                        name, 
                        surname, 
                        gender
                    ))
                else:
                    update_row = f"UPDATE users SET {column}='{new_id}' WHERE name='{name}' AND surname='{surname}';"
                    self.conn.run_query(update_row)

        # convert to dataframe
        # sort by dup key: try to remove dups with non-nans
        X = pd.DataFrame(rows, columns='user_id text_id name surname gender'.split())
        X = X.sort_values('user_id')
        Y = X[(~X.duplicated('user_id'))|(X['user_id'].isnull())]
        Y = Y.sort_values('text_id')
        Z = Y[(~Y.duplicated('text_id'))|(Y['text_id'].isnull())]
        rows_deduped = [tuple(row) for row in Z.to_records(index=False)]
        #rows = list(set(rows)) # old method. can break
        try:
            self.conn.insert('users', rows_deduped)
        except IntegrityError as e:
            print(e)
            pass

        return rows_deduped

    def _store_user_DEDUPED(self, users):
        df = self.conn.execute('SELECT * FROM users')
        rows = []
        for user in users:
            try:
                user_id = user.user_id
                name = user.name.replace("'", "").replace("\"", "")
                surname = user.surname.replace("'", "").replace("\"", "")
                gender = user.gender
                
                # 1) check if numeric or username
                # 2) check if id exists in db
                if user_id.isdecimal():
                    text_id = None
                    exists = int(user_id) in df.user_id.values
                    column = 'user_id'
                    new_id = user_id
                else:
                    text_id = user_id
                    user_id = None
                    exists = text_id in df.text_id.values
                    column = 'text_id'
                    new_id = text_id

                if not exists:
                    # if the name/surname combination does not exists, then it must be a new user
                    name_surname_combinations = df[(df.name==name)&(df.surname==surname)]
                    num_dups = len(name_surname_combinations)
                    if not num_dups:
                        rows.append((
                            user_id,
                            text_id,
                            name, 
                            surname, 
                            gender
                        ))
                    else:
                        print(f'Found {num_dups} instances of {name}, {surname}.')
                        # if we have text_id: get numeric_id
                        if user_id is None: 
                            print(f'Reading profile {text_id}')
                            if self.last_profile_call > datetime.now() - timedelta(seconds=5):
                                rsleep(5, 5, q=False)
                                self.last_profile_call = datetime.now()

                            user_id = get_profile(text_id, allow_extra_requests=False).get('id')
                            # if numeric_id exists, just update that row
                            # otherwise, neither numeric, nor username is in the db, then it must be a new user
                            if int(user_id) in df.user_id.values:
                                update_row = f"""
                                    UPDATE users SET {column}='{new_id}' WHERE user_id='{user_id}';
                                """
                                print(f'Setting text_id={new_id} to {user_id}')
                                self.conn.run_query(update_row)
                            else:
                                rows.append((
                                    user_id, 
                                    text_id,
                                    name, 
                                    surname, 
                                    gender
                                ))
                        else:

                            # for each duplicate name, surname, convert text_id to user_id and update
                            # If new_user_id matches any one of them, set found=true and do nothing
                            # If not found, must be a new user
                            found = False
                            meeseeks = df[(df.name==name)&(df.surname==surname)&(df.user_id.isna())].text_id
                            for dup_text_id in meeseeks:
                                print(f'Reading profile {dup_text_id}')
                                if self.last_profile_call > datetime.now() - timedelta(seconds=5):
                                    rsleep(5, 5, q=5)
                                    self.last_profile_call = datetime.now()
                                dup_user_id = get_profile(dup_text_id, allow_extra_requests=False).get('id')
                                if int(dup_user_id) == int(user_id):
                                    found = True

                                update_row = f"""
                                    UPDATE users SET user_id='{dup_user_id}' WHERE text_id='{dup_text_id}';
                                """
                                print(f'Setting user_id={dup_user_id} to {dup_text_id}')
                                self.conn.run_query(update_row)

                            if not found:
                                rows.append((
                                    user_id, 
                                    text_id,
                                    name, 
                                    surname, 
                                    gender
                                ))
                else:
                    print(f'User {new_id} already exists')
            except TemporarilyBanned as e:
                print(f'Temporarily banned, cannot store {new_id} to DB')
            except Exception as e:
                print(e)
                pass

        # convert to dataframe
        # sort by dup key: try to remove dups with non-nans
        X = pd.DataFrame(rows, columns='user_id text_id name surname gender'.split())
        X = X.sort_values('user_id')
        Y = X[(~X.duplicated('user_id'))|(X['user_id'].isnull())]
        Y = Y.sort_values('text_id')
        Z = Y[(~Y.duplicated('text_id'))|(Y['text_id'].isnull())]
        rows_deduped = [tuple(row) for row in Z.to_records(index=False)]
        #rows = list(set(rows)) # old method. can break
        try:
            self.conn.insert('users', rows_deduped)
        except IntegrityError as e:
            print(e)
            breakpoint()
            pass

        return rows_deduped

    def _store_react(self, reacts):
        df = self.conn.execute('SELECT post_id, user_id FROM post_reacts')
        user_ids = self.conn.execute('SELECT user_id, text_id FROM users')
        rows = []
        for react in reacts:
            if react.user_id.isdecimal():
                other_id = user_ids[user_ids.user_id==react.user_id].text_id
            else:
                other_id = user_ids[user_ids.text_id==react.user_id].user_id
            if len(other_id) == 0:
                cond_id = (df.user_id==react.user_id)
            else:
                cond_id = ((df.user_id==react.user_id)|(df.user_id==other_id.iloc[0]))

            exists = len(df[(df.post_id==react.post_id)&cond_id])
            if not exists:
                rows.append((
                    self.scrape_date,
                    react.post_id,
                    react.user_id,
                    react.react_id
                ))

        rows = list(set(rows))
        self.conn.insert('post_reacts', rows)
        return rows

    def _store_share(self, shares):
        df = self.conn.execute('SELECT post_id, user_id FROM post_shares')
        rows = []
        for share in shares:
            exists = len(df[(df.post_id==share.post_id)&(df.user_id==share.user_id)])
            if not exists:
                rows.append((
                    self.scrape_date,
                    share.post_id,
                    share.user_id,
                ))

        rows = list(set(rows))
        self.conn.insert('post_shares', rows)
        return rows

    def _store_comment(self, comments):
        df = self.conn.execute('SELECT comment_id FROM post_comments')
        rows = []
        for comment in comments:
            exists = comment.comment_id in df.comment_id.values
            if not exists:
                self.store(comment.replies)
                rows.append((
                    comment.comment_id,
                    self.scrape_date,
                    comment.post_id,
                    comment.create_time,
                    comment.user_id,
                    comment.num_reacts,
                    comment.num_replies
                ))

        rows = list(set(rows))
        self.conn.insert('post_comments', rows)
        return rows

    def _store_reply(self, replies):
        df = self.conn.execute('SELECT reply_id FROM comment_replies')
        rows = []
        for reply in replies:
            exists = reply.reply_id in df.reply_id.values
            if not exists:
                rows.append((
                    reply.reply_id,
                    self.scrape_date,
                    reply.parent_id,
                    reply.create_time,
                    reply.user_id,
                    reply.num_reacts,
                ))

        rows = list(set(rows))
        self.conn.insert('comment_replies', rows)
        return rows


class InstaStorage:

    def __init__(self):
        self.conn = Connector(**POSTGRES)
        self.scrape_date = datetime.now().date()
        self.last_profile_call = pd.Timestamp('2000-01-01')

    def store(self, obj):
        '''
        Can pass insta objects.
        If a list is passed, all items must be of the same object type
        '''
        if isinstance(obj, list):
            if len(obj) == 0: 
                print('Empty list passed')
                return
            rep = obj[0]
        else:
            rep = obj
            obj = [obj]

        otype = type(rep).__name__.lower()
        f = getattr(self, '_store_{}'.format(otype))
        rows = f(obj)
        print(f'{len(rows)} {otype}s inserted')

    def _store_page_fixed(self, pages):
        df = self.conn.execute('SELECT page_id FROM insta_pages')
        rows = []
        for page in pages:
            page_id = page.page_id
            exists = page_id in df.page_id.values
            if not exists:
                rows.append((page_id, page.username, 1))

        self.conn.insert('insta_pages', rows)
        return rows

    def _store_page_engagement(self, pages):
        df = self.conn.execute('SELECT page_id, scrape_date FROM insta_page_engagement')

        rows = []
        for page in pages:
            page_id = page.page_id
            exists = len(df[(df.scrape_date==self.scrape_date)&(df.page_id==page_id)])

            if not exists:
                rows.append((page_id, self.scrape_date, page.num_followers, page.num_following, page.num_media))
            else:
                print(f'{page_id}@{self.scrape_date} already exists')

        rows = list(set(rows))
        self.conn.insert('insta_page_engagement', rows)
        return rows

    def _store_page(self, pages):
        for page in pages:
            rows_fixed = self._store_page_fixed(pages)
            rows_eng = self._store_page_engagement(pages)

            if hasattr(page, 'medias'):
                self.store(page.medias)

        return rows_fixed + rows_eng

    def _store_media_fixed(self, medias):
        df = self.conn.execute('SELECT media_id FROM insta_media_fixed')
        rows = []
        for media in medias:
            media_id = int(media.pk)
            page_id = int(media.id.split('_')[1])
            exists = media_id in df.media_id.values
            if not exists:
                rows.append((
                        media_id,
                        media.code,
                        page_id,
                        media.taken_at,
                        media.media_type
                    ))
        rows = list(set(rows))
        self.conn.insert('insta_media_fixed', rows)
        return rows

    def _store_media_engagement(self, medias):
        
        df = self.conn.execute('SELECT media_id, scrape_date FROM insta_media_engagement')
        rows = []
        for media in medias:
            media_id = int(media.pk)
            
            exists = len(df[(df.media_id==media_id)&(df.scrape_date==self.scrape_date)])
            if not exists:
                rows.append((
                    media_id,
                    self.scrape_date,
                    media.comment_count,
                    media.like_count,
                    media.view_count,
                ))

        rows = list(set(rows))
        self.conn.insert('insta_media_engagement', rows)
        return rows

    def _store_media(self, medias):
        rows_fixed = self._store_media_fixed(medias)
        rows_eng = self._store_media_engagement(medias)
        return rows_fixed + rows_eng
