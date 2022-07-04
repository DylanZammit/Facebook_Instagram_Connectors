from fb_api import MyGraphAPI
import matplotlib.pyplot as plt
import pandas as pd
from connector import MySQLConnector
import os

insert_eng = """
    INSERT INTO post_engagement VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

insert_fixed = """
    INSERT INTO post_fixed VALUES (%s, %s, %s, %s, %s, %s)
"""

def main(page=None):
    conn = MySQLConnector(os.environ.get('SOCIAL_CONN'))
    #api = MyGraphAPI()
    api = MyGraphAPI(page=page)
    posts = api.get_object(page_name, fields='posts')['posts']['data']

    react_types = ['LIKE', 'LOVE', 'WOW', 'HAHA', 'SAD', 'ANGRY', 'NONE']
    react_count = {}
    res = {}

    today = pd.Timestamp.now().date()

    react_type = None
    params_eng = []
    params_fixed = []
    for post in posts:

        page_post_id = post['id']
        print(page_post_id)
        page_id, post_id = page_post_id.split('_')

        post_exists_query = f'SELECT COUNT(*) FROM post_fixed WHERE post_id={post_id}'
        post_exists = conn.execute(post_exists_query).iloc[0][0]
        
        
        react_field = ','.join(['reactions.type({}).limit(0).summary(1).as({})'.format(react, react) for react in react_types])
        fields = 'attachments,reactions.limit(0).summary(1).as(TOT),comments.summary(1),shares,created_time,message,permalink_url,' + react_field
        data = api.get_object(page_post_id, fields=fields)
        
        attachments = data['attachments']['data']
        media = next((att['media'] for att in attachments if 'media' in att), {})
        has_image = 'image' in media
        has_video = 'video' in media

        num_shares = data.get('shares', {'count': 0})['count']
        num_comments = data['comments']['summary']['total_count']
        create_time = data['created_time']
        permalink_url = data.get('permalink_url')

        react_count[post_id] = {'create_time': create_time, 'num_shares': num_shares}
        react_count[post_id].update(
            {react_type: data[react_type]['summary']['total_count'] for react_type in react_types}
        )
        react_count[post_id]['TOT'] = data['TOT']['summary']['total_count']
        react_count[post_id]['message'] = data.get('message', '')
        react_count[post_id]['url'] = permalink_url
        react_count[post_id]['num_comments'] = num_comments
        react_count[post_id]['scrape_date'] = str(today)

        res = react_count[post_id]

        post_scrape_exists_query = f"SELECT COUNT(*) FROM post_engagement WHERE post_id={post_id} AND scrape_date='{res['scrape_date']}'"
        post_scrape_exists = conn.execute(post_scrape_exists_query).iloc[0][0]

        if not post_scrape_exists:
            params_eng.append((
                post_id,
                res['scrape_date'],
                res['num_shares'],
                res['num_comments'],
                res['TOT'],
                res['LIKE'],
                res['HAHA'],
                res['LOVE'],
                res['WOW'],
                res['SAD'],
                res['ANGRY'],
            ))

        if not post_exists:
            params_fixed.append((
                post_id,
                page_id,
                res['message'],
                has_video,
                has_image,
                create_time
            ))


        print(f'Post ID = {page_post_id}')

    if len(params_eng):
        conn.insert(insert_eng, params_eng)
        print(f'{len(params_eng)} rows of engagement data inserted')
    else:
        print('No engagameent data to insert')

    if len(params_fixed):
        conn.insert(insert_fixed, params_fixed)
        print(f'{len(params_fixed)} rows of fixed data inserted')
    else:
        print('No fixed data to insert')

    react_count = pd.DataFrame(react_count).T
    print(react_count)
    ax = react_count.plot()
    ax.set_title(f'{page_name} reacts per post')
    plt.show()

if __name__ == '__main__':
    page_name = 'levelupmalta'
    main(page_name)
