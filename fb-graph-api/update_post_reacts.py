from fb_api import MyGraphAPI
import matplotlib.pyplot as plt
import pandas as pd
from connector import MySQLConnector

query = """
    INSERT INTO post VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

def main(page=None):
    #api = MyGraphAPI()
    api = MyGraphAPI(page=page)
    posts = api.get_object(page_name, fields='posts')['posts']['data']

    react_types = ['LIKE', 'LOVE', 'WOW', 'HAHA', 'SAD', 'ANGRY', 'NONE']
    react_count = {}
    res = {}

    today = pd.Timestamp.now().date()

    react_type = None
    params = []
    for post in posts:
        
        page_post_id = post['id']
        print(page_post_id)
        page_id, post_id = page_post_id.split('_')
        react_field = ','.join(['reactions.type({}).limit(0).summary(1).as({})'.format(react, react) for react in react_types])
        fields = 'reactions.limit(0).summary(1).as(TOT),comments.summary(1),shares,created_time,message,permalink_url,' + react_field
        data = api.get_object(page_post_id, fields=fields)

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
        params.append((
            post_id,
            res['scrape_date'],
            res['LIKE'],
            res['HAHA'],
            res['LOVE'],
            res['ANGRY'],
            res['SAD'],
            res['TOT'],
            res['num_shares'],
            res['num_comments'],
            res['message'],
            page_id,
            res['WOW'],
            # add create time
        ))

        print(f'Post ID = {page_post_id}')

    
    conn = MySQLConnector()
    conn.insert(query, params)
    print('data inserted')

    react_count = pd.DataFrame(react_count).T
    print(react_count)
    ax = react_count.plot()
    ax.set_title(f'{page_name} reacts per post')
    plt.show()

    if 1: react_count.to_csv(f'{page_name}.csv')

if __name__ == '__main__':
    page_name = 'levelupmalta'
    #page_name = 'mathstuitionmalta'
    main(page_name)
