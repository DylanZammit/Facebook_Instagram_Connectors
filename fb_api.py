from pyfacebook import GraphAPI
import pandas as pd
import yaml

with open('tokens.yml') as f:
    tokens = yaml.safe_load(f)

maths_token = tokens.get('maths_access_token')
page_name = 'mathstuitionmalta'

version = 'v14.0'
api = GraphAPI(access_token=maths_token,version=version)
posts = api.get_object(page_name, fields='posts')['posts']['data']

react_types = ['LIKE', 'LOVE', 'WOW', 'HAHA', 'SAD', 'ANGRY']
react_count = {}

react_type = None
for post in posts:
    page_post_id = post['id']
    post_id = page_post_id.split('_')[1]
    data = api.get_object(page_post_id, fields='shares,created_time,message,permalink_url')
    num_shares = data.get('shares', {'count': 0})['count']
    create_time = data['created_time']
    permalink_url = data.get('permalink_url')

    react_count[post_id] = {'create_time': create_time, 'shares': num_shares}
    react_count[post_id].update(
        {react_type: api.get(page_post_id+'/reactions', 
                             {'summary': 'total_count', 'type':react_type}
                            )['summary']['total_count'] for react_type in react_types})
    react_count[post_id]['message'] = data.get('message', '')
    react_count[post_id]['url'] = permalink_url

    print(f'Post ID = {page_post_id}')
    print(f'URL = {permalink_url}')
react_count = pd.DataFrame(react_count).T
print(react_count)
if 0: react_count.to_csv(f'{page_name}.csv')
