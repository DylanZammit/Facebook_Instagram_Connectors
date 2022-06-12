from pyfacebook import GraphAPI
from pprint import pprint
from IPython import embed
import yaml

with open('tokens.yml') as f:
    tokens = yaml.safe_load(f)

app_id = tokens.get('app_id')
secret = tokens.get('secret')
access_token = tokens.get('long_user_token')
maths_token = tokens.get('maths_access_token')

version = 'v14.0'
api = GraphAPI(access_token=maths_token,version=version)
posts = api.get_object('mathstuitionmalta', fields='posts')['posts']['data']
for post in posts:
    post_id = post['id']
    print(f'Post ID = {post_id}')
    data = api.get_object(post_id, fields='shares,reactions,likes.summary(true)')
    likes = api.get_object(post_id+'/reactions', kwargs={'summary': 'total_count'})
    pprint(data)
    print('-'*50)
    pprint(likes)
