import os
import yaml

sql_conn = os.environ.get('SOCIAL_CONN')
fb = os.environ.get('FB_ABI_CRED')
insta = os.environ.get('INSTA_CREDENTIALS')

if insta:
    with open(insta) as f:
        insta = yaml.safe_load(f)['insta']
else:
    print('no insta credentials found')

INSTA_USERNAME = insta.get('username')
INSTA_PASSWORD = insta.get('password')
