import os
import yaml

CRED = os.environ.get('INSIGHT_CREDENTIALS')

with open(CRED) as f:
    _cred = yaml.safe_load(f)

INSTA_USERNAME = _cred.get('insta', {}).get('username')
INSTA_PASSWORD = _cred.get('insta', {}).get('password')

EMAIL_USERNAME = _cred.get('email', {}).get('username')
EMAIL_PASSWORD = _cred.get('email', {}).get('password')

#POSTGRES = _cred['postgres']
POSTGRES = _cred.get('azure', {})
BASE_LOG = os.environ.get('INSIGHT_LOGS')

CHANNEL_TAG = _cred.get('pushbullet', {}).get('channel_tag')
PUSHBULLET_KEY = _cred.get('pushbullet', {}).get('access_token')
SIMPLEPUSH_KEY = _cred.get('simplepush', {}).get('access_token')
PUSH_NOTIFICATION_API_KEY = _cred.get('push_notification_api', {}).get('access_token')
