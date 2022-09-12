import os
import yaml

CRED = os.environ.get('INSIGHT_CREDENTIALS')

with open(CRED) as f:
    _cred = yaml.safe_load(f)

INSTA_USERNAME = _cred['insta'].get('username')
INSTA_PASSWORD = _cred['insta'].get('password')

EMAIL_USERNAME = _cred['email'].get('username')
EMAIL_PASSWORD = _cred['email'].get('password')

POSTGRES = _cred['postgres']
BASE_LOG = os.environ.get('INSIGHT_LOGS')
