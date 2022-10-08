from pyfacebook import GraphAPI
from pprint import pprint
import yaml
import os


class MyGraphAPI(GraphAPI):

    def __init__(self, token_fn=None, page=None, version='v14.0'):
        """
        token_fn - file name containing tokens
        page - use page_access_token instead of app_access_token
        version - version of FB GraphAPI
        """

        token_fn = os.environ.get('FB_API_CRED')
        with open(token_fn) as f:
            tokens = yaml.safe_load(f)
        self.tokens = tokens

        app_id = tokens.get('app_id')
        app_secret = tokens.get('app_secret')

        self.ig_user = tokens.get(f'{page}_ig_user', None)

        if page:
            access_token = tokens.get(f'{page}_access_token')
        else:
            access_token = tokens.get('long_user_token', None)
            if access_token is None:
                access_token = tokens.get('short_access_token', None)


        if not access_token:
            raise ValueError('No access token. If specifying page, make sure we have that page_access_token')

        super().__init__(
            app_id=app_id, 
            app_secret=app_secret, 
            access_token=access_token, 
            version=version
        )

    # TODO: save to yaml
    def populate_tokens(self, pages):
        lat = self.exchange_long_lived_user_access_token()['access_token']
        print(lat)
        page_tokens = {page: self.exchange_page_access_token(page, lat) for page in pages}
        pprint(page_tokens)

if __name__ == '__main__':
    from IPython import embed
    api = MyGraphAPI()
    api.populate_tokens(['levelupmalta'])
    embed()
