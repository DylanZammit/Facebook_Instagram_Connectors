# INSIGHT

This project is split up into different modules. Each should probably be its own repo. We give a brief description of the overall structure and each of the modules. `setup.py` is used for version control and `requirements.txt` are the required libraries that are needed for the project to run. Python 3.8+ is required for instagrapi. This dependency will not be required in prod when we only rely on API.

```
.
+-- setup.py
+-- requirements.txt
+-- connector
|   +-- __init__.py
+-- credentials
|   +-- footer.html
|   +-- header.html
+-- logger
|   +-- default.html
|   +-- post.html
+-- insight
|   +-- __init__.py
|   +-- facebook
|       +-- entities.py
|       +-- storage.py
|       +-- __init__.py
|       +-- api
|           +-- __init__.py
|           +-- update.py
|       +-- scraper
|           +-- __init__.py
|           +-- update.py
|   +-- instagram
|       +-- entities.py
|       +-- storage.py
|       +-- __init__.py
|       +-- api
|           +-- __init__.py
|           +-- update.py
|       +-- scraper
|           +-- __init__.py
|           +-- update.py
|   +-- utils.py
```

## Connector
This module facilitates instansiating the PG connector as it handles everything for you including the credentials. Something like the below will suffice.
```
# get POSTGRES credentials. Should be done automatically be default in
# connector module if none are specified
from credentials import POSTGRES
from connector import PGConnector as Connector

conn = Connector(**POSTGRES)
df = conn.execute('SELECT * FROM table')

rows = [
    (val11, val12),
    (val21, val22),
    ...
    (valn1, valn2)
]

conn.insert('table', rows)
```
## Credentials
An Env variable called `INSIGHT_CREDENTIALS` pointing to `.yml` file containing all the credentials. This should contain the keys
* postgres -> postgres credentials
* insta -> instagram credentials for scraping
* pushbullet -> pushbullet access_token and channel
```
from credentials import POSTGRES, EMAIL, USERNAME, etc...
```

## Logger
Makes it easy to import a logger that prints to stdout and a file. The below snippet shows what is required. An env var called `INSIGHT_LOGS` should be set to a directory which would contain the logs. The logs will be saved to `path/from/env/var/logfile_name_%Y_%m_%d.log`. Notice that the date is appended automatically.
```
from logger import mylogger

logger = mylogger('logfile_name')
logger.info('this is an info message')
logger.critical('this is critical!!')
```
## Insight

### entities.py
This contains empty classes of Facebook/Instagram entities such as `Page`, `Post`, `Comment` etc. These are convinent to structure data from the API, and there are almost no restrictions to instantiate a class. TODO: Enforce PK/ID on creation. These classes will also be useful when passing to `Storage` in `storage.py`, as the name of the class is used to call the correct function. In the subsection below, we will shows a simple example to create and store an entity in PG.
### facebook.storage.py
Handles storing of FB entities. The classes contain methods such as `_store_page`, `_store_page_dim` and `_store_page_fact` depending on the entity. However, one should always pass the entity class (or list of entities) to the `_store` method. The entity name will be parsed and sent to its respective method. NOTE: If a list is passed, it is assumed that all objects in the list are of the same entity. If you want to pass both a `Page` and its `Post`, this should be an appropriate attribute of the `Page` class. See example below
```
from insight.facebook.entities import Page, Post
from insight.facebook.storage import Storage

posts = [
    Post(**kwargs),
    Post(**kwargs),
    ...,
    Post(**kwargs)
]

page_tom = Page(
    page_id=123,
    page_name='timesofmalta',
    scrape_date='2022-01-01',
    num_followers=123,
    posts=posts # posts passed as an attribute
)

page_mt = Page(
    page_id=321,
    page_name='maltatoday',
    scrape_date='2022-01-01',
    num_followers=321
)

pages = [page_tom, page_mt]

storage = Storage()
storage.store(pages)
```
In the above example, each `Page` instance of `pages` is sent to `_store_page`, which in turn calls `_store_page_dim/fact` which in turn passes the `posts` list to `_store_post` and `_store_post_dim/fact` etc. The same can be said for `insight.instagram.storage`.
### facebook.api
The `__init__.py` class handles the GraphAPI class. Make sure you set an env var called `FB_API_CRED` which points to a `.yml` containing the appropriate access tokens.

`update.py` is the main ETL script that calls the API endpints. With a minimal amount of API calls, the FacebookExtractor parses the information into fb entities and calles the `FacebookStorage` class as in the example above. As of today, this script reads, parses and stores the entities `Page`, `Post`, `Comment`, including insights.
### instagram.api
Similar to above but for IG.
### facebook.scraper
Logs in facebook via a `cookies.txt` file stored in the same directory as the `__init__.py` and `update.py` files. Creates FB entities containing *only public data* and stores them in PG. Very sensitive to user-specific calls such as likes and comments. Proceed with caution to avoid getting the account banend. Will be deprecated in the future once we are given PPCA permission. Uses [this online scraper](https://github.com/kevinzg/facebook-scraper)
### instagram.scraper
Similar to above, uses the [`instagrapi` library](https://github.com/adw0rd/instagrapi) to scrape data. From my experience, IG is more sensitive to bans and limitation. It does not outright ban me, but fails to respond to HTTP requests. Will be deprecated in the future in favour of API once we are given PPCA permission.

