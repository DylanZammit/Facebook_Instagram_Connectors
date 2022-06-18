from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import json
import gender_guesser.detector as gender
import time
import yaml
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from IPython import embed
import os


class CookieHandler:

    def __init__(self, fn):
        self.fn = fn

    def save_cookies(self, cookies):
        data = json.dumps(cookies)
        with open(self.fn, 'w', encoding='utf-8') as f:
            f.write(data)

    def load_cookie(self, cookie):
        cookies = self.load_all()
        generator = (cookie for cookie in data if cookie['name'] == cookie)
        return next(generator, -1)

    def load_all(self):
        with open(self.fn, 'r') as f:
            cookies = json.load(f)
        return cookies

    def delete_cookie(self, cookie):
        raise NotImplementedError

    def delete_all(self):
        raise NotImplementedError


class FacebookAPI:
    
    def __init__(self, username, password,
                 headless=False, maximise=True,
                 cookie_fn='fb_cookie.json'):

        op = webdriver.ChromeOptions()
        if headless: op.add_argument('headless')
        capabilities = DesiredCapabilities.CHROME
        capabilities["goog:loggingPrefs"] = {"performance": "ALL"}

        op.add_experimental_option("prefs", { 
            "profile.default_content_setting_values.notifications": 1,
            "profile.managed_default_content_settings.images": 2
        })
        selop = {
            'disable_encoding': True  # Don't intercept/store any requests.
        }
        self.driver = webdriver.Chrome(
            options=op, 
            seleniumwire_options=selop, 
            desired_capabilities=capabilities)
        self.driver.implicitly_wait(5)

        if maximise: self.driver.maximize_window()
        self.username = username
        self.password = password
        self.logged_in = False
        self.login_url = 'https://www.facebook.com'
        self.cookie_handler = CookieHandler(cookie_fn)
        self.cookies = self.cookie_handler.load_all()
        self.SCROLL_PAUSE_TIME = 0.5


    def _sign_in(self):
        if self.logged_in: return
        driver = self.driver

        print('signing in')
        # Search & Enter the Email or Phone field & Enter Password
        username = driver.find_element(By.ID,"email")
        password = driver.find_element(By.ID,"pass")
        submit = driver.find_element(By.NAME, "login")

        time.sleep(1)
        username.send_keys(self.username)
        password.send_keys(self.password)
        submit.click()

    def _no_cookies(self):
        print('removing cookies popup')
        try:
            no_cookie_button = self.driver.find_element(By.XPATH,"//button[@data-cookiebanner='accept_only_essential_button']")
        except Exception as e:
            print(e)
        else:
            no_cookie_button.click()

    def highlight(self, element):
        """Highlights (blinks) a Selenium Webdriver element"""
        driver = element._parent
        def apply_style(s):
            driver.execute_script("arguments[0].setAttribute('style', arguments[1]);",element, s)
        original_style = element.get_attribute('style')
        apply_style("background: yellow; border: 2px solid red;")
        time.sleep(.3)
        apply_style(original_style)

    def goto(self, url):
        self.driver.get(self.login_url)
        for cookie in self.cookies:
            if cookie['name'] == 'presence': self.logged_in = True
            self.driver.add_cookie(cookie)

        print(url)
        if not self.logged_in:
            self.driver.get(self.login_url)
            self._no_cookies()
            self._sign_in()
            self.logged_in = True
            time.sleep(1)
        self.driver.get(url)

    def get_reacts(self, react_box):
        people = []
        driver = self.driver
        action = ActionChains(driver)

        yold, ynew = -1, 0
        i = 0
        #embed()
        prev_link = -1
        while True:
            print('scroll #{}'.format(i+1), end='\r')
            last_link = react_box.find_elements(By.TAG_NAME, 'a')[-1]
            if last_link == prev_link: break
            prev_link = last_link
            driver.execute_script('arguments[0].scrollIntoView()', last_link)
            time.sleep(self.SCROLL_PAUSE_TIME)
            i += 1

        #while yold != ynew:
        #    print('scroll #{}'.format(i+1), end='\r')
        #    scrollbar = react_box.find_element(By.CLASS_NAME, 'jk6sbkaj')
        #    if int(scrollbar.value_of_css_property('height')[0]) == 0: break
        #    self.highlight(scrollbar)
        #    yold = scrollbar.location['y']
        #    action.drag_and_drop_by_offset(scrollbar, 0, 100).perform()
        #    ynew = scrollbar.location['y']
        #    time.sleep(self.SCROLL_PAUSE_TIME)
        #    i += 1
        #    if 0:
        #        A = driver.wait_for_request('graphql', timeout=10)
        #        time.sleep(3)
        #        graph_requests = []
        #        for request in self.driver.requests:
        #            print(request.url)
        #            if 'graphql' in str(request.url):
        #                graph_requests.append(request)
        #                X = json.loads(request.response.body.decode())
        #                try:
        #                    Z = X['data']['node']['reactors']
        #                    embed()
        #                except:
        #                    pass
        #        breakpoint()

        links = react_box.find_elements(By.TAG_NAME, 'a')
        for i in range(1, len(links), 2):
            link = links[i]
            people.append(link.text)

        react_box.find_element(By.CSS_SELECTOR, '[aria-label=Close]').click()
        return people

    def post_likes(self, post_url=None):
        print('checking likes')
        driver = self.driver
        if post_url: self.goto(post_url)
        people = []
        driver.execute_script("window.scrollTo(0, 220)")
        A = driver.find_element(By.CSS_SELECTOR, 
            "[aria-label='See who reacted to this']")
        A.find_element(By.XPATH, '../div').click()

        react_box = driver.find_element(By.CSS_SELECTOR, "[aria-label='Reactions']")
        return self.get_reacts(react_box)


    def page_posts(self, url, num_posts=None):
        if not num_posts: num_posts = np.inf
        driver = self.driver
        self.goto(url)
        main_feed = driver.find_elements(By.CSS_SELECTOR, "[role='main']")[-1]
        main_feed = main_feed.find_element(By.CLASS_NAME, "k4urcfbm")
        try:
            main_feed = driver.find_elements(By.CSS_SELECTOR, "[role='feed']")[-1]
        except Exception as e:
            pass
        
        # Get scroll height
        last_height = driver.execute_script("return document.body.scrollHeight")

        posts = []
        time.sleep(self.SCROLL_PAUSE_TIME)
        while len(posts) < num_posts:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height and len(posts) > 0: break
            last_height = new_height
            time.sleep(self.SCROLL_PAUSE_TIME*2)
            posts = main_feed.find_elements(By.XPATH, "./div[contains(@class, 'k4urcfbm')]")
            print('Num posts loaded = {}'.format(len(posts)), end='\r')
        print()

        posts_reacts = {}
        posts_read = 0
        for i, post in enumerate(posts):
            while True:
                driver.execute_script("return arguments[0].scrollIntoView();", post)
                time.sleep(self.SCROLL_PAUSE_TIME)
                react_clicker = post.find_element(By.CSS_SELECTOR, "[aria-label='See who reacted to this']").find_element(By.XPATH, '../div')
                react_clicker.click()
                react_box = driver.find_element(By.CSS_SELECTOR,"[aria-label='Reactions']")
                #num_likes = int(react_box.find_element(By.XPATH, ".//div[@role='tab']/div/span/span").text)
                self.highlight(react_box)
                people = self.get_reacts(react_box)
                posts_read += 1
                posts_reacts[posts_read] = people
                num_likes_specific = len(people)
                print(f'Post #{i+1} likes = {num_likes_specific}')
                break
                if num_likes == num_likes_specific: break
            if posts_read == num_posts: break
        return posts_reacts


    def close(self):
        self.cookie_handler.save_all(self.driver.get_cookies())
        self.driver.close()


if __name__ == '__main__':
    url = 'https://www.facebook.com/timesofmalta/posts/pfbid0yE5ttL5xT3CWwiBnQc8k3gwUgBifqNEiHvFGUcAhdVDE35ZwThAA8M1QM3pD5U4yl'

    page_url = 'https://www.facebook.com/levelupmalta'
    #page_url = 'https://www.facebook.com/MathsTuitionMalta'
    page_url = 'https://www.facebook.com/timesofmalta'

    with open('credentials.yml') as f:
        credentials = yaml.safe_load(f)

    uname = credentials.get('username')
    pwd = credentials.get('password')
    headless = False
    api = FacebookAPI(username=uname, password=pwd, headless=headless)

    people = api.page_posts(page_url, 20)
    print(people)

    d = gender.Detector(case_sensitive=False)

    freqs = {}
    rpp = {}
    genders = {}
    for post_num, post_reacts in people.items():
        print(post_num)
        for person in post_reacts:
            genders[post_num] = {'male': 0, 'female': 0}
            if person not in freqs:
                freqs[person] = 1
            else:
                freqs[person] +=1

            first_name = person.split(' ')[0]
            gender = d.get_gender(first_name)
            if 'female' in gender:
                genders[post_num]['female'] += 1
            elif 'male' in gender:
                genders[post_num]['male'] += 1
            else:
                genders[post_num]['male'] += 0.5
                genders[post_num]['female'] += 0.5

        rpp[post_num] = len(post_reacts)

    rpp = pd.Series(rpp)
    freqs = pd.Series(freqs).sort_values(ascending=False)

    df_genders = pd.DataFrame(genders).T
    df_genders.T.plot.pie(subplots=True)
    plt.figure()
    df_genders.sum().plot.pie()

    plt.figure()

    plt.plot(rpp)
    plt.xlabel('Post number')
    plt.ylabel('Num of reacts')
    plt.title('Reacts per post for LevelUp')

    plt.figure()

    ax = freqs.iloc[:10].plot.bar(rot = 45)
    ax.set_title('Top 10 post reactors')
    plt.show()

    if 0:
        posts = ['https://www.facebook.com/411387069323462/posts/415688255560010/',
                 'https://www.facebook.com/411387069323462/posts/411394092656093/',
                 'https://www.facebook.com/411387069323462/posts/411401305988705/']

        likers = {url: api.post_likes(url) for url in posts}
        api.close()

        print(likers)
