from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
import yaml
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


class FacebookAPI:
    
    def __init__(self, username, password, headless=False, maximise=True):
        op = webdriver.ChromeOptions()
        if headless: op.add_argument('headless')
        op.add_experimental_option("prefs", { 
            "profile.default_content_setting_values.notifications": 1 
        })
        self.driver = webdriver.Chrome(options=op)
        if maximise: self.driver.maximize_window()
        self.username = username
        self.password = password
        self.logged_in = False
        self.login_url = 'https://www.facebook.com'

    def _sign_in(self):
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
            A = WebDriverWait(self.driver, 3).until(
                lambda x: x.find_element(By.XPATH,"//button[@data-cookiebanner='accept_only_essential_button']"))
        except Exception as e:
            print(e)
        else:
            A.click()

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
        while yold != ynew:
            print('scroll #{}'.format(i+1), end='\r')
            #breakpoint()
            scrollbar = react_box.find_element(By.CLASS_NAME, 'jk6sbkaj')
            self.highlight(scrollbar)
            #scrollbar = WebDriverWait(driver, 2).until(lambda x: x.find_element(By.CLASS_NAME, 'jk6sbkaj'))
            yold = scrollbar.location['y']
            action.drag_and_drop_by_offset(scrollbar, 0, 100).perform()
            ynew = scrollbar.location['y']
            time.sleep(0.5)
            i += 1

        # needed?
        react_box = WebDriverWait(driver, 10).until(lambda x: x.find_element(By.CSS_SELECTOR, "[aria-label='Reactions']")) 
        links = react_box.find_elements(By.TAG_NAME, 'a')
        for i in range(1, len(links), 2):
            link = links[i]
            people.append(link.text)

        driver.find_element(By.CSS_SELECTOR, '[aria-label=Close]').click()
        return people

    def post_likes(self, post_url=None):
        print('checking likes')
        driver = self.driver
        if post_url: self.goto(post_url)
        people = []
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 220)")
        A = WebDriverWait(driver, 10).until(
            lambda x: x.find_element(By.CSS_SELECTOR, "[aria-label='See who reacted to this']")
                       .find_element(By.XPATH, '../div')) 
        A.click()
        react_box = WebDriverWait(driver, 10).until(lambda x: x.find_element(By.CSS_SELECTOR, "[aria-label='Reactions']")) 
        return self.get_reacts(react_box)


    def page_posts(self, url, num_posts=None):
        if not num_posts: num_posts = np.inf
        driver = self.driver
        self.goto(url)
        feeds = WebDriverWait(driver, 5).until(lambda x: x.find_elements(By.CSS_SELECTOR, "[role='feed']"))
        feed = feeds[-1]
        SCROLL_PAUSE_TIME = 0.5

        # Get scroll height
        last_height = driver.execute_script("return document.body.scrollHeight")

        posts = []
        while len(posts) < num_posts:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

            posts = feed.find_elements(By.XPATH, './*')
            print('Num posts loaded = {}'.format(len(posts)), end='\r')
            
        posts_reacts = {}
        posts_read = 0
        for post in posts:
            if 'k4urcfbm' not in post.get_attribute('class'): continue
            driver.execute_script("return arguments[0].scrollIntoView();", post)
            time.sleep(0.5)
            post.find_element(By.CSS_SELECTOR, "[aria-label='See who reacted to this']").find_element(By.XPATH, '../div').click()
            react_box = WebDriverWait(driver, 3).until(lambda x: x.find_element(By.CSS_SELECTOR,"[aria-label='Reactions']"))
            people = self.get_reacts(react_box)
            posts_read += 1
            posts_reacts[posts_read] = people
            if posts_read == num_posts: break
        return posts_reacts


    def close(self):
        self.driver.close()


if __name__ == '__main__':
    url = 'https://www.facebook.com/timesofmalta/posts/pfbid0yE5ttL5xT3CWwiBnQc8k3gwUgBifqNEiHvFGUcAhdVDE35ZwThAA8M1QM3pD5U4yl'
    page_url = 'https://www.facebook.com/levelupmalta'

    with open('credentials.yml') as f:
        credentials = yaml.safe_load(f)

    uname = credentials.get('username')
    pwd = credentials.get('password')
    api = FacebookAPI(username=uname, password=pwd)

    people = api.page_posts(page_url)
    print(people)

    freqs = {}
    rpp = {}
    for post_num, post_reacts in people.items():
        print(post_num)
        for person in post_reacts:
            if person not in freqs:
                freqs[person] = 1
            else:
                freqs[person] +=1
        rpp[post_num] = len(post_reacts)

    rpp = pd.Series(rpp)
    freqs = pd.Series(freqs).sort_values(ascending=False)

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
