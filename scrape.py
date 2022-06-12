from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
import yaml


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

    def _sign_in(self):
        driver = self.driver
        print('signing in')
        # Search & Enter the Email or Phone field & Enter Password
        username = driver.find_element(By.ID,"email")
        password = driver.find_element(By.ID,"pass")
        submit = driver.find_element(By.ID, "loginbutton")

        time.sleep(3)
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
        self.driver.get(url)
        if not self.logged_in:
            self._no_cookies()
            self._sign_in()
            self.logged_in = True

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
        action = ActionChains(driver)

        yold, ynew = -1, 0
        i = 0
        while yold != ynew:
            print('scroll #{}'.format(i+1), end='\r')
            scrollbar = WebDriverWait(driver, 2).until(lambda x: x.find_element(By.CLASS_NAME, 'jk6sbkaj'))
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

    def close(self):
        self.driver.close()


if __name__ == '__main__':
    url = 'https://www.facebook.com/timesofmalta/posts/pfbid0yE5ttL5xT3CWwiBnQc8k3gwUgBifqNEiHvFGUcAhdVDE35ZwThAA8M1QM3pD5U4yl'

    with open('credentials.yml') as f:
        credentials = yaml.safe_load(f)

    uname = credentials.get('username')
    pwd = credentials.get('password')
    api = FacebookAPI(username=uname, password=pwd)

    posts = ['https://www.facebook.com/411387069323462/posts/415688255560010/',
             'https://www.facebook.com/411387069323462/posts/411394092656093/',
             'https://www.facebook.com/411387069323462/posts/411401305988705/']

    likers = {url: api.post_likes(url) for url in posts}
    api.close()

    print(likers)
