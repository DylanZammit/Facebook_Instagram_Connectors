from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
import yaml

def highlight(driver, element):
    """Highlights (blinks) a Selenium Webdriver element"""
    driver = element._parent
    def apply_style(s):
     driver.execute_script("arguments[0].setAttribute('style', arguments[1]);",element, s)
    original_style = element.get_attribute('style')
    apply_style("background: yellow; border: 2px solid red;")
    time.sleep(.3)
    apply_style(original_style)

url = 'https://www.facebook.com/timesofmalta/posts/pfbid0yE5ttL5xT3CWwiBnQc8k3gwUgBifqNEiHvFGUcAhdVDE35ZwThAA8M1QM3pD5U4yl'

with open('credentials.yml') as f:
    credentials = yaml.safe_load(f)

uname = credentials.get('username')
pwd = credentials.get('password')

def sign_in(driver):
    print('signing in')
    # Search & Enter the Email or Phone field & Enter Password
    username = driver.find_element(By.ID,"email")
    password = driver.find_element(By.ID,"pass")
    submit = driver.find_element(By.ID, "loginbutton")

    time.sleep(3)
    username.send_keys(uname)
    password.send_keys(pwd)
    submit.click()

def who_liked(driver):
    print('checking likes')
    people = []
    A = WebDriverWait(driver, 10).until(lambda x: x.find_element(By.CSS_SELECTOR, "[aria-label='See who reacted to this']")) 

    A.find_element(By.XPATH, '../div').click()

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

    react_box = WebDriverWait(driver, 10).until(lambda x: x.find_element(By.CSS_SELECTOR, "[aria-label='Reactions']")) 
    links = react_box.find_elements(By.TAG_NAME, 'a')
    for i in range(1, len(links), 2):
        link = links[i]
        people.append(link.text)

    driver.find_element(By.CSS_SELECTOR, '[aria-label=Close]').click()
    return people

def no_cookies(driver):
    print('removing cookies popup')
    time.sleep(1)
    driver.find_element(By.XPATH,"//button[@data-cookiebanner='accept_only_essential_button']").click()

react_class = 'gjzvkazv'
react_box_class = 'k7i0oixp'

op = webdriver.ChromeOptions()
# don't open browser
# op.add_argument('headless')
op.add_experimental_option("prefs", { 
    "profile.default_content_setting_values.notifications": 1 
})

with webdriver.Chrome(options=op) as driver:
    driver.maximize_window()

    driver.get(url)
    no_cookies(driver)
    sign_in(driver)
    people = who_liked(driver)
    print(people)
    print(f'Num ppl: {len(people)}')

    breakpoint()
