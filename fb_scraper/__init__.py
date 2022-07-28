from entities import Page
from storage import Storage
from selenium.webdriver.common.proxy import Proxy, ProxyType
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from facebook_scraper import *
from IPython import embed

if __name__ == '__main__':
    cookies = 'cookies.txt'
    page_name = 'timesofmalta'

    set_cookies(cookies)

    software_names = [SoftwareName.CHROME.value]
    operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
    user_agent_rotator = UserAgent(software_names=software_names, opearting_systems=operating_systems, limit=100)
    user_agent = user_agent_rotator.get_random_user_agent()

    set_user_agent(user_agent)

    latest_date = datetime.now() - timedelta(hours=2)
    page = Page(page_name)
    page.get_details()
    page.get_page_posts(latest_date=latest_date)

    store = Storage()
    store.store(page)
    embed()

