import numpy as np
import regex as re
import time

id2react = {
    1: 'LIKE',
    2: 'LOVE',
    3: 'HAHA',
    4: 'ANGRY',
    5: 'SAD',
    6: 'WOW'
}

react2id = {v: k for k, v in id2react.items()}

def extract_id(link):
    reg_identifier = r'^(?:.*)\/(?:pages\/[A-Za-z0-9-]+\/)?(?:profile\.php\?id=)?([A-Za-z0-9.]+)'
    user_id = re.match(reg_identifier, link).group(1)
    return user_id


def extract_namesurname(name_surname):
    name, surname = name_surname.split()[0], ' '.join(name_surname.split()[1:])
    return name, surname


def rsleep(t, cap=10, q=True):
    sleep = t+np.random.randint(0, cap)
    if not q: 
        print(f'Sleep for {sleep} seconds...', end='', flush=True)
    time.sleep(sleep)
    if not q: print('done', flush=True, end='\r')

    
