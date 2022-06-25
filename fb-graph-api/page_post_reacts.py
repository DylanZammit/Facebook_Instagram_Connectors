from fb_api import MyGraphAPI
import matplotlib.pyplot as plt
import pandas as pd


def main(page=None):
    api = MyGraphAPI(page=page)
    posts = api.get_object(page_name, fields='posts')['posts']['data']

    react_types = ['LIKE', 'LOVE', 'WOW', 'HAHA', 'SAD', 'ANGRY']
    react_count = {}

    react_type = None
    for post in posts:
        page_post_id = post['id']
        post_id = page_post_id.split('_')[1]
        data = api.get_object(page_post_id, fields='shares,created_time,message,permalink_url')
        num_shares = data.get('shares', {'count': 0})['count']
        create_time = data['created_time']
        permalink_url = data.get('permalink_url')

        react_count[post_id] = {'create_time': create_time, 'shares': num_shares}
        react_count[post_id].update(
            {react_type: api.get(page_post_id+'/reactions', 
                                 {'summary': 'total_count', 'type':react_type}
                                )['summary']['total_count'] for react_type in react_types})
        react_count[post_id]['message'] = data.get('message', '')
        react_count[post_id]['url'] = permalink_url

        print(f'Post ID = {page_post_id}')
    react_count = pd.DataFrame(react_count).T
    print(react_count)
    ax = react_count.plot()
    ax.set_title(f'{page_name} reacts per post')
    plt.show()
    if 1: react_count.to_csv(f'{page_name}.csv')

if __name__ == '__main__':
    page_name = 'levelupmalta'
    #page_name = 'mathstuitionmalta'
    main(page_name)
