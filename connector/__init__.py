import mysql.connector
import pandas as pd
import yaml


class MySQLConnector:

    def __init__(self, fn=None, host=None, usr=None, pwd=None, db=None):
        creds = {}

        if fn:
            with open(fn) as f:
                creds = yaml.safe_load(f)['db']

        self.kwargs = {
            'host': host if host else creds.get('host'),
            'user': usr if usr else creds.get('user'),
            'password': pwd if pwd else creds.get('pwd'),
            'database': db if db else creds.get('name'),
            'buffered': True
        }

    @property
    def connection(self):
        return mysql.connector.connect(**self.kwargs)

    def execute(self, query, params=None):
        if params is None: params = ()
        assert 'insert' not in query.lower()

        with mysql.connector.connect(**self.kwargs) as conn:
            with conn.cursor() as cursor:
                if isinstance(params, tuple):
                    execute = cursor.execute
                elif isinstance(params, list):
                    execute = cursor.executemany
                execute(query, params)
                df = cursor.fetchall()
                columns = cursor.column_names
            conn.commit()
        df = pd.DataFrame(df, columns=columns)
        return df

    def insert(self, query, params=None):
        if params is None: params = ()
        assert 'insert' in query.lower()

        with mysql.connector.connect(**self.kwargs) as conn:
            with conn.cursor() as cursor:
                if isinstance(params, tuple):
                    execute = cursor.execute
                elif isinstance(params, list):
                    execute = cursor.executemany
                execute(query, params)
            conn.commit()


if __name__ == '__main__':
    conn = MySQLConnector()
    df = conn.execute('select * from pages')
    print(df)

