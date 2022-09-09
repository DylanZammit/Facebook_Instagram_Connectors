import mysql.connector
import pandas.io.sql as sqlio
import psycopg2
import pandas as pd
import yaml


class PGConnector:

    def __init__(self, fn=None, host=None, usr=None, pwd=None, db=None, schema=None):
        creds = {}

        if fn:
            with open(fn) as f:
                creds = yaml.safe_load(f)['postgres']

        schema = schema if schema else creds.get('schema', None)
        self.kwargs = {
            'host': host if host else creds.get('host'),
            'user': usr if usr else creds.get('user'),
            'password': pwd if pwd else creds.get('pwd'),
            'database': db if db else creds.get('name'),
            'options': f'-c search_path={schema}'
        }

    @property
    def connection(self):
        return psycopg2.connect(**self.kwargs)

    def execute(self, query, params=None):
        if params is None: params = ()
        assert 'insert' not in query.lower()

        with psycopg2.connect(**self.kwargs) as conn:
            df = sqlio.read_sql_query(query, conn, params=params)
            conn.commit()
        return df

    def run_query(self, query):
        with psycopg2.connect(**self.kwargs) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()

    def insert(self, table, params):
        if len(params) == 0 or params is None: return
        if isinstance(params, tuple):
            params = [params]
        n_params = len(params[0])
        query_vals = ('%s,'*n_params)[:-1]
        query = 'INSERT INTO {} VALUES ({})'.format(table, query_vals)

        with psycopg2.connect(**self.kwargs) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params)
            conn.commit()

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

    def run_query(self, query):
        with mysql.connector.connect(**self.kwargs) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()

    def insert(self, table, params):
        if len(params) == 0 or params is None: return
        if isinstance(params, tuple):
            params = [params]
        n_params = len(params[0])
        query_vals = ('%s,'*n_params)[:-1]
        query = 'INSERT INTO {} VALUES ({})'.format(table, query_vals)

        with mysql.connector.connect(**self.kwargs) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params)
            conn.commit()


if __name__ == '__main__':
    import os
    conn = PGConnector(os.environ.get('SOCIAL_CONN'))
    df = conn.execute('select * from pages')
    print(df)
    breakpoint()

