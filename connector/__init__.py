import pandas.io.sql as sqlio
import psycopg2
import pandas as pd


class PGConnector:

    def __init__(self, host=None, user=None, pwd=None, db=None, schema=None):

        self.kwargs = {
            'host': host,
            'user': user,
            'password': pwd,
            'database': db,
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


if __name__ == '__main__':
    import os
    from credentials import POSTGRES
    conn = PGConnector(POSTGRES)
    df = conn.execute('select * from pages')
    print(df)
    breakpoint()

