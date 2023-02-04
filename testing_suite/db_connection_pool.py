import psycopg2
import logging
from time import sleep
from typing import Optional



class DatabaseConnectionPool():
    def __init__(self, dbname:str=None, user:str=None, password:str=None, host:str=None, port:str=None) -> None:
        if dbname is None:
            raise ValueError('dbname is required')
        if user is None:
            raise ValueError('user is required')
        if password is None:
            raise ValueError('password is required')
        if host is None:
            raise ValueError('host is required')
        if port is None:
            logging.warning('port is not specified, defaulting to 5432')

        max_retries = 5
        delay_between_retries = 10
        for _ in range(max_retries):
            try:
                connect_kwargs = {
                    'dbname': dbname,
                    'user': user,
                    'password': password,
                    'host': host,
                    'port': port
                }

                self.pool = psycopg2.pool.ThreadedConnectionPool(1, 60, **connect_kwargs)
            except Exception as e:
                logging.error(f"Failed to connect pool to database ({e}), retrying in {delay_between_retries} seconds...")
                sleep(delay_between_retries)
                continue
            else:
                break

    
    def _get_connection(self) -> psycopg2.extensions.connection:
        return self.pool.getconn()
    
    def execute(self, query: str) -> Optional[list]:
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        if cur.description is None:
            res = None
        else:
            res = cur.fetchall()
        self.pool.putconn(conn)
        
        return res

    def explain_analyze(self, query: str) -> dict:
        res = self.execute("EXPLAIN ANALYZE " + query)

        # example of res:
        # [
        #   ('Aggregate  (cost=2186.93..2188.19 rows=1 width=32) (actual time=1873.477..1873.478 rows=1 loops=1)',), 
        #   ('  ->  Seq Scan on nyc_census_blocks  (cost=0.00..2064.94 rows=38794 width=243) (actual time=0.009..2.217 rows=38794 loops=1)',), 
        #   ('Planning Time: 0.896 ms',), 
        #   ('Execution Time: 1879.822 ms',)
        # ]

        measured_time = {
            'Planning Time': res[-2][0].split(':')[1].strip(),
            'Execution Time': res[-1][0].split(':')[1].strip()
        }

        return measured_time


    def discard_all(self) -> None:
        self.execute('DISCARD ALL')


    def set_isolation_lvl(self, level: str = None) -> None:

        isolation_levels = {
            'read_uncommitted': 0,
            'read_committed': 1,
            'repeatable_read': 2,
            'serializable': 3,
        }

        if level not in isolation_levels.keys():
            logging.warning("Invalid isolation level: {}".format(level))
            raise ValueError('Invalid isolation level')


        if not level:
            logging.warning('Isolation level not specified, defaulting to read_uncommitted')
            level = isolation_levels['read_uncommitted']
        else:
            level = isolation_levels[level]
        
        conn = self._get_connection()
        conn.set_isolation_level(level)
        conn.commit()
        self.pool.putconn(conn)

