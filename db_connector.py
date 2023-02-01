import psycopg2
import logging

class DatabaseConnector():
    def __init__(self, dbname:str=None, user:str=None, password:str=None, host:str=None, port:str=None):
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

        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        self.cur = self.conn.cursor()
    

    def execute(self, query: str) -> None:
        self.cur.execute(query)
        self.conn.commit()
    

    def fetch(self, query: str) -> list:
        self.cur.execute(query)
        return self.cur.fetchall()


    def close(self) -> None:
        self.cur.close()
        self.conn.close()


    def explain_analyze(self, query: str) -> dict:
        self.cur.execute("EXPLAIN ANALYZE " + query)
        res = self.cur.fetchall()

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
        self.cur.execute("DISCARD ALL")
        self.conn.commit()


    def set_isolation_lvl(self, level: str = None) -> None:
        isolation_levels = {
            'read_uncommitted': 0,
            'read_committed': 1,
            'repeatable_read': 2,
            'serializable': 3,
        }

        if not level:
            logging.warning('Isolation level not specified, defaulting to read_uncommitted')
            level = isolation_levels['read_uncommitted']
        else:
            level = isolation_levels[level]
        
        self.conn.set_isolation_level(level)
        self.conn.commit()
