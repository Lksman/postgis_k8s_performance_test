import logging
import re
import sys
import time
from custom_thread import CustomThread
from db_connection_pool import DatabaseConnectionPool
from data_to_csv import CSVWriter
from psycopg2.pool import ThreadedConnectionPool
import config
import utils

ENTRY = {
    'db_type': None,
    'isolation_lvl': None,
    'concurrent': 0,
    'query': None,
    'planning_time': None,
    'execution_time': None
}


def configure_logging(id: str) -> None:
    if id is None:
        stdout_handler = logging.StreamHandler(stream=sys.stdout)
        handlers = [stdout_handler]
    else:
        # maybe rotating file handler?
        file_handler = logging.FileHandler(filename=f'logs/performance_{id}.log')
        stdout_handler = logging.StreamHandler(stream=sys.stdout)
        handlers = [file_handler, stdout_handler]

    logging.basicConfig(
        level=logging.DEBUG, 
        format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
        handlers=handlers
    )



def execute_query_and_build_entry(isolation_lvl: str = None, concurrent: int = 1, query: str = None, db_type: str = None, pool: ThreadedConnectionPool = None) -> dict:

    db_pool = DatabaseConnectionPool(**config.db_configs[db_type])
    res = db_pool.explain_analyze(query)

    current_entry = ENTRY.copy()
    current_entry['db_type'] = db_type
    current_entry['isolation_lvl'] = isolation_lvl
    current_entry['concurrent'] = concurrent
    current_entry['query'] = re.sub(r'\s+', ' ', query)
    current_entry['planning_time'] = res['Planning Time']
    current_entry['execution_time'] = res['Execution Time']

    return current_entry



if __name__ == '__main__':


    # setting up test parameters
    db_types = ['single_node', 'cluster']
    isolation_levels = ['read_uncommitted', 'read_committed', 'repeatable_read', 'serializable']
    concurrent_connections = [1, 10, 50]

    # db_types = ['local']    
    # isolation_levels = ['read_uncommitted', 'serializable']
    # concurrent_connections = [10]

    # clean stale logs and csv files
    # utils.clean_dir()

    for db_type in db_types:
        # set up logging
        configure_logging(db_type)
        
        # creating aux. tables to prevent memory errors on expensive queries
        logging.info("creating aux. tables")

        db_pool = DatabaseConnectionPool(**config.db_configs[db_type])
        for query in config.aux_queries.values():
            db_pool.execute(query)

        # setting up csv writer
        csv_writer = CSVWriter(f'data/performance_{db_type}.csv')
        csv_writer.fieldnames = list(ENTRY.keys())
        csv_writer.write_header()

        # testing performance
        for isolation_lvl in isolation_levels:
            logging.info("Testing performance on {} with isolation level {}".format(db_type, isolation_lvl))
            db_pool.set_isolation_lvl(isolation_lvl)

            threads: list[CustomThread] = []
            for concurrent in concurrent_connections:
                logging.info("Testing performance of {} concurrent connections".format(concurrent))
                queries = config.queries.values()
                for query in queries:

                    logging.info("Testing performance of query {}".format(re.sub(r'\s+', ' ', query)))
                    for _ in range(concurrent):
                        t = CustomThread(target=execute_query_and_build_entry, args=(isolation_lvl, concurrent, query, db_type, db_pool))
                        t.start()
                        threads.append(t)
                        time.sleep(0.1)


                    for t in threads:
                        t.join()
                        logging.debug("Thread {} finished".format(t.name))

                    for t in threads:
                        if type(t.value) == 'NoneType':
                            logging.error("Thread {} returned None".format(t.name))
                            raise ValueError("Thread {} returned None".format(t.name))
                        else:
                            csv_writer.append_entry(t.value)

                    threads = []

    logging.info("Done")