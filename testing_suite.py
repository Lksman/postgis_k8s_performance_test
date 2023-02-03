import logging
import re
import sys
import time
from custom_thread import CustomThread
from db_connector import DatabaseConnector
from data_to_csv import CSVWriter
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
        file_handler = logging.FileHandler(filename=f'performance_{id}.log')
        stdout_handler = logging.StreamHandler(stream=sys.stdout)
        handlers = [file_handler, stdout_handler]

    logging.basicConfig(
        level=logging.DEBUG, 
        format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
        handlers=handlers
    )


def set_db_isolation_lvl(db: DatabaseConnector, isolation_lvl: str) -> None:
    if isolation_lvl not in ['serializable', 'repeatable_read', 'read_committed', 'read_uncommitted']:
        logging.warning("Invalid isolation level: {}".format(isolation_lvl))
        raise ValueError('Invalid isolation level')
    db.set_isolation_lvl(isolation_lvl)
    

def execute_single_query(isolation_lvl: str = None, concurrent: int = 1, query: str = None, db_type: str = None) -> dict:
    #logging.info("Testing performance on {} with {} concurrent connections and isolation level {}".format(db_type, concurrent, isolation_lvl))

    db = DatabaseConnector(**config.db_configs[db_type])
    res = db.explain_analyze(query)
    db.close()

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
    # concurrent_connections = [1]

    # clean stale logs and csv files
    # utils.clean_dir()

    for db_type in db_types:


        # set up logging
        configure_logging(db_type)
        
        # creating aux. tables to prevent memory errors on expensive queries
        logging.info("creating aux. tables")
        db = DatabaseConnector(**config.db_configs[db_type])
        for query in config.aux_queries.values():
            db.execute(query)
        db.close()

        # setting up csv writer
        csv_writer = CSVWriter(f'performance_{db_type}.csv')
        csv_writer.fieldnames = list(ENTRY.keys())
        csv_writer.write_header()

        # testing performance
        for isolation_lvl in isolation_levels:
            logging.info("Testing performance on {} with isolation level {}".format(db_type, isolation_lvl))

            db = DatabaseConnector(**config.db_configs[db_type])
            set_db_isolation_lvl(db, isolation_lvl)
            db.close()

            threads: list[CustomThread] = []
            for concurrent in concurrent_connections:
                logging.info("Testing performance of {} concurrent connections".format(concurrent))
                queries = config.queries.values()
                for query in queries:
                    logging.info("Testing performance of query {}".format(re.sub(r'\s+', ' ', query)))
                    for _ in range(concurrent):
                        t = CustomThread(target=execute_single_query, args=(isolation_lvl, concurrent, query, db_type))
                        threads.append(t)
                        t.start()

                    for t in threads:
                        t.join()
                        logging.debug("Thread {} finished".format(t.name))

                    for t in threads:
                        csv_writer.append_entry(t.value)

                    threads = []
                    #logging.info("Waiting for 1 minute to stabilize the system")
                    #time.sleep(60) # wait for 1 minute to stabilize the system


    logging.info("Done")