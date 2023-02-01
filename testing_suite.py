import config
import logging
import re
from custom_thread import CustomThread
from db_connector import DatabaseConnector

ENTRY = {
    'db_type': None,
    'isolation_lvl': None,
    'concurrent': 0,
    'query': None,
    'planning_time': None,
    'execution_time': None
}


def configure_logging() -> None:
    logging.basicConfig(filename='performance.log', filemode='w', level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')


def set_db_isolation_lvl(db: DatabaseConnector, isolation_lvl: str) -> None:
    if isolation_lvl not in ['serializable', 'repeatable_read', 'read_committed', 'read_uncommitted']:
        logging.warning("Invalid isolation level: {}".format(isolation_lvl))
        raise ValueError('Invalid isolation level')
    db.set_isolation_lvl(isolation_lvl)
    

def execute_single_query(isolation_lvl: str = None, concurrent: int = 1, query: str = None, db_type: str = None) -> dict:
    logging.info("Testing performance on {} with {} concurrent connections and isolation level {}".format(db_type, concurrent, isolation_lvl))
    if db_type == 'single_node':
        db = DatabaseConnector(**config.db_single_node_details)
    elif db_type == 'cluster':
        db = DatabaseConnector(**config.db_cluster_details)
    elif db_type == 'local':
        db = DatabaseConnector(**config.db_local_details)
    else:
        raise ValueError("Invalid db_type")

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
    # set up logging
    configure_logging()

    list_of_entries = list()

    # db_types = ['local', 'single_node', 'cluster']
    db_types = ['local']

    #isolation_levels = ['read_uncommitted', 'read_committed', 'repeatable_read', 'serializable']
    isolation_levels = ['read_uncommitted']


    for db_type in db_types:
        for isolation_lvl in isolation_levels:
            if db_type == 'local':
                db = DatabaseConnector(**config.db_local_details)
                set_db_isolation_lvl(db, isolation_lvl)
                db.close()
            if db_type == 'single_node':
                db = DatabaseConnector(**config.db_single_node_details)
                set_db_isolation_lvl(db, isolation_lvl)
                db.close()
            if db_type == 'cluster':
                db = DatabaseConnector(**config.db_cluster_details)
                set_db_isolation_lvl(db, isolation_lvl)
                db.close()
            

            threads = []
            for concurrent in [1]:
                #queries = config.queries.values()
                queries = [config.queries['ConvexHull']]
                for query in queries:
                    for _ in range(concurrent):
                        t = CustomThread(target=execute_single_query, args=(isolation_lvl, concurrent, query, db_type))
                        threads.append(t)
                        t.start()
                        t.join()
                        logging.debug("Thread {} finished".format(t.name))
                    logging.debug("All threads finished")
                logging.debug("All queries finished")
            

            for t in threads:
                list_of_entries.append(t.value)

    for v in list_of_entries:
        print(v)