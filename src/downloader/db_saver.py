import logging

from api_response_parser import APIResponseParser
from config import VACANCY_TYPES, CLICKHOUSE_PARAMS

from clickhouse_driver import Client


logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DBSaver:

    insert_query = """
        INSERT INTO
            vacancy (*)
        VALUES
    """

    select_ids_query = """
        SELECT
            id
        FROM
            vacancy
    """

    test_query = """
        SHOW DATABASES
    """

    def __init__(self):
        self.parser = APIResponseParser(VACANCY_TYPES)
        self.db_connection = Client(**CLICKHOUSE_PARAMS)

        try:
            _ = self.db_connection.execute(self.test_query)
            logger.debug('DB Connection established.')
        except:
            logger.error('DB connection. Check parameters of connection')
            raise ValueError('Check input parameters:\n{}'.format(
                CLICKHOUSE_PARAMS,
            ))

        self.saved_ids = set(self.db_connection.execute(self.select_ids_query))
        logger.info(f'In DB already consists {len(saved_vacancy_ids)} samples.')

    def check_duplicates(self, batch):
        input_ids = set(map(lambda sample: sample.get('id'), batch))
        if not input_ids.isdisjoint(self.saved_ids):
            logger.warning(
                f'Detected {len(self.saved_ids.difference(input_ids))} duplicate elements.',
            )
            actual_ids = input_ids.difference(self.saved_ids)
            batch = tuple(filter(lambda id_: id_ in actual_ids, actual_ids))

        self.saved_ids.update(actual_ids)
        return batch

    def save_batch(self, batch):
        batch = self.parser.parse_batch(batch)
        batch = self.check_duplicates(batch)
        size = self.db_connection.execute(self.insert_query, batch)
        logger.info(f'Batch with size {size} saved to DB.')
