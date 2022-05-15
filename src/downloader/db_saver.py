import logging
import json

from api_response_parser import APIResponseParser
from config import VACANCY_TYPES, CLICKHOUSE_PARAMS

from clickhouse_driver import Client


logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
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
            logger.info('DB Connection established.')
        except:
            logger.error('DB connection. Check parameters of connection')
            raise ValueError('Check input parameters:\n{}'.format(
                CLICKHOUSE_PARAMS,
            ))

        self.saved_ids = set(self.db_connection.execute(self.select_ids_query))
        logger.info(f'In DB already consists {len(self.saved_ids)} samples.')

    @staticmethod
    def filter_batch_by_ids(batch, ids):
        return tuple(filter(lambda vacancy: (vacancy['id'], ) in ids, batch))

    @staticmethod
    def get_ids(batch):
        return set(map(lambda sample: (sample.get('id'), ), batch))

    def check_duplicates(self, batch):
        input_ids = self.get_ids(batch)
        # если есть пересечение
        if not input_ids.isdisjoint(self.saved_ids):
            # выводим сколько таких пересечений обнаружено
            logger.warning(
                f'Detected {len(self.saved_ids.intersection(input_ids))} duplicated elements.',
            )
            # и отсеиваем общие элементы
            input_ids = input_ids.difference(self.saved_ids)
            batch = self.filter_batch_by_ids(batch, input_ids) # фильтруем батч согласно уникальным id

        self.saved_ids.update(input_ids)
        return batch

    def save_batch(self, batch):
        batch = self.parser.parse_batch(batch)
        batch = self.check_duplicates(batch)
        try:
            current_size = self.db_connection.execute(self.insert_query, batch)
            logger.info(f'Batch with size {current_size} saved to DB.')

        except:
            logger.warning(f'Error in inserting. Skipping.')

        logger.info(f'--------- There are {len(self.saved_ids)} saved samples now. ---------')
