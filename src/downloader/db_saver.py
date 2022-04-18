import logging
import json

from api_response_parser import APIResponseParser
from config import VACANCY_TYPES, CLICKHOUSE_PARAMS

from clickhouse_driver import Client


logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class JSONEncoder(json.JSONEncoder):

    # overload method default
    def default(self, obj):

        # Match all the types you want to handle in your converter
        if isinstance(obj, datetime):
            return arrow.get(obj).isoformat()
        # Call the default method for other types
        return json.JSONEncoder.default(self, obj)


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
    def get_ids(batch):
        return set(map(lambda sample: sample.get('id'), batch))

    def check_duplicates(self, batch):
        input_ids = self.get_ids(batch)
        if not input_ids.isdisjoint(self.saved_ids):
            logger.warning(
                f'Detected {len(self.saved_ids.difference(input_ids))} duplicate elements.',
            )
            input_ids = input_ids.difference(self.saved_ids)
            batch = tuple(filter(lambda id_: id_ in input_ids, input_ids))

        self.saved_ids.update(input_ids)
        return batch

    def save_batch(self, batch):
        batch = self.parser.parse_batch(batch)
        batch = self.check_duplicates(batch)
        try:
            size = self.db_connection.execute(self.insert_query, batch)
        except:
            logger.warning(f'Error in inserting. Scipping.')
            with open('./failed.json', 'w') as f:
                json.dump(batch, f)

        logger.info(f'Batch with size {size} saved to DB.')
        logger.info(f'--------- There are {len(self.saved_ids)} saved samples now. ---------')
