import logging

from api_response_parser import APIResponseParser
from config import VACANCY_TYPES

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DBSaver:
    def __init__(self):
        self.parser = APIResponseParser(VACANCY_TYPES)

    def save_batch(self, batch):
        batch = self.parser.parse_batch(batch)
        logger.info('batch saved to DB')
