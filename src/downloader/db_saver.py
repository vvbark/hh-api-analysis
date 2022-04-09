import logging

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DBSaver:
    def save_batch(self, batch):
        logger.info('batch saved to DB')

