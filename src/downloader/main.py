import time
import argparse
import logging
import shutil

from api_caller import APICaller
from db_saver import DBSaver

from config import PROFESSIONS_PATH

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--period',
                        type=int,
                        default=1,
                        help='Period of sending API requests in seconds')
    parser.add_argument('--mask',
                        type=str,
                        default='https://api.hh.ru/vacancies',
                        help='API URL')
    parser.add_argument('--per_page',
                        type=int,
                        default=100,
                        help='number of items per page')
    parser.add_argument('--area',
                        type=int,
                        default=2,
                        help='id of the area - default Saint-Petersburg')

    args = parser.parse_args()

    caller_params = {
        'per_page': args.per_page,
        'area': args.area
    }

    caller = APICaller(args.mask, **caller_params)
    saver = DBSaver()

    logger.info('Downloader started')

    while True:
        with open(PROFESSIONS_PATH, encoding='utf-8') as professions:
            for profession in professions:
                caller_params1 = {'text': '{0}'.format(profession), 'per_page': args.per_page, 'area': args.area}
                caller = APICaller(args.mask, **caller_params1)
                logger.info("'{0}' query requested".format(profession))
                for batch in caller:
                    if len(batch) == 0:
                        logger.warning('Empty batch received')
                        break
                    saver.save_batch(batch)
                    total, used, _ = shutil.disk_usage("/")
                    logger.info(f'Hard disk filled in {used / total * 100:.2f}%')
                    if args.period != 0:
                        time.sleep(args.period)
