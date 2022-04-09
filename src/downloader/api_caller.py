import requests
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class APICaller:
    def __init__(self, mask, **api_params):
        self.mask = mask
        self.params = api_params

        self.page = 1

        self.params.update({
            'page': self.page
        })

    def get_batch(self):
        self.params['page'] = self.page
        response = requests.get(self.mask, self.params)

        if response.status_code >= 400:
            logger.error(f'{response.json()}')
            raise RuntimeError(f'Status code: {response.status_code}\n message: {response.json()}')

        response = response.json()
        self.page += 1

        if 'items' in response:
            logger.info(f'got {len(response["items"])} items')
            return response['items']
        else:
            logger.error(f'{response}')
            raise KeyError('No items in response')
