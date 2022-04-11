import logging
import mypy.api

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from datetime import datetime

from .config import VACANCY_TYPES


logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class APIResponseParser:

    DATE_REGEX = '%Y-%m-%dT%X%z'

    def __init__(self, types = VACANCY_TYPES):
        self.types = types

    @staticmethod
    def dict_expand(key, dictionary):
        return {f'{key}_{subkey}': dictionary.get(subkey) for subkey in dictionary.keys()}

    @staticmethod
    def list_expand(key, listing):
        return {f'{key}_{subkey}': [dic[subkey] for dic in listing] for subkey in listing[0]}

    def map_fields(self, sample):
        default_dict = defaultdict(lambda: None, sample)
        result = {}
        for key in self.types.keys():
            try:
                result[key] = eval(self.types[key])(default_dict[key])
            except:
                result[key] = default_dict[key]
        return result

    def expand_dict_before_inserting(self, sample):
        """Функция подготавливает батч данных перед"""
        return sample

    # TODO: Посмотреть как можно упростить этот подход к развертыванию json-а
    def prepare_sample(self, sample):
        """Функция разворачивает вложенные словари и складывает ключи через символ `_`."""
        dicts = dict(filter(lambda x: type(x[1]) is dict, sample.items()))
        while dict in map(type, sample.values()):
            for key, value in dicts.items():
                sample.update(self.dict_expand(key, value))
                sample.pop(key)
            dicts = dict(filter(lambda x: type(x[1]) is dict, sample.items()))

        lists = dict((filter(lambda x: type(x[1]) is list, sample.items())))
        while dict in map(lambda x: type(x[1][0]) if len(x[1]) != 0 else None, lists.items()):
            for key, value in lists.items():
                if value == []:
                    sample[key] = None
                else:
                    sample.update(self.list_expand(key, value))
                    sample.pop(key)
                lists = dict((filter(lambda x: type(x[1]) is list, sample.items())))

        return sample

    def parse_batch(self, batch):
        batch = map(self.prepare_sample, batch)
        batch = map(self.map_fields, batch)
        batch = tuple(batch)
        logger.info(f'batch with length {len(batch)} parsed successfully')
        return batch
