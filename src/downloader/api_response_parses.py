import logging
import mypy.api

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from datetime import datetime

from .batch_objects import BatchResponse
from .config import VACANCY_TYPES


logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class APIResponseParser(meta=ABCMeta):

    def __init__(self, types: dict = VACANCY_TYPES) -> None:
        self.types = types
        self.fields = tuple(types.keys())

    def map_fields(self, sample: dict) -> dict:
        temp_default_dict = defaultdict(lambda: None, sample)
        return {key: d[key] for key in fields}

    @staticmethod
    def check_type(value, typ: str) -> bool:
        program_text = 'from typing import *; v: {} = {}'.format(typ, repr(value))
        _, _, exit_code = mypy.api.run(['-c', program_text])
        return exit_code == 0

    @staticmethod
    def dict_expand(key: str, dictionary: dict) -> dict:
        return {f'{key}_{subkey}': dictionary.get(subkey) for subkey in dictionary.keys()}

    @staticmethod
    def list_expand(key: str, listing: dict) -> dict:
        return {f'{key}_{subkey}': [dic[subkey] for dic in listing] for subkey in listing[0]}

    @abstractmethod
    def prepare_sample(self, sample: dict) -> dict:
        pass

    @abstractmethod
    def check_sample(self, sample: dict) -> dict:
        pass

    def parse_batch(self, batch: BatchResponse) -> BatchResponse:
        batch.map(prepare_sample)
        batch.map(check_sample)
        batch.applymap()
        return batch


class VacanciesAPIResponseParser(APIResponseParser):

    DATE_REGEX = '%Y-%m-%dT%X%z'

    # TODO: Посмотреть как можно упростить этот подход к развертыванию json-а
    def prepare_sample(self, sample: dict) -> dict:
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

        sample = self.map_fields(sample)
        return sample

    def check_sample(self, sample: dict) -> dict:
        """Функция берет словарь и прогоняет его через маппинг типов с проверкой."""
        for key, dict_value in sample.items():
            if dict_value == None:
                continue

            if not self.check_type(dict_value, self.types[key]):
                logger.error(f'Parsing failed on type checking. Type of `{key}` field: {type(dict_value)} != {type(self.types[key])}.')
                raise TypeError('Fields does not match.')
