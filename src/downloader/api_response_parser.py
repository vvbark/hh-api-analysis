import logging

from collections import defaultdict

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class APIResponseParser:
    def __init__(self, types):
        self.types = types

    @staticmethod
    def type_transformation(type_):
        def func(feature):
            if feature != None:
                return type_(feature)
            return feature
        return func

    @staticmethod
    def __dict_expand(key, dictionary):
        return {f'{key}_{subkey}': dictionary.get(subkey) for subkey in dictionary.keys()}

    @staticmethod
    def __list_expand(key, listing):
        return {f'{key}_{subkey}': [dic[subkey] for dic in listing] for subkey in listing[0]}

    def _map_fields(self, sample):
        default_dict = defaultdict(lambda: None, sample)
        return {
            key: self.type_transformation(
                self.types[key])(default_dict[key]
            ) for key in self.types.keys()
        }

    def _expand_dict_before_inserting(self, sample):
        """Функция подготавливает батч данных перед вставкой в бд"""
        return sample

    # TODO: Посмотреть как можно упростить этот подход к развертыванию json-а
    def _prepare_sample(self, sample):
        """Функция разворачивает вложенные словари и складывает ключи через символ `_`."""
        dicts = dict(filter(lambda x: type(x[1]) is dict, sample.items()))
        while dict in map(type, sample.values()):
            for key, value in dicts.items():
                sample.update(self.__dict_expand(key, value))
                sample.pop(key)
            dicts = dict(filter(lambda x: type(x[1]) is dict, sample.items()))

        lists = dict((filter(lambda x: type(x[1]) is list, sample.items())))
        while dict in map(lambda x: type(x[1][0]) if len(x[1]) != 0 else None, lists.items()):
            for key, value in lists.items():
                if value == []:
                    sample[key] = None
                else:
                    sample.update(self.__list_expand(key, value))
                    sample.pop(key)
                lists = dict((filter(lambda x: type(x[1]) is list, sample.items())))

        return sample

    def parse_batch(self, batch):
        """Основной метод для """
        batch = map(self._prepare_sample, batch)
        batch = map(self._map_fields, batch)
        batch = map(self._expand_dict_before_inserting, batch)
        batch = tuple(batch)
        logger.info(f'batch with length {len(batch)} parsed successfully')
        return batch
