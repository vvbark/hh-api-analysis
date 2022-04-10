class BatchResponse:

    def __init__(self, batch):
        self.batch = tuple(batch)
        self._iterator = None

    def __str__(self):
        return self.batch.__str__()
    
    def __repr__(self):
        return self.batch.__repr__()
    
    def __len__(self):
        return len(self.batch)

    def __getitem__(self, items):
        return self.batch.__getitem__(items)

    def __iter__(self):
        return iter(self.batch)

    def map(self, func: object):
        if not self._iterator:
            self._iterator = map(func, self.batch)
        else:
            self._iterator = map(func, self._iterator)

    def applymap(self):
        if not self._iterator:
            raise AttributeError('Oops. Prepare map functions first. Look BatchResponse.map()')
        else:
            return self.__class__(tuple(self._iterator))
    
    def apply(self, func: object):
        self.map(func)
        return self.applymap()

    def prepare_before_inserting(self):
        """Метод для преподготовки батча данных перед вставкой в БД."""
        pass
