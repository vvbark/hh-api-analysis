class BatchResponse:

    def __init__(self, batch: list) -> None:
        self.batch = tuple(batch)
        self._iterator = None

    def __len__(self):
        return self.batch.__len__

    def __getitem__(self, items):
        return self.batch.__getitem__(items)

    def __iter__(self):
        return iter(self.batch)

    def map(self, func: function) -> None:
        if not self._iterator:
            self._iterator = map(func, self.batch)
        else:
            self._iterator = map(func, self._iterator)

    def applymap(self) -> None:
        if self._iterator:
            raise AttributeError('Oops. Prepare map functions first. Look BatchResponse.map()')
        else:
            self.batch = tuple(self._iterator)

    def apply(self, func: function) -> None:
        self.batch = tuple(map(func, self.batch))

    def prepare_before_inserting(self):
        """Метод для преподготовки батча данных перед вставкой в БД."""
        pass
