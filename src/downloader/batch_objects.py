class BatchResponse:

    def __init__(self, samples: list) -> None:
        self.samples = samples

    def __len__(self):
        return self.samples.__len__

    def __getitem__(self, items):
        return self.samples.__getitem__(items)

    def expand_before_inserting(self):
        """Метод для преподготовки батча данных перед вставкой в БД."""
        pass
