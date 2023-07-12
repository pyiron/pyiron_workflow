class DotDict(dict):
    def __getattr__(self, item):
        return self.__getitem__(item)

    def __setattr__(self, key, value):
        self[key] = value

    def __dir__(self):
        return set(super().__dir__() + list(self.keys()))
