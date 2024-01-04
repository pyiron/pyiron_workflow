class DotDict(dict):
    def __getattr__(self, item):
        try:
            return self.__getitem__(item)
        except KeyError:
            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute '{item}'"
            )

    def __setattr__(self, key, value):
        self[key] = value

    def __dir__(self):
        return set(super().__dir__() + list(self.keys()))

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        for k, v in state.items():
            self.__dict__[k] = v

    def to_list(self):
        """A list of values (order not guaranteed)"""
        return list(self.values())
