from abc import ABC, abstractmethod
from json import dumps


class HasToDict(ABC):
    @abstractmethod
    def to_dict(self):
        pass

    def _repr_json_(self):
        return self.to_dict()

    @property
    def info(self):
        print(dumps(self.to_dict(), indent=2))
