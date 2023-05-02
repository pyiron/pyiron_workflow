from abc import ABC, abstractmethod
from json import dumps

from IPython.display import JSON


class HasToDict(ABC):
    @abstractmethod
    def to_dict(self):
        pass

    def _repr_json_(self):
        return self.to_dict()

    def info(self):
        print(dumps(self.to_dict(), indent=2))

    def repr_json(self):
        return JSON(self.to_dict())
