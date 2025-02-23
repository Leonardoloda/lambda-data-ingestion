from abc import ABC, abstractmethod

class Step(ABC):
    next_step = None

    @abstractmethod
    def execute(self, **kwargs: dict):
        pass

    def next(self, step) -> None:
        self.next_step = step
