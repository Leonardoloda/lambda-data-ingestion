from step import Step


class DatabaseStep(Step):
    def __init__(self, engine):
        self.engine = engine

    def execute(self, **kwargs):
        if "entities" not in kwargs:
            raise ValueError("Missing entities")
