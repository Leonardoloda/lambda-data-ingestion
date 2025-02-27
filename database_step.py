from sqlalchemy import Engine
from sqlalchemy.orm import Session

from step import Step


class DatabaseStep(Step):
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def execute(self, **kwargs):
        if "entities" not in kwargs:
            raise ValueError("Missing entities")

        entities = kwargs.get("entities")

        with Session(self.engine) as session:
            session.add_all(entities)

            session.commit()
