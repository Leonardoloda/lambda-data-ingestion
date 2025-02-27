from step import Step


class ExtractStep(Step):

    def __init__(self, queue: str) -> None:
        super().__init__()

        self.queue = queue

    def execute(self, **kwargs) -> list[str]:
        if "event" not in kwargs:
            raise ValueError("Missing 'event' in kwargs")

        event = kwargs["event"]

        if "records" not in event:
            raise ValueError("Missing records from event")

        records = event.get("records")

        if self.queue not in records:
            raise ValueError(f"Wrong queue configured {self.queue}")

        topic_records = records.get(self.queue)

        values = [topic_records[i].get("value") for i in range(len(topic_records))]

        if len(values) == 0:
            raise ValueError("Not a valid value fount")

        return values
