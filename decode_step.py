from base64 import b64decode

from step import Step

class DecodeStep(Step):
    def execute(self, **kwargs) -> list[str]:
        if "values" not in kwargs:
            raise ValueError("Missing values")

        values = kwargs.get("values")

        payloads = [b64decode(value).decode("ascii") for value in values]

        return payloads
