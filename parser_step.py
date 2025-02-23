from xml.etree.ElementTree import fromstring

from step import Step

class ParserStep(Step):
    def execute(self, **kwargs) -> list:
        if "payloads" not in kwargs:
            raise ValueError("Missing argument")

        payloads = kwargs.get("payloads")

        trees = [fromstring(payload) for payload in payloads]

        return trees
