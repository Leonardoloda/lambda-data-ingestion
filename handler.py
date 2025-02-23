import logging

from extract_step import ExtractStep
from decode_step import DecodeStep
from parser_step import ParserStep
from map_step import MapStep

logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s", level=logging.INFO)

extract = ExtractStep(queue="RAW-ROSTER-JCT-int-0")
decode = DecodeStep()
parser = ParserStep()
mapper = MapStep()


def handler(event={}):
    try:
        logging.info("Started lambda for event: %s", event)

        values = extract.execute(event=event)
        logging.info("Values extracted from event %s", values)

        payloads = decode.execute(values=values)
        logging.info("Payloads decoded %s", payloads)

        roots = parser.execute(payloads=payloads)
        logging.info("parsed %s xmls", len(roots))

        entities = mapper.execute(roots=roots)

    except ValueError as e:
        logging.error("ValueError encountered: %s", e)


handler()
