import logging

from os import getenv
from dotenv import load_dotenv

from connection_factory import ConnectionFactory
from extract_step import ExtractStep
from decode_step import DecodeStep
from parser_step import ParserStep
from map_step import MapStep

logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s", level=logging.INFO)
load_dotenv()

TOPIC_NAME = getenv("TOPIC_NAME")
DB_USER = getenv("DB_USER")
DB_PASSWORD = getenv("DB_PASSWORD")
DB_HOST = getenv("DB_HOST")
DB_PORT = getenv("DB_PORT")
DB_NAME = getenv("DB_NAME")

connection_factory = ConnectionFactory(
    username=DB_USER, password=DB_PASSWORD, port=DB_PORT, database=DB_NAME, host=DB_HOST
)

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
