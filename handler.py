import logging

from os import getenv
from dotenv import load_dotenv


from connection_factory import ConnectionFactory
from extract_step import ExtractStep
from decode_step import DecodeStep
from parser_step import ParserStep
from map_step import MapStep
from database_step import DatabaseStep

from base import Base

logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s", level=logging.INFO)
load_dotenv()

TOPIC_NAME = getenv("TOPIC_NAME")

DB_USER = getenv("DB_USER")
DB_PASSWORD = getenv("DB_PASSWORD")
DB_HOST = getenv("DB_HOST")
DB_PORT = getenv("DB_PORT")
DB_NAME = getenv("DB_NAME")

connection_factory = ConnectionFactory(
    connection_config={
        "username": DB_USER,
        "password": DB_PASSWORD,
        "port": DB_PORT,
        "database": DB_NAME,
        "host": DB_HOST,
    }
)

connection = connection_factory.create_connection()

Base.metadata.create_all(connection)

extract = ExtractStep(queue=TOPIC_NAME)
decode = DecodeStep()
parser = ParserStep()
mapper = MapStep()
database = DatabaseStep(engine=connection)


def handler(event):
    try:
        logging.info("Started lambda for event: %s", event)

        values = extract.execute(event=event)
        logging.info("Values extracted from event %s", values)

        payloads = decode.execute(values=values)
        logging.info("Payloads decoded %s", payloads)

        roots = parser.execute(payloads=payloads)
        logging.info("parsed %s xmls", len(roots))

        entities = mapper.execute(roots=roots)
        logging.info("%s about to be inserted" % len(entities))

        database.execute(entities=entities)

    except ValueError as e:
        logging.error("ValueError encountered: %s", e)


handler(
    {
        "eventSource": "aws:kafka",
        "eventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
        "bootstrapServers": "b-2.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092",
        "records": {
            "RAW-ROSTER-JCT-int-0": [
                {
                    "topic": "RAW-ROSTER-JCT-int",
                    "partition": 0,
                    "offset": 15,
                    "timestamp": 1545084650987,
                    "timestampType": "CREATE_TIME",
                    "key": "abcDEFghiJKLmnoPQRstuVWXyz1234==",
                    "value": "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPGdldFJvc3RlcnNSZXNwb25zZSB4bWxucz0iaHR0cDovL3NlcnZpY2UuamNtcy5qZXBwZXNlbi5jb20vUm9zdGVyU3RhdGVPdXRwdXQvdjUuMSIgdmVyc2lvbj0iNS4xIgogICAgc291cmNlPSJSb3N0ZXJTdGF0ZU91dHB1dCIgY3JlYXRlZD0iMjAyNC0wNy0zMVQxNDozODoyNloiPgogICAgPHJvc3RlciBjcmV3SWQ9IjAxNzUzMSIgc3VyTmFtZT0iVGhvbXBzb24iIGdpdmVuTmFtZT0iS2FyZW4iIGhvbWVCYXNlPSJZVUwiIGNyZXdDYXRlZ29yeT0iRk9QUyIKICAgICAgICBlbXBsb3llcj0iQUMiIHN0YXJ0PSIyMDIzLTEwLTE5VDA3OjQ1OjAwWiIgZW5kPSIyMDIzLTEwLTIwVDAyOjA1OjAwWiI+CiAgICAgICAgPGFzc2lnbm1lbnQgcm9sZT0iQ0EiPgogICAgICAgICAgICA8dGFzawogICAgICAgICAgICAgICAgbmFtZT0ibmFtZSIKICAgICAgICAgICAgICAgIGFjdGl2aXR5VHlwZT0iYWN0aXZpdHlUeXBlIgogICAgICAgICAgICAgICAgYWN0aXZpdHlHcm91cD0iYWN0aXZpdHlHcm91cCIKICAgICAgICAgICAgICAgIGlzT25EdXR5PSJ0cnVlIgogICAgICAgICAgICAgICAgc3RhcnRUaW1lPSIyMDIzLTExLTE1VDA5OjAwOjAwWiIKICAgICAgICAgICAgICAgIHN0YXJ0VGltZU9mZnNldD0iLVBUNEgiCiAgICAgICAgICAgICAgICBlbmRUaW1lPSIxNjk3LTAyLTAxVDAwOjAwOjAwWiIKICAgICAgICAgICAgICAgIGVuZFRpbWVPZmZzZXQ9Ii1QVDRIIgogICAgICAgICAgICAgICAgaXNDYW5jZWxsZWQ9InRydWUiCiAgICAgICAgICAgICAgICBhY3Rpdml0eUNhdGVnb3J5PSJjYXQiCiAgICAgICAgICAgICAgICBpc1BlcnNvbmFsPSJ0cnVlIgogICAgICAgICAgICAgICAgc3RhdGlvbj0iQ08iCiAgICAgICAgICAgICAgICBlbmRTdGF0aW9uPSJQQVIiCiAgICAgICAgICAgICAgICB2ZW51ZT0idmVudWUiCiAgICAgICAgICAgID4KICAgICAgICAgICAgICAgIDxhbm5vdGF0aW9uPgogICAgICAgICAgICAgICAgICAgIDxuZWVkPgogICAgICAgICAgICAgICAgICAgICAgICA8cG9zaXRpb24gbmFtZT0iQ0EiIGNvdW50PSIxIiAvPgogICAgICAgICAgICAgICAgICAgICAgICA8cG9zaXRpb24gbmFtZT0iRk8iIGNvdW50PSIxIiAvPgogICAgICAgICAgICAgICAgICAgICAgICA8cG9zaXRpb24gbmFtZT0iQVAiIGNvdW50PSIxIiAvPgogICAgICAgICAgICAgICAgICAgICAgICA8cG9zaXRpb24gbmFtZT0iR0oiIGNvdW50PSIxIiAvPgogICAgICAgICAgICAgICAgICAgICAgICA8cG9zaXRpb24gbmFtZT0iR1kiIGNvdW50PSIxIiAvPgogICAgICAgICAgICAgICAgICAgICAgICA8cG9zaXRpb24gbmFtZT0iRkEiIGNvdW50PSIxIiAvPgogICAgICAgICAgICAgICAgICAgICAgICA8cG9zaXRpb24gbmFtZT0iU0QiIGNvdW50PSIxIiAvPgogICAgICAgICAgICAgICAgICAgIDwvbmVlZD4KICAgICAgICAgICAgICAgIDwvYW5ub3RhdGlvbj4KICAgICAgICAgICAgICAgIDx0cmFpbmluZ1RhZz5UcmFpbmluZzwvdHJhaW5pbmdUYWc+CiAgICAgICAgICAgICAgICA8dHJhaW5pbmdUYWc+VGFnczwvdHJhaW5pbmdUYWc+CiAgICAgICAgICAgICAgICA8YXNzaWduZWRUcmFpbmluZ1RhZz5hc3NpZ25lZDwvYXNzaWduZWRUcmFpbmluZ1RhZz4KICAgICAgICAgICAgICAgIDxhc3NpZ25lZFRyYWluaW5nVGFnPnRyYWluaW5nPC9hc3NpZ25lZFRyYWluaW5nVGFnPgogICAgICAgICAgICAgICAgPGFzc2lnbmVkVHJhaW5pbmdUYWc+dGFnczwvYXNzaWduZWRUcmFpbmluZ1RhZz4KICAgICAgICAgICAgPC90YXNrPgogICAgICAgIDwvYXNzaWdubWVudD4KICAgIDwvcm9zdGVyPgo8L2dldFJvc3RlcnNSZXNwb25zZT4=",
                    "headers": [],
                }
            ]
        },
    }
)
