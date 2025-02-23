from logging import getLogger

from extract_step import ExtractStep
from decode_step import DecodeStep
from parser_step import ParserStep
from map_step import MapStep

logger = getLogger()

extract = ExtractStep(queue="RAW-ROSTER-JCT-int-0")
decode = DecodeStep()
parser = ParserStep()
mapper = MapStep()

def handler(event = {}):
    values = extract.execute(event=event)
    payloads = decode.execute(values=values)
    roots = parser.execute(payloads=payloads)
    entities = mapper.execute(roots=roots)

    print(entities)
