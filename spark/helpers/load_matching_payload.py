import logging
from pyspark.sql.functions import coalesce, lit, col

HVID = [
    'hvid',
    'parentId',
    'parentid'
]
DEFAULT_ATTRS = [
    'threeDigitZip',
    'yearOfBirth',
    'gender',
    'state',
    'age'
]


def load_matching_payload(runner, location, extra_cols):
    """
    Load matching data for a provider
    """

    # all keys needed from the payload
    total_keys = set(DEFAULT_ATTRS + extra_cols)

    raw_payload = runner.sqlContext.read.json(location)

    # fail if any requested column is missing from the payload
    for k in total_keys:
        if k not in raw_payload.columns:
            logging.warning("Column does not exist in payload: " + k)
            raw_payload = raw_payload.withColumn(k, lit(""))

    final_payload = raw_payload.select([
        coalesce(*HVID), *map(lambda x: col(x), total_keys)
    ])
    final_payload.registerTempTable("matching_payload")
