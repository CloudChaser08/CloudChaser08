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


def load(runner, location, extra_cols=[]):
    """
    Load matching data for a provider
    """

    # all keys needed from the payload
    total_attrs = set(DEFAULT_ATTRS + extra_cols)
    total_cols = set(list(total_attrs) + HVID)

    raw_payload = runner.sqlContext.read.json(location)

    # fail if any requested column is missing from the payload
    for k in total_cols:
        if k not in raw_payload.columns:
            logging.warning("Column does not exist in payload: " + k)
            raw_payload = raw_payload.withColumn(k, lit(""))

    final_payload = raw_payload.select(
        [coalesce(*HVID).alias('hvid')] + map(lambda x: col(x), total_attrs)
    )

    runner.sqlContext.sql('DROP TABLE IF EXISTS matching_payload')
    final_payload.registerTempTable("matching_payload")
