import logging
from pyspark.sql.functions import coalesce, lit, col

HVID = [
    'parentId',
    'parentid',
    'hvid'
]
DEFAULT_ATTRS = [
    'isInvalid',
    'threeDigitZip',
    'yearOfBirth',
    'gender',
    'state',
    'age'
]


def load(runner, location, extra_cols=None):
    """
    Load matching data for a provider
    """

    if extra_cols is None:
        extra_cols = []

    # all keys needed from the payload
    total_attrs = set(DEFAULT_ATTRS + extra_cols)

    raw_payload = runner.sqlContext.read.json(location)

    # log any requested column that is missing from the payload
    for k in total_attrs:
        if k not in raw_payload.columns:
            logging.warning("Column does not exist in payload: " + k)
            raw_payload = raw_payload.withColumn(k, lit(None))

    # remove hvid columns missing from the payload
    global HVID
    HVID = filter(lambda c: c in raw_payload.columns, HVID)

    if not HVID:
        logging.warning("No HVID columns found in this payload.")

    final_payload = raw_payload.select(
        (
            [coalesce(*map(lambda x: col(x), HVID)).alias('hvid')]
            if HVID else [lit(None).alias('hvid')]
        ) + map(lambda x: col(x), total_attrs)
    )

    runner.sqlContext.sql('DROP TABLE IF EXISTS matching_payload')
    final_payload.registerTempTable("matching_payload")
