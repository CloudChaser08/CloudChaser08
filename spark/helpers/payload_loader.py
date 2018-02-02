import logging
from pyspark.sql.functions import coalesce, col
import spark.helpers.postprocessor as postprocessor

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


def load(runner, location, extra_cols=None, table_name='matching_payload'):
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
            raw_payload = postprocessor.add_null_column(k)(raw_payload)

    # remove hvid columns missing from the payload
    relevant_hvid_columns = filter(lambda c: c in raw_payload.columns, HVID)

    if not relevant_hvid_columns:
        logging.warning("No HVID columns found in this payload.")
        final_payload = postprocessor.add_null_column('hvid')(raw_payload)
    else:
        final_payload = raw_payload.select([
            coalesce(*map(lambda x: col(x), relevant_hvid_columns)).alias('hvid')
        ] + map(lambda x: col(x), total_attrs))

    runner.sqlContext.sql('DROP TABLE IF EXISTS {}'.format(table_name))
    final_payload.registerTempTable(table_name)
