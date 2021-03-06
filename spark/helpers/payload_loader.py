"""payload loader default schema"""
import logging
from pyspark.sql.functions import coalesce, col, input_file_name
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
    'age',
    'patientId',
    'recordId',
    'personId',
    'claimId',
    'hvJoinKey',
    'matchStatus',
    'flexibleMatchingUsed',
    'topCandidatesConfidence'
]


def load(runner, location, extra_cols=None, table_name='matching_payload', return_output=False,
         partitions=200, cache=False, load_file_name=False, allow_empty=False):
    """
    Load matching data for a provider
    """

    if extra_cols is None:
        extra_cols = []

    # all keys needed from the payload
    total_attrs = set(DEFAULT_ATTRS + extra_cols)

    try:
        raw_payload = runner.sqlContext.read.json(location)
    except Exception as e:
        if allow_empty:
            raw_payload = runner.sqlContext.createDataFrame([()], [])
        else:
            raise(e)

    # log any requested column that is missing from the payload
    for k in total_attrs:
        if k.lower() not in [_col.lower() for _col in raw_payload.columns]:
            logging.warning("Column does not exist in payload: " + k)
            raw_payload = postprocessor.add_null_column(k)(raw_payload)

    # remove hvid columns missing from the payload
    relevant_hvid_columns = [c for c in HVID if c in raw_payload.columns]

    if not relevant_hvid_columns:
        logging.warning("No HVID columns found in this payload.")
        final_payload = postprocessor.add_null_column('hvid')(raw_payload)
    else:
        cols_to_select = [
            coalesce(*[col(x) for x in relevant_hvid_columns]).alias('hvid')
        ] + [col(x) for x in total_attrs]
        final_payload = raw_payload.select(cols_to_select)

    if load_file_name:
        final_payload = final_payload.withColumn('input_file_name', input_file_name())

    final_payload = final_payload.repartition(partitions)
    if cache:
        final_payload = final_payload.cache_and_track(table_name)

    if return_output:
        return final_payload
    else:
        runner.sqlContext.sql('DROP TABLE IF EXISTS {}'.format(table_name))
        final_payload.createOrReplaceTempView(table_name)


def load_all(runner, location_prefix, matching_payloads_module):
    """
    Load all the matching payload tables specified in the module
    """
    table_conf = matching_payloads_module.TABLE_CONF
    for table in table_conf:
        if len(table_conf) == 1:
            loc = location_prefix
            table_name = 'matching_payload'
        else:
            loc = location_prefix + table
            table_name = 'matching_payload_' + table

        load(runner, loc, extra_cols=table_conf[table].extra_columns, table_name=table_name,
             partitions=5000, cache=True, load_file_name=True)
