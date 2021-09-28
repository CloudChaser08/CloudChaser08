"""
careset schema
"""
from pyspark.sql.functions import col, lit
import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader


def load(runner, input_path_prefix):
    """
    Load in the transactions to an in memory table
    """
    table = 'careset_transactions'
    columns = ['npi_class', 'npi', 'cpt_code', 'patient_count', 'claim_count']
    df = records_loader.load(runner, input_path_prefix, columns, 'csv', ',')

    postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
        )(df).where((col('patient_count') != lit('0')) | (col('claim_count') != lit('0'))) \
             .createOrReplaceTempView(table)
