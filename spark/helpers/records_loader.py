from pyspark.sql.types import StructType, StructField, StringType

def load(runner, location, columns, file_type, delimiter=',', header=False):
    """
    Load transaction data for a provider
    """

    schema = StructType([StructField(c, StringType(), True) for c in columns])

    if file_type == 'csv':
        df = runner.sqlContext.read.csv(location, schema=schema, sep=delimiter, header=header)
        df = df or runner.sqlContext([], schema=schema)
        return df
