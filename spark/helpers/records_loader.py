from pyspark.sql.types import StructType, StructField, StringType
import spark.helpers.postprocessor as postprocessor

def load(runner, location, columns, file_type, delimiter=',', header=False):
    """
    Load transaction data for a provider
    """

    schema = StructType([StructField(c, StringType(), True) for c in columns])

    if file_type == 'csv':
        df = runner.sqlContext.read.csv(location, schema=schema, sep=delimiter, header=header)
    elif file_type == 'orc':
        df = runner.sqlContext.read.schema(schema).orc(location)
    else:
        raise ValueError("Unsupported file type: {}".format(file_type))

    return df

# Simple way to load all transaction tables without writing any additional code
# so long as the follow all our conventions
def load_and_clean_all(runner, location_prefix, transactions_module, file_type, delimiter=',', header=False, partitions=0):
    for table in transactions_module.TABLES:
        loc = location_prefix if len(transactions_module.TABLES) == 1 else location_prefix + table
        df = load(runner, loc, transactions_module.TABLE_COLUMNS[table], file_type, delimiter, header)

        if partitions > 0:
            df = df.repartition(partitions)

        postprocessor \
            .compose(postprocessor.trimmify, postprocessor.nullify)(df) \
            .cache_and_track(table) \
            .createOrReplaceTempView(table)
