from pyspark.sql.types import StructType, StructField, StringType
import spark.helpers.postprocessor as postprocessor
import pyspark.sql.functions as F
import logging

def load(runner, location, columns=None, file_type=None, delimiter=',', header=False,
         schema=None, source_table_conf=None, load_file_name=False, file_name_col='input_file_name',
         spark_context=None, confirm_schema=False):
    """
    Load transaction data for a provider
    """
    if source_table_conf is not None:
        file_type = source_table_conf.file_type

    if file_type == 'parquet':
        df = runner.sqlContext.read.parquet(location)
        if load_file_name:
            df = df.withColumn(file_name_col, F.input_file_name())
        cols = [
            df[c.name].cast('string').alias(c.name) if c.dataType.typeName() == 'binary' else df[
                c.name] for c in df.schema]
        df = df.select(cols)
        return df

    if source_table_conf is not None:
        schema = source_table_conf.schema
        delimiter = source_table_conf.separator
        confirm_schema = source_table_conf.confirm_schema
        if columns is None:
            columns = source_table_conf.columns

    if schema is None:
        schema = StructType([StructField(c, StringType(), True) for c in columns])

    if file_type == 'csv':
        df = runner.sqlContext.read.csv(location, schema=schema, sep=delimiter, header=header)
        temp_df = runner.sqlContext.read.csv(location, sep=delimiter, header=header)
        if len(temp_df.schema) > len(schema):
            raise Exception(
                "Error: Table {} - Number of columns in data file ({}) exceeds expected schema ({})".format(
                    location, len(temp_df.schema), len(schema)
                )
            )
        if confirm_schema:
            if len(temp_df.schema) < len(schema):
                raise Exception(
                    "Error: Table {} - Number of columns in data file ({}) is less than expected schema ({})".format(
                        location, len(temp_df.schema), len(schema)
                    )
                )

    elif file_type == 'orc':
        df = runner.sqlContext.read.schema(schema).orc(location)
    elif file_type == 'json':
        df = runner.sqlContext.read.schema(schema).json(location)
    elif file_type == 'fixedwidth':
        # fixed width start position from position 1
        df = runner.sqlContext.read.text(location)
        cols = []
        start_pos = 1
        for col, width in columns:
            cols.append(df.value.substr(start_pos, width).alias(col))
            start_pos += width
        df = df.select(*cols)
    else:
        raise ValueError("Unsupported file type: {}".format(file_type))

    if load_file_name:
        df = df.withColumn(file_name_col, F.input_file_name())

    return df


# Simple way to load all transaction tables without writing any additional code
# so long as the follow all our conventions
# DEPRECATED in favor of load_and_clean_all_v2
def load_and_clean_all(runner, location_prefix, transactions_module, file_type, delimiter=',', header=False, partitions=0):
    logging.warn("load_and_clean_all is deprecated in favor of load_and_clean_all_v2")
    for table in transactions_module.TABLES:
        loc = location_prefix if len(transactions_module.TABLES) == 1 else location_prefix + table
        df = load(runner, loc, transactions_module.TABLE_COLUMNS[table], file_type, delimiter, header)

        if partitions > 0:
            df = df.repartition(partitions)
        postprocessor \
            .compose(postprocessor.trimmify, postprocessor.nullify)(df) \
            .cache_and_track(table) \
            .createOrReplaceTempView(table)


def load_and_clean_all_v2(runner,
                          location_prefix,
                          transactions_module,
                          partitions=0,
                          load_file_name=False,
                          file_name_col='input_file_name',
                          cache_tables=True,
                          spark_context=None):
    for table in transactions_module.TABLE_CONF:
        loc = location_prefix if len(transactions_module.TABLE_CONF) == 1 else location_prefix + table
        conf = transactions_module.TABLE_CONF[table]
        df = load(runner, loc, source_table_conf=conf, load_file_name=load_file_name,
                  file_name_col=file_name_col, spark_context=spark_context)

        if partitions > 0:
            df = df.repartition(partitions)

        if conf.trimmify_nullify and cache_tables:
            df = (postprocessor
                  .compose(postprocessor.trimmify, postprocessor.nullify)(df)
                  .cache_and_track(table))
        elif conf.trimmify_nullify and not cache_tables:
            df = (postprocessor
                  .compose(postprocessor.trimmify, postprocessor.nullify)(df))
        elif cache_tables:
            df = df.cache_and_track(table)

        df.createOrReplaceTempView(table)
