import datetime
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import Row


def rand_col(data_type, max_val=100000, choices=None, date_range=None):
    """
    Generates a spark column
    """
    parse_info = data_type.jsonValue()
    choices = choices if choices else ["item" + str(item) for item in range(100)]
    date_range = date_range if date_range else ["2018-01-01", "2020-12-31"]
    start_date, end_date = [
        datetime.datetime.strptime(x, "%Y-%m-%d")
        for x in date_range
    ]
    date_choices = [
        start_date + datetime.timedelta(days=x)
        for x in range((end_date - start_date).days + 1)
    ]

    if parse_info == "long" or parse_info == "integer":
        return f.round(f.rand() * max_val).cast(parse_info)
    elif parse_info == "float" or parse_info == "double":
        return (f.rand() * max_val).cast(parse_info)
    elif parse_info == "string":
        array_col = f.array([f.lit(x) for x in choices])
        return array_col[(f.rand() * f.size(array_col)).cast("int")]
    elif parse_info == "date":
        array_col = f.array([f.lit(x) for x in date_choices])
        return array_col[(f.rand() * f.size(array_col)).cast("int")]
    else:
        print(parse_info)
        raise Exception

def generate_data_from_schema(spark, spark_schema, rows=100):
    """
    Generates a dataframe from a schema specification
    """
    rows = [Row() for i in range(rows)]
    dataframe = spark.createDataFrame(rows, StructType([]))

    for item in spark_schema.fields:
        dataframe = dataframe.withColumn(item.name, rand_col(item.dataType))

    return dataframe