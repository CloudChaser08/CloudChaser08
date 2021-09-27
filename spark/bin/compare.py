"""
compare
"""
import argparse
import sys

from pyspark.sql.functions import row_number 
from pyspark.sql.window import Window

from spark.spark_setup import init

""" Compare two datasets stored in s3. 

    :Example: 
    $ spark-submit compare.py path1 path2 --drop colA, colB, colC
"""


def compare_schemas(source, target): 
    """ Compare schemas of two dataframes. 
        
        Check that the target schema is a subset of the source schema. 
        We check both column names and types.

        :type source: pyspark.sql.DataFrame
        :type target: pyspark.sql.DataFrame
        :rtype: boolean  
    """
    for column in target.dtypes:
        if column not in source.dtypes:
            return False
    else:
        return True


def normalize_schemas(source, target):
    """ Normalize schemas of source and target dataframes. 

    Convert the source schema to match the target by dropping
    any columns from the source that are not present in the target.

    :type source: pyspark.sql.DataFrame
    :type target: pyspark.sql.DataFrame
    :rtype: pyspark.sql.DataFrame
    """
    for column in target.columns:
        if column not in target.columns:
            source = source.drop(column)
    return source


def percent_match(source, target):
    """ Calculate the percentage of rows that match.

    Label rows with a row count for unique elements to preserve duplicates. 

    :Example: 
        Row('John', 'Smith', row_count=1)
        Row('Jane', 'Doe',   row_count=1)
        Row('Mike', 'Snow',  row_count=1)
        Row('John', 'Smith', row_count=2)
        Row('John', 'Smith', row_count=3)
        Row('Jane', 'Doe',   row_count=2)
    
    Next, perform an intersection - collecting the unique elements in common to both source and target. 

    :return: intersection.count() / source.count()
    :return: intersection.count() / target.count()

    :type source: pyspark.sql.DataFrame
    :type target: pyspark.sql.DataFrame
    :rtype: (float, float)
    """
    source = \
        source.withColumn("row_count", row_number().over(Window.partitionBy(source.columns).orderBy(source.columns)))
    target = \
        target.withColumn("row_count", row_number().over(Window.partitionBy(target.columns).orderBy(target.columns)))
    intersection = source.intersect(target)
    source_percent_match = intersection.count() / float(source.count())
    target_percent_match = intersection.count() / float(target.count())
    return source_percent_match, target_percent_match


if __name__ == '__main__':

    # Parse arguments. 
    parser = argparse.ArgumentParser()
    parser.add_argument("src_loc", type=str, help="source location")
    parser.add_argument("trg_loc", type=str, help="target location")
    parser.add_argument("--drop", type=str, nargs='+', help="the list of columns to drop from "
                                                            "both tables")
    args = parser.parse_known_args()[0]

    # initialize spark context.
    spark, sqlContext = init("Comparison")

    # load data into dataframes.
    in_source = spark.read.parquet(args.src_loc)
    in_target = spark.read.parquet(args.trg_loc)

    # Drop columns
    if args.drop: 
        for col in args.drop:
            in_source = in_source.drop(col)
            in_target = in_target.drop(col)
    
    # compare schemas.
    schema_match = compare_schemas(in_source, in_target)
    if schema_match:
        print("Schema Match: OK")
    else: 
        print("Schema Match: FAIL")
        sys.exit(-1)

    # normalize source schema.
    in_source = normalize_schemas(in_source, in_target)

    # Compare row counts.
    source_row_count = in_source.count()
    target_row_count = in_target.count()
    print("Source row count: " + str(source_row_count))
    print("Target row count: " + str(target_row_count))

    # compare duplicate counts.
    source_unique_row_count = in_source.distinct().count()
    target_unique_row_count = in_target.distinct().count()
    print("Source Duplicate Row Count: " + str(source_row_count - source_unique_row_count))
    print("Target Duplicate Row Count: " + str(target_row_count - target_unique_row_count))

    # compare data content.
    src_match, trg_match = percent_match(in_source, in_target)
    print("Source Table Match: " + str(src_match))
    print("Target Table Match: " + str(trg_match))
    if src_match != 1.0 or trg_match != 1.0:
        print("Compare: FAIL")
    else:
        print("Compare: OK")
