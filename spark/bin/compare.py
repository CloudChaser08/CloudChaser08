from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number 
from pyspark.sql.window import Window
import argparse
import os

""" Compare two datasets stored in s3. 

This script loads source and target data sets, specified as an s3 path, into spark dataframes. 
The steps include:
    1. Compare schemas (column name, type)
    2. Drop user specified columns 
    3. Noramlize tables to have same schema
    4. Compare row counts. 
    5. Compare table contents. 

This script is run as a spark-job via spark-submit. e.g.

$ spark-submit compare.py path1 path2 --drop colA, colB, colC
"""

# Parse arguments. 
parser = argparse.ArgumentParser()
parser.add_argument("src_loc", type=str, help="source location")
parser.add_argument("trg_loc", type=str, help="target location")
parser.add_argument("--drop", type=str, nargs='+', help="the list of columns to drop from both tables")
parser.add_argument("--local", type=bool, help="local mode")
args = parser.parse_args()

src_loc = args.src_loc 
trg_loc = args.trg_loc
local = args.local or True
drop = args.drop or []  

# Initialize Spark Session.
spark = SparkSession.builder \
			.master("local[*]" if local else "yarn") \
			.appName("Compare DataFrames") \
			.getOrCreate()


# Load source and target data.
# TODO: add support for csv, psv, json, ect..
source = spark.read.parquet(src_loc)
target = spark.read.parquet(trg_loc)

# Compare schemas.
#
# We only need to verify that the target schema is a subset of the source schema. 
# We check both column names and types. 
#
for col in target.dtypes:
	if col not in source.dtypes:
		print("Compare Schemas: FAIL")
		os.exit(1)
else:
	print("Compare Schemas: OK")

# Drop user specified columns from both source and target.
# 
# Certain columns may never match, e.g. creation_date. so here we drop those columns
# 
for col in drop: 
    source = source.drop(drop)
    target = target.drop(drop)

# Normalize source table.
#
# Drop columns from source that aren't in target, so we can compare. 
# 
for col in target.columns:
    if col not in target.columns:
        source = source.drop(col)

# Compare row counts.
source_row_count = source.count()
target_row_count = target.count()
print("Source row count: " + str(source_row_count))
print("Target row count: " + str(target_row_count))
if (source_row_count != target_row_count): 
	print("Row Count Compare: FAIL")
else: 
	print("Row Count Compare: OK")

# Compare duplicate counts.
source_unique_row_count = source.distinct().count()
target_unique_row_count = target.distinct().count()
print("Source Duplicate Row Count: " + str(source_row_count - source_unique_row_count))
print("Target Duplicate Row Count: " + str(target_row_count - target_unique_row_count))

# Show percentage match.
#
# Since df.intersect(df2) only collects the distinct elements in common, first we add a row count to each
# duplicate, so that way we include those duplicates. 
#
source = source.withColumn("row_count", row_number().over(Window.partitionBy(source.columns).orderBy(source.columns)))
target = target.withColumn("row_count", row_number().over(Window.partitionBy(target.columns).orderBy(target.columns)))
intersection = source.intersect(target)

source_percent_match = intersection.count() / float(source.count())
target_percent_match = intersection.count() / float(target.count())
print("Source percent match: " + str(source_percent_match))
print("Target percent match: " + str(target_percent_match))
if source_percent_match != 1.0 and \
   target_percent_match != 1.0:
   print("Data Comparison: FAIL")
else: 
   print("Data Comparison: OK")



