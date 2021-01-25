import argparse

from spark.runner import Runner
from spark.spark_setup import init
from pyspark.sql import Window
from pyspark.sql.functions import col
import spark.helpers.payload_loader as payload_loader


def run(spark, runner, nextgen_source, crosswalk_source):
    nextgen_df = runner.sqlContext.read.parquet(nextgen_source)
    crosswalk_df = runner.sqlContext.read.parquet(crosswalk_source).withColumnRenamed('hvid', 'crosswalk_hvid')

    nextgen_df.join(crosswalk_df, nextgen_df.hvid == crosswalk_df.nextgen_id, 'inner') \
              .withColumn('hvid', col('crosswalk_hvid')) \
              .drop('crosswalk_hvid', 'nextgen_id') \
              .createOrReplaceTempView('new_nextgen_output')


def main(args):
    # init
    spark, sqlContext = init("Nextgen EMR Crosswalk")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.nextgen_source, args.crosswalk_source)

    sqlContext.sql('select * from new_nextgen_output') \
              .repartition(20) \
              .write.parquet(args.nextgen_output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--nextgen_source', type=str)
    parser.add_argument('--crosswalk_source', type=str)
    parser.add_argument('--nextgen_output', type=str)
    args = parser.parse_args()
    main(args)

