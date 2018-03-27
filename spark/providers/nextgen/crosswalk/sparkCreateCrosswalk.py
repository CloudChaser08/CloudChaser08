import argparse

from spark.runner import Runner
from spark.spark_setup import init
from pyspark.sql import Window
from pyspark.sql.functions import lit, col, concat_ws, input_file_name, udf, isnull, lead
import spark.helpers.payload_loader as payload_loader

def get_enterprise_id(filename):
    return filename.split('/')[-1].split('_')[2]


def run(spark, runner, source):
    payload_df = payload_loader.load(runner, source, ['hvJoinKey', 'matchScore'], return_output=True)
    
    enterprise_udf = udf(get_enterprise_id)

    # We will use this window to make sure we select the HVID-NGID pair
    # that had the highest matchScore.
    window = Window.orderBy('matchScore').partitionBy('hvJoinKey')

    payload_df.select(['hvid', 'hvJoinKey', 'matchScore']) \
              .withColumn('enterprise_id', enterprise_udf(input_file_name())) \
              .withColumn('nextgen_id', concat_ws('_', lit('NG'), col('enterprise_id'), col('hvJoinKey'))) \
              .withColumn('next_match_score', lead('matchScore', 1).over(window)) \
              .where(isnull('next_match_score')) \
              .drop('next_match_score') \
              .select(['hvid', 'nextgen_id']) \
              .createOrReplaceTempView('nextgen_crosswalk')


def main(args):
    # init
    spark, sqlContext = init("Nextgen EMR Crosswalk")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.source)

    sqlContext.sql('select * from nextgen_crosswalk') \
              .repartition(1000) \
              .write.parquet(args.dest)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=str)
    parser.add_argument('--dest', type=str)
    args = parser.parse_args()
    main(args)

