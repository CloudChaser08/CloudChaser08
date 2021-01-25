import argparse
import pyspark.sql.functions as FN
import spark.helpers.payload_loader as payload_loader

from spark.runner import Runner
from spark.spark_setup import init
from pyspark.sql import Window


def get_enterprise_id(filename):
    filename = filename.replace('.xwalk', '')
    filename = filename.replace('NG_HV', 'HV_LSSA')
    return filename.split('/')[-1].split('.')[0].split('_')[2]


def get_batch_date(filename):
    filename = filename.replace('.xwalk', '')
    filename = filename.replace('NG_HV', 'HV_LSSA')
    return filename.split('/')[-1].split('.')[0].split('_')[3]


def run(spark, runner):
    source_1 = 's3://salusv/matching/payload/emr/nextgen/{2017/12,2018/01,2018/02,2018/03,2018/04,2018/05}/*/recurring/'
    source_2 = 's3://salusv/matching/payload/emr/nextgen/2018/*/*/crosswalk/'
    payload_df = payload_loader.load(runner, source_1, return_output=True, load_file_name=True)\
        .union(payload_loader.load(runner, source_2, return_output=True, load_file_name=True))

    enterprise_udf = FN.udf(get_enterprise_id)
    batch_date_udf = FN.udf(get_batch_date)

    # We will use this window to make sure we select the HVID-NGID pair
    # that had the highest matchScore.
    window = Window.orderBy('batch_date').partitionBy('nextgen_id')

    payload_df.select('hvid', 'hvJoinKey', 'input_file_name') \
              .withColumn('enterprise_id', enterprise_udf(FN.col('input_file_name'))) \
              .withColumn('batch_date', batch_date_udf(FN.col('input_file_name'))) \
              .withColumn('nextgen_id', FN.concat_ws('_', FN.lit('118'), FN.col('enterprise_id'), FN.col('hvJoinKey'))) \
              .select(['hvid', 'batch_date', 'nextgen_id']) \
              .withColumn('next_date', FN.lead('batch_date', 1).over(window)) \
              .where(FN.isnull('next_date')) \
              .drop('next_date', 'batch_date') \
              .where(~FN.isnull('hvid')) \
              .createOrReplaceTempView('nextgen_crosswalk')


def main(args):
    # init
    spark, sql_context = init("Nextgen EMR Crosswalk")

    # initialize runner
    runner = Runner(sql_context)

    run(spark, runner)

    sql_context.sql('select * from nextgen_crosswalk') \
              .repartition(1000) \
              .write.parquet(args.dest)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dest', type=str)
    args = parser.parse_args()
    main(args)
