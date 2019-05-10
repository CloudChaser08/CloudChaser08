#! /usr/bin/python
import argparse
from datetime import datetime
import dateutil.tz as tz
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.records_loader as records_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.constants as constants
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
import pyspark.sql.functions as F
import transactions_lhv1, transactions_lhv2
import spark.helpers.schema_enforcer as schema_enforcer
import subprocess
import json

VALID_MANUFACTURERS = [
        m.lower() for m in 
        ['Amgen', 'Novartis', 'Lilly']
    ]

def run(spark, runner, group_id, run_version, test=False, airflow_test=False):
    if test:
        incoming_path = file_utils.get_abs_path(
            __file__, '../../../test/providers/liquidhub/custom/resources/incoming/'
        ) + '/{}/'.format(group_id)
        matching_path = file_utils.get_abs_path(
            __file__, '../../../test/providers/liquidhub/custom/resources/matching/'
        ) + '/{}/'.format(group_id)
        output_dir = '/tmp/staging/' + group_id + '/'
    elif airflow_test:
        matching_path = 's3a://salusv/testing/dewey/airflow/e2e/lhv2/custom/payload/{}/'.format(group_id)
        matching_path = 's3a://salusv/testing/dewey/airflow/e2e/lhv2/custom/incoming/{}/'.format(group_id)
        output_dir = '/tmp/staging/' + group_id + '/'
    else:
        incoming_path = 's3a://salusv/incoming/custom/lhv2/{}/'.format(group_id)
        matching_path = 's3a://salusv/matching/payload/custom/lhv2/{}/'.format(group_id)
        output_dir = constants.hdfs_staging_dir + group_id + '/'

    # Possible group id patterns
    # LHV1_<source>_PatDemo_YYYYMMDD_v#
    # LHV1_<manufacturer>_<source>_YYYYMMDD_v#
    # LHV2_<source>_PatDemo_YYYYMMDD_v#
    # LHV2_<manufacturer>_<source>_YYYYMMDD_v#

    (lh_version, manufacturer, source_name, batch_date, batch_version) = group_id.split('_')[:5]

    payload_loader.load(runner, matching_path, ['claimId', 'topCandidates', 'matchStatus', 'hvJoinKey', 'isWeak', 'providerMatchId'])
    if 'LHV1' in group_id:
        records_loader.load_and_clean_all(runner, incoming_path, transactions_lhv1, 'csv', '|')
        source_patient_id_col = 'source_patient_id'
    else:
        records_loader.load_and_clean_all(runner, incoming_path, transactions_lhv2, 'csv', '|')
        source_patient_id_col = 'claimId'

    # Special handling for Accredo
    no_transactional = spark.table('liquidhub_raw').count() == 0
    if 'accredo' == source_name.lower() and no_transactional:
        # This version of the feed doesn't have an hvJoinKey, so create one to reduce
        # downstream burden
        df = spark.table('matching_payload').withColumn('hvJoinKey', F.monotonically_increasing_id()).cache()
        df.createOrReplaceTempView('matching_payload')
        df.count()

        df = spark.table('matching_payload').select(F.col('hvJoinKey').alias('hvjoinkey')) \
            .withColumn('manufacturer', F.lit(manufacturer)) \
            .withColumn('source_name', F.lit(source_name)) \

        source_patient_id_col = 'personId'

        schema_enforcer.apply_schema(df, spark.table('liquidhub_raw').schema) \
            .createOrReplaceTempView('liquidhub_raw')

    # If the manufacturer name is not in the data, it will be in the group id
    if 'PatDemo' not in group_id:
        spark.table('liquidhub_raw') \
            .withColumn('manufacturer', F.lit(manufacturer)) \
            .createOrReplaceTempView('liquidhub_raw')

    content = runner.run_spark_script('normalize.sql',
                                      [['source_patient_id_col', source_patient_id_col, False]],
                                      return_output=True)

    schema = StructType([
            StructField('hvid', StringType(), True),
            StructField('source_patient_id', StringType(), True),
            StructField('source_name', StringType(), True),
            StructField('brand', StringType(), True),
            StructField('manufacturer', StringType(), True),
        ] + [
            StructField('filler_' + str(i), StringType(), True) for i in xrange(1, 8)
        ] + [
            StructField('weak_match', StringType(), True),
            StructField('custom_hv_id', StringType(), True),
            StructField('provider_meta', StringType(), True),
            StructField('matching_meta', StringType(), True)
        ]
    )

    content = schema_enforcer.apply_schema(content, schema)
    header = spark.createDataFrame(
        [tuple(
            ['HVID', 'Source Patient Id', 'Source Name', 'Brand', 'Manufacturer'] +
            ['Filler'] * 7 +
            ['Weak Match', 'Custom HV ID', 'Provider Meta', 'Matching Meta'])],
        schema=schema
    )
    deliverable = header.union(content).coalesce(1)

    deliverable.createOrReplaceTempView('liquidhub_deliverable')

    content.select('source_name', 'manufacturer').distinct() \
            .createOrReplaceTempView('liquidhub_summary')

    # Identify data from unexpected manufacturers
    bad_content = content.select('source_patient_id', 'source_name', F.coalesce(F.col('manufacturer'), F.lit('UNKNOWN')).alias('manufacturer')) \
            .where((F.lower(F.col('manufacturer')).isin(VALID_MANUFACTURERS) == False) | F.isnull(F.col('manufacturer')))

    small_bad_manus = bad_content \
            .groupBy(F.concat(F.lower(F.col('source_name')), F.lit('|'), F.lower(F.col('manufacturer'))).alias('manu')) \
            .count().where('count <= 5').collect()

    small_bad_manus = [r.manu for r in small_bad_manus]

    few_bad_rows  = bad_content.where(
                F.concat(F.lower(F.col('source_name')), F.lit('|'), F.lower(F.col('manufacturer'))).alias('manu').isin(small_bad_manus)
            ).groupBy('source_name', 'manufacturer') \
            .agg(F.collect_set('source_patient_id').alias('bad_patient_ids'), F.count('manufacturer').alias('bad_patient_count'))

    lots_bad_rows = bad_content.where(
                F.concat(F.lower(F.col('source_name')), F.lit('|'), F.lower(F.col('manufacturer'))).alias('manu').isin(small_bad_manus) == False
            ).groupBy('source_name', 'manufacturer') \
            .agg(F.count('manufacturer').alias('bad_patient_count')) \
            .select('source_name', 'manufacturer', F.lit(None).cast(ArrayType(StringType())).alias('bad_patient_ids'), 'bad_patient_count')

    few_bad_rows.union(lots_bad_rows).createOrReplaceTempView('liquidhub_error')

    # The beginning of the output file should be the same the group_id
    # Then today's date and version number of the data processing run
    # (1 for the first run of this group, 2 for the second, etc)
    # and then any file ID that HealthVerity wants, we'll use a combination
    # of the original group date and version number
    output_file_name  = '_'.join([lh_version, manufacturer, source_name])
    if not test:
        output_file_name += '_' + datetime.now(tz.gettz('America/New York')).date().isoformat().replace('-', '')
    else:
        output_file_name += '_' + datetime(2018, 7, 15).date().isoformat().replace('-', '')
    output_file_name += '_' + batch_version
    output_file_name += '_' + batch_date + 'v' + str(run_version) + '.txt.gz'

    if test:
        return output_file_name
    if not test:
        normalized_records_unloader.unload_delimited_file(
            spark, runner, 'hdfs:///staging/' + group_id + '/', 'liquidhub_deliverable',
            output_file_name=output_file_name)
        with open('/tmp/summary_report_' + group_id + '.txt', 'w') as fout:
            summ = spark.table('liquidhub_summary').collect()
            fout.write('\n'.join(['{}|{}'.format(r.source_name, r.manufacturer) for r in summ]))
        subprocess.check_call(['hadoop', 'fs', '-put', '/tmp/summary_report_' + group_id + '.txt', 'hdfs:///staging/' + group_id + '/summary_report_' + group_id + '.txt'])
        err = spark.table('liquidhub_error').collect()
        if len(err) != 0:
            with open('/tmp/error_report_' + group_id + '.txt', 'w') as fout:
                fout.write('\n'.join(['{}|{}|{}|{}'.format(r.source_name, r.manufacturer, r.bad_patient_count, json.dumps(r.bad_patient_ids)) for r in err]))
            subprocess.check_call(['hadoop', 'fs', '-put', '/tmp/error_report_' + group_id + '.txt', 'hdfs:///staging/' + group_id + '/error_report_' + group_id + '.txt'])
        

def main(args):
    # init
    spark, sqlContext = init("Liquidhub Mastering")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.group_id, args.run_version, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/spark-output/'
    else:
        output_path = 's3a://salusv/deliverable/lhv2/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--group_id', type=str)
    parser.add_argument('--run_version', type=str, default='1')
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
