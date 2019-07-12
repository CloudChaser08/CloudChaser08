#! /usr/bin/python
import argparse
from datetime import datetime, timedelta
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.pharmacyclaims_common_model_v6 import schema as pharma_schema
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv
import subprocess
from pyspark.sql.functions import lit, md5

TEXT_FORMAT = """
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\\n'
STORED AS TEXTFILE
"""
PARQUET_FORMAT = "STORED AS PARQUET"
DELIVERABLE_LOC = 'hdfs:///cardinal_pds_deliverable/'
EXTRA_COLUMNS = ['tenant_id']

def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')
    org_num_partitions = spark.conf.get('spark.sql.shuffle.partitions')

    # NOTE: VERIFY THAT THIS IS TRUE BEFORE MERGING
    setid = 'PDS.' + date_obj.strftime('%Y%m%d')

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_pds/pharmacyclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_pds/pharmacyclaims/resources/matching/'
        ) + '/'
        normalized_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_pds/pharmacyclaims/resources/normalized/'
        ) + '/'
        table_format = TEXT_FORMAT
        deliverable_path = '/tmp/cardinal_pds_deliverable/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pds/pharmacyclaims/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pds/pharmacyclaims/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        normalized_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pds/pharmacyclaims/normalized/'
        table_format = TEXT_FORMAT
        deliverable_path = DELIVERABLE_LOC
    else:
        input_path = 's3a://salusv/incoming/pharmacyclaims/cardinal_pds/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/pharmacyclaims/cardinal_pds/{}/'.format(
            date_input.replace('-', '/')
        )
        normalized_path = 's3a://salusv/warehouse/parquet/pharmacyclaims/2018-02-05/part_provider=cardinal_pds/'
        table_format = PARQUET_FORMAT
        deliverable_path = DELIVERABLE_LOC

    min_date = '2011-01-01'
    max_date = date_input

    payload_loader.load(runner, matching_path, ['hvJoinKey'])

    column_length = len(spark.read.csv(input_path, sep='|').columns)
    if column_length == 85:
        runner.run_spark_script('load_transactions.sql', [
            ['input_path', input_path]
        ])
    else:
        runner.run_spark_script('load_transactions_v2.sql', [
            ['input_path', input_path]
        ])

    df = runner.sqlContext.sql('select * from transactions')
    df = postprocessor.nullify(df, ['NULL', 'Unknown', '-1', '-2'])
    postprocessor.trimmify(df).createTempView('transactions')

    normalized_output = runner.run_spark_script('normalize.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ], return_output=True)

    postprocessor.compose(
        schema_enforcer.apply_schema_func(pharma_schema, cols_to_keep=EXTRA_COLUMNS),
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id='39', vendor_id='42', filename=setid),
        pharm_priv.filter
    )(
        normalized_output
    ).persist().createTempView('pharmacyclaims_common_model')

    spark.table('pharmacyclaims_common_model').count()

    properties = """
PARTITIONED BY (part_best_date string)
{}
LOCATION '{}'
""".format(table_format, normalized_path)

    runner.run_spark_script('../../../common/pharmacyclaims_common_model_v6.sql', [
        ['external', 'EXTERNAL', False],
        ['additional_columns', [], False],
        ['table_name', 'normalized_claims', False],
        ['properties', properties, False]
    ])

    if test:
        subprocess.check_call(['rm', '-rf', deliverable_path])
    else:
        subprocess.check_call(['hadoop', 'fs', '-rm', '-f', '-R', deliverable_path])

    runner.run_spark_script('create_cardinal_deliverable.sql', [
        ['location', deliverable_path],
        ['partitions', org_num_partitions, False]
    ])

    # Drop Cardinal-specific columns before putting data in the warehouse
    spark.table('pharmacyclaims_common_model').drop(*EXTRA_COLUMNS) \
        .createOrReplaceTempView('pharmacyclaims_common_model')

    # Remove the ids Cardinal created for their own purposes and de-obfuscate the HVIDs
    clean_hvid_sql = """SELECT *,
            slightly_deobfuscate_hvid(cast(hvid as integer), 'Cardinal_MPI-0') as clear_hvid
        FROM pharmacyclaims_common_model"""
    df = runner.sqlContext.sql(clean_hvid_sql)
    df.withColumn('hvid', df.clear_hvid).drop('clear_hvid')             \
            .withColumn('pharmacy_other_id', md5(df.pharmacy_other_id)) \
            .createOrReplaceTempView('pharmacyclaims_common_model')

    curr_mo = date_obj.strftime('%Y-%m')
    prev_mo = (datetime.strptime(curr_mo + '-01', '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m')
    for mo in [curr_mo, prev_mo]:
        runner.run_spark_query("ALTER TABLE normalized_claims ADD PARTITION (part_best_date='{0}') LOCATION '{1}part_best_date={0}/'".format(mo, normalized_path))

    runner.run_spark_script('clean_out_reversed_claims.sql')

    new_reversed = runner.sqlContext.sql("SELECT * FROM pharmacyclaims_common_model WHERE concat_ws(':', record_id, data_set) IN (SELECT * from reversed)").withColumn('logical_delete_reason', lit('Reversed Claim'))
    old_reversed = runner.sqlContext.sql("SELECT * FROM normalized_claims WHERE concat_ws(':', record_id, data_set) IN (SELECT * from reversed)").drop('part_best_date').withColumn('logical_delete_reason', lit('Reversed Claim'))
    new_not_reversed = runner.sqlContext.sql("SELECT * FROM pharmacyclaims_common_model WHERE concat_ws(':', record_id, data_set) NOT IN (SELECT * from reversed)")
    old_not_reversed = runner.sqlContext.sql("SELECT * FROM normalized_claims WHERE concat_ws(':', record_id, data_set) NOT IN (SELECT * from reversed)").drop('part_best_date')

    new_reversed.union(old_reversed).union(new_not_reversed).union(old_not_reversed).createTempView('pharmacyclaims_common_model_final')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims_common_model_v6.sql', 'cardinal_pds',
            'pharmacyclaims_common_model_final', 'date_service', date_input
        )


def main(args):
    # init
    spark, sqlContext = init("CardinalRx")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path      = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pds/pharmacyclaims/spark-output/'
        deliverable_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pds/pharmacyclaims/spark-deliverable-output/'
    else:
        output_path      = 's3a://salusv/warehouse/parquet/pharmacyclaims/2018-02-05/'
        deliverable_path = 's3://salusv/deliverable/cardinal_pds-0/'

    # NOTE: 05/23/2019 - Cardinal has requested that some PDS not be added to the warehouse and
    # sold through marketplace. Pending additional details and logic, do no copy normalized data into
    # the warehouse
    #normalized_path = 's3://salusv/warehouse/parquet/pharmacyclaims/2018-02-05/part_provider=cardinal_pds/'
    #curr_mo = args.date[:7]
    #prev_mo = (datetime.strptime(curr_mo + '-01', '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m')
    #for mo in [curr_mo, prev_mo]:
    #    subprocess.check_call(['aws', 's3', 'rm', '--recursive', '{}part_best_date={}/'.format(normalized_path, mo)])
    #normalized_records_unloader.distcp(output_path)

    subprocess.check_call([
        's3-dist-cp', '--s3ServerSideEncryption', '--src',
        DELIVERABLE_LOC, '--dest', deliverable_path + args.date.replace('-', '/') + '/'
    ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
