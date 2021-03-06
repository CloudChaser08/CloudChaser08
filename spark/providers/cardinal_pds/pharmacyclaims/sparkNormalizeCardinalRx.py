"""
cardinal pds
"""
#! /usr/bin/python
import argparse
from datetime import datetime, timedelta
import subprocess
from pyspark.sql.functions import lit, md5, col
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.pharmacyclaims import schemas as pharma_schemas
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


TEXT_FORMAT = """
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\\n'
STORED AS TEXTFILE
"""
PARQUET_FORMAT = "STORED AS PARQUET"
DELIVERABLE_LOC = 'hdfs:///cardinal_pds_deliverable/'
EXTRA_COLUMNS = ['tenant_id', 'hvm_approved']

schema = pharma_schemas['schema_v6']
OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pds/pharmacyclaims/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/' + schema.output_directory


def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')
    org_num_partitions = spark.conf.get('spark.sql.shuffle.partitions')
    # NOTE: VERIFY THAT THIS IS TRUE BEFORE MERGING
    setid = 'PDS.' + date_obj.strftime('%Y%m%d')

    script_path = __file__
    has_hvm_approved = False
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
    elif airflow_test:
        input_path = 's3a://salusv/incoming/pharmacyclaims/cardinal_pds/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/pharmacyclaims/cardinal_pds/{}/'.format(
            date_input.replace('-', '/')
        )
        normalized_path = \
            's3://salusv/testing/dewey/airflow/e2e/cardinal_pds/pharmacyclaims/normalized/'
        table_format = TEXT_FORMAT
    else:
        input_path = 's3a://salusv/incoming/pharmacyclaims/cardinal_pds/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/pharmacyclaims/cardinal_pds/{}/'.format(
            date_input.replace('-', '/')
        )
        normalized_path = OUTPUT_PATH_PRODUCTION + '/part_provider=cardinal_pds/'
        table_format = PARQUET_FORMAT

    min_date = '2011-01-01'
    max_date = date_input

    payload_loader.load(runner, matching_path, ['hvJoinKey'])

    column_length = len(spark.read.csv(input_path, sep='|').columns)
    if column_length == 86:
        runner.run_spark_script('load_transactions.sql', [
            ['input_path', input_path]
        ])
    else:
        runner.run_spark_script('load_transactions_v2.sql', [
            ['input_path', input_path]
        ])
        has_hvm_approved = True

    df = runner.sqlContext.sql('select * from transactions')
    df = postprocessor.nullify(df, ['NULL', 'Unknown', '-1', '-2'])
    postprocessor.trimmify(df).createTempView('transactions')

    normalized_output = runner.run_spark_script('normalize.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ], return_output=True)

    postprocessor.compose(
        schema_enforcer.apply_schema_func(schema.schema_structure, cols_to_keep=EXTRA_COLUMNS),
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

    runner.run_spark_script('../../../common/pharmacyclaims/sql/pharmacyclaims_common_model_v6.sql', [
        ['external', 'EXTERNAL', False],
        ['additional_columns', [], False],
        ['table_name', 'normalized_claims', False],
        ['properties', properties, False]
    ])

    tbl = spark.table('pharmacyclaims_common_model')
    (
        tbl.select(*[col(c).cast('string').alias(c) for c in tbl.columns])
        .createOrReplaceTempView('pharmacyclaims_common_model_strings')
    )

    # Remove the ids Cardinal created for their own purposes and de-obfuscate the HVIDs
    clean_hvid_sql = """SELECT *,
            slightly_deobfuscate_hvid(cast(hvid as integer), 'Cardinal_MPI-0') as clear_hvid
        FROM pharmacyclaims_common_model"""
    if has_hvm_approved:
        clean_hvid_sql += """ WHERE hvm_approved = '1'"""
    else:
        clean_hvid_sql += """ WHERE false """

    df = runner.sqlContext.sql(clean_hvid_sql).drop(*EXTRA_COLUMNS)

    df.withColumn('hvid', df.clear_hvid).drop('clear_hvid')\
        .withColumn('pharmacy_other_id', md5(df.pharmacy_other_id))\
        .createOrReplaceTempView('pharmacyclaims_common_model')

    curr_mo = date_obj.strftime('%Y-%m')
    prev_mo = (datetime.strptime(curr_mo + '-01', '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m')
    for mo in [curr_mo, prev_mo]:
        runner.run_spark_query(
            "ALTER TABLE normalized_claims "
            "ADD PARTITION (part_best_date='{0}') "
            "LOCATION '{1}part_best_date={0}/'".format(mo, normalized_path))

    runner.run_spark_script('clean_out_reversed_claims.sql')

    new_reversed = runner.sqlContext.sql(
        "SELECT * FROM pharmacyclaims_common_model "
        "WHERE concat_ws(':', record_id, data_set) IN (SELECT * from reversed)").withColumn(
            'logical_delete_reason', lit('Reversed Claim'))
    old_reversed = runner.sqlContext.sql(
        "SELECT * FROM normalized_claims WHERE concat_ws(':', record_id, data_set) IN (SELECT * from reversed)").drop(
            'part_best_date').withColumn('logical_delete_reason', lit('Reversed Claim'))
    new_not_reversed = runner.sqlContext.sql(
        "SELECT * FROM pharmacyclaims_common_model "
        "WHERE concat_ws(':', record_id, data_set) NOT IN (SELECT * from reversed)")
    old_not_reversed = runner.sqlContext.sql(
        "SELECT * FROM normalized_claims "
        "WHERE concat_ws(':', record_id, data_set) NOT IN (SELECT * from reversed)").drop('part_best_date')

    new_reversed.union(old_reversed).union(new_not_reversed).union(old_not_reversed).createTempView(
        'pharmacyclaims_common_model_final')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims',
            'pharmacyclaims/sql/pharmacyclaims_common_model_v6.sql', 'cardinal_pds',
            'pharmacyclaims_common_model_final', 'date_service', date_input
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='CardinalRx',
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # init
    spark, sql_context = init("CardinalRx")

    # initialize runner
    runner = Runner(sql_context)

    if args.airflow_test:
        output_path = OUTPUT_PATH_TEST
    else:
        output_path = OUTPUT_PATH_PRODUCTION

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    normalized_path = OUTPUT_PATH_PRODUCTION + '/part_provider=cardinal_pds/'
    curr_mo = args.date[:7]
    prev_mo = (datetime.strptime(curr_mo + '-01', '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m')
    for mo in [curr_mo, prev_mo]:
        subprocess.check_call(['aws', 's3', 'rm', '--recursive',
                               '{}part_best_date={}/'.format(normalized_path, mo)])

    logger.log("Moving files to s3")
    if args.airflow_test:
        normalized_records_unloader.distcp(output_path)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(output_path)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    main(args)
