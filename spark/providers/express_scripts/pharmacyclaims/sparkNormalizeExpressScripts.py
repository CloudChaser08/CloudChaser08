#! /usr/bin/python
import argparse
from datetime import datetime, timedelta
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv
from pyspark.sql.functions import lit

TEXT_FORMAT = """
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\\n'
STORED AS TEXTFILE
"""
PARQUET_FORMAT = "STORED AS PARQUET"

def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    # NOTE: VERIFY THAT THIS IS TRUE BEFORE MERGING
    setid = '10130X001_HV_RX_Claims_D' + date_obj.strftime('%Y%m%d')

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/express_scripts/pharmacyclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/express_scripts/pharmacyclaims/resources/matching/'
        ) + '/'
        normalized_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/express_scripts/pharmacyclaims/resources/normalized/'
        ) + '/'
        table_format = TEXT_FORMAT
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/express_scripts/pharmacyclaims/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/express_scripts/pharmacyclaims/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        normalized_path = 's3://salusv/testing/dewey/airflow/e2e/express_scripts/pharmacyclaims/normalized/'
        table_format = TEXT_FORMAT
    else:
        input_path = 's3a://salusv/incoming/pharmacyclaims/esi/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/pharmacyclaims/esi/{}/'.format(
            date_input.replace('-', '/')
        )
        normalized_path = 's3a://salusv/warehouse/parquet/pharmacyclaims/2017-06-02/part_provider=express_scripts/'
        table_format = PARQUET_FORMAT

    min_date = '2008-09-01'
    max_date = date_input

    runner.run_spark_script('../../../common/pharmacyclaims_common_model_v3.sql', [
        ['external', '', False],
        ['table_name', 'pharmacyclaims_common_model', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, matching_path, ['RXNumber', 'hvJoinKey'])

    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    postprocessor.trimmify(
        postprocessor.nullify(runner.sqlContext.sql('select * from transactions'))
    ).createTempView('transactions')

    runner.run_spark_script('normalize.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])

    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id='16', vendor_id='17', filename=setid),
        pharm_priv.filter
    )(
        runner.sqlContext.sql('select * from pharmacyclaims_common_model')
    ).createTempView('pharmacyclaims_common_model')

    properties = """
PARTITIONED BY (part_best_date string)
{}
LOCATION '{}'
""".format(table_format, normalized_path)

    runner.run_spark_script('../../../common/pharmacyclaims_common_model_v3.sql', [
        ['external', 'EXTERNAL', False],
        ['table_name', 'normalized_claims', False],
        ['properties', properties, False]
    ])

    curr_mo = date_obj.strftime('%Y-%m')
    prev_mo = (datetime.strptime(curr_mo + '-01', '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m')
    for mo in [curr_mo, prev_mo]:
        runner.run_spark_query("ALTER TABLE normalized_claims ADD PARTITION (part_best_date='{0}') LOCATION '{1}part_best_date={0}/'".format(mo, normalized_path))

    new_not_reversed = runner.sqlContext.sql("SELECT * FROM pharmacyclaims_common_model WHERE claim_id NOT IN (SELECT pharmacy_claim_ref_id from transactions)")
    old_not_reversed = runner.sqlContext.sql("SELECT * FROM normalized_claims WHERE claim_id NOT IN (SELECT pharmacy_claim_ref_id from transactions)").drop('part_best_date')

    new_reversed.union(old_reversed).union(new_not_reversed).union(old_not_reversed).createTempView('pharmacyclaims_common_model_final')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims_common_model_v3.sql', 'express_scripts',
            'pharmacyclaims_common_model_final', 'date_service', date_input
        )


def main(args):
    # init
    spark, sqlContext = init("ExpressScriptsRx")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/express_scripts/pharmacyclaims/spark-output/'
    else:
        output_path = 's3a://salusv/warehouse/parquet/pharmacyclaims/2017-06-02/'

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')
    curr_mo = date_obj.strftime('%Y-%m')
    prev_mo = (datetime.strptime(curr_mo + '-01', '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m')
    for mo in [curr_mo, prev_mo]:
        subprocess.check_call(['aws', 's3', 'rm', '--recursive',
            's3://salusv/warehouse/parquet/pharmacyclaims/2017-06-02/part_provider=express_scripts/part_best_date={}'.format(mo)])

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
