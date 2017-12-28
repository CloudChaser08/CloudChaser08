#! /usr/bin/python
import argparse
import time
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.file_utils as file_utils
import spark.helpers.postprocessor as postprocessor
from spark.helpers.privacy import medicalclaims as priv_medicalclaims

TODAY = time.strftime('%Y-%m-%d', time.localtime())

def run(spark, runner, date_input, test=False):
    script_path = __file__

    if test:
        EMDEON_IN = file_utils.get_abs_path(
            script_path, '../../../test/providers/emdeon/medicalclaims/resources/input/'
        ) + '/'
        EMDEON_MATCHING = file_utils.get_abs_path(
            script_path, '../../../test/providers/emdeon/medicalclaims/resources/matching/'
        ) + '/'
        EMDEON_PAYER_MAPPING = file_utils.get_abs_path(
            script_path, '../../../test/providers/emdeon/medicalclaims/resources/payer_mapping/'
        ) + '/'
    else:
        EMDEON_IN = 's3a://salusv/incoming/medicalclaims/emdeon/'
        EMDEON_MATCHING = 's3a://salusv/matching/payload/medicalclaims/emdeon/'
        EMDEON_PAYER_MAPPING = 's3://salusv/reference/emdeon/'

    setid = '{}_Claims_US_CF_D_deid.dat'.format(date_input.replace('-', ''))

    runner.run_spark_script('create_helper_tables.sql')
    runner.run_spark_script('../../../common/zip3_to_state.sql')
    runner.run_spark_script('load_payer_mapping.sql', [
        ['payer_mapping', EMDEON_PAYER_MAPPING]
    ])
    if date_input < '2015-10-01':
        runner.run_spark_script('../../../common/load_hvid_parent_child_map.sql')

    date_path = date_input.replace('-', '/')

    runner.run_spark_script('../../../common/medicalclaims_common_model.sql', [
        ['table_name', 'medicalclaims_common_model', False],
        ['properties', '', False]
    ])
    if date_input < '2015-08-01':
        runner.run_spark_script('load_transactions.sql', [
            ['input_path', EMDEON_IN + date_path + '/payload/']
        ])
    else:
        runner.run_spark_script('load_transactions.sql', [
            ['input_path', EMDEON_IN + date_path + '/']
        ])

    # before 2015-10-01 we did not include the parentId in the matching
    # payload for exact matches, so there is a separate table to
    # reconcile that
    if date_input < '2015-10-01':
        runner.run_spark_script('load_matching_payload_v1.sql', [
            ['matching_path', EMDEON_MATCHING + date_path + '/']
        ])
    else:
        runner.run_spark_script('load_matching_payload_v2.sql', [
            ['matching_path', EMDEON_MATCHING + date_path + '/']
        ])

    runner.run_spark_script('split_raw_transactions.sql', [
        ['min_date', '2012-01-01'],
        ['max_date', date_input]
    ])
    runner.run_spark_script('normalize_professional_claims.sql')
    runner.run_spark_script('normalize_institutional_claims.sql')

    # Privacy filtering
    postprocessor.compose(
        postprocessor.trimmify, postprocessor.nullify,
        postprocessor.add_universal_columns(
            feed_id='10', vendor_id='11', filename=setid
        ),
        priv_medicalclaims.filter
    )(
        runner.sqlContext.sql('select * from medicalclaims_common_model')
    ).createTempView('medicalclaims_common_model')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'medicalclaims', 'medicalclaims_common_model.sql', 'emdeon',
            'medicalclaims_common_model', 'date_service', date_input
        )


def main(args):

    # init spark
    spark, sqlContext = init("Emdeon DX")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date)

    spark.stop()

    S3_EMDEON_OUT = 's3://salusv/warehouse/parquet/medicalclaims/2017-02-24/'
    normalized_records_unloader.distcp(S3_EMDEON_OUT)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--debug', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
