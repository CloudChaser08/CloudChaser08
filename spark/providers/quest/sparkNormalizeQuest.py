#! /usr/bin/python
import argparse
import time
from datetime import timedelta, datetime
from pyspark.sql.functions import monotonically_increasing_id
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.privacy.labtests as lab_priv

TODAY = time.strftime('%Y-%m-%d', time.localtime())


def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid = 'HealthVerity_' + \
            date_obj.strftime('%Y%m%d') + \
            (date_obj + timedelta(days=1)).strftime('%m%d')

    vendor_feed_id = '18'
    vendor_id = '7'

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../test/providers/quest/resources/input/year/month/day/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../test/providers/quest/resources/matching/year/month/day/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/quest/labtests/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/quest/labtests/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/labtests/quest/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/labtests/quest/{}/'.format(
            date_input.replace('-', '/')
        )

    trunk_path = input_path + 'trunk/'
    addon_path = input_path + 'addon/'

    # provider addon information will be at the month level
    prov_addon_path = '/'.join(input_path.split('/')[:-2]) + '/provider_addon/'
    prov_matching_path = '/'.join(matching_path.split('/')[:-2]) + '/provider_addon/'

    hvm_available_history_date = postprocessor.get_gen_ref_date(runner.sqlContext, vendor_feed_id, "HVM_AVAILABLE_HISTORY_START_DATE")
    earliest_valid_service_date = postprocessor.get_gen_ref_date(runner.sqlContext, vendor_feed_id, "EARLIEST_VALID_SERVICE_DATE")
    hvm_historical_date = hvm_available_history_date if hvm_available_history_date else \
        earliest_valid_service_date if earliest_valid_service_date else datetime.date(1901, 1, 1)
    max_date = date_input

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    # create helper tables
    runner.run_spark_script('create_helper_tables.sql')

    runner.run_spark_script('../../common/lab_common_model_v3.sql', [
        ['table_name', 'lab_common_model', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'], table_name='original_mp')
    payload_loader.load(runner, prov_matching_path, ['claimId'], table_name='augmented_with_prov_attrs_mp')

    if date_obj.strftime('%Y%m%d') >= '20171016':
        runner.run_spark_script('load_and_merge_transactions_v2.sql', [
            ['trunk_path', trunk_path],
            ['addon_path', addon_path]
        ])
    elif date_obj.strftime('%Y%m%d') >= '20160831':
        runner.run_spark_script('load_and_merge_transactions.sql', [
            ['trunk_path', trunk_path],
            ['addon_path', addon_path],
            ['prov_addon_path', prov_addon_path]
        ])
    else:
        runner.run_spark_script('load_transactions.sql', [
            ['input_path', input_path],
            ['prov_addon_path', prov_addon_path]
        ])

    for transactional_table in ['transactional_raw', 'original_mp', 'augmented_with_prov_attrs_mp']:
        postprocessor.compose(
            postprocessor.trimmify, postprocessor.nullify
        )(
            runner.sqlContext.sql('select * from {}'.format(transactional_table))
        ).createTempView('{}'.format(transactional_table))

    runner.run_spark_script('normalize.sql', [
        ['join', (
            'q.accn_id = mp.claimid AND mp.hvJoinKey = q.hv_join_key'
            if date_obj.strftime('%Y%m%d') >= '20160831' else 'q.accn_id = mp.claimid'
        ), False]
    ])

    postprocessor.compose(
        postprocessor.add_universal_columns(
            feed_id=vendor_feed_id,
            vendor_id=vendor_id,
            filename=setid
        ),
        lab_priv.filter,
        postprocessor.apply_date_cap(
            runner.sqlContext, 'date_service', max_date, vendor_feed_id, 'EARLIEST_VALID_SERVICE_DATE'
        ),
        postprocessor.apply_date_cap(
            runner.sqlContext, 'date_specimen', max_date, vendor_feed_id, 'EARLIEST_VALID_SERVICE_DATE'
        )
    )(
        runner.sqlContext.sql('select * from lab_common_model')
    ).createTempView('lab_common_model')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'lab', 'lab_common_model_v3.sql', 'quest',
            'lab_common_model', 'date_service', date_input,
            hvm_historical_date=datetime(
                hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
            )
        )


def main(args):
    # init
    spark, sqlContext = init("Quest")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/quest/labtests/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/labtests/2017-02-16/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
