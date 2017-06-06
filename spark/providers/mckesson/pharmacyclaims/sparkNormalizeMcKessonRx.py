#! /usr/bin/python
import argparse
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv


def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid = 'HVUnRes.Record.' + date_obj.strftime('%Y%m%d')

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/matching/'
        ) + '/'
    else:
        input_path = 's3a://salusv/incoming/pharmacyclaims/mckesson/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/pharmacyclaims/mckesson/{}/'.format(
            date_input.replace('-', '/')
        )

    min_date = '2010-03-01'
    max_date = date_input

    runner.run_spark_script('../../../common/pharmacyclaims_common_model.sql', [
        ['table_name', 'pharmacyclaims_common_model', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    postprocessor.trimmify(runner.sqlContext.sql('select * from transactions')).createTempView('transactions')

    runner.run_spark_script('normalize.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])

    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id='33', vendor_id='86', filename=setid),
        pharm_priv.filter
    )(
        runner.sqlContext.sql('select * from pharmacyclaims_common_model')
    ).createTempView('pharmacyclaims_common_model')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims_common_model.sql', 'mckesson',
            'pharmacyclaims_common_model', 'date_service', date_input
        )


def main(args):
    # init
    spark, sqlContext = init("McKessonRx")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    output_path = 's3a://salusv/warehouse/parquet/pharmacyclaims/2017-06-02/'
    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
