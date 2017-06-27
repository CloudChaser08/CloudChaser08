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

runner = None
spark = None


def load(input_path, restriction_level):
    """
    Function for loading transactional data
    """
    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path],
        ['restriction_level', restriction_level, False]
    ])
    postprocessor.trimmify(
        runner.sqlContext.sql('select * from {}_transactions'.format(restriction_level))
    ).createTempView('{}_transactions'.format(restriction_level))


def postprocess_and_unload(date_input, test, restricted):
    """
    Function for unloading normalized data
    """
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid_template = '{}.Record.' + date_obj.strftime('%Y%m%d')
    setid = setid_template.format('HVRes' if restricted else 'HVUnRes')
    provider = 'mckesson_res' if restricted else 'mckesson'
    restriction_level = 'restricted' if restricted else 'unrestricted'

    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id='33', vendor_id='86', filename=setid),
        pharm_priv.filter
    )(
        runner.sqlContext.sql('select * from {}_pharmacyclaims_common_model'.format(restriction_level))
    ).createTempView('{}_pharmacyclaims_common_model'.format(restriction_level))

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims_common_model.sql', provider,
            '{}_pharmacyclaims_common_model'.format(restriction_level), 'date_service', date_input
        )


def run(spark_in, runner_in, date_input, mode, test=False, airflow_test=False):
    script_path = __file__

    global spark, runner
    spark = spark_in
    runner = runner_in

    if test:
        unres_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/input/'
        ) + '/'
        unres_matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/matching/'
        ) + '/'
        res_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/input-res/'
        ) + '/'
        res_matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson/pharmacyclaims/resources/matching-res/'
        ) + '/'
    elif airflow_test:
        unres_input_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson/pharmacyclaims/out/{}/'.format(
            date_input.replace('-', '/')
        )
        unres_matching_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson/pharmacyclaims/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        unres_input_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson_res/pharmacyclaims/out/{}/'.format(
            date_input.replace('-', '/')
        )
        unres_matching_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson_res/pharmacyclaims/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        unres_input_path = 's3a://salusv/incoming/pharmacyclaims/mckesson/{}/'.format(
            date_input.replace('-', '/')
        )
        res_input_path = 's3a://salusv/incoming/pharmacyclaims/mckesson_res/{}/'.format(
            date_input.replace('-', '/')
        )
        unres_matching_path = 's3a://salusv/matching/payload/pharmacyclaims/mckesson/{}/'.format(
            date_input.replace('-', '/')
        )
        res_matching_path = 's3a://salusv/matching/payload/pharmacyclaims/mckesson_res/{}/'.format(
            date_input.replace('-', '/')
        )

    min_date = '2010-03-01'
    max_date = date_input

    def normalize(input_path, matching_path, restricted=False):
        """
        Generic function for running normalization in any mode
        """
        restriction_level = 'restricted' if restricted else 'unrestricted'
        runner.run_spark_script('../../../common/pharmacyclaims_common_model.sql', [
            ['table_name', '{}_pharmacyclaims_common_model'.format(restriction_level), False],
            ['properties', '', False]
        ])

        # unrestricted has already been loaded
        if restricted:
            load(input_path, restriction_level)

        payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

        runner.run_spark_script('normalize.sql', [
            ['min_date', min_date],
            ['max_date', max_date],
            ['restriction_level', restriction_level, False],
            [
                'filter',
                'WHERE NOT EXISTS ('
                + 'SELECT prescriptionkey '
                + 'FROM unrestricted_transactions ut '
                + 'WHERE t.prescriptionkey = ut.prescriptionkey'
                + ')' if restricted else '', False
            ]
        ])

        postprocess_and_unload(date_input, test, restricted)

    # in any case, we need to load the unrestricted data
    load(unres_input_path, 'unrestricted')

    # run normalization for appropriate mode(s)
    if mode in ['restricted', 'both']:
        normalize(res_input_path, res_matching_path, True)

    if mode in ['unrestricted', 'both']:
        normalize(unres_input_path, unres_matching_path)


def main(args):

    # init spark
    spark, sqlContext = init("McKessonRx")

    # initialize runner
    runner = Runner(sqlContext)

    # figure out what mode we're in
    if args.restricted == args.unrestricted:
        mode = 'both'
    elif args.restricted:
        mode = 'restricted'
    elif args.unrestricted:
        mode = 'unrestricted'

    run(spark, runner, args.date, mode, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson/pharmacyclaims/spark-output/'
    else:
        output_path = 's3a://salusv/warehouse/parquet/pharmacyclaims/2017-06-02/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    parser.add_argument('--restricted', default=False, action='store_true')
    parser.add_argument('--unrestricted', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
