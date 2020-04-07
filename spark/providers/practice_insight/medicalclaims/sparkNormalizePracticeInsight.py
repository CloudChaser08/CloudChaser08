#! /usr/bin/python
import argparse
import time
from datetime import datetime
import calendar

from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.file_utils as file_utils
import spark.helpers.explode as explode
import spark.providers.practice_insight.medicalclaims.udf as pi_udf
import spark.helpers.postprocessor as postprocessor

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


TODAY = time.strftime('%Y-%m-%d', time.localtime())
AIRFLOW_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/practice_insight/medicalclaims/'

OUTPUT_PATH_TEST = AIRFLOW_TEST_DIR + 'spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/medicalclaims/2017-02-24/'

input_path, min_date, max_date, matching_path, setid = \
    None, None, None, None, None


def run_part(
        spark, runner, part, date_input, shuffle_partitions, test=False, airflow_test=False
):
    """
    Normalize one of 4 parts of practice insight
    """

    # initialize globals on first part
    if part == '1':
        global input_path, min_date, max_date, matching_path, setid

        # register practice insight udfs:
        runner.sqlContext.registerFunction(
            'generate_place_of_service_std_id',
            pi_udf.generate_place_of_service_std_id
        )
        runner.sqlContext.registerFunction(
            'generate_inst_type_of_bill_std_id',
            pi_udf.generate_inst_type_of_bill_std_id
        )

        # Increase default partitions to avoid memory errors
        #
        # This is necessary due to the relatively large size of each
        # chunk of Practice Insight data to be normalized, as well as
        # the relative complexity of the normalization SQL
        runner.sqlContext.setConf(
            "spark.sql.shuffle.partitions", shuffle_partitions
        )

        date_obj = datetime.strptime(date_input, '%Y-%m-%d')

        if test:
            input_path = file_utils.get_abs_path(
                __file__, '../../../test/providers/practice_insight/medicalclaims/resources/input/'
            ) + '/'
            matching_path = file_utils.get_abs_path(
                __file__, '../../../test/providers/practice_insight/medicalclaims/resources/matching/'
            )
            max_date = '2016-12-31'
            setid = 'TEST'

        elif airflow_test:
            input_path = AIRFLOW_TEST_DIR + 'out/{}/{}/'.format(
                str(date_obj.year),
                str(date_obj.month).zfill(2)
            )

            max_date = date_obj.strftime('%Y-%m-') \
                       + str(calendar.monthrange(date_obj.year, date_obj.month)[1])
            matching_path = AIRFLOW_TEST_DIR + 'payload/{}/{}/'.format(
                str(date_obj.year),
                str(date_obj.month).zfill(2)
            )
            setid = 'HV.data.837.' + str(date_obj.year) + '.' \
                    + date_obj.strftime('%b').lower() + '.csv.gz'

        else:
            input_path = 's3a://salusv/incoming/medicalclaims/practice_insight/{}/{}/'.format(
                str(date_obj.year),
                str(date_obj.month).zfill(2)
            )

            if date_obj.year <= 2016:
                max_date = str(date_obj.year) + '-12-31'
                matching_path = 's3a://salusv/matching/payload/medicalclaims/practice_insight/{}/'.format(
                    str(date_obj.year)
                )
                setid = 'HV.data.837.' + str(date_obj.year) + '.csv.gz_' \
                        + str(date_obj.month)
            else:
                max_date = date_obj.strftime('%Y-%m-') \
                           + str(calendar.monthrange(date_obj.year, date_obj.month)[1])
                matching_path = 's3a://salusv/matching/payload/medicalclaims/practice_insight/{}/{}/'.format(
                    str(date_obj.year),
                    str(date_obj.month).zfill(2)
                )
                setid = 'HV.data.837.' + str(date_obj.year) + '.' \
                        + date_obj.strftime('%b').lower() + '.csv.gz'

        min_date = '2010-01-01'

        # create helper tables
        runner.run_spark_script('create_helper_tables.sql')
        payload_loader.load(runner, matching_path, ['claimId'])

    # end init #

    runner.run_spark_script('../../../common/medicalclaims_common_model.sql', [
        ['table_name', 'medicalclaims_common_model', False],
        ['properties', '', False]
    ])

    # load transactions and payload
    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path + part + '/']
    ])

    postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )(
        runner.sqlContext.sql('SELECT * FROM transactional_raw')
    ).createOrReplaceTempView('transactional_raw')

    # create explosion maps
    runner.run_spark_script('create_exploded_diagnosis_map.sql')
    runner.run_spark_script('create_exploded_procedure_map.sql')

    postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )(
        runner.sqlContext.sql('SELECT * FROM exploded_diag_codes')
    ).createOrReplaceTempView('exploded_diag_codes')

    postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )(
        runner.sqlContext.sql('SELECT * FROM exploded_proc_codes')
    ).createOrReplaceTempView('exploded_proc_codes')

    # normalize
    runner.run_spark_script('normalize.sql', [
        [
            'date_service_sl',
            """
            CASE
            WHEN extract_date(transactional.svc_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) IS NOT NULL
            AND diags.diag_code IN (transactional.diag_cd_1,
                transactional.diag_cd_2, transactional.diag_cd_3,
                transactional.diag_cd_4)
            THEN CAST(extract_date(transactional.svc_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) AS DATE)
            WHEN extract_date(transactional.stmnt_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) IS NOT NULL
            THEN CAST(extract_date(transactional.stmnt_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) AS DATE)
            ELSE MIN(
            CAST(extract_date(transactional.svc_from_dt, '%Y%m%d',
            CAST('{min_date}' as date), CAST('{max_date}' as date)) AS DATE)
            ) OVER(PARTITION BY transactional.src_claim_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            END
            """.format(
                min_date=min_date,
                max_date=max_date
            ), False
        ],
        [
            'date_service_inst',
            """
            CASE
            WHEN extract_date(transactional.svc_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) IS NOT NULL
            THEN CAST(extract_date(transactional.svc_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) AS DATE)
            WHEN extract_date(transactional.stmnt_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) IS NOT NULL
            THEN CAST(extract_date(transactional.stmnt_from_dt, '%Y%m%d',
                CAST('{min_date}' as date), CAST('{max_date}' as date)) AS DATE)
            ELSE MIN(
            CAST(extract_date(transactional.svc_from_dt, '%Y%m%d',
            CAST('{min_date}' as date), CAST('{max_date}' as date)) AS DATE)
            ) OVER(PARTITION BY transactional.src_claim_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            END
            """.format(
                min_date=min_date,
                max_date=max_date
            ), False
        ],
        [
            'place_of_service_std_id',
            """
            generate_place_of_service_std_id(
                transactional.claim_type_cd,
                transactional.pos_cd,
                transactional.fclty_type_pos_cd,
                transactional.diag_cd_1,
                transactional.diag_cd_2,
                transactional.diag_cd_3,
                transactional.diag_cd_4,
                diags.diag_code
            )
            """,
            False
        ],
        ['setid', setid + '_' + part],
        ['today', TODAY],
        ['feedname', '22'],
        ['vendor', '3'],
        ['min_date', '2010-01-01'],
        ['max_date', max_date]
    ])

    # explode date ranges
    explode.explode_medicalclaims_dates(runner)

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'medicalclaims', 'medicalclaims_common_model.sql',
            'practice_insight', 'medicalclaims_common_model', 'date_service',
            args.date + '_' + part, unload_partition_count=50
        )

        runner.sqlContext.dropTempTable('medicalclaims_common_model')


def main(args):
    # init
    spark, sqlContext = init("Practice Insight")

    # initialize runner
    runner = Runner(sqlContext)

    for part in ['1', '2', '3', '4']:
        run_part(
            spark, runner, part, args.date, args.shuffle_partitions, airflow_test=args.airflow_test
        )

    if not args.airflow_test:
        logger.log_run_details(
            provider_name='Practice_Fusion',
            data_type=DataType.MEDICAL_CLAIMS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=args.date
        )

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(OUTPUT_PATH_TEST)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH_PRODUCTION)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--shuffle_partitions', type=str, default="1200")
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
