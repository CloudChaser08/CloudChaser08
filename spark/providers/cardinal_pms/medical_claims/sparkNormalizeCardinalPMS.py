#! /usr/bin/python
import argparse

from spark.runner import Runner
from spark.spark_setup import init

import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.explode as explode
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.medicalclaims as medical_priv

def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path = '../../test/providers/cardinal_pms/medicalclaims/resources/input/'
        )
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/out/{}'\
                        .format(date_input.replace('-', '/')
    else:
        input_path = 's3://salusv/incoming/medicalclaims/cardinal_pms/{}/'\
                        .format(date_input.replace('-', '/')

    # Note: This routine does not contain any matching payloads

    # Create the medical claims table to store the results in
    runner.run_spark_script('../../common/medicalclaims_common_model.sql', [
        ['table_name', 'medicalclaims_common_model', False],
        ['properties', '', False]
    )

    # Load the transactions into raw, un-normalized tables
    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    # Remove leading and trailing whitespace from any strings
    postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(
        runner.sqlContext.sql('select * from transactional_cardinal_pms')
    ).createTempView('transactional_cardinal_pms')

    # Create exploder table for service-line
    explode.generate_exploder_table(spark, 5, 'service_line_exploder')

    # Normalize service-line
    runner.run_spark_script('normalize_service_line.sql', [])

    # Create exploder table for claim
    explode.generate_exploder_table(spark, 8, 'claim_exploder')

    # Normalize claim
    runner.run_spark_script('normalize_claim.sql', [])

    # Postprocessing
    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_column(
            feed_id='41',
            vendor_id='188',
            # TODO: this is not right, fix when we know what the name format is.
            filename='PMS_Claims_{}.psv'.format(date_obj.strftime('%Y%m%d'))
        ),
        medical_priv.filter
    )(
        runner.sqlContext.sql('select * from medicalclaims_common_model')
    ).createTempView('medicalclaims_common_model')
    
    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'medicalclaims', 'medicalclaims_common_model.sql', 'cardinal_pms',
            'medicalclaims_common_model', 'date_service', date_input
        )


def main(args):
    # init
    spark, sqlContext = init('Cardinal PMS')

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/medical_claims/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/text/medical_claims/cardinal_pms/'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
