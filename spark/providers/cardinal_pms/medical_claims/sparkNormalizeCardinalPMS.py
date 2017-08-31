#! /usr/bin/python
import argparse
import spark.helpers.file_utils as file_utils
from spark.runner import Runner
from spark.spark_setup import init

import spark.helpers.normalized_records_unloader as normalized_records_unloader

def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    set_id = 'TBD' #TODO: fill in with file format when found out

    if test:
        input_path = file_utils.get_abs_path(
            script_path = '../../test/providers/cardinal_pms/resources/input/'
        )
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/out/{}'\
                        .format(date_path)
    else:
        input_path = 's3://salusv/incoming/consumer/mindbody/{}/'\
                        .format(date_path)

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
    postprocessor.trimmify(runner.sqlContext.sql('select * from claim'))\
                    .createTempView('claim')
    postprocessor.trmmify(runner.sqlContext.sql('seelct * from claim_lines'))\
                    .createTempView('claim_lines')
    posprocessor.trimmify(runner.sqlContext.sql('select * from claim_files'))\
                    .createTempView('claim_files')

    # Normalize the transactional data into the
    # medical claim common model
    runner.run_spark_script('normalize.sql', [])

    # Postprocessing
    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_column(feed_id='41', vendor_id='188', filename=set_id)
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
