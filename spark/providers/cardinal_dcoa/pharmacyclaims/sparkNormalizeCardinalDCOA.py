#! /usr/bin/python
import argparse
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharmacy_priv

def run(spark, runner, date_input, num_output_files=20, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')
    date_path = date_input.replace('-', '/')
    
    setid = 'dcoa_data_{}'.format(date_obj.strftime('%Y%m%d'))
    
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_dcoa/pharmacyclaims/resources/input/'
        )
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/dcoa/out/{}/'\
                        .format(date_path)
    else:
        input_path = 's3://salusv/incoming/pharmacyclaims/cardinal_dcoa/{}/'\
                        .format(date_path)

    runner.run_spark_script('../../../common/pharmacyclaims_common_model_v3.sql', [
        ['table_name', 'pharmacyclaims_common_model', False],
        ['properties', '', False],
        ['external', '', False]
    ])

    # Point Hive to the location of the transaction data
    # and describe its schema
    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    # Remove leading and trailing whitespace from any strings
    postprocessor.trimmify(runner.sqlContext.sql('select * from cardinal_dcoa_transactions'))\
                    .createTempView('cardinal_dcoa_transactions')

    # Normalize the transaction data into the
    # pharmacyclaims common model using transaction data
    runner.run_spark_script('normalize.sql', [ ])

    # Postprocessing 
    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id='44', vendor_id='42', filename=setid),
        pharmacy_priv.filter
    )(
        runner.sqlContext.sql('select * from pharmacyclaims_common_model')
    ).createTempView('pharmacyclaims_common_model')

    if not test:
        # Create the delivery
        if airflow_test:
            output_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/dcoa/spark-output/'
        else:
            output_path = 's3://salusv/deliverable/cardinal_dcoa/{}/'.format(date_path)
            
        delivery_df = runner.sqlContext.sql('select * from pharmacyclaims_common_model')
        delivery_df.repartition(num_output_files).write.csv(path=output_path, compression="gzip", sep="|", quoteAll=True, header=True)


def main(args):
    # Initialize Spark
    spark, sqlContext = init("Cardinal DCOA")

    # Initialize the Spark Runner
    runner = Runner(sqlContext)

    # Run the normalization routine
    run(spark, runner, args.date, airflow_test=args.airflow_test, \
            num_output_files=args.num_output_files)
    
    # Tell spark to shutdown
    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    parser.add_argument('--num_output_files', default=20, type=int)
    args = parser.parse_args()
    main(args)
