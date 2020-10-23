#! /usr/bin/python
import argparse
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.pharmacyclaims import schemas as pharma_schemas
import spark.helpers.file_utils as file_utils
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharmacy_priv
from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger

def run(spark, runner, date_input, num_output_files=1, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')
    date_path = date_input.replace('-', '/')

    setid = 'dcoa_data_{}'.format(date_obj.strftime('%Y%m%d'))

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_dcoa/pharmacyclaims/resources/input/'
        )
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_dcoa/out/{}/'\
                        .format(date_path)
    else:
        input_path = 's3://salusv/incoming/pharmacyclaims/cardinal_dcoa/{}/'\
                        .format(date_path)

    dcoa_schema = StructType(pharma_schemas['schema_v6'].schema_structure.fields + [
        StructField('patient_type_vendor', StringType(), True),
        StructField('outlier_vendor', StringType(), True),
        StructField('monthly_patient_days_vendor', StringType(), True),
        StructField('extended_fee_vendor', StringType(), True),
        StructField('discharges_vendor', StringType(), True),
        StructField('discharge_patient_days_vendor', StringType(), True),
        StructField('total_patient_days_vendor', StringType(), True),
        StructField('pharmacy_name', StringType(), True),
        StructField('pharmacy_address', StringType(), True),
        StructField('pharmacy_service_area_vendor', StringType(), True),
        StructField('pharmacy_master_service_area_vendor', StringType(), True,),
        StructField('original_product_code_qualifier', StringType(), True, ),
        StructField('original_product_code', StringType(), True, )
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
    normalized_output = runner.run_spark_script('normalize.sql', return_output=True)

    # Postprocessing
    postprocessor.compose(
        lambda x: schema_enforcer.apply_schema(x, dcoa_schema),
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id='44', vendor_id='42', filename=setid),
        pharmacy_priv.filter
    )(
        normalized_output
    ).createTempView('pharmacyclaims_common_model')

    if not test:
        # Create the delivery
        if airflow_test:
            output_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_dcoa/delivery/{}/'.format(date_path)
        else:
            output_path = 's3://salusv/deliverable/cardinal_dcoa/{}/'.format(date_path)

        delivery_df = runner.sqlContext.sql('select * from pharmacyclaims_common_model')
        delivery_df.repartition(num_output_files).write.csv(path=output_path, compression="gzip", sep="|", quoteAll=True, header=True)

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Cardinal_DCOA',
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=input_path,
            data_source_matching_path="",
            output_path=output_path,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # Initialize Spark
    spark, sqlContext = init("Cardinal DCOA")

    # Initialize the Spark Runner
    runner = Runner(sqlContext)

    # Run the normalization routine
    run(spark, runner, args.date, airflow_test=args.airflow_test, \
            num_output_files=args.num_output_files)

    if not args.airflow_test:
        RunRecorder().record_run_details()

    # Tell spark to shutdown
    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    parser.add_argument('--num_output_files', default=20, type=int)
    args = parser.parse_args()
    main(args)
