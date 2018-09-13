#! /usr/bin/python
import argparse
from datetime import datetime
import dateutil.tz as tz
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.records_loader as records_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.constants as constants
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import udf, lit
import transactions
import spark.helpers.schema_enforcer as schema_enforcer
import subprocess
import uuid

def run(spark, runner, group_id, date=None, test=False, airflow_test=False):
    if test:
        incoming_path = file_utils.get_abs_path(
            __file__, '../../../test/providers/haystack/custom/resources/incoming/'
        ) + '/{}/'.format(group_id)
        matching_path = file_utils.get_abs_path(
            __file__, '../../../test/providers/haystack/custom/resources/matching/'
        ) + '/{}/'.format(group_id)
        output_dir = '/tmp/staging/' + group_id + '/'
    elif airflow_test:
        if date:
            matching_path = 's3a://salusv/testing/dewey/airflow/e2e/haystack/custom/payload/{}/'.format(date.replace('-', '/'))
            matching_path = 's3a://salusv/testing/dewey/airflow/e2e/haystack/custom/incoming/{}/'.format(date.replace('-', '/'))
            output_dir = '/tmp/staging/' + date.replace('-', '/') + '/'
        else:
            matching_path = 's3a://salusv/testing/dewey/airflow/e2e/haystack/custom/payload/{}/'.format(group_id)
            matching_path = 's3a://salusv/testing/dewey/airflow/e2e/haystack/custom/incoming/{}/'.format(group_id)
            output_dir = '/tmp/staging/' + group_id + '/'
    else:
        if date:
            incoming_path = 's3a://salusv/incoming/custom/haystack/testing/{}/'.format(date.replace('-', '/'))
            matching_path = 's3a://salusv/matching/payload/custom/haystack/testing/{}/'.format(date.replace('-', '/'))
            output_dir = constants.hdfs_staging_dir + date.replace('-', '/') + '/'
        else:
            incoming_path = 's3a://salusv/incoming/custom/haystack/testing/{}/'.format(group_id)
            matching_path = 's3a://salusv/matching/payload/custom/haystack/testing/{}/'.format(group_id)
            output_dir = constants.hdfs_staging_dir + date.replace('-', '/') + '/'

    runner.sqlContext.registerFunction(
        'gen_uuid', lambda: str(uuid.uuid4())
    )

    payload_loader.load(runner, matching_path, ['matchStatus', 'hvJoinKey', 'matchScore'])
    records_loader.load_and_clean_all(runner, incoming_path, transactions, 'csv', '|')
    
    content = runner.run_spark_script('normalize.sql', return_output=True)

    schema = StructType([
        StructField(c, StringType(), True) for c in [
            'hvid', 'temporary_id', 'match_score', 'year_of_birth', 'age',
            'gender', 'zip3', 'state', 'status', 'symptoms', 'hcp_1_npi',
            'hcp_1_first_name', 'hcp_1_last_name', 'hcp_1_address', 
            'hcp_1_city', 'hcp_1_state', 'hcp_1_zip_code', 'hcp_1_email',
            'hcp_1_role', 'hcp_2_npi', 'hcp_2_first_name', 'hcp_2_last_name',
            'hcp_2_address', 'hcp_2_city', 'hcp_2_state', 'hcp_2_zip_code',
            'hcp_2_email', 'hcp_2_role', 'treating_site_npi',
            'treating_site_name', 'payer_id', 'payer_name', 'payer_plan_id', 
            'payer_plan_name', 'data_source', 'crm_id_1', 'crm_id_2',
            'activity_date'
        ]
    ])

    content = schema_enforcer.apply_schema(content, schema)
    header = spark.createDataFrame(
        [tuple([
            'HVID', 'Temporary ID', 'Match Score', 'Year Of Birth', 'Age',
            'Gender', 'Zip3', 'State', 'Status', 'Symptoms', 'HCP 1 NPI',
            'HCP 1 First Name', 'HCP 1 Last Name', 'HCP 1 Address',
            'HCP 1 City', 'HCP 1 State', 'HCP 1 Zip Code', 'HCP 1 Email',
            'HCP 1 Role(s)', 'HCP 2 NPI', 'HCP 2 First Name',
            'HCP 2 Last Name', 'HCP 2 Address', 'HCP 2 City', 'HCP 2 State',
            'HCP 2 Zip Code', 'HCP 2 Email', 'HCP 2 Role(s)',
            'Treating Site NPI', 'Treating Site Name', 'Payer ID', 'Payer Name',
            'Payer Plan ID', 'Payer Plan Name', 'Data Source', 'CRM ID 1',
            'CRM ID 2', 'Activity Date'
        ])],
        schema=schema
    )
    deliverable = header.union(content).coalesce(1)

    deliverable.createOrReplaceTempView('haystack_deliverable')

    if date:
        output_file_name = date + '_daily_response.psv.gz'
    else:
        output_file_name = group_id + '_daily_response.psv.gz'

    if test:
        return output_file_name
    if not test:
        if date:
            normalized_records_unloader.unload_delimited_file(
                spark, runner, 'hdfs:///staging/' + date.replace('-', '/') + '/', 'haystack_deliverable',
                output_file_name=output_file_name)
            pass
        else:
            normalized_records_unloader.unload_delimited_file(
                spark, runner, 'hdfs:///staging/' + group_id + '/', 'haystack_deliverable',
                output_file_name=output_file_name)
        

def main(args):
    # init
    spark, sqlContext = init("Haystack Mastering")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.group_id, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/haystack/custom/spark-output/'
    else:
        output_path = 's3a://salusv/deliverable/haystack/testing/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--group_id', type=str)
    parser.add_argument('--date', type=str, default=None)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
