"""
haystack normalize
"""
#! /usr/bin/python
import argparse
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.records_loader as records_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.constants as constants
from pyspark.sql.types import StringType, StructType, StructField
from spark.providers.haystack.custom import transactions
import spark.helpers.schema_enforcer as schema_enforcer
import uuid

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/haystack/custom/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3a://salusv/deliverable/haystack/{}/'


def run(spark, runner, channel, group_id, date=None, test=False, airflow_test=False):
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
            matching_path = \
                's3a://salusv/testing/dewey/airflow/e2e/haystack/custom/payload/{}/'\
                    .format(date.replace('-', '/'))
            incoming_path = \
                's3a://salusv/testing/dewey/airflow/e2e/haystack/custom/incoming/{}/'\
                    .format(date.replace('-', '/'))
            output_dir = '/tmp/staging/' + date.replace('-', '/') + '/'
        else:
            matching_path = 's3a://salusv/testing/dewey/airflow/e2e/haystack/custom/payload/{}/'\
                .format(group_id)
            incoming_path = 's3a://salusv/testing/dewey/airflow/e2e/haystack/custom/incoming/{}/'\
                .format(group_id)
            output_dir = '/tmp/staging/' + group_id + '/'
    else:
        if date:
            incoming_path = 's3a://salusv/incoming/custom/haystack/{}/{}/'.\
                format(channel, date.replace('-', '/'))
            matching_path = \
                's3a://salusv/matching/payload/custom/haystack/{}/{}/'.\
                    format(channel, date.replace('-', '/'))
            output_dir = constants.hdfs_staging_dir + date.replace('-', '/') + '/'
        else:
            incoming_path = 's3a://salusv/incoming/custom/haystack/{}/{}/'.\
                format(channel, group_id)
            matching_path = 's3a://salusv/matching/payload/custom/haystack/{}/{}/'.\
                format(channel, group_id)
            output_dir = constants.hdfs_staging_dir + group_id + '/'

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
    header = [
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
    ]
    deliverable = \
        content.select(*[content[content.columns[i]].alias(header[i]) for i in range(len(header))])

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
                spark, runner, 'hdfs:///staging/part_dt=' + date + '/', 'haystack_deliverable',
                output_file_name=output_file_name, header=True)
        else:
            normalized_records_unloader.unload_delimited_file(
                spark, runner, 'hdfs:///staging/' + group_id + '/', 'haystack_deliverable',
                output_file_name=output_file_name, header=True)

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Haystack_Mastering',
            data_type=DataType.CUSTOM,
            data_source_transaction_path=incoming_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION.format(channel),
            run_type=RunType.MARKETPLACE,
            input_date=date
        )


def main(args):
    # init
    spark, sql_context = init("Haystack Mastering")

    # initialize runner
    runner = Runner(sql_context)

    run(spark, runner, args.channel, args.group_id, date=args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(OUTPUT_PATH_TEST)
    else:
        hadoop_time = \
                normalized_records_unloader.timed_distcp(OUTPUT_PATH_PRODUCTION.format(args.channel))
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--group_id', type=str)
    parser.add_argument('--date', type=str, default=None)
    parser.add_argument('--channel', type=str, required=True, choices={'dev', 'test', 'prod'})
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    main(args)
