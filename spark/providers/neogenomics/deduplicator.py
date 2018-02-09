import boto3
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import lit, rank, desc, col
from pyspark.sql.utils import AnalysisException

import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
from spark.providers.neogenomics.transactional_schemas import results_schema, tests_schema


def get_date_from_input_path(input_path):
    """
    Return the date assocated with a given input_path
    """
    if input_path[-1] != '/':
        input_path = input_path + '/'
    return '-'.join(input_path.split('/')[-4:-1])


def get_previous_dates(input_path):
    """
    Get a list of dates prior to the current input path
    """
    date_input = get_date_from_input_path(input_path)
    if input_path.startswith('s3'):
        return set([
            transactional_date for transactional_date in [
                '-'.join(el['Key'].split('/')[3:6])
                for el in boto3.client('s3').list_objects_v2(
                        Bucket=input_path.split('/')[2],
                        Prefix='/'.join(input_path.split('/')[3:-4]),
                )['Contents']
            ] if transactional_date < date_input
        ])
    else:
        return set([
            transactional_date for transactional_date in [
                '-'.join(el.split('/')[-5:-2])
                for el in file_utils.recursive_listdir('/'.join(input_path.split('/')[:-3]))
            ] if transactional_date < date_input and 'deduplicated' not in transactional_date
        ])


def load_matching_payloads(runner, matching_path):
    """
    Create payload table from all matching payloads
    """
    date_input = get_date_from_input_path(matching_path)
    all_dates = sorted(get_previous_dates(matching_path)) + [date_input]

    # create payloads table
    reduce(DataFrame.unionAll, [
        payload_loader.load(runner, matching_path.replace(
            date_input.replace('-', '/'), date.replace('-', '/')
        ), ['personId'], return_output=True).withColumn(
            'vendor_date', lit(date)
        ).withColumn(
            'prev_vendor_date', lit(all_dates[i - 1] if i > 0 else '1900-01-01')
        ) for i, date in enumerate(all_dates)
    ]).createOrReplaceTempView('matching_payloads')


def load_and_deduplicate_transaction_table(
        runner, input_path, is_results=False, test=False
):
    date_input = get_date_from_input_path(input_path)

    entity = 'results' if is_results else 'tests'
    entity_schema = results_schema if is_results else tests_schema
    primary_key = ['test_order_id', 'result_name'] if is_results else ['test_order_id']

    deduplicated_save_path = 'deduplicated/{}/'.format(entity)

    previous_dates = get_previous_dates(input_path)
    most_recent_date_path = input_path.replace(
        date_input.replace('-', '/'), sorted(previous_dates)[-1].replace('-', '/')
    )

    try:
        # try to load the previously saved deduplicated test data if
        # it exists
        previous_data = [
            runner.sqlContext.read.parquet(
                most_recent_date_path + deduplicated_save_path
            )
        ]

    except AnalysisException:
        # previously saved deduplicated data does not exist, gather
        # all raw test data
        previous_data = [
            runner.sqlContext.read.csv(
                path=input_path.replace(
                    date_input.replace('-','/'),
                    previous_date.replace('-', '/')
                ) + '{}/'.format(entity), schema=entity_schema, sep='|'
            ).dropDuplicates(primary_key).withColumn(
                'vendor_date', lit(previous_date)
            ) for previous_date in sorted(previous_dates)
        ]

    deduplication_window = Window.orderBy(desc('vendor_date')).partitionBy(*[col(name) for name in primary_key])

    # deduplicate tests and save work in the input_path
    deduplicated_data = reduce(DataFrame.unionAll, previous_data + [
        runner.sqlContext.read.csv(
            path=input_path + '{}/'.format(entity), schema=entity_schema, sep='|'
        ).dropDuplicates(primary_key).withColumn(
            'vendor_date', lit(date_input)
        )
    ]).withColumn('rank', rank().over(deduplication_window)).filter(
        col('rank') == 1
    )
    deduplicated_data = deduplicated_data.select(
        *[c for c in deduplicated_data.columns if c != 'rank']
    ).repartition(1 if test else 100)
    deduplicated_data.write.parquet(input_path + deduplicated_save_path)

    deduplicated_data.createOrReplaceTempView('transactional_{}'.format(entity))
