"""
    Writes SQL for data layouts
"""

import json
import os

import boto3

S3_OUTPUT_BUCKET = "healthveritydev"
S3_OUTPUT_KEY = "marketplace_stats/sql_scripts/{}/{}/{}"


DATAFEED_VERSION_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_datafeedversion " \
                                       "(data_layout, datafeed_id, version) " \
                                       "VALUES ('{data_layout}', '{datafeed_id}', '{version}');"


def _write_sql_file(datafeed_id, new_version_query, version_name):
    """
    Given a SQL query, write a .sql file for running it,
    and upload that runnable file to S3.
    """
    output_dir = 'output/{}/'.format(version_name)
    filename = '{}_layout_version.sql'.format(datafeed_id)
    try:
        os.makedirs(output_dir)
    except OSError:
        # version_name directory already exists, which isn't a problem
        pass

    # Write SQL to file
    with open(output_dir + filename, 'w') as runnable_sql_file:
        runnable_sql_file.write('BEGIN;\n')
        runnable_sql_file.writelines([new_version_query])
        runnable_sql_file.write('\nCOMMIT;')

    # Upload .sql file to S3
    boto3.client('s3').upload_file(
        output_dir + filename,
        S3_OUTPUT_BUCKET,
        S3_OUTPUT_KEY.format(datafeed_id, version_name, filename)
    )


def create_runnable_sql_file(datafeed_id, data_layout, version_name):
    """
    Create a SQL statement for creating a new data_layout version,
    and then write it to a .sql file on S3.
    """

    # Create SQL query for making new data_layout version
    data_layout_json = json.dumps(
        data_layout.to_dict()
    ).replace("'", "''")  # need to escape single quotes
    new_version_query = DATAFEED_VERSION_INSERT_SQL_TEMPLATE.format(
        data_layout=data_layout_json,
        datafeed_id=datafeed_id,
        version=version_name
    )

    # Write .sql file onto current machine, then upload it to S3
    _write_sql_file(datafeed_id, new_version_query, version_name)
