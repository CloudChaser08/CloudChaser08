import json
import os

import boto3


S3_OUTPUT_DIR = "s3://healthveritydev/marketplace_stats/sql_scripts/{}/"  # TODO: Make configurable

DATAFEED_VERSION_INSERT_SQL_TEMPLATE = (
    """
    INSERT INTO marketplace_datafeedversion
    (data_layout, data_feed_id, version)
    VALUES ('{data_layout}', '{data_feed_id}', '{version}');
    """
)


def _write_sql_file(datafeed_id, new_version_query, quarter):
    """
    Given a SQL query, write a .sql file for running it,
    and upload that runnable file to S3.
    """
    output_dir = 'output/{}/'.format(quarter)
    filename = '{}_layout_version.sql'.format(datafeed_id)
    try:
        os.makedirs(output_dir)
    except:
        pass

    # write SQL to file
    with open(output_dir + filename, 'w') as runnable_sql_file:
        runnable_sql_file.write('BEGIN;\n')
        runnable_sql_file.writelines([new_version_query])
        runnable_sql_file.write('COMMIT;\n')

    # Upload SQL file to S3
    boto3.client('s3').upload_file(
        output_dir + filename,
        S3_OUTPUT_DIR.split('/')[2],
        '/'.join(S3_OUTPUT_DIR.format(quarter).split('/')[3:]) + filename
    )


def create_runnable_sql_file(datafeed_id, version_name, quarter, data_layout):
    """
    Create a SQL statement for creating a new data_layout version,
    and then write it to a .sql file on S3.
    """

    # Create SQL query for making new data_layout version
    data_layout_json = json.dumps(data_layout)
    new_version_query = DATAFEED_VERSION_INSERT_SQL_TEMPLATE.format(
        data_layout=data_layout_json,  data_feed_id=datafeed_id, version=version_name
    )

    # Write .sql file and upload it to S3
    _write_sql_file(datafeed_id, new_version_query, quarter)
