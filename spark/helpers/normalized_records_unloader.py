import logging
import time
import subprocess
import spark.helpers.constants as constants
import os

def get_rel_path(relative_filename):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            relative_filename
        )
    )

def mk_move_file(file_date):
    def move_file(part_file):
        if part_file[-3:] == ".gz":
            old_pf = part_file.split(' ')[-1].strip()
            new_pf = '/'.join(old_pf.split('/')[:-1] + [file_date + '_' + old_pf.split('/')[-1]])
            subprocess.check_call(['hadoop', 'fs', '-mv', old_pf, new_pf])

    return move_file

def unload(spark, runner, data_type, provider, date_column, file_date, S3_output_location):
    """
    Unload normalized data into partitions based on
    a date column
    """

    NOW = time.strftime('%Y-%m-%dT%H%M%S', time.localtime())
    table_loc = '/text/{}/{}'.format(data_type, NOW)
    runner.run_spark_script(get_rel_path('../../../../common/{}_common_model.sql'.format(data_type)), [
        ['table_name', 'final_unload', False],
        ['properties', constants.unload_properties_template.format(table_loc), False]
    ])
    runner.run_spark_script(get_rel_path('../../../../common/unload_common_model.sql'), [
        ['select_statement', "SELECT *, '{}' as part_provider, 'NULL' as part_best_date FROM {}_common_model WHERE {} is NULL".format(provider, data_type, date_column), False],
        ['partitions', '20', False]
    ])
    runner.run_spark_script(get_rel_path('../../../../common/unload_common_model.sql'), [
        ['select_statement', "SELECT *, '{0}' as part_provider, regexp_replace({2}, '-..$', '') as part_best_date FROM {1}_common_model WHERE {2} IS NOT NULL".format(provider, data_type, date_column), False],
        ['partitions', '20', False]
    ])

    part_files = subprocess.check_output(['hadoop', 'fs', '-ls', '-R', table_loc]).strip().split("\n")

    spark.sparkContext.parallelize(part_files).foreach(mk_move_file(file_date))

    subprocess.check_call(['s3-dist-cp', '--s3ServerSideEncryption', '--src', table_loc, '--dest', S3_output_location])
    subprocess.check_call(['hadoop', 'fs', '-rm', '-r', table_loc])
