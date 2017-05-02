import subprocess
import spark.helpers.constants as constants
import spark.helpers.file_utils as file_utils
import os

def mk_move_file(file_date):
    def move_file(part_file):
        if part_file.find("part-") > -1:
            old_pf = part_file.split(' ')[-1].strip()
            new_pf = '/'.join(old_pf.split('/')[:-1] + [file_date + '_' + old_pf.split('/')[-1]])
            subprocess.check_call(['hadoop', 'fs', '-mv', old_pf, new_pf])

    return move_file


def partition_and_rename(spark, runner, data_type, common_model_script, provider, table_name, date_column, file_date):
    """
    Unload normalized data into partitions based on
    a date column
    """

    runner.run_spark_script(file_utils.get_rel_path(__file__, '../../../../common/{}'.format(common_model_script)), [
        ['table_name', 'final_unload', False],
        ['properties', constants.unload_properties_template.format(constants.hdfs_staging_dir), False]
    ])
    runner.run_spark_script(file_utils.get_rel_path(__file__, '../../../../common/unload_common_model.sql'), [
        ['select_statement', "SELECT *, '{}' as part_provider, 'NULL' as part_best_date FROM {} WHERE {} is NULL".format(provider, table_name, date_column), False],
        ['partitions', '20', False]
    ])
    runner.run_spark_script(file_utils.get_rel_path(__file__, '../../../../common/unload_common_model.sql'), [
        ['select_statement', "SELECT *, '{0}' as part_provider, regexp_replace({2}, '-..$', '') as part_best_date FROM {1} WHERE {2} IS NOT NULL".format(provider, table_name, date_column), False],
        ['partitions', '20', False]
    ])

    part_files = subprocess.check_output(['hadoop', 'fs', '-ls', '-R', constants.hdfs_staging_dir]).strip().split("\n")

    spark.sparkContext.parallelize(part_files).repartition(1000).foreach(mk_move_file(file_date))


def distcp(dest):
    subprocess.check_call(['s3-dist-cp', '--s3ServerSideEncryption',
                           '--src', constants.hdfs_staging_dir,
                           '--dest', dest])
