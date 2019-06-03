from spark.common.census_driver import CensusDriver, SAVE_PATH, PACKAGE_PATH
from spark.helpers import normalized_records_unloader

import importlib
import inspect
import os
import re
import subprocess


class Grocery8451CensusDriver(CensusDriver):

    CLIENT_NAME = '8451'
    OPPORTUNITY_ID = 'hvXXXXX2'
    NUM_PARTITIONS = 20

    def __init__(self, end_to_end_test=False):
        super(_8451GrocceryCensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY_ID,
                                                        end_to_end_test=end_to_end_test)

    def transform(self):
        # Since this module is in the package, its file path will contain the
        # package path. Remove that in order to find the location of the
        # transformation scripts
        #
        # If we do not explicitly define the path here, run_all_spark_scripts will
        # be default use ... /spark/target/dewey.zip/spark/census/ .. which will result in an error.
        #
        census_module = importlib.import_module(self.__module__)
        scripts_directory = '/'.join(inspect.getfile(census_module).replace(PACKAGE_PATH, '').split('/')[:-1] + [''])
        content = self._runner.run_all_spark_scripts(variables=[['salt', self._salt]],
                                                     directory_path=scripts_directory)
        return content.coalesce(1)

    def save(self, dataframe, batch_date):
        output_path = SAVE_PATH + '{year}/{month:02d}/{day:02d}/'.format(
                year=batch_date.year, month=batch_date.month, day=batch_date.day
            )

        if self._test:

            def clean_up_output():
                subprocess.check_call(['rm', '-rf', output_path])

            def list_dir(path):
                return os.listdir(path)

            def rename_file(old, new):
                os.rename(old, new)

        else:

            def clean_up_output():
                subprocess.check_call(['hadoop', 'fs', '-rm', '-f', '-R', output_path])

            def list_dir(path):
                return [
                    f.split(' ')[-1].strip().split('/')[-1]
                    for f in subprocess.check_output(['hdfs', 'dfs', '-ls', path]).split('\n')
                    if f.split(' ')[-1].startswith('hdfs')
                ]

            def rename_file(old, new):
                subprocess.check_call(['hdfs', 'dfs', '-mv', old, new])

        clean_up_output()

        dataframe.repartition(self.NUM_PARTITIONS).write.csv(output_path, sep="|", header=True, compression="gzip")

        # rename output files to desired name
        # this step removes the spark hash added to the name by default
        for filename in [f for f in list_dir(output_path) if f[0] != '.' and f != "_SUCCESS"]:
            new_name = 'part-' + re.match('''part-([0-9]+)[.-].*''', filename).group(1) + '.gz'
            rename_file(output_path + filename, output_path + new_name)

    def copy_to_s3(self, batch_date=None):
        output_path = SAVE_PATH + '{year}/{month:02d}/{day:02d}/'.format(
                year=batch_date.year, month=batch_date.month, day=batch_date.day
            )
        normalized_records_unloader.distcp(self._output_path, src=output_path)
