import inspect
import importlib
import re
import subprocess

import spark.common.std_census as std_census

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from spark.common.census_driver import CensusDriver, SAVE_PATH
from spark.helpers import normalized_records_unloader

class _8451CensusDriver(CensusDriver):

    CLIENT_NAME = '8451'
    OPPORTUNITY_ID = 'hvXXXXXX'
    NUM_PARTITIONS = 20
    SALT = "hvidhvXXXXXX"

    def __init__(self, end_to_end_test=False):
        super(_8451CensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY_ID, end_to_end_test=end_to_end_test)

    def transform(self, date_input=None):

        # Since this module is in the package, its file path will contain the
        # package path. Remove that in order to find the location of the
        # transformation scripts
        #
        # If we do not explicitly define the path here, run_all_spark_scripts will
        # be default use ... /spark/target/dewey.zip/spark/census/ .. which will result in an error.
        #
        content = self._runner.run_all_spark_scripts(variables=[['SALT', self.SALT],
                                                                ['VDR_FILE_DT', date_input.strftime('%Y-%m-%d')]])

        # Only include the file_name in the data_set
        def data_set_name(full_path):
            return full_path.split("/")[-1]

        udf_data_set_name = udf(data_set_name, StringType())
        content = content.withColumn('data_set', udf_data_set_name("data_set"))

        return content.coalesce(1)

    def save(self, dataframe, batch_date):
        dataframe.createOrReplaceTempView('deliverable')

        output_file_name_prefix = 'part-'

        output_path = SAVE_PATH + '{year}/{month:02d}/{day:02d}/'.format(
                year=batch_date.year, month=batch_date.month, day=batch_date.day
            )

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

        dataframe.repartition(self.NUM_PARTITIONS).write.csv(output_path, sep="|", header=True, compression='gzip')

        # rename output files to desired name
        # this step removes the spark hash added to the name by default
        # e.g. part-00000-746f59d5-b38f-4afc-b211-ff2e02e17b7c-c000.csv is renamed to part-00000.psv.gz
        for filename in [f for f in list_dir(output_path) if f[0] != '.' and f != "_SUCCESS"]:
                new_name = output_file_name_prefix + re.match('''part-([0-9]+)[.-].*''', filename).group(1) + '.psv.gz'
                rename_file(output_path + filename, output_path + new_name)

    def copy_to_s3(self, batch_date=None):
        output_path = SAVE_PATH + '{year}/{month:02d}/{day:02d}/'.format(
                year=batch_date.year, month=batch_date.month, day=batch_date.day
            )
        normalized_records_unloader.distcp(self._output_path, src=output_path)
