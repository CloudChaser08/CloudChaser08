import os
import re
import subprocess

from spark.common.census_driver import CensusDriver, SAVE_PATH

class Grocery8451CensusDriver(CensusDriver):

    CLIENT_NAME = '8451'
    OPPORTUNITY_ID = 'hvXXXXXX_grocery'
    NUM_PARTITIONS = 20
    SALT = "hvid8451"

    def __init__(self, end_to_end_test=False):
        super(Grocery8451CensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY_ID,
                                                      end_to_end_test=end_to_end_test)

    def transform(self):
        # By default, run_all_spark_scripts will run all sql scripts in the working directory
        content = self._runner.run_all_spark_scripts(variables=[['SALT', self.SALT]])
        return content

    def save(self, dataframe, batch_date, chunk_idx=None):
        output_path = SAVE_PATH + '{year}/{month:02d}/{day:02d}/'.format(
            year=batch_date.year, month=batch_date.month, day=batch_date.day
        )

        output_file_name_template = '{year}{month:02d}{day:02d}_response_{{}}'.format(
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
        # e.g. part-00081-35b44b47-2b52-4430-a12a-c4ed31c7bfd5-c000.psv.gz becomes <date>_response00081.psv.gz
        #
        for filename in [f for f in list_dir(output_path) if f[0] != '.' and f != "_SUCCESS"]:
            part_number = re.match('''part-([0-9]+)[.-].*''', filename).group(1)
            if chunk_idx is not None:
                part_number = int(part_number) + chunk_idx * self.NUM_PARTITIONS
            new_name = output_file_name_template.format(str(part_number).zfill(5)) + '.psv.gz'
            rename_file(output_path + filename, output_path + new_name)
