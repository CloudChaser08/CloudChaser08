"""
8451 hvXXXXXX grocery driver
"""
import importlib
import spark.helpers.payload_loader as payload_loader
import spark.helpers.records_loader as records_loader
from spark.common.utility.logger import log
import os
import re
import subprocess
from spark.common.census_driver import CensusDriver, SAVE_PATH

DRIVER_MODULE_NAME = 'driver'
SALUSV = 's3://salusv/'


class Grocery8451CensusDriver(CensusDriver):

    CLIENT_NAME = '8451'
    OPPORTUNITY_ID = 'hvXXXXXX_grocery'
    NUM_PARTITIONS = 100
    SALT = "hvid8451"
    LOADED_PAYLOADS = False

    def __init__(self, end_to_end_test=False):
        super(Grocery8451CensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY_ID,
                                                      end_to_end_test=end_to_end_test)
        self._column_length = None

    def load(self, batch_date, batch_id, chunk_records_files=None):
        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)

        log('Loading')
        records_path = self._records_path_template.format(
            client=self.CLIENT_NAME, opp_id=self.OPPORTUNITY_ID, batch_id_path=_batch_id_path
        )

        matching_path = self._matching_path_template.format(
            client=self.CLIENT_NAME, opp_id=self.OPPORTUNITY_ID, batch_id_path=_batch_id_path
        )

        matching_payloads_schemas_module = \
            self.__module__.replace(DRIVER_MODULE_NAME, self._matching_payloads_module_name)
        matching_payloads_schemas = importlib.import_module(matching_payloads_schemas_module)

        # _column_length should only be set the first time load is called.
        # This is important when chunking files.
        if self._column_length is None:
            self._column_length = len(self._spark.read.csv(records_path, sep='|').columns)

        if self._column_length == 63:
            records_schemas = importlib.import_module('.records_schemas', __package__)
        else:
            records_schemas = importlib.import_module('.records_schemas_v2', __package__)

        if chunk_records_files:
            records_path = SALUSV + '{' + ','.join(chunk_records_files) + '}'

        log("Loading records")
        records_loader.load_and_clean_all_v2(self._runner, records_path,
                                             records_schemas, load_file_name=True
                                             )

        if not self.LOADED_PAYLOADS:
            log("Loading payloads")
            self.LOADED_PAYLOADS = True
            payload_loader.load_all(self._runner, matching_path, matching_payloads_schemas)

    def transform(self, batch_date, batch_id):
        log('Transforming')
        # By default, run_all_spark_scripts will run all sql scripts in the working directory
        content = self._runner.run_all_spark_scripts(variables=[['SALT', self.SALT]])
        return content

    def _clean_up_output(self, output_path):
        if self._test:
            subprocess.check_call(['rm', '-rf', output_path])
        else:
            subprocess.check_call(['hadoop', 'fs', '-rm', '-f', '-R', output_path])

    def _list_dir(self, path):
        if self._test:
            return os.listdir(path)
        else:
            log('path: ' + path)
            files = [
                f.split(' ')[-1].strip().split('/')[-1]
                for f in
                str(subprocess.check_output(['hdfs', 'dfs', '-ls', path])).split('\\n')
                if f.split(' ')[-1].startswith('hdfs')
            ]
            log('files: ' + ", ".join(files))
            return [f.split(' ')[-1].strip().split('/')[-1] for f in files]

    def _rename_file(self, old, new):
        if self._test:
            os.rename(old, new)
        else:
            subprocess.check_call(['hdfs', 'dfs', '-mv', old, new])

    def save(self, dataframe, batch_date, batch_id, chunk_idx=None, header=True):
        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)

        # This customer wants this data delivered to their sftp in the following format:
        #       <path_to_8451_sftp>/pickup/YYYYMMDD/
        log("Running queries and collecting data")
        output_path = SAVE_PATH + _batch_id_path + '/'

        str_date = _batch_id_value.replace('-', '')

        output_file_name_template = 'hv_effo_groc_' + str_date + '_{{}}'.format(
            year=batch_date.year, month=batch_date.month, day=batch_date.day
        )
        self._clean_up_output(output_path)
        log("Repartition and write files to hdfs")
        dataframe.repartition(self.NUM_PARTITIONS).write.csv(output_path, sep="|", header=True, compression="gzip")

        # rename output files to desired name this step removes the spark hash added to the name
        # by default e.g. part-00081-35b44b47-2b52-4430-a12a-c4ed31c7bfd5-c000.psv.gz becomes
        # <date>_response00081.psv.gz
        #
        log("Renaming files")
        for filename in [f for f in self._list_dir(output_path) if f[0] != '.' and f != "_SUCCESS"]:
            part_number = re.match('''part-([0-9]+)[.-].*''', filename).group(1)
            if chunk_idx is not None:
                part_number = int(part_number) + chunk_idx * self.NUM_PARTITIONS
            new_name = output_file_name_template.format(str(part_number).zfill(5)) + '.psv.gz'
            self._rename_file(output_path + filename, output_path + new_name)
