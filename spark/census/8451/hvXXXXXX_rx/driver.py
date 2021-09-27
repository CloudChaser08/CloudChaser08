from datetime import date
import os
import re
import subprocess
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.records_loader as records_loader
from spark.common.utility.logger import log
from .records_schemas import PDC_SCHEMA
from spark.common.census_driver import CensusDriver, SAVE_PATH


class _8451CensusDriver(CensusDriver):

    CLIENT_NAME = '8451'
    OPPORTUNITY_ID = 'hvXXXXXX_rx'
    NUM_PARTITIONS = 20
    SALT = 'hvid8451'
    BAD_PDC_DATE = date(2019, 3, 30)
    PDC_LOCATION = 's3://salusv/incoming/census/8451/hvXXXXXX/20190330_pdc_fix/'

    def __init__(self, end_to_end_test=False):
        super(_8451CensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY_ID,
                                                salt=self.SALT, end_to_end_test=end_to_end_test)

    def load(self, batch_date, batch_id, chunk_records_files=None):
        if chunk_records_files:
            raise ValueError(
                "Chunking is not currently supported in this module")

        super(_8451CensusDriver, self).load(batch_date, batch_id)

        if batch_date == self.BAD_PDC_DATE:
            df = records_loader.load(self._runner, self.PDC_LOCATION, source_table_conf=PDC_SCHEMA)
        else:
            # load empty dataframe with pdc file schema
            df = self._spark.createDataFrame([], PDC_SCHEMA.schema)
        df.createOrReplaceTempView('pdc_fix')

        if not self._test:
            external_table_loader.load_ref_gen_ref(self._sqlContext)

    def transform(self, batch_date, batch_id):
        return self._runner.run_all_spark_scripts(variables=[['salt', self._salt]])

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

        output_file_name_template = '{batch_id}_response_{{}}'.format(
            year=batch_date.year, month=batch_date.month, day=batch_date.day,
            batch_id=_batch_id_value
        )
        self._clean_up_output(output_path)
        log("Repartition and write files to hdfs")
        dataframe.repartition(self.NUM_PARTITIONS).write.csv(output_path, sep="|", header=True, compression="gzip")

        # rename output files to desired name
        # this step removes the spark hash added to the name by default
        # e.g. part-00081-35b44b47-2b52-4430-a12a-c4ed31c7bfd5-c000.psv.gz becomes <batch_id>_response_00081.psv.gz
        #
        log("Renaming files")
        for filename in [f for f in self._list_dir(output_path) if f[0] != '.' and f != "_SUCCESS"]:
            part_number = re.match('''part-([0-9]+)[.-].*''', filename).group(1)
            if chunk_idx is not None:
                part_number = int(part_number) + chunk_idx * self.NUM_PARTITIONS
            new_name = output_file_name_template.format(str(part_number).zfill(5)) + '.psv.gz'
            self._rename_file(output_path + filename, output_path + new_name)
