"""
HV003114 UBC HUB Lash census driver
"""
from subprocess import check_output
from datetime import datetime
import importlib
import inspect

from spark.common.census_driver import CensusDriver, SAVE_PATH
from spark.common.utility.logger import log
from spark.runner import PACKAGE_PATH
import spark.helpers.normalized_records_unloader as normalized_records_unloader


class LashCensusDriver(CensusDriver):
    """
    Lash driver
    """
    CLIENT_NAME = "lash"
    OPPORTUNITY_ID = "hv003114"

    def __init__(
        self, client_name=None, opportunity_id=None, salt=None, test=False, end_to_end_test=False
    ):
        super(LashCensusDriver, self).__init__(
            self.CLIENT_NAME, self.OPPORTUNITY_ID, salt=salt,
            test=test, end_to_end_test=end_to_end_test
        )
    
    def save(self, dataframe, batch_date, batch_id, chunk_idx=None, header=True):
        # same as normal census driver save, except not compressed and with .txt filenames
        log("Saving results to the local file system")
        dataframe.createOrReplaceTempView('deliverable')
        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)
        formatted_save_path = SAVE_PATH + '{batch_id_path}/'.format(
            batch_id_path=_batch_id_path
        )
        output_file_name_template = '{batch_id_value}_response_{{part_num}}.txt'
        normalized_records_unloader.unload_delimited_file(
            self._spark, self._runner, formatted_save_path,
            'deliverable',
            output_file_name_template=output_file_name_template.format(
                batch_id_value=_batch_id_value
            ),
            test=self._test, header=header,
            compression='none'
        )

    def transform(self, batch_date, batch_id):
        log("Transforming records")
        
        # LASH provides dates but not times for consenter preferences,
        # so add the batch ID's time (from the filename) but maintain the date in the row
        log("Attempting to pull datetime from batch ID: {}".format(batch_id))
        batch_dt = datetime.strptime(batch_id, '%Y%m%d%H%M%S')
        timestamp = batch_dt.strftime('%H:%M:%S')

        log("Retrieved timestamp {} from batch ID".format(timestamp))
        # Since this module is in the package, its file path will contain the
        # package path. Remove that in order to find the location of the
        # transformation scripts
        census_module = importlib.import_module(self._base_package)
        scripts_directory = '/'.join(inspect.getfile(census_module).replace(PACKAGE_PATH, '').split('/')[:-1] + [''])
        content = self._runner.run_all_spark_scripts(variables=[['salt', self._salt], ['timestamp', timestamp]],
                                                     directory_path=scripts_directory)

        return content
