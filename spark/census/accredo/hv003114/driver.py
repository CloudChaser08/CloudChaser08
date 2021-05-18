"""
HV003114 UBC HUB Lash census driver
"""
from subprocess import check_output
import os

from spark.common.census_driver import CensusDriver, SAVE_PATH
from spark.common.utility.logger import log
import spark.helpers.normalized_records_unloader as normalized_records_unloader

class AccredoCensusDriver(CensusDriver):
    """
    Accredo driver
    """
    CLIENT_NAME = "accredo"
    OPPORTUNITY_ID = "hv003114"

    def __init__(
        self, client_name=None, opportunity_id=None, salt=None, test=False, end_to_end_test=False
    ):
        super(AccredoCensusDriver, self).__init__(
            self.CLIENT_NAME, self.OPPORTUNITY_ID, salt=salt,
            test=test, end_to_end_test=end_to_end_test
        )

    def save(self, dataframe, batch_date, batch_id, chunk_idx=None, header=True):
        # same as normal census driver save, except not compressed
        log("Saving results to the local file system")
        dataframe.createOrReplaceTempView('deliverable')
        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)
        formatted_save_path = SAVE_PATH + '{batch_id_path}/'.format(
            batch_id_path=_batch_id_path
        )
        normalized_records_unloader.unload_delimited_file(
            self._spark, self._runner, formatted_save_path,
            'deliverable',
            output_file_name_template=self._output_file_name_template.format(
                batch_id_value=_batch_id_value
            ),
            test=self._test, header=header,
            compression='none'
        )

        # rename output files to .txt
        output_files = check_output(['hadoop', 'fs', 'ls', formatted_save_path])

        for filename in output_files.split('\n'):
            filename_without_ext = os.path.splitext(os.path.basename(filename))[0]
            new_filename = '{}.txt'.format(filename_without_ext)
            check_output(['hadoop', 'fs', 'mv', formatted_save_path + filename, formatted_save_path + new_filename])
