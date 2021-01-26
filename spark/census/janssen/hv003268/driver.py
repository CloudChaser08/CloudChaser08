from spark.common.census_driver import CensusDriver
from spark.common.utility.logger import log
import spark.helpers.normalized_records_unloader as normalized_records_unloader


class JanssenCensusDriver(CensusDriver):
    CLIENT_NAME = 'janssen'
    OPPORTUNITY_ID = 'hv003268'

    def __init__(self, salt, end_to_end_test=False):
        super(JanssenCensusDriver, self).__init__(
            self.CLIENT_NAME, self.OPPORTUNITY_ID, end_to_end_test=end_to_end_test, salt=salt)

        self._output_file_name_template = 'ec_sh_{batch_id_value}_{date}_response.csv.bz2'

    def save(self, dataframe, batch_date, batch_id, chunk_idx=None, header=True):
        log("Saving results to the local file system")
        dataframe.createOrReplaceTempView('deliverable')
        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)
        _file_date = _batch_id_value.split('_')[-1][:8]
        normalized_records_unloader.unload_delimited_file(
            self._spark, self._runner, 'hdfs:///staging/{batch_id_path}/'.format(
                batch_id_path=_batch_id_path
            ),
            'deliverable',
            output_file_name_template=self._output_file_name_template.format(
                batch_id_value=_batch_id_value, date=_file_date
            ),
            test=self._test, header=header, quote=False, compression='bzip2'
        )
        normalized_records_unloader.unload_delimited_file(
            self._spark, self._runner, 'hdfs:///staging2/{batch_id_path}/'.format(
                batch_id_path=_batch_id_path
            ),
            'rwd_cohort',
            output_file_name_template='{batch_id_value}_consented_hvids.txt'.format(
                batch_id_value=_batch_id_value
            ),
            test=self._test, header=False, quote=False, compression=None
        )

    def copy_to_s3(self, batch_date=None, batch_id=None):
        log("Copying files to: " + self._output_path)
        normalized_records_unloader.distcp(self._output_path)
        normalized_records_unloader.distcp('s3://salusv/incoming/cohort/janssen/hv003268/', '/staging2/')

