import importlib
import spark.helpers.payload_loader as payload_loader
import spark.helpers.records_loader as records_loader
import spark.common.utility.logger as logger
from spark.common.census_driver import CensusDriver, SAVE_PATH

DRIVER_MODULE_NAME = 'driver'
SALUSV = 's3://salusv/'


class Grocery8451CensusDriver(CensusDriver):

    CLIENT_NAME = '8451'
    OPPORTUNITY_ID = 'hvXXXXXX_grocery'
    NUM_PARTITIONS = 5000
    SALT = "hvid8451"
    LOADED_PAYLOADS = False

    def __init__(self, end_to_end_test=False):
        super(Grocery8451CensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY_ID,
                                                      end_to_end_test=end_to_end_test)
        self._column_length = None

    def load(self, batch_date, batch_id, chunk_records_files=None):
        logger.log('Loading')
        batch_id_path = '{year}/{month:02d}/{day:02d}'.format(year=batch_date.year,
                                                              month=batch_date.month,
                                                              day=batch_date.day)
        records_path = self._records_path_template.format(
            client=self.CLIENT_NAME, opp_id=self.OPPORTUNITY_ID, batch_id_path=batch_id_path
        )

        matching_path = self._matching_path_template.format(
            client=self.CLIENT_NAME, opp_id=self.OPPORTUNITY_ID, batch_id_path=batch_id_path
        )

        matching_payloads_schemas_module = self.__module__.replace(DRIVER_MODULE_NAME, self._matching_payloads_module_name)
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

        logger.log("Loading records")
        records_loader.load_and_clean_all_v2(self._runner, records_path,
                                             records_schemas, load_file_name=True,
                                             partitions=self.NUM_PARTITIONS)

        if not self.LOADED_PAYLOADS:
            logger.log("Loading payloads")
            self.LOADED_PAYLOADS = True
            payload_loader.load_all(self._runner, matching_path, matching_payloads_schemas)

    def transform(self, batch_date, batch_id):
        logger.log('Transforming')

        # By default, run_all_spark_scripts will run all sql scripts in the working directory
        content = self._runner.run_all_spark_scripts(variables=[['SALT', self.SALT]])
        return content

    def save(self, dataframe, batch_date, batch_id, chunk_idx=None):
        logger.log("Save")
        output_path = SAVE_PATH + '{year}/{month:02d}/{day:02d}/'.format(
            year=batch_date.year, month=batch_date.month, day=batch_date.day
        )
        logger.log("Saving partitions to file system")
        output_path = 's3://salusv/deliverable/8451/hvXXXXXX_grocery/{year}/{month:02d}/{day:02d}/'.format(
            year=batch_date.year, month=batch_date.month, day=batch_date.day
        )

        dataframe.write.csv(output_path, sep="|", header=True, mode="append", compression="gzip")
        dataframe.unpersist()
        logger.log("Done writing files.")

