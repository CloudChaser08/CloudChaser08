from spark.common.census_driver import CensusDriver, DRIVER_MODULE_NAME
from spark.helpers.file_utils import FileSystemType, util_functions_factory

import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.records_loader as records_loader

import importlib

SAVE_PATH = "hdfs:///staging/{batch_id}/"
LOCAL_SAVE_PATH = "/tmp/staging/{batch_id}/"


class CardinalAPICensusDriver(CensusDriver):
    """" Driver Class for Cardinal API Spark Routine.

    The operations performed in the routine are:


    Load:
    ---
    Load the matching data from a deid file in s3 with prefix:
    * s3://salusv/matching/payload/census/cardinal_mpi_api/hvXXXXX3/{batch_id}/

    Load the records data from a psv file in s3 with prefix:
    * s3://salusv/incoming/census/cardinal_mpi_api/hvXXXXX3/{batch_id}

    The records file contains rows with the following format:

    <deid_payload>|<job_id>|<client_id>|<b64encode callback data>


    Transform
    ---
    Execute 1_normalize.sql which transforms the data from the payload and creates a resulting dataframe.
    The payload is joined with the records file where job_id = hvJoinKey.

    Save
    ---
    Save the resulting dataframe from transformation to the hdfs, or to the local disk if in test mode.
    The file saved contains rows of json, where an example is:

    {
        "hvid": <obfuscated hvid>,
        "claim_id": <claim_id>,
        "client_id": <client_id>,
        "job_id": "<job_id>",
        "callback_data": <callback url base64-encoded>,
        "errors": <deid errors in json format>
    }

    Compress and rename the file to {batch_id}_response.json.gz.


    Copy to S3
    ---
    Copy the file to the s3 delivery location: s3://salusv/deliverable/cardinal_mpi_api/{batch_id}/


    """

    CLIENT_NAME = 'cardinal'
    NUM_PARTITIONS = 1

    def __init__(self, opportunity_id, end_to_end_test=False, test=False):

        super(CardinalAPICensusDriver, self).__init__(
            client_name=self.CLIENT_NAME,
            opportunity_id=opportunity_id,
            end_to_end_test=end_to_end_test,
            test=test
        )

    def transform(self):

        # Override parent definition to not include a header
        stage = "test" if self._end_to_end_test else "prod"
        content = self._runner.run_all_spark_scripts([
            ["stage", stage]
        ])

        return content

    def save(self, df, batch_date, batch_id):

        # Use local file system if test, else use HDFS
        if self._test:
            save_path = LOCAL_SAVE_PATH.format(batch_id=batch_id)
            file_type = FileSystemType.LOCAL
        else:
            save_path = SAVE_PATH.format(batch_id=batch_id)
            file_type = FileSystemType.HDFS

        output_file_name_template = "{batch_id}_response".format(batch_id=batch_id)

        clean_up_output, list_dir, rename_file = util_functions_factory(file_type)

        clean_up_output(save_path)

        df.repartition(self.NUM_PARTITIONS).write.option("dropFieldIfAllNull", False).json(save_path, compression="gzip")

        # rename file
        # part-{part_number}-{uuid}.json.gzip becomes {batch_id}_response.json.gz
        for filename in [f for f in list_dir(save_path) if f[0] != '.' and f != "_SUCCESS"]:
            new_name = output_file_name_template + '.json.gz'
            rename_file(save_path + filename, save_path + new_name)
