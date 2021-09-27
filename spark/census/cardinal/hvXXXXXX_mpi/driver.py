"""
cardinal hvXXXXX driver
"""
import subprocess

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, lit  # pylint: disable=no-name-in-module

from spark.common.utility.logger import log
from spark.common.census_driver import CensusDriver, SAVE_PATH


class CardinalMPICensusDriver(CensusDriver):
    def __init__(self, client_name, opportunity_id, end_to_end_test=False, test=False):
        super().__init__(
            client_name=client_name, opportunity_id=opportunity_id,
            end_to_end_test=end_to_end_test, test=test
        )

        self._output_file_name_template = 'cardinal_mpi_matched_{batch_id_value}.psv.gz'

    def load(self, batch_date, batch_id, chunk_records_files=None):
        super().load(batch_date, batch_id, chunk_records_files)

        # topCandidates is suppose to be a column of array type. If all the values
        # are NULL it ends up being a string type. Replace it with an array type
        # column of all nulls so the routine doesn't break
        if self._spark.table('matching_payload').where('topcandidates IS NOT NULL').count() == 0:
            # pylint: disable=not-callable
            null_array_column = udf(lambda x: None, ArrayType(ArrayType(StringType(), True), True))(lit(None))
            self._spark.table('matching_payload') \
                .withColumn('topcandidates', null_array_column) \
                .createOrReplaceTempView('matching_payload')

        # This routine outputs to a CSV-backed HIVE table so that null values are not encapsulated
        # by quotes (spark.write.csv behavior in Spark 2.3 or lower)
        self._spark.sql('SET hive.exec.compress.output=true')
        self._spark.sql('SET mapreduce.output.fileoutputformat.compress=true')
        self._spark.sql('SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec')
        self._spark.sql('DROP TABLE IF EXISTS cardinal_mpi_model')

    def save(self, dataframe, batch_date, batch_id, chunk_idx=None, header=True):
        log("Saving results to the local file system")

        # Move the CSV file created by outputting to a HIVE table to the typical location used
        # when writing CSV files from Spark in a standard Census routine so downstream steps
        # don't have to change
        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)
        output_file_name = self._output_file_name_template.format(
            batch_id_value=_batch_id_value
        )
        files = subprocess.check_output(
            ['hadoop', 'fs', '-ls', '/tmp/cardinal_mpi/']).decode().split('\n')
        temp_output_file = [f.split(' ')[-1] for f in files if f.endswith('.gz')][0]
        subprocess.check_call([
            'hadoop', 'fs', '-mkdir', '-p', SAVE_PATH + _batch_id_path
        ])
        subprocess.check_call([
            'hadoop', 'fs', '-mv', temp_output_file,
            SAVE_PATH + _batch_id_path + '/' + output_file_name
        ])
