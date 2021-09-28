"""driver ubc avexis"""
import importlib
import inspect

from pyspark.sql.types import StructType, StructField, StringType

from spark.common.census_driver import CensusDriver
from spark.common.utility.logger import log
from spark.runner import PACKAGE_PATH

MPL_URL = 's3://salusv/reference/census/ubc_avexis/HV004223/mpl/'
MPL_COLUMNS = ['privateidone', 'gender', 'firstservicedate', 'hvid']
MPL_START = 500

class UbcAvexisCensusDriver(CensusDriver):
    def load(self, batch_date, batch_id, chunk_records_files=None):
        super().load(batch_date, batch_id, chunk_records_files)

        try:
            self._spark.read.parquet(MPL_URL).createOrReplaceTempView('historically_assigned_ids')
        except:
            schema = StructType([StructField(col, StringType(), True) for col in MPL_COLUMNS])
            self._spark.createDataFrame([], schema)\
                .createOrReplaceTempView('historically_assigned_ids')

    def transform(self, batch_date, batch_id):
        log("Transforming records")

        res = self._spark.\
            sql("SELECT max(cast(hvid as int)) FROM historically_assigned_ids").collect()
        max_id = str(res[0][0] or MPL_START)

        census_module = importlib.import_module(self._base_package)

        # Since this module is in the package, its file path will contain the
        # package path. Remove that in order to find the location of the
        # transformation scripts
        scripts_directory = '/'.join(inspect.getfile(census_module).replace(PACKAGE_PATH, '').split('/')[:-1] + [''])
        content = self._runner.run_all_spark_scripts(variables=[['max_assigned_id', max_id, False]],
                                                     directory_path=scripts_directory)

        return content

    def save(self, dataframe, batch_date, batch_id, chunk_idx=None, header=True):
        super().save(dataframe, batch_date, batch_id, chunk_idx, header)

        old_ids = self._spark.table('id_previously_assigned').select(*MPL_COLUMNS)
        new_ids = self._spark.table('id_newly_assigned').select(*MPL_COLUMNS)
        old_ids.union(new_ids).distinct().repartition(1).write.parquet(MPL_URL, mode='overwrite')
