from spark.spark_setup import init
from spark.runner import Runner
import spark.helpers.file_utils as file_utils

from spark.qa.datafeed import Datafeed
import spark.qa.datatypes as types

spark, sqlContext = init("Emdeon Example", True)
spark_sql_runner = Runner(sqlContext)

script_path = __file__
EMDEON_SRC_DATA_LOCATION = file_utils.get_abs_path(
    script_path, '../../test/providers/emdeon/medicalclaims/resources/input/2017/08/31/'
)
EMDEON_TARGET_DATA_LOCATION = file_utils.get_abs_path(
    script_path, '../../test/qa/resources/emdeon-test-out/'
)

# load source data
spark_sql_runner.run_spark_script(
    '../../providers/emdeon/medicalclaims/load_transactions.sql',
    [["input_path", EMDEON_SRC_DATA_LOCATION]],
    source_file_path=script_path
)

source_data = {
    "emdeon_dx_raw_local": sqlContext.sql('select * from emdeon_dx_raw_local')
}

# load target data
target_data = sqlContext.read.parquet(EMDEON_TARGET_DATA_LOCATION)

# create an instance of Datafeed
datafeed = Datafeed(
    datatype=types.MEDICALCLAIMS,
    source_data=source_data,
    target_data=target_data,
    source_data_claim_full_name='emdeon_dx_raw_local.column1',
    source_data_service_line_full_name='emdeon_dx_raw_local.column3',
    target_data_claim_column_name='claim_id',
    target_data_service_line_column_name='service_line_number'
)

# run tests
datafeed.run_checks()

# stop spark
spark.stop()
