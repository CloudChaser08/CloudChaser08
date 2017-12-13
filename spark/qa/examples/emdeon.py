from spark.spark_setup import init
from spark.runner import Runner
import spark.helpers.file_utils as file_utils

import spark.qa.datafeed as datafeed

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
emdeon_datafeed = datafeed.standard_medicalclaims_datafeed(
    source_data=source_data,
    target_data=target_data,
    source_claim_id_full_name='emdeon_dx_raw_local.column1',
    source_service_line_number_full_name='emdeon_dx_raw_local.column3',
    skip_target_full_fill_columns=['part_provider', 'part_best_date', 'model_version']
)

# run tests
emdeon_datafeed.run_checks()

# stop spark
spark.stop()
