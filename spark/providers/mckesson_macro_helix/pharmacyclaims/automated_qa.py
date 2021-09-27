from spark.spark_setup import init
from spark.runner import Runner
import spark.helpers.payload_loader as payload_loader
import spark.qa.datafeed as datafeed

spark, sqlContext = init("Mckesson Macrohelix")
spark_sql_runner = Runner(sqlContext)

script_path = __file__

# note that this runs on all of their data, modify these paths to
# select a subset of their data if this is undesireable
MCKESSON_SRC_TRANSACTIONS_LOCATION = 's3a://salusv/sample/mckesson_macrohelix/transactions/'
MCKESSON_MATCHING_PAYLOAD_LOCATION = 's3a://salusv/sample/mckesson_macrohelix/payload/'
MCKESSON_TARGET_DATA_LOCATION = 's3a://salusv/sample/mckesson_macrohelix/normalized/*/*'

payload_loader.load(
    spark_sql_runner, MCKESSON_MATCHING_PAYLOAD_LOCATION,
    extra_cols=['claimId', 'patientId', 'hvJoinKey']
)

spark_sql_runner.run_spark_script('./load_transactions.sql', [
    ['input_path', MCKESSON_SRC_TRANSACTIONS_LOCATION]
], source_file_path=script_path)

source_data = {
    "transactions": sqlContext.sql(
        'SELECT t.*, p.hvid as hvid FROM mckesson_macrohelix_transactions t '
        'LEFT OUTER JOIN matching_payload p ON t.hvJoinKey = p.hvJoinKey'
    )
}

# load target data
target_data = sqlContext.read.parquet(MCKESSON_TARGET_DATA_LOCATION)

# create datafeed instances
pharmacyclaims_datafeed = datafeed.standard_pharmacyclaims_datafeed(
    source_data=source_data,
    target_data=target_data,
    source_hvid_full_name='transactions.hvid',
    source_claim_id_full_name='transactions.row_id'
)

# run tests
pharmacyclaims_datafeed.run_checks('pharmacy_results.out')

# stop spark
spark.stop()
