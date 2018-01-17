from pyspark.sql.functions import col, upper, trim

from spark.spark_setup import init
from spark.runner import Runner
import spark.providers.guardant_health.labtests.transaction_schemas as transaction_schemas
import spark.helpers.payload_loader as payload_loader

import spark.qa.datafeed as datafeed

spark, sqlContext = init("Transmed EMR")
spark_sql_runner = Runner(sqlContext)

GUARDANT_HEALTH_SRC_DATA_LOCATION = 's3a://salusv/incoming/labtests/guardant_health/2017/12/18/'
GUARDANT_HEALTH_MATCHING_PAYLOAD_LOCATION = 's3a://salusv/matching/payload/labtests/guardant_health/2017/12/18/'
GUARDANT_HEALTH_TARGET_DATA_LOCATION = 's3a://salusv/warehouse/parquet/labtests/2018-01-16/part_provider=guardant_health/*'

payload_loader.load(spark_sql_runner, GUARDANT_HEALTH_MATCHING_PAYLOAD_LOCATION, extra_cols=['hvJoinKey'])

sqlContext.read.csv(
    GUARDANT_HEALTH_SRC_DATA_LOCATION, schema=transaction_schemas.schema
).registerTempTable('transactions')

filtered_transactions_source = sqlContext.sql(
    "SELECT t.*, mp.hvid FROM transactions t "
    "LEFT JOIN matching_payload mp ON t.hvjoinkey = mp.hvjoinkey "
    "WHERE REGEXP_REPLACE(coalesce(trim(upper(physician_country)), 'UNITED STATES'), 'UNITED STATES', '') = '' "
    "AND REGEXP_REPLACE(coalesce(trim(upper(patient_country)), 'UNITED STATES'), 'UNITED STATES', '') = ''"
)

source_data = {
    "transactions": filtered_transactions_source
}

# load target data
target_data = sqlContext.read.parquet(GUARDANT_HEALTH_TARGET_DATA_LOCATION)

# create datafeed instances
labtests_datafeed = datafeed.standard_labtests_datafeed(
    source_data=source_data,
    target_data=target_data,
    source_hvid_full_name='transactions.hvid',
    source_claim_id_full_name='transactions.row_id',
    source_test_ordered_name_full_name='transactions.gene'
)

# run tests
labtests_datafeed.run_checks('labtest_results.out')

# stop spark
spark.stop()
