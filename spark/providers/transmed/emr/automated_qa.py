"""
automated QA
"""
from spark.spark_setup import init
from spark.runner import Runner
import spark.providers.transmed.emr.transaction_schemas as transaction_schemas
import spark.helpers.payload_loader as payload_loader

import spark.qa.datafeed as datafeed

spark, sqlContext = init("Transmed EMR")
spark_sql_runner = Runner(sqlContext)

TRANSMED_SRC_DATA_LOCATION = 's3a://salusv/incoming/emr/transmed/2017/11/29/'
TRANSMED_MATCHING_PAYLOAD_LOCATION = 's3a://salusv/matching/payload/emr/transmed/2017/11/29/'
TRANSMED_TARGET_DATA_LOCATION_TEMPLATE = \
    's3a://salusv/warehouse/parquet/emr/2018-01-05/{}/part_hvm_vdr_feed_id=54/part_mth=2017-11/'

payload_loader.load(spark_sql_runner, TRANSMED_MATCHING_PAYLOAD_LOCATION, extra_cols=['claimId'])

source_data = {
    "cancerepisode'": sqlContext.read.csv(
        TRANSMED_SRC_DATA_LOCATION + 'cancerepisode/',
        schema=transaction_schemas.cancerepisode, sep='\t'
    ),

    "treatmentsite'": sqlContext.read.csv(
        TRANSMED_SRC_DATA_LOCATION + 'treatmentsite/',
        schema=transaction_schemas.treatmentsite, sep='\t'
    ),

    "matching_payload": sqlContext.sql('select * from matching_payload')
}

# load target data
clinical_observation_target_data = \
    sqlContext.read.parquet(TRANSMED_TARGET_DATA_LOCATION_TEMPLATE.format('clinical_observation'))
diagnosis_target_data = \
    sqlContext.read.parquet(TRANSMED_TARGET_DATA_LOCATION_TEMPLATE.format('diagnosis'))
procedure_target_data = \
    sqlContext.read.parquet(TRANSMED_TARGET_DATA_LOCATION_TEMPLATE.format('procedure'))
lab_result_target_data = \
    sqlContext.read.parquet(TRANSMED_TARGET_DATA_LOCATION_TEMPLATE.format('lab_result'))

# create datafeed instances
clinical_observation_datafeed = datafeed.emr_clinical_observation_datafeed(
    source_data=source_data,
    target_data=clinical_observation_target_data,
    source_hvid_full_name='matching_payload.hvid',
    skip_unique_match_pairs=['hvid']
)
diagnosis_datafeed = datafeed.emr_diagnosis_datafeed(
    source_data=source_data,
    target_data=diagnosis_target_data,
    source_hvid_full_name='matching_payload.hvid',
    skip_unique_match_pairs=['hvid']
)
procedure_datafeed = datafeed.emr_procedure_datafeed(
    source_data=source_data,
    target_data=procedure_target_data,
    source_hvid_full_name='matching_payload.hvid',
    skip_unique_match_pairs=['hvid']
)
lab_result_datafeed = datafeed.emr_lab_result_datafeed(
    source_data=source_data,
    target_data=lab_result_target_data,
    source_hvid_full_name='matching_payload.hvid',
    skip_unique_match_pairs=['hvid']
)

# run tests
clinical_observation_datafeed.run_checks('clinical_observation_results.out')
diagnosis_datafeed.run_checks('diagnosis_results.out')
procedure_datafeed.run_checks('procedure_results.out')
lab_result_datafeed.run_checks('lab_result_results.out')

# stop spark
spark.stop()
