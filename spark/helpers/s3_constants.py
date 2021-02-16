# Various s3 paths used for normalization output
# If you need to use a new output path, consider adding it here instead of in a local variable

RECORDS_PATH = 's3://salusv/incoming/{data_type}/{provider_name}/{year}/{month:02d}/{day:02d}/'
MATCHING_PATH = 's3://salusv/matching/payload/{data_type}/{provider_name}/{year}/{month:02d}/{day:02d}/'
PRODUCTION_PATH = 's3://salusv/warehouse/parquet/'
TRANSFORM_PATH = 's3://salusv/warehouse/transformed/'
RESTRICTED_PATH = 's3://salusv/warehouse/restricted/'
DATAMART_PATH = 's3://salusv/warehouse/datamart/{opp_id}/' # opp_id is the provider's Opportunity ID
E2E_OUTPUT_PATH = 's3://salusv/testing/dewey/airflow/e2e/'
E2E_DATAMART_PATH = 's3://salusv/testing/dewey/airflow/e2e/datamart/{opp_id}/'
