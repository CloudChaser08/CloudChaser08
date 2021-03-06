"""
allscripts custom era normalize
"""
import argparse
import spark.providers.allscripts.custom_era.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.era.detail import schemas as detail_schemas
from spark.common.era.summary import schemas as summary_schemas
from spark.helpers.s3_constants import DATAMART_PATH, E2E_DATAMART_PATH


def run(date_input, end_to_end_test=False, test=False, spark=None, runner=None):
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'allscripts'
    provider_partition_name = '83'
    opportunity_id = 'definitive_hv002886'

    additional_output_path = DATAMART_PATH if not end_to_end_test else E2E_DATAMART_PATH
    additional_output_path = additional_output_path + '{}/'.format(opportunity_id)

    output_table_names_to_schemas = {
        'veradigm_era_detail': detail_schemas['schema_v5'],
        'veradigm_era_summary': summary_schemas['schema_v5']
    }

    additional_output_schemas = {
        'veradigm_era_detail': detail_schemas['schema_v5_daily'],
        'veradigm_era_summary': summary_schemas['schema_v5_daily']
    }

    # ------------------------ Common for all providers -----------------------

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        use_ref_gen_values=True,
        vdr_feed_id=83,
        unload_partition_count=2,
        additional_output_path=additional_output_path,
        additional_output_schemas=additional_output_schemas
    )

    driver.init_spark_context()
    driver.load()
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()

    driver.copy_to_output_path()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    run(args.date, args.end_to_end_test)
