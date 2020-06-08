import argparse
import spark.providers.change.era.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims_common_model import schemas as medicalclaims_schemas
from spark.common.era.detail import schemas as detail_schemas
from spark.common.era.summary import schemas as summary_schemas


if __name__ == "__main__":
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'change'
    output_table_names_to_schemas = {
        'change_835_normalized_detail_final': detail_schemas['schema_v6'],
        'change_835_normalized_summary_final': summary_schemas['schema_v6']
    }
    provider_partition_name = provider_name

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')

    args = parser.parse_args()
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        load_date_explode=False,
        unload_partition_count=50,
        vdr_feed_id=186,
        use_ref_gen_values=True
    )

    driver.init_spark_context()
    driver.load(cache_tables=False, payloads=False)
    driver.transform()

    driver.save_to_disk()
    driver.log_run()
    driver.stop_spark()
    driver.copy_to_output_path()
