import argparse
import spark.providers.change.medicalclaims.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims_common_model import schemas as medicalclaims_schemas


if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'change'
    output_table_names_to_schemas = {
        'change_837_05_norm_final': medicalclaims_schemas['schema_v10'],
    }
    provider_partition_name = 'change'

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
        output_to_transform_path=True,
        vdr_feed_id=185,
        use_ref_gen_values=True,
        unload_partition_count=40,
        load_date_explode=False
    )
    driver.init_spark_context()
    driver.load()

    # only 2 columns are needed from the following 2 tables.
    # Select only the columns we need, then broadcast in the sql
    driver.spark.sql('select pcn, UPPER(claimid) as claimid from passthrough group by 1, 2')\
        .createOrReplaceTempView('pas_tiny')
    driver.spark.sql('select patient_gender, UPPER(claim_number) as claim_number from plainout group by 1, 2')\
        .createOrReplaceTempView('pln_tiny')

    driver.transform()
    driver.save_to_disk()
    driver.log_run()
    driver.stop_spark()
    driver.copy_to_output_path()