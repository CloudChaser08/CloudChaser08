import argparse
import spark.providers.labcorp_covid.labtests.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtest_schemas
import spark.helpers.external_table_loader as external_table_loader
import spark.common.utility.logger as logger

_ref_schema = 'darch'
_ref_table = 'labcorp_specialty_crosswalk'

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'labcorp_covid'
    output_table_names_to_schemas = {
        'labcorp_covid_labtest': labtest_schemas['schema_v9'],
    }
    provider_partition_name = 'labcorp_covid'

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
        use_ref_gen_values=True,
        vdr_feed_id=201
    )

    driver.init_spark_context()

    """
    labecorp-covid is using speciality code external tables to 
    get speciality description.
    """
    logger.log('Loading external table:{}.{}'.format(_ref_schema, _ref_table))
    external_table_loader.load_analytics_db_table(
        driver.sql_context, _ref_schema, _ref_table, 'labcorp_spec'
    )
    driver.spark.table('labcorp_spec').cache().createOrReplaceTempView('labcorp_spec')

    driver.load()

    driver.transform()
    driver.save_to_disk()
    driver.log_run()
    driver.stop_spark()
    driver.copy_to_output_path()