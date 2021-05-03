import argparse
import datetime
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas
import spark.providers.neogenomics.labv3.transactional_schemas as transactional_schemas

 # ------------------------ Provider specific configuration -----------------------

FEED_ID = '32'

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/labtests/2017-02-16/'

provider_name = 'neogenomics'
output_table_names_to_schemas = {
    'neogenomics_labtests': schemas['schema_v7']
}

# ------------------------ Common for all providers -----------------------

def main(args):

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_name,
        transactional_schemas,
        output_table_names_to_schemas,
        args.date,
        args.test,
        args.end_to_end_test,
        load_date_explode=False,
        unload_partition_count=1,
        vdr_feed_id=FEED_ID,
        use_ref_gen_values=True,
        output_to_transform_path=False
    )

    driver.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
