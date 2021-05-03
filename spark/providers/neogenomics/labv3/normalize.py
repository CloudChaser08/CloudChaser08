import argparse
import datetime
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas
import spark.providers.neogenomics.labv3.transactional_schemas as transactional_schemas

 # ------------------------ Provider specific configuration -----------------------

feed_id = '32'
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
        vdr_feed_id=feed_id,
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
