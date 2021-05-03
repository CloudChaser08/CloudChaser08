import argparse

from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtest_schemas
import spark.providers.ambry_genetics.labtests.transactional_schemas as source_table_schemas


if __name__ == '__main__':
    output_table_names_to_schemas = {
        'ambry_labtest_rnd3': labtest_schemas['schema_v8'],
    }

    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()

    date_input = args.date
    end_to_end_test = args.end_to_end_test

    driver = MarketplaceDriver(
        "ambry_genetics",
        "ambry",
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test
    )

    driver.run()
