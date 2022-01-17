"""
inovalon enrollments normalize
"""
import argparse
import spark.providers.inovalon.enrollmentrecords.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.enrollment import schemas as enrollment_schemas
from datetime import datetime, date

v_cutoff_date = '2021-02-01'

if __name__ == "__main__":

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'inovalon'

    if datetime.strptime(date_input, '%Y-%m-%d').date() < \
            datetime.strptime(v_cutoff_date, '%Y-%m-%d').date():
        schema = enrollment_schemas['schema_v5']
    else:
        schema = enrollment_schemas['schema_v6']

    output_table_names_to_schemas = {
        'inovalon_enr2_norm_final': schema,
    }
    provider_partition_name = provider_name

    # ------------------------ Common for all providers -----------------------

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        load_date_explode=False,
        unload_partition_count=40,
        vdr_feed_id=179,
        use_ref_gen_values=True,
        output_to_transform_path=False
    )

    driver.run()
