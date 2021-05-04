import argparse
import spark.providers.ihm.emr.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.emr.encounter import schemas as encounter_schemas
from spark.common.emr.diagnosis import schemas as diagnosis_schemas
from spark.common.emr.procedure import schemas as procedure_schemas
from spark.common.emr.lab_test import schemas as lab_test_schemas
from spark.common.emr.medication import schemas as medication_schemas
# import spark.helpers.external_table_loader as external_table_loader


if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'ihm'
    output_table_names_to_schemas = {
        'ihm_emr_norm_emr_enc': encounter_schemas['schema_v10'],
        'ihm_emr_norm_emr_diag': diagnosis_schemas['schema_v10'],
        'ihm_emr_norm_emr_proc': procedure_schemas['schema_v12'],
        'ihm_emr_norm_emr_labtest': lab_test_schemas['schema_v3'],
        'ihm_emr_norm_emr_medication': medication_schemas['schema_v11']
    }
    provider_partition_name = '210'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
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
        vdr_feed_id=210,
        output_to_transform_path=False
    )

    driver.run()
