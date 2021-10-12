"""
CCF normalization
"""
import argparse
import spark.providers.ccf.custom_mart.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.datamart.custom_mart.ccf import schemas as ccf_custom_schemas
import spark.common.utility.logger as logger

HAS_DELIVERY_PATH = True

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'ccf'
    output_table_names_to_schemas = {
        'ccf_obs_crf_final': ccf_custom_schemas['clinical_observation_crf'],
        'ccf_obs_emr_final': ccf_custom_schemas['clinical_observation_emr'],
        'ccf_demographics_final': ccf_custom_schemas['demography'],
        'ccf_diagnosis_crf_final': ccf_custom_schemas['diagnosis_crf'],
        'ccf_diagnosis_emr_final': ccf_custom_schemas['diagnosis_emr'],
        'ccf_encounter_crf_final': ccf_custom_schemas['encounter_crf'],
        'ccf_encounter_emr_final': ccf_custom_schemas['encounter_emr'],
        'ccf_labs_crf_final': ccf_custom_schemas['lab_test_crf'],
        'ccf_labs_emr_final': ccf_custom_schemas['lab_test_emr'],
        'ccf_patient_history_final': ccf_custom_schemas['patient_history'],
        'ccf_patient_problem_final': ccf_custom_schemas['patient_problem'],
        'ccf_rx_crf_final': ccf_custom_schemas['prescription_crf'],
        'ccf_rx_emr_final': ccf_custom_schemas['prescription_emr'],
        'ccf_proc_crf_final': ccf_custom_schemas['procedure_crf'],
        'ccf_proc_emr_final': ccf_custom_schemas['procedure_emr'],
        'ccf_vac_crf_final': ccf_custom_schemas['vaccine_crf'],
        'ccf_vac_emr_final': ccf_custom_schemas['vaccine_emr']
    }
    provider_partition_name = 'ccf'

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
        vdr_feed_id=0,
        use_ref_gen_values=False,
        unload_partition_count=10,
        load_date_explode=False,
        output_to_delivery_path=HAS_DELIVERY_PATH,
        output_to_transform_path=False
    )

    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load()

    logger.log('Start transform')
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')
