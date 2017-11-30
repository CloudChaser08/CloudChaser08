import spark.qa.datatypes as types

# This dictionary describes which checks to run for each datatype
datatype_config = {
    types.MEDICALCLAIMS: [
        'test_all_claims_in_src_and_target',
        'test_all_service_lines_in_src_and_target',
        'test_all_hvids_in_src_and_target',
        'test_valid_gender_values_in_target',
        'test_valid_patient_state_values_in_target',
        'test_full_fill_rate_for_record_id',
        'test_full_fill_rate_for_created_date',
        'test_full_fill_rate_for_model_version',
        'test_full_fill_rate_for_data_set',
        'test_full_fill_rate_for_data_feed',
        'test_full_fill_rate_for_data_vendor',
        'test_full_fill_rate_for_provider_partition',
        'test_full_fill_rate_for_date_partition'
    ],
    types.PHARMACYCLAIMS: [
        'test_all_claims_in_src_and_target',
        'test_all_hvids_in_src_and_target',
        'test_valid_gender_values_in_target',
        'test_valid_patient_state_values_in_target',
        'test_full_fill_rate_for_record_id',
        'test_full_fill_rate_for_created_date',
        'test_full_fill_rate_for_model_version',
        'test_full_fill_rate_for_data_set',
        'test_full_fill_rate_for_data_feed',
        'test_full_fill_rate_for_data_vendor',
        'test_full_fill_rate_for_provider_partition',
        'test_full_fill_rate_for_date_partition'
    ],
    types.LABTESTS: [
        'test_all_claims_in_src_and_target',
        'test_all_hvids_in_src_and_target',
        'test_valid_gender_values_in_target',
        'test_valid_patient_state_values_in_target',
        'test_full_fill_rate_for_record_id',
        'test_full_fill_rate_for_created_date',
        'test_full_fill_rate_for_model_version',
        'test_full_fill_rate_for_data_set',
        'test_full_fill_rate_for_data_feed',
        'test_full_fill_rate_for_data_vendor',
        'test_full_fill_rate_for_provider_partition',
        'test_full_fill_rate_for_date_partition'
    ]
}
