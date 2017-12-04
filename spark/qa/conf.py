import spark.qa.datatypes as types

# This dictionary describes which checks to run for each datatype
datatype_config = {
    types.MEDICALCLAIMS: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.PHARMACYCLAIMS: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.LABTESTS: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.EVENTS: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.ENROLLMENT: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.EMR_ENCOUNTER: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.EMR_PROCEDURE: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.EMR_DIAGNOSIS: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.EMR_PROVIDER_ORDER: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.EMR_LAB_ORDER: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.EMR_LAB_RESULT: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.EMR_MEDICATION: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.EMR_CLINICAL_OBSERVATION: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ],
    types.EMR_VITAL_SIGN: [
        'test_all_unique_vals_in_src_and_target',
        'test_validations',
        'test_full_fill_rates'
    ]
}
