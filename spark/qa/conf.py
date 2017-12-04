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
    ]
}
