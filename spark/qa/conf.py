# This dictionary describes which checks to run for each datatype
datatype_config = {
    "medicalclaims": [
        'test_all_claims_in_src_and_target',
        'test_all_service_lines_in_src_and_target',
        'test_all_hvids_in_src_and_target',
        'test_valid_gender_values_in_target'
    ],
    "pharmacyclaims": [
        'test_all_claims_in_src_and_target',
        'test_all_hvids_in_src_and_target',
        'test_valid_gender_values_in_target'
    ],
    "labtests": [
        'test_all_claims_in_src_and_target',
        'test_all_hvids_in_src_and_target',
        'test_valid_gender_values_in_target'
    ]
}
