import spark.helpers.udf.medicalclaims_helpers as mh


def test_get_diagnosis_with_priority():
    # can accept None
    assert mh.get_diagnosis_with_priority(None, None) is None

    # clean input
    assert mh.get_diagnosis_with_priority("diag1:diag2:diag3", "1:2:3") == 'diag1_1:diag2_2:diag3_3'

    # indexing starting at 0 should have no impact
    assert mh.get_diagnosis_with_priority("diag1:diag2:diag3", "0:1:2") == 'diag1_1:diag2_2:diag3_3'

    # indexing with letters works, case insensitive
    assert mh.get_diagnosis_with_priority("diag1:diag2:diag3", "A:B:C") == 'diag1_1:diag2_2:diag3_3'
    assert mh.get_diagnosis_with_priority("diag1:diag2:diag3", "a:b:c") == 'diag1_1:diag2_2:diag3_3'

    # blank pointers are ignored
    assert mh.get_diagnosis_with_priority("diag1:diag2:diag3", "a:b::c") == 'diag1_1:diag2_2:diag3_3'

    # there can be more pointers than diags
    assert mh.get_diagnosis_with_priority("diag1:diag2:diag3", "1:2:3:4:5") == 'diag1_1:diag2_2:diag3_3'
