import pytest

import shutil
import logging

import spark.providers.cardinal.emr.sparkNormalizeCardinalEMR as cardinal_emr
import spark.helpers.file_utils as file_utils

clinical_observation_results = []
lab_result_results = []
encounter_results = []
medication_results = []
procedure_results = []
diagnosis_results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('clinical_observation_common_model')
    spark['sqlContext'].dropTempTable('diagnosis_common_model')
    spark['sqlContext'].dropTempTable('encounter_common_model')
    spark['sqlContext'].dropTempTable('medication_common_model')
    spark['sqlContext'].dropTempTable('procedure_common_model')

    spark['sqlContext'].dropTempTable('demographics_transactions')
    spark['sqlContext'].dropTempTable('diagnosis_transactions')
    spark['sqlContext'].dropTempTable('encounter_transactions')
    spark['sqlContext'].dropTempTable('lab_transactions')
    spark['sqlContext'].dropTempTable('dispense_transactions')

    try:
        shutil.rmtree(file_utils.get_abs_path(__file__, './resources/output/'))
    except:
        logging.warn('No output directory.')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    cardinal_emr.run(spark['spark'], spark['runner'], '2017-08-31', True)
    global clinical_observation_results, lab_result_results, encounter_results, \
        medication_results, procedure_results, diagnosis_results
    clinical_observation_results = spark['sqlContext'].sql('select * from clinical_observation_common_model') \
                                                      .collect()
    lab_result_results = spark['sqlContext'].sql('select * from lab_result_common_model') \
                                            .collect()
    encounter_results = spark['sqlContext'].sql('select * from encounter_common_model') \
                                           .collect()
    medication_results = spark['sqlContext'].sql('select * from medication_common_model') \
                                            .collect()
    procedure_results = spark['sqlContext'].sql('select * from procedure_common_model') \
                                           .collect()
    diagnosis_results = spark['sqlContext'].sql('select * from diagnosis_common_model') \
                                           .collect()


def test_deduplication():
    """All samples have duplicates - ensure there are none in the output
    """
    for res in [clinical_observation_results, lab_result_results, encounter_results,
                medication_results, procedure_results, diagnosis_results]:
        assert len(res) == len(set(res))


def test_clin_obs_priv_filter():
    assert filter(lambda r: r.hv_clin_obsn_id == '31_id-31', clinical_observation_results)[0].clin_obsn_diag_cd \
        == 'TESTDIAG0'


def test_clin_obs_result_cd_explosion():
    assert sorted([r.clin_obsn_result_cd for r in clinical_observation_results if r.hv_clin_obsn_id == '31_id-31']) \
        == ['STAGE_OF_DIS', 'STG_CRIT_DESC']

    filter(lambda r: r.hv_clin_obsn_id == '31_id-31', clinical_observation_results)[0].clin_obsn_diag_cd


def test_lab_res_nulls():
    assert map(
        lambda r: r.lab_test_nm,
        filter(lambda r: r.hv_lab_result_id in ['31_id-11', '31_id-12', '31_id-13'], lab_result_results)
    ) == [None, None, None]


def test_medication_zeros_nullified(spark):
    """Ensure that 0's converted to NULL in medication data
    """
    dispense_transactions = spark['sqlContext'].sql('select * from dispense_transactions') \
                                               .collect()
    assert [(r.qty, r.num_doses) for r in dispense_transactions if r.id == 'id-41'] == [('0', '0')]
    assert [(r.medctn_dispd_qty, r.medctn_admin_unt_qty) for r in medication_results if r.hv_medctn_id == '31_id-41'] == [(None, None)]

    assert len(encounter_results) == len(set(encounter_results))


def test_cleanup(spark):
    cleanup(spark)
