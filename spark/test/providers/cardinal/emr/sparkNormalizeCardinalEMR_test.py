import pytest

import shutil
import logging
import os
import datetime
import gzip

import spark.providers.cardinal.emr.sparkNormalizeCardinalEMR as cardinal_emr
import spark.helpers.file_utils as file_utils
import spark.helpers.udf.general_helpers as gen_helpers

from pyspark.sql import Row

clinical_observation_results = []
lab_result_results = []
encounter_results = []
medication_results = []
procedure_results = []
diagnosis_results = []

script_path = __file__
output_test_location = file_utils.get_abs_path(script_path, './resources/output/')
delivery_test_location = file_utils.get_abs_path(script_path, './resources/delivery/')


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

    spark['sqlContext'].dropTempTable('ref_gen_ref')

    try:
        shutil.rmtree(output_test_location)
    except:
        logging.warn('No output directory.')

    try:
        shutil.rmtree(delivery_test_location)
    except:
        logging.warn('No delivery directory.')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='40',
            gen_ref_domn_nm='EARLIEST_VALID_DIAGNOSIS_DATE',
            gen_ref_itm_nm=None,
            gen_ref_1_dt=datetime.date(2016, 1, 1),
            whtlst_flg=None
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_medctn.clin_obsn_diag_desc',
            gen_ref_itm_nm='WHITELISTED CLIN OBS DIAG DESC VAL 1',
            gen_ref_1_dt=None,
            whtlst_flg='Y',
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_medctn.clin_obsn_diag_desc',
            gen_ref_itm_nm='WHITELISTED CLIN OBS DIAG DESC VAL 2',
            gen_ref_1_dt=None,
            whtlst_flg='Y',
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_medctn.clin_obsn_diag_desc',
            gen_ref_itm_nm='BLACKLISTED CLIN OBS DIAG DESC VAL',
            gen_ref_1_dt=None,
            whtlst_flg='N',
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

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
    assert filter(lambda r: r.hv_clin_obsn_id == '40_id-31', clinical_observation_results)[0].clin_obsn_diag_cd \
        == 'TESTDIAG0'


def test_clin_obs_result_cd_explosion():
    assert sorted([r.clin_obsn_result_cd for r in clinical_observation_results if r.hv_clin_obsn_id == '40_id-31']) \
        == ['STAGE_OF_DIS', 'STG_CRIT_DESC']

    filter(lambda r: r.hv_clin_obsn_id == '40_id-31', clinical_observation_results)[0].clin_obsn_diag_cd


def test_lab_res_nulls():
    assert map(
        lambda r: r.lab_test_nm,
        filter(lambda r: r.hv_lab_result_id in ['40_id-11', '40_id-12', '40_id-13'], lab_result_results)
    ) == [None, None, None]


def test_medication_zeros_nullified(spark):
    """Ensure that 0's converted to NULL in medication data
    """
    dispense_transactions = spark['sqlContext'].sql('select * from dispense_transactions') \
                                               .collect()
    assert [(r.qty, r.num_doses) for r in dispense_transactions if r.id == 'id-41'] == [('0', '0')]
    assert [(r.medctn_dispd_qty, r.medctn_admin_unt_qty) for r in medication_results if r.hv_medctn_id == '40_id-41'] == [(None, None)]

    assert len(encounter_results) == len(set(encounter_results))


def test_date_capping():
    """
    Ensure dates were capped according to gen ref table
    """
    # diag_dt should be capped based on EARLIEST_VALID_DIAGNOSIS_DATE
    for r in diagnosis_results:
        if r.vdr_diag_id == 'id-31':
            assert r.diag_dt == '2016-01-02'
        elif r.vdr_diag_id in ('id-32', 'id-33'):
            assert not r.diag_dt

    # enc_start_date should not be capped -- EARLIEST_VALID_SERVICE_DATE does not exist
    assert [r.enc_start_dt for r in encounter_results if r.vdr_enc_id == 'id-0'][0] == datetime.date(2000, 1, 1)


def test_whitelisting():
    """
    Ensure whitelist is properly applied
    """
    for r in clinical_observation_results:
        if r.vdr_clin_obsn_id == 'id-34':
            assert r.clin_obsn_diag_desc == 'WHITELISTED CLIN OBS DIAG DESC VAL 1'
        elif r.vdr_clin_obsn_id == 'id-35':
            assert r.clin_obsn_diag_desc == 'WHITELISTED CLIN OBS DIAG DESC VAL 2'
        else:
            assert not r.clin_obsn_diag_desc


def test_first_last_name_parsing():
    assert [(r.medctn_ordg_prov_frst_nm, r.medctn_ordg_prov_last_nm)
            for r in medication_results if r.hv_medctn_id in ['40_id-41', '40_id-42', '40_id-43']] \
        == [('wayne', 'gretzky'), ('michael', 'jordan, phd'), (None, 'prince')]


def test_diagnosis_priority():
    assert [r.diag_prty_cd for r in diagnosis_results][:4] == ['1', '10', '2', '1']


def test_data_export():
    "Ensure that the exported data is exactly the same as the warehouse data"
    for common_model in [
            ('clinical_observation', clinical_observation_results),
            ('diagnosis', diagnosis_results),
            ('encounter', encounter_results),
            ('lab_result', lab_result_results),
            ('medication', medication_results),
            ('procedure', procedure_results)
    ]:
        # ensure this common model was exported
        assert common_model[0] in os.listdir(delivery_test_location)

        # open exported file
        with gzip.open(
                delivery_test_location + '/' + common_model[0] + '/part-00000.gz'
        ) as exported_results:

            # strip quotes and split gzip by line and delimiter (|)
            exported_results_full = [[el.replace('"', '') for el in res.split('|')]
                                     for res in exported_results.read().splitlines()]

            # ensure both result sets have same length
            assert len(exported_results_full) == len(common_model[1])

            # ensure the results are the same
            for result_row_index, res in enumerate(exported_results_full):
                for result_col_index in range(len(res)):
                    coalesced_res = (res[result_col_index] if res[result_col_index] != '' else None)

                    # hvid will need to be deobfuscated
                    if common_model[1][result_row_index][result_col_index] == common_model[1][result_row_index].hvid:
                        if common_model[1][result_row_index].hvid:
                            assert str(gen_helpers.slightly_deobfuscate_hvid(
                                int(coalesced_res), 'Cardinal_MPI-0'
                            )) == str(common_model[1][result_row_index][result_col_index])
                        else:
                            assert not coalesced_res

                    # all other columns should be the same
                    else:
                        assert str(coalesced_res) == str(common_model[1][result_row_index][result_col_index])


def test_cleanup(spark):
    cleanup(spark)
