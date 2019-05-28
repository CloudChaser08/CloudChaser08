import pytest

import os
import gzip
import shutil
import datetime

import spark.providers.cardinal.emr.sparkNormalizeCardinalEMR as cardinal_emr
import spark.helpers.file_utils as file_utils

from pyspark.sql import Row

clinical_observation_results = []
lab_result_results = []
encounter_results = []
medication_results = []
provider_order_results = []
procedure_results = []
diagnosis_results = []
vital_sign_results = []

clinical_observation_delivery_results = []
lab_result_delivery_results = []
encounter_delivery_results = []
medication_delivery_results = []
provider_order_delivery_results = []
procedure_delivery_results = []
diagnosis_delivery_results = []
vital_sign_delivery_results = []

script_path = __file__
output_test_location = file_utils.get_abs_path(script_path, './resources/output/')
delivery_test_location = file_utils.get_abs_path(script_path, './resources/delivery/')


def load_deliverable(table_name):
    path = file_utils.get_abs_path(script_path, './resources/delivery/{}/'.format(table_name))
    delivery_results = []

    for f in [f for f in os.listdir(path) if f[0] != '.']:
        with gzip.open(
                file_utils.get_abs_path(script_path, './resources/delivery/{}/{}'.format(table_name, f)), 'r'
        ) as delivery:
            delivery_results.extend(delivery.read().splitlines())

    return delivery_results


def cleanup(spark):
    """
    Teardown after testing
    """
    spark['sqlContext'].dropTempTable('transactions_demographics')
    spark['sqlContext'].dropTempTable('transactions_diagnosis')
    spark['sqlContext'].dropTempTable('transactions_encounter')
    spark['sqlContext'].dropTempTable('transactions_lab')
    spark['sqlContext'].dropTempTable('transactions_dispense')

    spark['sqlContext'].dropTempTable('ref_gen_ref')

    try:
        shutil.rmtree(output_test_location)
    except:
        pass

    try:
        shutil.rmtree(delivery_test_location)
    except:
        pass


@pytest.mark.usefixtures("spark")
def test_init(spark):
    """
    Initialize all normalized result sets based on test data
    """
    cleanup(spark)

    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='40',
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_cd=None,
            gen_ref_itm_nm='',
            gen_ref_itm_desc=None,
            gen_ref_1_dt=datetime.date(2016, 1, 1),
            whtlst_flg=None
        ),
        Row(
            hvm_vdr_feed_id='40',
            gen_ref_domn_nm='EARLIEST_VALID_DIAGNOSIS_DATE',
            gen_ref_cd=None,
            gen_ref_itm_nm='',
            gen_ref_itm_desc=None,
            gen_ref_1_dt=datetime.date(2016, 2, 1),
            whtlst_flg=None
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_enc.enc_typ_cd',
            gen_ref_cd='WHITELISTED TYP CD 1',
            gen_ref_itm_nm='',
            gen_ref_itm_desc=None,
            gen_ref_1_dt=None,
            whtlst_flg='Y'
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_enc.enc_typ_cd',
            gen_ref_cd='WHITELISTED TYP CD 2',
            gen_ref_itm_nm='',
            gen_ref_itm_desc=None,
            gen_ref_1_dt=None,
            whtlst_flg='Y'
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_enc.enc_typ_cd',
            gen_ref_cd='BLACKLISTED TYP CD 2',
            gen_ref_itm_nm='',
            gen_ref_itm_desc=None,
            gen_ref_1_dt=None,
            whtlst_flg='N'
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_diag.diag_resltn_desc',
            gen_ref_cd=None,
            gen_ref_itm_nm='',
            gen_ref_itm_desc='whiteLISTED resolution desc',
            gen_ref_1_dt=None,
            whtlst_flg='Y'
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_clin_obsn.clin_obsn_desc',
            gen_ref_cd=None,
            gen_ref_itm_nm='',
            gen_ref_itm_desc='WHiTeListed clin desc',
            gen_ref_1_dt=None,
            whtlst_flg='Y'
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_diag.diag_meth_cd',
            gen_ref_cd='WHITELISTED METHD OF DIAGNOSIS',
            gen_ref_itm_nm='',
            gen_ref_itm_desc=None,
            gen_ref_1_dt=None,
            whtlst_flg='Y'
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    cardinal_emr.run(spark['spark'], spark['runner'], '2017-08-31', 1, '2017-08-31', True)
    global clinical_observation_results, lab_result_results, encounter_results, medication_results, \
        provider_order_results, procedure_results, diagnosis_results, vital_sign_results, \
        clinical_observation_delivery_results, lab_result_delivery_results, encounter_delivery_results, \
        medication_delivery_results, provider_order_delivery_results, procedure_delivery_results, \
        diagnosis_delivery_results, vital_sign_delivery_results

    encounter_results = spark['sqlContext'].sql('select * from normalized_encounter') \
                                           .collect()
    encounter_delivery_results = load_deliverable('encounter')

    diagnosis_results = spark['sqlContext'].sql('select * from normalized_diagnosis') \
                                           .collect()
    diagnosis_delivery_results = load_deliverable('diagnosis')

    procedure_results = spark['sqlContext'].sql('select * from normalized_procedure') \
                                           .collect()
    procedure_delivery_results = load_deliverable('procedure')

    provider_order_results = spark['sqlContext'].sql('select * from normalized_provider_order') \
                                                .collect()
    provider_order_delivery_results = load_deliverable('provider_order')

    lab_result_results = spark['sqlContext'].sql('select * from normalized_lab_result') \
                                            .collect()
    lab_result_delivery_results = load_deliverable('lab_result')

    medication_results = spark['sqlContext'].sql('select * from normalized_medication') \
                                            .collect()
    medication_delivery_results = load_deliverable('medication')

    clinical_observation_results = spark['sqlContext'].sql('select * from normalized_clinical_observation') \
                                                      .collect()
    clinical_observation_delivery_results = load_deliverable('clinical_observation')

    vital_sign_results = spark['sqlContext'].sql('select * from normalized_vital_sign') \
                                            .collect()
    vital_sign_delivery_results = load_deliverable('vital_sign')


def test_hvids():
    """
    Ensure HVIDs are deobfuscated in output tables, and are still
    obfuscated in the delivery
    """

    # encounters
    assert sorted(set([res.hvid for res in encounter_results])) == ['100000', '100001', '100002']
    assert sorted(set([enc.split('|')[13] for enc in encounter_delivery_results])) \
        == ['"1483966080"', '"1483966081"', '"1483966082"']

    # diagnoses
    assert sorted(set([res.hvid for res in diagnosis_results])) == ['100000', '100001', '100002']
    assert sorted(set([diag.split('|')[13] for diag in diagnosis_delivery_results])) \
        == ['"1483966080"', '"1483966081"', '"1483966082"']

    # procedures
    assert sorted(set([res.hvid for res in procedure_results])) == ['100000', '100001', '100002']
    assert sorted(set([proc.split('|')[13] for proc in procedure_delivery_results])) \
        == ['"1483966080"', '"1483966081"', '"1483966082"']

    # provider order
    assert sorted(set([res.hvid for res in provider_order_results])) == ['100000', '100001', '100002']
    assert sorted(set([prov_ord.split('|')[13] for prov_ord in provider_order_delivery_results])) \
        == ['"1483966080"', '"1483966081"', '"1483966082"']

    # lab result
    assert sorted(set([res.hvid for res in lab_result_results])) == ['100000', '100001', '100002']
    assert sorted(set([lab_result.split('|')[17] for lab_result in lab_result_delivery_results])) \
        == ['"1483966080"', '"1483966081"', '"1483966082"']

    # medication
    assert sorted(set([res.hvid for res in medication_results])) == ['100000', '100001', '100002']
    assert sorted(set([medication.split('|')[17] for medication in medication_delivery_results])) \
        == ['"1483966080"', '"1483966081"', '"1483966082"']

    # clinical_observation
    assert sorted(set([res.hvid for res in clinical_observation_results])) == ['100000', '100002']
    assert sorted(set([clinical_observation.split('|')[13] for clinical_observation in clinical_observation_delivery_results])) \
        == ['"1483966080"', '"1483966082"']

    # vital_sign
    assert sorted(set([res.hvid for res in vital_sign_results])) == ['100000', '100001', '100002']
    assert sorted(set([vital_sign.split('|')[13] for vital_sign in vital_sign_delivery_results])) \
        == ['"1483966080"', '"1483966081"', '"1483966082"']


def test_encounter_cardinality():
    """
    Ensure the correct amount of encounters were normalized
    """
    assert len(encounter_results) == 3


def test_encounter_date_cap():
    """
    Ensure the correct date caps were applied to the encounter table
    """
    for res in encounter_results:
        if res.hv_enc_id == '40_enc-0':
            assert not res.enc_start_dt
            assert not res.enc_end_dt
        elif res.hv_enc_id == '40_enc-1':
            assert res.enc_start_dt == '2017-01-01'
            assert res.enc_end_dt == '2017-01-10'
        elif res.hv_enc_id == '40_enc-2':
            assert res.enc_start_dt == '2017-01-01'
            assert not res.enc_end_dt


def test_encounter_whitelist():
    """
    Ensure custom whitelist was applied to the encounter table
    """
    for res in encounter_results:
        if res.hv_enc_id == '40_enc-0':
            assert res.enc_typ_cd == 'WHITELISTED TYP CD 1'
            assert res.enc_typ_cd_qual == 'VENDOR'
        elif res.hv_enc_id == '40_enc-1':
            assert res.enc_typ_cd == 'WHITELISTED TYP CD 2'
            assert res.enc_typ_cd_qual == 'VENDOR'
        elif res.hv_enc_id == '40_enc-2':
            assert not res.enc_typ_cd
            assert not res.enc_typ_cd_qual


def test_diagnosis_cardinality():
    """
    Ensure the correct amount of diagnosis records were normalized
    """
    assert len(diagnosis_results) == 3


def test_diagnosis_date_cap():
    """
    Ensure the correct amount of diagnosis records were normalized
    """
    for res in diagnosis_results:
        if res.hv_diag_id == '40_diag-0':
            assert not res.diag_dt
            assert res.diag_resltn_dt == '2017-01-01'
        elif res.hv_diag_id == '40_diag-1':
            assert res.diag_start_dt == '2016-02-01'
            assert res.diag_end_dt == '2017-01-10'
        elif res.hv_diag_id == '40_diag-2':
            assert res.diag_start_dt == '2016-02-01'
            assert not res.diag_end_dt


def test_diagnosis_whitelist():
    """
    Ensure custom whitelist was applied to the diagnosis table
    """
    for res in diagnosis_results:
        if res.hv_diag_id == '40_diag-0':
            assert res.diag_resltn_desc == 'whiteLISTED resolution desc'
            assert res.diag_meth_cd == 'WHITELISTED METHD OF DIAGNOSIS'
            assert res.diag_meth_cd_qual == 'VENDOR'
        elif res.hv_diag_id == '40_diag-1':
            assert not res.diag_resltn_desc
            assert res.diag_meth_cd == 'WHITELISTED METHD OF DIAGNOSIS'
            assert res.diag_meth_cd_qual == 'VENDOR'
        elif res.hv_diag_id == '40_diag-2':
            assert not res.diag_resltn_desc
            assert not res.diag_meth_cd
            assert not res.diag_meth_cd_qual


def test_diagnosis_cleaning():
    """
    Ensure diagnosis codes are cleansed in the diagnosis table
    """
    assert sorted([res.diag_cd for res in diagnosis_results]) \
        == ['TESTDIAG0', 'TESTDIAG1', 'TESTDIAG2']


def test_procedure_cardinality():
    """
    Ensure the correct amount of procedure records were normalized
    """
    assert len(procedure_results) == 6


def test_procedure_date_cap():
    """
    Ensure the correct amount of procedure records were normalized
    """
    for res in procedure_results:
        if res.hv_proc_id == '40_enc_pat-visit-id-0':
            assert not res.enc_dt
            assert not res.proc_dt
        elif res.hv_proc_id == '40_enc_pat-visit-id-1':
            assert res.enc_dt == datetime.date(2017, 01, 01)
            assert res.proc_dt == datetime.date(2017, 01, 01)
        elif res.hv_proc_id == '40_enc_pat-visit-id-2':
            assert res.enc_dt == datetime.date(2017, 01, 01)
            assert res.proc_dt == datetime.date(2017, 01, 01)
        elif res.hv_proc_id == '40_ord_id-0':
            assert res.enc_dt == datetime.date(2017, 02, 01)
            assert res.proc_dt == datetime.date(2017, 01, 01)
            assert res.data_captr_dt == datetime.date(2016, 01, 01)
        elif res.hv_proc_id == '40_ord_id-1':
            assert res.enc_dt == datetime.date(2017, 02, 01)
            assert res.proc_dt == datetime.date(2017, 01, 01)
            assert res.data_captr_dt == datetime.date(2016, 01, 01)
        elif res.hv_proc_id == '40_ord_id-2':
            assert res.enc_dt == datetime.date(2017, 02, 01)
            assert res.proc_dt == datetime.date(2017, 01, 01)
            assert res.data_captr_dt == datetime.date(2016, 01, 01)


def test_procedure_data_cleaning():
    """
    Ensure the procedure codes and diagnosis codes are cleaned
    properly in the procedure results
    """
    for res in procedure_results:
        if res.hv_proc_id == '40_ord_id-0':
            assert res.proc_cd == 'MYCPT'
            assert res.proc_diag_cd == 'ICD10'
        elif res.hv_proc_id == '40_ord_id-1':
            assert res.proc_cd == 'MYCPT'
            assert res.proc_diag_cd == 'ICD10'
        elif res.hv_proc_id == '40_ord_id-2':
            assert res.proc_cd == 'MYCPT'
            assert res.proc_diag_cd == 'ICD10'


def test_provider_order_cardinality():
    """
    Ensure the correct amount of provider order records were normalized
    """
    assert len(provider_order_results) == 3


def test_provider_order_data_cleaning():
    """
    Ensure the provider order codes and diagnosis codes are cleaned
    properly in the provider_order results
    """
    assert sorted(set([res.prov_ord_cd for res in provider_order_results])) == ['MYCPT']
    assert sorted(set([res.prov_ord_diag_cd for res in provider_order_results])) == ['ICD10']


def test_provider_order_first_last_name_parsing():
    """
    Ensure the ordg_prov_frst_nm and last_nm are correct
    """
    for res in provider_order_results:
        if res.hv_prov_ord_id == '40_id-0':
            assert res.ordg_prov_frst_nm == 'wayne'
            assert res.ordg_prov_last_nm == 'gretzky, phd'
        elif res.hv_prov_ord_id == '40_id-1':
            assert res.ordg_prov_frst_nm == 'michael'
            assert res.ordg_prov_last_nm == 'jordan'
        elif res.hv_prov_ord_id == '40_id-2':
            assert not res.ordg_prov_frst_nm
            assert res.ordg_prov_last_nm == 'prince'


def test_lab_result_cardinality():
    """
    Ensure the correct amount of lab_result records were normalized
    """
    assert len(lab_result_results) == 3


def test_lab_result_date_cap():
    """
    Ensure the correct amount of lab_result records were normalized
    """
    for res in lab_result_results:
        if res.hv_lab_result_id == '40_000878FC-CE79-4E90-AE5B-4C9742F5374A-2':
            assert not res.lab_test_execd_dt
        elif res.hv_lab_result_id == '40_0004B13F-0CC3-4D7B-B63C-EB2FDEEC3DCE-1':
            assert res.lab_test_execd_dt == datetime.date(2017, 01, 01)
        elif res.hv_lab_result_id is None:
            assert res.lab_test_execd_dt == datetime.date(2017, 01, 01)
        else:
            raise AssertionError("Unexpected result id: {}".format(res.hv_lab_result_id))


def test_lab_result_data_cleaning():
    """
    Ensure the loinc codes are cleaned properly in the lab_result results
    """
    for res in lab_result_results:
        if res.hv_lab_result_id == '40_000878FC-CE79-4E90-AE5B-4C9742F5374A-2':
            assert res.lab_test_loinc_cd == '20192'
        elif res.hv_lab_result_id == '40_0004B13F-0CC3-4D7B-B63C-EB2FDEEC3DCE-1':
            assert res.lab_test_loinc_cd == ''
        elif res.hv_lab_result_id is None:
            assert res.lab_test_loinc_cd == '92018'
        else:
            raise AssertionError("Unexpected result id: {}".format(res.hv_lab_result_id))


def test_medication_cardinality():
    """
    Ensure the correct amount of medication records were normalized
    """
    assert len(medication_results) == 3


def test_medication_data_cleaning():
    """
    Ensure the rx nums are cleaned properly in the medication results
    """
    for res in medication_results:
        if res.hv_medctn_id in ['40_id-0', '40_id-1']:
            assert res.rx_num == '4910e8b9e3ea6bc0ef54dc914803caef'
        elif res.hv_medctn_id in ['40_id-2']:
            assert not res.rx_num


def test_clinical_observation_cardinality():
    """
    Ensure the correct amount of clinical_observation records were normalized
    """
    assert len(clinical_observation_results) == 8


def test_clinical_observation_result_cd():
    """
    Ensure the result codes were exploded and translated correctly
    """
    for res in clinical_observation_results:
        if res.clin_obsn_result_cd == 'stg_crit_desc':
            assert res.clin_obsn_result_cd_qual == 'CANCER_STAGE_PATHOLOGY'
        elif res.clin_obsn_result_cd == 'stage_of_disease':
            assert res.clin_obsn_result_cd_qual == 'DISEASE_STAGE'
        elif res.clin_obsn_result_cd == 'cancer_stage':
            assert res.clin_obsn_result_cd_qual == 'CANCER_STAGE'
        elif res.clin_obsn_result_cd == 'CANCER_STAGE_T':
            assert res.clin_obsn_result_cd_qual == 'CANCER_STAGE_T'
        elif res.clin_obsn_result_cd == 'CANCER_STAGE_N':
            assert res.clin_obsn_result_cd_qual == 'CANCER_STAGE_N'
        elif res.clin_obsn_result_cd == 'CANCER_STAGE_M':
            assert res.clin_obsn_result_cd_qual == 'CANCER_STAGE_M'

        assert len(
            [res for res in clinical_observation_results if res.clin_obsn_result_cd == 'stg_crit_desc']
        ) == 1
        assert len(
            [res for res in clinical_observation_results if res.clin_obsn_result_cd == 'stage_of_disease']
        ) == 1
        assert len(
            [res for res in clinical_observation_results if res.clin_obsn_result_cd == 'cancer_stage']
        ) == 1
        assert len(
            [res for res in clinical_observation_results if res.clin_obsn_result_cd == 'cancer_stage_t']
        ) == 2
        assert len(
            [res for res in clinical_observation_results if res.clin_obsn_result_cd == 'cancer_stage_n']
        ) == 1
        assert len(
            [res for res in clinical_observation_results if res.clin_obsn_result_cd == 'cancer_stage_m']
        ) == 2


def test_clinical_observation_whitelist():
    """
    Ensure the clin_obsn_desc was whitelisted correctly
    """
    for res in clinical_observation_results:
        if res.hv_clin_obsn_id == '40_diag-id-0':
            assert res.clin_obsn_desc == 'WHiTeListed clin desc'
        else:
            assert not res.clin_obsn_desc


def test_vital_sign_cardinality():
    """
    Ensure the correct amount of vital_sign records were normalized
    """
    assert len(vital_sign_results) == 18


def test_vital_sign_type_cd():
    """
    Ensure the type codes were exploded and translated correctly
    """
    assert sorted(set([res.vit_sign_typ_cd for res in vital_sign_results])) == [
        'CARD_FN_DEC', 'CARD_FN_RAW', 'CDAI', 'DAS28', 'HEIGHT', 'NJC28', 'PAIN',
        'PGA', 'PTGA', 'RAPID3', 'SDAI', 'SJC28', 'STANFORD_HAQ', 'TJC28', 'WEIGHT'
    ]

    for res in vital_sign_results:
        if res.vit_sign_typ_cd == 'RAPID3':
            assert res.vit_sign_msrmt == 'rapid3'
        elif res.vit_sign_typ_cd == 'SDAI':
            assert res.vit_sign_msrmt == 'sdai'
        elif res.vit_sign_typ_cd == 'CDAI':
            assert res.vit_sign_msrmt == 'cdai'
        elif res.vit_sign_typ_cd == 'DAS28':
            assert res.vit_sign_msrmt == 'das28'
        elif res.vit_sign_typ_cd == 'STANFORD_HAQ':
            assert res.vit_sign_msrmt == 'stanford_haq'
        elif res.vit_sign_typ_cd == 'NJC28':
            assert res.vit_sign_msrmt == 'njc28'
        elif res.vit_sign_typ_cd == 'TJC28':
            assert res.vit_sign_msrmt == 'tjc28'
        elif res.vit_sign_typ_cd == 'SJC28':
            assert res.vit_sign_msrmt == 'sjc28'
        elif res.vit_sign_typ_cd == 'PAIN':
            assert res.vit_sign_msrmt == 'pain'
        elif res.vit_sign_typ_cd == 'CARD_FN_RAW':
            assert res.vit_sign_msrmt == 'card_fn_raw'
        elif res.vit_sign_typ_cd == 'CARD_FN_DEC':
            assert res.vit_sign_msrmt == 'card_fn_dec'
        elif res.vit_sign_typ_cd == 'PGA':
            assert res.vit_sign_msrmt == 'pga'
        elif res.vit_sign_typ_cd == 'PTGA':
            assert res.vit_sign_msrmt == 'ptga'
        elif res.vit_sign_typ_cd == 'HEIGHT':
            res.vit_sign_msrmt is None

    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'RAPID3']
    ) == 1
    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'CDAI']
    ) == 2
    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'SDAI']
    ) == 1
    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'DAS28']
    ) == 1
    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'STANFORD_HAQ']
    ) == 1
    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'NJC28']
    ) == 1
    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'TJC28']
    ) == 1
    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'SJC28']
    ) == 1
    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'PAIN']
    ) == 1
    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'CARD_FN_RAW']
    ) == 2
    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'CARD_FN_DEC']
    ) == 2
    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'PGA']
    ) == 1
    assert len(
        [res for res in vital_sign_results if res.vit_sign_typ_cd == 'PTGA']
    ) == 1


def test_cleanup(spark):
    cleanup(spark)
