import pytest

from pyspark.sql.types import Row

import spark.providers.transmed.emr.sparkNormalizeTransmedEMR as transmed_emr
import spark.helpers.file_utils as file_utils

clinical_observation_results = []
lab_result_results = []
procedure_results = []
diagnosis_results = []

script_path = __file__
output_test_location = file_utils.get_abs_path(script_path, './resources/output/')


def cleanup(spark):
    pass


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='',
            gen_ref_domn_nm='',
            gen_ref_itm_nm='',
            gen_ref_1_dt='',
            whtlst_flg='',
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_lab_result.lab_result_nm',
            gen_ref_itm_nm='POSITIVE',
            gen_ref_1_dt=None,
            whtlst_flg='Y',
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_lab_result.lab_result_nm',
            gen_ref_itm_nm='NEGATIVE',
            gen_ref_1_dt=None,
            whtlst_flg='Y',
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_lab_result.lab_test_vdr_cd',
            gen_ref_itm_nm='POSITIVE TYPE',
            gen_ref_1_dt=None,
            whtlst_flg='Y',
        ),
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm='emr_lab_result.lab_test_vdr_cd',
            gen_ref_itm_nm='NEGATIVE TYPE',
            gen_ref_1_dt=None,
            whtlst_flg='Y',
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    transmed_emr.run(spark['spark'], spark['runner'], '2017-08-31', True)
    global clinical_observation_results, lab_result_results, \
        procedure_results, diagnosis_results
    clinical_observation_results = spark['sqlContext'].sql(
        'select * from normalized_clinical_observation'
    ).collect()
    lab_result_results = spark['sqlContext'].sql(
        'select * from normalized_lab_result'
    ).collect()
    procedure_results = spark['sqlContext'].sql(
        'select * from normalized_procedure'
    ).collect()
    diagnosis_results = spark['sqlContext'].sql(
        'select * from normalized_diagnosis'
    ).collect()


def test_diags_parsed():
    assert sorted(set([res.clin_obsn_diag_cd for res in clinical_observation_results])) \
        == ['C209', 'C220', 'C341', 'C446']

    assert sorted(set([res.lab_test_diag_cd for res in lab_result_results])) \
        == ['C421', 'C509', 'C569']

    assert sorted(set([res.proc_diag_cd for res in procedure_results])) \
        == ['C209', 'C220', 'C341', 'C348', 'C421', 'C446', 'C509', 'C569', 'C774']

    assert sorted(set([res.diag_cd for res in diagnosis_results])) \
        == ['C209', 'C220', 'C341', 'C348', 'C421', 'C446', 'C509', 'C569', 'C774']


def test_clin_obsn_cancer_stages():
    sorted([
        (res.clin_obsn_result_cd, res.clin_obsn_result_cd_qual)
        for res in clinical_observation_results if res.hv_clin_obsn_id == '54_ce-pk4'
    ]) == [('814 - ADENOCARCINOMA, NOS', 'CANCER_STAGE_PATHOLOGY'), ('UNK', 'CANCER_STAGE')]

    sorted([
        (res.clin_obsn_result_cd, res.clin_obsn_result_cd_qual)
        for res in clinical_observation_results if res.hv_clin_obsn_id == '54_ce-pk6'
    ]) == [('HIST', 'CANCER_STAGE_PATHOLOGY'), ('STAGE', 'CANCER_STAGE'), ('TUMGRADE', 'CANCER_STAGE_T')]


def test_lab_res_vdr_cd_res_nm():
    sorted([(res.hv_lab_result_id, res.lab_test_vdr_cd, res.lab_result_nm) for res in lab_result_results]) \
        == [('54_ce-pk0', None, 'POSITIVE'),
            ('54_ce-pk0', 'NEGATIVE TYPE', 'NEGATIVE'),
            ('54_ce-pk0', 'NEGATIVE TYPE', 'NEGATIVE'),
            ('54_ce-pk1', 'POSITIVE TYPE', 'POSITIVE'),
            ('54_ce-pk2', 'NEGATIVE TYPE', 'NEGATIVE')]


def test_cleanup(spark):
    cleanup(spark)
