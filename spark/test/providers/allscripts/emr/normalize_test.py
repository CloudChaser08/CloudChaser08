import pytest

import shutil
import datetime

import spark.providers.allscripts.emr.normalize as allscripts_emr
import spark.providers.allscripts.emr.udf as allscripts_emr_udf
import spark.helpers.file_utils as file_utils

from pyspark.sql import Row

clinical_observation_results = lab_result_results = encounter_results = medication_results = \
    procedure_results = diagnosis_results = lab_order_results = provider_order_results = \
    vital_sign_results = None

script_path = __file__


def cleanup(spark):
    spark['sqlContext'].dropTempTable('ref_gen_ref')

    try:
        shutil.rmtree(file_utils.get_abs_path(script_path, './resources/output/'))
    except:
        pass


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='25',
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_cd='',
            gen_ref_1_dt=datetime.date(2016, 1, 1),
            gen_ref_1_txt='',
            gen_ref_2_txt='',
            gen_ref_itm_desc='',
            whtlst_flg=''
        ),
        Row(
            hvm_vdr_feed_id='25',
            gen_ref_domn_nm='allscripts_emr.vitals',
            gen_ref_itm_nm='INCH',
            gen_ref_cd='HEIGHT',
            gen_ref_1_dt=datetime.date(2016, 1, 1),
            gen_ref_1_txt='HEIGHT',
            gen_ref_2_txt='INCHES',
            gen_ref_itm_desc=None,
            whtlst_flg='Y'
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    allscripts_emr.run(spark['spark'], spark['runner'], '2016-12-01', test=True)

    global clinical_observation_results, lab_result_results, encounter_results, \
        medication_results, procedure_results, diagnosis_results, lab_order_results, \
        provider_order_results, vital_sign_results

    clinical_observation_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/clinical_observation/*/*')
    ).collect()
    lab_result_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/lab_result/*/*')
    ).collect()
    encounter_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/encounter/*/*')
    ).collect()
    medication_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/medication/*/*')
    ).collect()
    procedure_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/procedure/*/*')
    ).collect()
    diagnosis_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/diagnosis/*/*')
    ).collect()
    lab_order_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/lab_order/*/*')
    ).collect()
    provider_order_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/provider_order/*/*')
    ).collect()
    vital_sign_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/vital_sign/*/*')
    ).collect()


def test_remove_last_chars_udf():
    assert allscripts_emr_udf.remove_last_chars(None, 2) is None
    assert allscripts_emr_udf.remove_last_chars('', 2) is None
    assert allscripts_emr_udf.remove_last_chars('1', 2) is None
    assert allscripts_emr_udf.remove_last_chars('12', 2) is None
    assert allscripts_emr_udf.remove_last_chars('123', 2) == '1'
    assert allscripts_emr_udf.remove_last_chars('1234', 2) == '12'


def test_deduplication():
    """
    Some samples have duplicates - ensure there are no duplicates in the output
    """
    for res in [clinical_observation_results, lab_result_results, encounter_results,
                medication_results, procedure_results, diagnosis_results]:
        assert len(res) == len(set(res))

    assert sorted([res.hv_enc_id for res in encounter_results]) \
        == sorted(set([res.hv_enc_id for res in encounter_results]))


def test_clin_obsn_explosion():
    assert sorted([
        (res.hv_clin_obsn_id, res.clin_obsn_substc_cd, res.clin_obsn_substc_cd_qual)
        for res in clinical_observation_results
    ]) == [
        ('25_gen2patientid-0_algid-0_0', 'DDI', 'DDI'),
        ('25_gen2patientid-0_algid-0_0', 'RXNORM', 'RXNORM'),
        ('25_gen2patientid-1_algid-1_0', 'GPI', 'GPI'),
        ('25_gen2patientid-2_algid-2_0', None, None)
    ]


def test_diag_alt_cd():
    assert set(
        [(res.diag_alt_cd, res.diag_alt_cd_qual)
         for res in diagnosis_results]
    ) == {
        ('level1: level2: level3', 'LEVEL1_LEVEL2_LEVEL3'),
        ('level2', 'LEVEL1_LEVEL2_LEVEL3'),
        ('level3', 'LEVEL1_LEVEL2_LEVEL3')
    }


def test_diag_cd_explosion():
    assert sorted(
        [(res.hv_diag_id, res.diag_cd, res.diag_cd_qual) for res in diagnosis_results]
    ) == [
        ('25_gen2patientid-0_prbid-0_0', None, None),
        ('25_gen2patientid-1_prbid-1_0', 'V700', '01'),
        ('25_gen2patientid-1_prbid-1_0', 'Z0000', '02'),
        ('25_gen2patientid-2_prbid-2_0', 'V700', '01')
    ]


def test_lab_ord_explosion():
    assert set([
        (res.hv_lab_ord_id, res.lab_ord_diag_cd, res.lab_ord_diag_cd_qual) for res in lab_order_results
    ]) == set([
        ('25_gen2patientid-0_orderid-6_1', None, '02'),
        ('25_gen2patientid-0_orderid-6_1', 'V90', '01'),
        ('25_gen2patientid-1_orderid-7_1', None, '02'),
        ('25_gen2patientid-1_orderid-7_1', 'V90', '01'),
        ('25_gen2patientid-2_orderid-8_1', None, '02'),
        ('25_gen2patientid-2_orderid-8_1', 'V90', '01')
    ])


def test_lab_result_rec_stat_cd():
    assert sorted(
        [(res.hv_lab_result_id, res.rec_stat_cd) for res in lab_result_results]
    ) == [
        ('25_gen2patientid-0_resid-0_0', '0'),
        ('25_gen2patientid-1_resid-1_0', '1'),
        ('25_gen2patientid-2_resid-2_0', None)
    ]


def test_medctn_alt_substc_cd():
    assert sorted(
        [(res.hv_medctn_id, res.medctn_alt_substc_cd, res.medctn_alt_substc_cd_qual)
         for res in medication_results]
    ) == [('25_gen2patientid-0_medid-0_0', '02100020000105', 'GPI'),
          ('25_gen2patientid-0_medid-0_0', '25915', 'DDI'),
          ('25_gen2patientid-0_medid-0_0', '309112', 'RXNORM'),
          ('25_gen2patientid-1_medid-1_0', '02100020000105', 'GPI'),
          ('25_gen2patientid-1_medid-1_0', '25915', 'DDI'),
          ('25_gen2patientid-1_medid-1_0', '309112', 'RXNORM'),
          ('25_gen2patientid-2_medid-2_0', '02100020000105', 'GPI'),
          ('25_gen2patientid-2_medid-2_0', '25915', 'DDI'),
          ('25_gen2patientid-2_medid-2_0', '309112', 'RXNORM')]


def test_proc_explosion():
    assert set(
        [(res.hv_proc_id, res.proc_cd, res.proc_cd_qual, res.proc_diag_cd, res.proc_diag_cd_qual)
         for res in procedure_results]
    ) == set([
        ('25_gen2patientid-0_orderid-0_1', '0000', 'HCPCS', None, '02'),
        ('25_gen2patientid-0_orderid-0_1', '0000', 'HCPCS', 'V90', '01'),
        ('25_gen2patientid-0_orderid-0_1', '36475', 'CPTCODE', None, '02'),
        ('25_gen2patientid-0_orderid-0_1', '36475', 'CPTCODE', 'V90', '01'),
        ('25_gen2patientid-0_prbid-0_0', None, None, None, None),
        ('25_gen2patientid-0_vacid-0_17194404700007', '0', 'VACCINES.CVX', None, None),
        ('25_gen2patientid-1_orderid-1_1', '0000', 'HCPCS', None, '02'),
        ('25_gen2patientid-1_orderid-1_1', '0000', 'HCPCS', 'V90', '01'),
        ('25_gen2patientid-1_orderid-1_1', '36475', 'CPTCODE', None, '02'),
        ('25_gen2patientid-1_orderid-1_1', '36475', 'CPTCODE', 'V90', '01'),
        ('25_gen2patientid-1_prbid-1_0', None, None, 'V700', '01'),
        ('25_gen2patientid-1_prbid-1_0', None, None, 'Z0000', '02'),
        ('25_gen2patientid-1_vacid-1_17194404700007', '0', 'VACCINES.CVX', None, None),
        ('25_gen2patientid-2_orderid-2_1', '0000', 'HCPCS', None, '02'),
        ('25_gen2patientid-2_orderid-2_1', '0000', 'HCPCS', 'V90', '01'),
        ('25_gen2patientid-2_orderid-2_1', '36475', 'CPTCODE', None, '02'),
        ('25_gen2patientid-2_orderid-2_1', '36475', 'CPTCODE', 'V90', '01'),
        ('25_gen2patientid-2_prbid-2_0', None, None, 'V700', '01'),
        ('25_gen2patientid-2_vacid-2_17194404700007', '0', 'VACCINES.CVX', None, None)
    ])


def test_prov_ord_explosion():
    assert set(
        [(res.hv_prov_ord_id, res.prov_ord_diag_cd, res.prov_ord_diag_cd_qual)
         for res in provider_order_results]
    ) == set([
        ('25_gen2patientid-0_orderid-3_1', None, '02'),
        ('25_gen2patientid-0_orderid-3_1', 'V90', '01'),
        ('25_gen2patientid-1_orderid-4_1', None, '02'),
        ('25_gen2patientid-1_orderid-4_1', 'V90', '01'),
        ('25_gen2patientid-2_orderid-5_1', None, '02'),
        ('25_gen2patientid-2_orderid-5_1', 'V90', '01')
    ])

def test_vit_sign_backfill():
    assert set(
        [(res.hv_vit_sign_id, res.vit_sign_msrmt, res.vit_sign_uom)
         for res in vital_sign_results]
    ) == set([
        ('25_gen2patientid-0_vitid-0_0', '64', 'INCHES'), # First one is "backfilled"
        ('25_gen2patientid-1_vitid-1_0', '63', 'INCHES'),
        ('25_gen2patientid-2_vitid-2_0', '63', 'INCHES')
    ])

def test_cleanup(spark):
    cleanup(spark)
