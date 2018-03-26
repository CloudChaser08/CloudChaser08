import pytest

import shutil
import datetime

import spark.providers.allscripts.emr.sparkNormalizeAllscriptsEMR as allscripts_emr
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
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    allscripts_emr.run(spark['spark'], spark['runner'], '2016-12-01', True)

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


def test_deduplication():
    """
    Some samples have duplicates - ensure there are no duplicates in the output
    """
    for res in [clinical_observation_results, lab_result_results, encounter_results,
                medication_results, procedure_results, diagnosis_results]:
        assert len(res) == len(set(res))

    # Encounter has warehouse data that should be incorporated but still deduped
    assert '25_gen2patientid-1_apptid-1_OLD_UNIQUE' in [res.hv_enc_id for res in encounter_results]

    assert sorted([res.hv_enc_id for res in encounter_results]) \
        == sorted(set([res.hv_enc_id for res in encounter_results]))


def test_cleanup(spark):
    cleanup(spark)
