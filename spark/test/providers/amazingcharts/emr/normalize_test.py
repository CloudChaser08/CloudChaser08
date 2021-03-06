import pytest

import shutil
import datetime

import spark.providers.amazingcharts.emr.normalize as amazingcharts_emr
import spark.helpers.file_utils as file_utils

from pyspark.sql import Row

clinical_observation_results = lab_result_results = encounter_results = medication_results = \
    procedure_results = diagnosis_results = lab_order_results = provider_order_results = \
    vital_sign_results = None

script_path = __file__


def cleanup(spark):
    spark['sqlContext'].dropTempTable('ref_gen_ref')
    spark['sqlContext'].dropTempTable('gen_ref_whtlst')

    try:
        shutil.rmtree(file_utils.get_abs_path(script_path, './resources/output/'))
    except:
        pass


@pytest.mark.usefixtures("spark")
def test_init(spark):
    print("--------------------------------start cleanup--------------------------------")
    cleanup(spark)
    print("--------------------------------start parallelize--------------------------------")
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='5',
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

    spark['spark'].sparkContext.parallelize([
        Row(
            gen_ref_cd='',
            gen_ref_nm='diag_snomed_cd',
            gen_ref_domn_nm='SNOMED',
            gen_ref_whtlst_flg='Y'
        )
    ]).toDF().createOrReplaceTempView('gen_ref_whtlst')
    print("--------------------------------start test--------------------------------")
    amazingcharts_emr.run(
        date_input='2021-08-31', test=True, end_to_end_test=False, spark=spark['spark'],
        runner=spark['runner'])
    print("--------------------------------result collect--------------------------------")
    global clinical_observation_results, lab_result_results, encounter_results, \
        medication_results, procedure_results, diagnosis_results, \
        provider_order_results, vital_sign_results

    clinical_observation_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/emr/*/clinical_observation/*')
    ).collect()
    lab_result_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/emr/*/lab_result/*')
    ).collect()
    encounter_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/emr/*/encounter/*')
    ).collect()
    medication_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/emr/*/medication/*')
    ).collect()
    procedure_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/emr/*/procedure/*')
    ).collect()
    diagnosis_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/emr/*/diagnosis/*')
    ).collect()
    # vital_sign_results = spark['sqlContext'].read.parquet(
    #     file_utils.get_abs_path(script_path, './resources/output/emr/*/vital_sign/*')
    # ).collect()


def test_something():
    print(len(clinical_observation_results))
    print(len(lab_result_results))
    print(len(encounter_results))
    print(len(medication_results))
    print(len(procedure_results))
    print(len(diagnosis_results))
    # print(len(vital_sign_results))
