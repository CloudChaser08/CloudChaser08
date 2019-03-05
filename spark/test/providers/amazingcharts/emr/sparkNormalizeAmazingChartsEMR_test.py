import pytest

import shutil
import datetime

import spark.providers.amazingcharts.emr.sparkNormalizeAmazingChartsEMR as amazingcharts_emr
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

    amazingcharts_emr.run(spark['spark'], spark['runner'], '2018-12-01', test=True)

    global clinical_observation_results, lab_result_results, encounter_results, \
        medication_results, procedure_results, diagnosis_results, \
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
    vital_sign_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/output/vital_sign/*/*')
    ).collect()


def test_something():
    print len(clinical_observation_results)
    print len(lab_result_results)
    print len(encounter_results)
    print len(medication_results)
    print len(procedure_results)
    print len(diagnosis_results)
    print len(vital_sign_results)
