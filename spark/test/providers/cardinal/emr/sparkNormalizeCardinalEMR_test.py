import pytest

import shutil
import logging

import spark.providers.cardinal.emr.sparkNormalizeCardinalEMR as cardinal_emr
import spark.helpers.file_utils as file_utils

clinical_observation_results = []


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
    global clinical_observation_results
    clinical_observation_results = spark['sqlContext'].sql('select * from clinical_observation_common_model') \
                                                      .collect()


def test_priv_filter():
    assert clinical_observation_results.filter(lambda r: r.hv_clin_obsn_id == '31_id-31')[0].clin_obsn_diag_cd \
        == 'TESTDIAG0'


def test_cleanup(spark):
    cleanup(spark)
