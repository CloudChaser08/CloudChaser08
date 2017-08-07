import pytest

import datetime
import shutil
import logging

import spark.providers.cardinal.emr.sparkNormalizeCardinalEMR as cardinal_emr
import spark.helpers.file_utils as file_utils

procedure_results = []


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
    cardinal_emr.run(spark['spark'], spark['runner'], '2016-12-31', True)
    global procedure_results
    procedure_results = spark['sqlContext'].sql('select * from unrestricted_procedure_common_model') \
                                           .collect()


def test_date_parsing():
    "Ensure that dates are correctly parsed"
    sample_row = filter(lambda r: r.claim_id == 'prescription-key-0', procedure_results)[0]

    assert sample_row.date_service == datetime.date(2011, 1, 30)
    assert sample_row.date_authorized == datetime.date(2011, 1, 30)
    assert sample_row.date_authorized == datetime.date(2011, 1, 30)


def test_cleanup(spark):
    cleanup(spark)
