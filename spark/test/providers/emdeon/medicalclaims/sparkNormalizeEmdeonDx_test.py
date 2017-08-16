import pytest

import shutil
import logging

import spark.providers.emdeon.medicalclaims.sparkNormalizeEmdeonDX as emdeon
import spark.helpers.file_utils as file_utils

results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('medicalclaims_common_model')

    try:
        shutil.rmtree(file_utils.get_abs_path(__file__, './resources/output/'))
    except:
        logging.warn('No output directory.')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    emdeon.run(spark['spark'], spark['runner'], '2017-08-31', True)
    global results
    results = spark['sqlContext'].sql('select * from medicalclaims_common_model') \
                                 .collect()


def test_unrelated_professional_explosions():
    """Ensure the explosion created the correct number of rows on a
    professional claim for unrelated diagnoses

    These diagnoses should be exploded by date based on the full claim
     (2017-01-01 - 2017-01-03)
    """
    for unrelated_diag in ['DIAG5', 'DIAGADMIT']:
        assert sorted([(res.date_service.strftime('%Y-%m-%d'), res.date_service_end.strftime('%Y-%m-%d'))
                       for res in results if res.claim_id == 'claim-0' and res.diagnosis_code == unrelated_diag]) \
                           == [('2017-01-01', '2017-01-01'), ('2017-01-02', '2017-01-02'),
                               ('2017-01-03', '2017-01-03')]


def test_related_professional_explosions():
    """Ensure the explosion created the correct number of rows on a
    professional claim for related diagnoses

    These diagnoses are related to service line 1 and so they should
     be exploded by date based on the dates on that service line
     (2017-01-01 - 2017-01-05)
    """

    for related_diag in ['DIAGPRIMARY', 'DIAG2', 'DIAG3']:
        assert sorted([(res.date_service.strftime('%Y-%m-%d'), res.date_service_end.strftime('%Y-%m-%d'))
                       for res in results if res.claim_id == 'claim-0' and res.diagnosis_code == related_diag]) \
                           == [('2017-01-01', '2017-01-01'), ('2017-01-02', '2017-01-02'),
                               ('2017-01-03', '2017-01-03'), ('2017-01-04', '2017-01-04'),
                               ('2017-01-05', '2017-01-05')]


def test_cleanup(spark):
    cleanup(spark)
