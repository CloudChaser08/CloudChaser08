import pytest

import shutil
import logging

import spark.providers.cardinal_rcm.medicalclaims.sparkNormalizeCardinalRcmDX as cardinal_rcm
import spark.helpers.file_utils as file_utils

results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('medicalclaims_common_model')
    spark['sqlContext'].dropTempTable('transactions')

    try:
        shutil.rmtree(file_utils.get_abs_path(__file__, './resources/output/'))
    except:
        logging.warn('No output directory.')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    cardinal_rcm.run(spark['spark'], spark['runner'], '2017-12-31', True)
    global results
    results = spark['sqlContext'].sql('select * from medicalclaims_common_model') \
                                 .collect()


def test_diag_explosion():
    "Ensure diags are exploded"
    sample_diag_rows = filter(lambda r: r.claim_id == 'claim-0', results)
    sample_nodiag_rows = filter(lambda r: r.claim_id != 'claim-0', results)

    assert len(sample_diag_rows) == 4
    assert len(sample_nodiag_rows) == 9


def test_procedure_diag_explosion():
    "Ensure procedures are included without diagnoses and vice-versa"
    sample_rows = filter(lambda r: r.claim_id == 'claim-0', results)

    # one null where the proc code was populated
    assert len(filter(lambda r: r.diagnosis_code is None, sample_rows)) == 1

    # 3 nulls where the diag codes are populated
    assert len(filter(lambda r: r.procedure_code is None, sample_rows)) == 3

    assert len(filter(lambda r: (r.procedure_code and r.diagnosis_code)
                      or (not r.procedure_code and not r.diagnosis_code) , sample_rows)) == 0


def test_prov_npi():
    "Ensure that npi fields are populated"
    sample_npi_row = filter(lambda r: r.claim_id == 'claim-1', results)[0]

    assert sample_npi_row.prov_rendering_npi == "RENDPROVNPID"


def test_cleanup(spark):
    cleanup(spark)
