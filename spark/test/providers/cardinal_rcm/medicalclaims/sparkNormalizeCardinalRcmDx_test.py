import pytest

import shutil
import logging
import datetime
from pyspark.sql import Row

import spark.providers.cardinal_rcm.medicalclaims.sparkNormalizeCardinalRcmDX as cardinal_rcm
import spark.helpers.file_utils as file_utils

results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('medicalclaims_common_model')
    spark['sqlContext'].dropTempTable('transactions')
    spark['sqlContext'].dropTempTable('ref_gen_ref')

    try:
        shutil.rmtree(file_utils.get_abs_path(__file__, './resources/output/'))
    except:
        logging.warn('No output directory.')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='29',
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createTempView('ref_gen_ref')

    cardinal_rcm.run(spark['spark'], spark['runner'], '2017-12-31', True)
    global results
    results = spark['sqlContext'].sql('select * from medicalclaims_common_model') \
                                 .collect()


def test_diag_explosion():
    "Ensure diags are exploded"
    sample_diag_rows = [r for r in results if r.claim_id == 'claim-0']
    sample_nodiag_rows = [r for r in results if r.claim_id != 'claim-0']

    assert len(sample_diag_rows) == 4
    assert len(sample_nodiag_rows) == 9

    assert set([r.diagnosis_code for r in sample_diag_rows if r.service_line_number]) \
        == set(['DIAG1', 'DIAG4'])

    # 'PRINC_DIAG' was privacy filtered
    assert set([r.diagnosis_code for r in sample_diag_rows if not r.service_line_number]) \
        == set([None, 'DIAG7'])


def test_prov_npi():
    "Ensure that npi fields are populated"
    sample_npi_row = [r for r in results if r.claim_id == 'claim-1'][0]

    assert sample_npi_row.prov_rendering_npi == "9999999999"


def test_cleanup(spark):
    cleanup(spark)
