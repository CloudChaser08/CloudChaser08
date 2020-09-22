import pytest

import datetime
from pyspark.sql import Row
import shutil
import logging

import spark.helpers.file_utils as file_utils
import spark.providers.quest.normalize as quest

results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('lab_common_model')
    spark['sqlContext'].dropTempTable('ref_gen_ref')
    spark['sqlContext'].dropTempTable('original_mp')
    spark['sqlContext'].dropTempTable('augmented_with_prov_attrs_mp')

    try:
        shutil.rmtree(file_utils.get_abs_path(__file__, './resources/output/'))
    except:
        logging.warn('No output directory.')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='18',
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(2013, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createTempView('ref_gen_ref')

    quest.run(spark['spark'], spark['runner'], '2016-12-31', True)
    global results
    results = spark['sqlContext'].sql('select * from lab_common_model') \
                                 .collect()


def test_year_of_birth_cap():
    "Ensure that year of birth capping was applied"
    assert [r for r in results if r.claim_id == '2073344007_17897'][0] \
        .patient_year_of_birth == '1927'


def test_date_parsing():
    "Ensure that dates are correctly parsed"
    assert [r for r in results if r.claim_id == '2073344007_17897'][0] \
        .date_service == datetime.date(2016, 12, 1)


def test_diag_explosion():
    "Ensure that diagnosis codes were exploded on '^'"
    diags = [
        str(r.diagnosis_code) for r in
        [r for r in results if r.claim_id == '2073344012_17897']
    ]
    diags.sort()

    assert diags == ['DIAG1', 'DIAG4', 'DIAG6']


def test_nodiag_inclusion():
    "Ensure that claims with no diagnosis codes were included"
    claim = [r for r in results if r.claim_id == '2073344008_17897']

    assert len(claim) == 1


def test_diagnosis_qual_translation():
    "Ensure that diagnosis code qualifiers were correctly translated to HV"
    assert [r for r in results if r.claim_id == '2073344009_17897'][0] \
        .diagnosis_code_qual == '02'

    assert [r for r in results if r.claim_id == '2073344012_17897'][0] \
        .diagnosis_code_qual == '01'


def test_provider_derived_hvids_added():
    "Ensure that hvids are derived from the provider addon file first"
    assert [r for r in results if r.claim_id.startswith('2073344007')][0] \
        .hvid == '9999'

    assert [r for r in results if r.claim_id.startswith('2073344008')][0] \
        .hvid == '9999'

    assert [r for r in results if r.claim_id.startswith('2073344009')][0] \
        .hvid == '0000'


def test_provider_information_appended():
    "Ensure that hvids are derived from the provider addon file first"
    assert [r for r in results if r.claim_id.startswith('2073344007')][0].ordering_npi == '1376766659'

    assert [r for r in results if r.claim_id.startswith('2073344009')][0].ordering_zip == '19147'


def test_cleanup(spark):
    cleanup(spark)
