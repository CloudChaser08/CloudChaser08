import pytest

import datetime

import spark.providers.caris.sparkNormalizeCaris as caris

legacy_results = []
results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('lab_common_model')
    spark['sqlContext'].dropTempTable('raw_transactional')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    # new model run
    caris.run(spark['spark'], spark['runner'], '2017-04-01', True)
    global legacy_results
    legacy_results = spark['sqlContext'].sql('select * from lab_common_model') \
                                        .collect()

    cleanup(spark)

    caris.run(spark['spark'], spark['runner'], '2017-05-01', True)
    global results
    results = spark['sqlContext'].sql('select * from lab_common_model') \
                                 .collect()


def test_legacy_date_parsing():
    assert filter(lambda r: r.claim_id == 'patient-6_ods-6', legacy_results)[0] \
        .date_service == datetime.date(2005, 4, 20)
    assert filter(lambda r: r.claim_id == 'patient-7_ods-7', legacy_results)[0] \
        .date_service == datetime.date(2017, 3, 17)


def test_date_parsing():
    assert filter(lambda r: r.claim_id == 'patient-0_deid-0', results)[0] \
        .date_service == datetime.date(2017, 3, 25)


def test_labtests_translated():
    assert len(filter(
        lambda r: r.claim_id == 'patient-1_deid-1' and r.test_ordered_name == 'IHC_ERCC1',
        results
    ))


def test_ngs_offering():
    assert len(filter(
        lambda r: r.claim_id == 'patient-0_deid-0' and r.test_ordered_name == 'NGS_OFFERING_X',
        results
    ))
    assert len(filter(
        lambda r: r.claim_id == 'patient-6_deid-6' and r.test_ordered_name == 'NGS_OFFERING_A',
        results
    ))


def test_sign_out_date_added():
    "Ensure that year of birth capping was applied"
    assert filter(lambda r: r.claim_id == '000001_00000', results)[0] \
        .date_service == datetime.date(2010, 3, 9)


def test_accession_date_added():
    "Ensure that year of birth capping was applied"
    assert filter(lambda r: r.claim_id == '000002_00001', results)[0] \
        .date_service == datetime.date(2017, 17, 3)


def test_date_parsing():
    "Ensure that dates are correctly parsed"
    assert filter(lambda r: r.claim_id == '2073344007_17897', results)[0] \
        .date_service == datetime.date(2016, 12, 1)


def test_diag_explosion():
    "Ensure that diagnosis codes were exploded on '^'"
    diags = map(
        lambda r: str(r.diagnosis_code),
        filter(lambda r: r.claim_id == '2073344012_17897', results)
    )
    diags.sort()

    assert diags == ['DIAG1', 'DIAG4', 'DIAG6']


def test_nodiag_inclusion():
    "Ensure that claims with no diagnosis codes were included"
    claim = filter(lambda r: r.claim_id == '2073344008_17897', results)

    assert len(claim) == 1


def test_diagnosis_qual_translation():
    "Ensure that diagnosis code qualifiers were correctly translated to HV"
    assert filter(lambda r: r.claim_id == '2073344009_17897', results)[0] \
        .diagnosis_code_qual == '02'

    assert filter(lambda r: r.claim_id == '2073344012_17897', results)[0] \
        .diagnosis_code_qual == '01'



def test_cleanup(spark):
    cleanup(spark)
