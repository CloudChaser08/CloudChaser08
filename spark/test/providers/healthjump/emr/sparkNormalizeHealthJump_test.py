import pytest

import datetime
from pyspark.sql import Row

import spark.providers.healthjump.emr.sparkNormalizeHealthJump as healthjump

medication_results = []
procedure_results = []
lab_order_results = []
diagnosis_results = []


def cleanup(spark):
    spark['sqlContext'].sql('DROP TABLE IF EXISTS demographics_transactions')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS cpt_transactions')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS diagnosis_transactions')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS loinc_transactions')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS ndc_transactions')
    spark['sqlContext'].sql('DROP VIEW IF EXISTS ref_gen_ref')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='47',
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(2000, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createTempView('ref_gen_ref')

    healthjump.run(spark['spark'], spark['runner'], '2017-05-01', True)

    global medication_results, procedure_results, lab_order_results, diagnosis_results
    medication_results = spark['sqlContext'].sql('select * from medication_common_model') \
                                            .collect()
    procedure_results = spark['sqlContext'].sql('select * from procedure_common_model') \
                                           .collect()
    lab_order_results = spark['sqlContext'].sql('select * from lab_order_common_model') \
                                           .collect()
    diagnosis_results = spark['sqlContext'].sql('select * from diagnosis_common_model') \
                                           .collect()


def test_date_parsing():
 
    # date with slashes
    assert [r.proc_dt for r in procedure_results if r.hvid == 'hvid-9'] == [ datetime.date(2010, 4, 1) ]

    # date without slashes
    assert [r.proc_dt for r in procedure_results if r.hvid == 'hvid-8'] == [ datetime.date(2009, 10, 9) ]

    # no date
    assert [r.medctn_admin_dt for r in medication_results if r.hvid == 'hvid-9'] == [ datetime.date(2011, 10, 11) ] 
    assert [r.medctn_admin_dt for r in medication_results if r.hvid == 'hvid-0'] == [None]


def test_loinc_cleaning():
    assert [r.lab_ord_loinc_cd for r in lab_order_results if r.hvid == 'hvid-0'] == ['28852']


def test_cpt_cleaning():
    # 2<digit<5 code
    assert [r.proc_cd for r in procedure_results if r.hvid == 'hvid-4'] == [None]
    assert [r.proc_cd_1_modfr for r in procedure_results if r.hvid == 'hvid-4'] == [None]

    # 5 digit code
    assert [r.proc_cd for r in procedure_results if r.hvid == 'hvid-0'] == ['97110']
    assert [r.proc_cd_1_modfr for r in procedure_results if r.hvid == 'hvid-0'] == [None]

    assert [r.proc_cd for r in procedure_results if r.hvid == 'hvid-1'] == ['97140']
    assert [r.proc_cd_1_modfr for r in procedure_results if r.hvid == 'hvid-0'] == [None]

    # 2 digit code
    assert [r.proc_cd for r in procedure_results if r.hvid == 'hvid-5'] == [None]
    assert [r.proc_cd_1_modfr for r in procedure_results if r.hvid == 'hvid-5'] == ['01']

    # 7 digit code
    assert [r.proc_cd for r in procedure_results if r.hvid == 'hvid-9'] == ['97110']
    assert [r.proc_cd_1_modfr for r in procedure_results if r.hvid == 'hvid-9'] == ['02']

    # >7 digit code
    assert [r.proc_cd for r in procedure_results if r.hvid == 'hvid-6'] == ['99213']
    assert [r.proc_cd_1_modfr for r in procedure_results if r.hvid == 'hvid-6'] == [None]


def test_ndc_cleaning():
    # code too short
    assert [r.medctn_ndc for r in medication_results if r.hvid == 'hvid-4'] == [None]

    # code too long
    # NOTE: As of 2018-03-16, codes no longer have a max cap on length
    #       see spark/helpers/udf/post_normalization_clean.py:clean_up_ndc_code()
    assert [r.medctn_ndc for r in medication_results if r.hvid == 'hvid-0'] == ['00093506001111']

    # no code
    assert [r.medctn_ndc for r in medication_results if r.hvid == 'hvid-5'] == [None]

    # 11 digits
    assert [r.medctn_ndc for r in medication_results if r.hvid == 'hvid-1'] == ['00113060462']

    # 10 digits
    assert [r.medctn_ndc for r in medication_results if r.hvid == 'hvid-2'] == ['1184501477']


def test_cleanup(spark):
    cleanup(spark)
