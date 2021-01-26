import datetime
import pytest

from pyspark.sql import Row
import spark.providers.xifin.medicalclaims.normalize as xifin

results = None


def cleanup(spark):
    pass


@pytest.mark.usefixtures('spark')
def test_init(spark):
    cleanup(spark)
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id=xifin.FEED_ID,
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        ),
        Row(
            hvm_vdr_feed_id=xifin.FEED_ID,
            gen_ref_domn_nm='HVM_AVAILABLE_HISTORY_START_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    xifin.run(spark['spark'], spark['runner'], '2018-04-09', test=True)
    global results
    results = spark['sqlContext'].sql('select * from xifin_medicalclaims') \
                                 .collect()


def test_cardinality():
    assert len(results) == 32


def test_diagnosis_distinct_values():
    assert sorted(set([
        (res.claim_id, res.vendor_test_id, res.diagnosis_priority, res.diagnosis_code, res.diagnosis_code_qual)
        for res in results
    ])) == [('0_accn_id-0', 'test_id-0', '1', 'DIAGCODEPROC01', None),
            ('0_accn_id-0', 'test_id-0', '2', 'DIAGCODEPROC02', None),
            ('0_accn_id-0', 'test_id-0', '3', 'DIAGCODEPROC03', None),
            ('0_accn_id-0', 'test_id-0', '4', 'DIAGCODEDIAG0', '01'),
            ('0_accn_id-0', 'test_id-5', '1', 'DIAGCODEPROC01', '02'),
            ('0_accn_id-1', 'test_id-1', '1', 'DIAGCODEDIAG1', '02'),
            ('0_accn_id-2', 'test_id-2', '1', 'DIAGCODEPROC21', None),
            ('0_accn_id-2', 'test_id-2', '2', 'DIAGCODEPROC22', None),
            ('0_accn_id-2', 'test_id-2', '3', 'DIAGCODEPROC23', None),
            ('0_accn_id-2', 'test_id-2', '4', 'DIAGCODEPROC24', None),
            ('0_accn_id-2', 'test_id-3', '1', 'DIAGCODEDIAG23', '01'),
            ('0_accn_id-2', 'test_id-4', '1', 'DIAGCODEDIAG24', '02')]


def test_procedure_code():
    assert sorted(set([
        (
            res.claim_id, res.vendor_test_id, res.procedure_code, res.procedure_code_qual, res.procedure_modifier_1,
            res.procedure_modifier_2, res.procedure_modifier_3, res.procedure_modifier_4
        ) for res in results
    ])) == [('0_accn_id-0', 'test_id-0', 'PROC', 'HC', 'mo', 'mo', 'mo', 'mo'),
            ('0_accn_id-0', 'test_id-5', None, None, None, None, None, None),
            ('0_accn_id-1', 'test_id-1', None, None, 'mo', 'mo', 'mo', 'mo'),
            ('0_accn_id-2', 'test_id-2', 'PROC', 'HC', 'mo', 'mo', 'mo', 'mo'),
            ('0_accn_id-2', 'test_id-3', 'PROC', 'HC', 'mo', 'mo', 'mo', 'mo'),
            ('0_accn_id-2', 'test_id-4', None, None, None, None, None, None)]


def test_claim_transaction_amt():
    assert sorted(set([
        (res.claim_id, res.claim_transaction_amount, res.claim_transaction_amount_qual)
        for res in results
    ])) == [('0_accn_id-0', 0.0, 'DEMO.EXPECT_PRICE'),
            ('0_accn_id-0', 100.0, 'DEMO.RETRO_BILL_PRICE'),
            ('0_accn_id-0', 300.0, 'DEMO.RETRO_TRADE_DISCOUNT_AMOUNT'),
            ('0_accn_id-0', 500.0, 'DEMO.TRADE_DISCOUNT_AMOUNT'),
            ('0_accn_id-0', 1000.0, 'DEMO.DUE_AMT'),
            ('0_accn_id-1', None, None),
            ('0_accn_id-2', None, None)]


def test_phys_info():
    assert sorted(set(
        [(res.prov_referring_npi, res.prov_referring_name_1, res.prov_referring_name_2) for res in results]
    )) == [
        ('0000000001', 'GRETZKY', 'WAYNE'),
        ('0000000001', 'JOHN', 'ELTON'),
        ('0000000001', 'JORDAN', 'MICHAEL')
    ]


def test_cleanup(spark):
    cleanup(spark)
