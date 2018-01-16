import pytest

import datetime
from pyspark.sql import Row

import spark.providers.guardant_health.labtests.sparkNormalizeGuardantHealth as guardant_health

results = []


def cleanup(spark):
    pass


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='58',
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(2013, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createTempView('ref_gen_ref')

    guardant_health.run(spark['spark'], spark['runner'], '2016-12-31', True)
    global results
    results = spark['sqlContext'].sql('select * from normalized_labtests') \
                                 .collect()


def test_country_filter():
    assert sorted(set([r.claim_id for r in results])) == ['row-0', 'row-1']


def test_result_explode():
    assert sorted([[r.result, r.result_name] for r in results if r.claim_id == 'row-0']) \
        == [['CHROMOSOME', 'chromosome'],
            ['EXON', 'exon'],
            ['MAF_PERCENTAGE', 'maf_percentage'],
            ['MUTATION_AA', 'mutation_aa'],
            ['MUTATION_NT', 'mutation_nt'],
            ['REF_SEQ_TRANS', 'ref_seq_transcript_id']]

    assert sorted([[r.result, r.result_name] for r in results if r.claim_id == 'row-1']) \
        == [[None, None]]


def test_cleanup(spark):
    cleanup(spark)
