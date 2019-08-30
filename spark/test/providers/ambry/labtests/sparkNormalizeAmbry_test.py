import datetime
import pytest

import logging

from pyspark.sql import Row
import spark.providers.ambry.labtests.sparkNormalizeAmbry as ambry 

results = []

def cleanup(spark):
    spark['sqlContext'].dropTempTable('labtests_common_model')
    spark['sqlContext'].dropTempTable('ambry_transactions')
    spark['sqlContext'].dropTempTable('ambry_transactions_gene_exploded')
    spark['sqlContext'].dropTempTable('exploder')
    spark['sqlContext'].dropTempTable('ref_gen_ref')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id = '43',
            gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm = '',
            gen_ref_1_dt = datetime.date(1901, 1, 1),
            whtlst_flg = '' 
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')
    ambry.run(spark['spark'], spark['runner'], '2018-01-01', test=True)
    global results
    results = spark['sqlContext'].sql('select * from labtests_common_model') \
                                 .collect()


# 1 row with 3 genes tested and 2 diags = 6 rows
# 1 row with 5 genes tested and 1 diag  = 5 rows
# 1 row with 0 genes tested and 1 diag  = 1 row
# 1 row with 1 gene tested and 0 diags  = 1 row
# 1 row with 0 genes tested and 0 diags = 1 row
#                                       = 14 rows
def test_number_of_target_rows_correct_length():
    assert len(results) == 14


def test_number_of_target_rows_per_source_row_correct_length():
    assert len([x for x in results if x.claim_id == 'a']) == 6
    assert len([x for x in results if x.claim_id == 'b']) == 5
    assert len([x for x in results if x.claim_id == 'c']) == 1
    assert len([x for x in results if x.claim_id == 'd']) == 1
    assert len([x for x in results if x.claim_id == 'e']) == 1


def test_test_battery_name_field_is_populated():
    for r in results:
        assert r.test_battery_name is not None


def test_test_ordered_name_is_populated():
    assert len([r for r in results if r.test_ordered_name is not None]) == 12


def test_cleanup(spark):
    cleanup(spark)



