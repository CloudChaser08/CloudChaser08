import pytest
import datetime
import spark.providers.express_scripts.enrollmentrecords.normalize as esi

results = []
ref_data = []
new_phi1 = []
ref_phi1 = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('esi_norm_final')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    esi.run('2017-05-01', end_to_end_test=False, test=True, spark=spark['spark'], runner=spark['runner'])
    global results, ref_data, new_phi1, ref_phi1
    results = spark['sqlContext'].sql('select * from esi_norm_final').collect()
    ref_data = spark['sqlContext'].sql('select * from local_phi').collect()
    new_phi1 = spark['sqlContext'].sql('select * from new_phi').collect()
    ref_phi1 = spark['sqlContext'].sql('select * from ref_phi').collect()


def test_new_phi():
    """test_new_phi"""
    assert len(new_phi1) == 5


def test_ref_phi():
    """test_ref_phi"""
    assert len(ref_phi1) == 8


def test_phi_merge():
    """Check that old reference data is merged with new reference data"""
    assert len(ref_data) == 13


def test_no_orphaned_records():
    """Check that enrollment records where the patient_id doesn't match
    any patients in the reference data aren't in the final table"""
    assert len([r for r in results if r.date_start == datetime.date(2017, 4, 22)]) == 0


def test_date_start_parsing():
    """Check that date_state is parsed and not capped even when it's absurd"""
    assert len([r for r in results if r.date_start == datetime.date(1901, 1, 1)]) != 0


def test_end_date_parsing():
    """Check that date_end is parsed and not capped even when it's absurd"""
    assert len([r for r in results if r.date_end == datetime.date(3000, 1, 1)]) != 0


def test_cleanup(spark):
    cleanup(spark)
