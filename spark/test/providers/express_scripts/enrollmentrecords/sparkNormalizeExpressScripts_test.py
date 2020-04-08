import pytest
import datetime
import spark.providers.express_scripts.enrollmentrecords.sparkNormalizeExpressScriptsEnrollment as esi

results = []
ref_data = []
@pytest.mark.usefixtures("spark")
def test_init(spark):
    esi.run(spark['spark'], spark['runner'], '2017-05-01', True)
    global results
    results = spark['sqlContext'].sql('select * from enrollment_common_model') \
                                 .collect()
    global ref_data
    ref_data = spark['sqlContext'].sql('select * from local_phi') \
                                 .collect()

def test_phi_merge():
    "Check that old reference data is merged with new reference data" 
    assert len(ref_data) == 13

def test_no_orphaned_records():
    """Check that enrollment records where the patient_id doesn't match
    any patients in the reference data aren't in the final table"""
    assert len([r for r in results if r.date_start == datetime.date(2017, 4, 22)]) == 0

def test_date_start_parsing():
    "Check that date_state is parsed and not capped even when it's absurd"
    assert len([r for r in results if r.date_start == datetime.date(1901, 1, 1)]) != 0

def test_end_date_parsing():
    "Check that date_end is parsed and not capped even when it's absurd"
    assert len([r for r in results if r.date_end == datetime.date(3000, 1, 1)]) != 0

def test_cleanup(spark):
    spark['sqlContext'].dropTempTable('lab_common_model')

