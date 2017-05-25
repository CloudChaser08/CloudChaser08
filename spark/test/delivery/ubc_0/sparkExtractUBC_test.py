import pytest
from datetime import datetime, date
import hashlib
import spark.delivery.ubc_0.sparkExtractUBC as ubc_extract
import os

pharmacy_export   = []
enrollment_export = []
pharmacyclaims_record_count = 0
enrollment_record_count     = 0
@pytest.mark.usefixtures("spark")
def test_init(spark):
    spark['runner'].run_spark_script('resources/pharmacyclaims.sql')
    spark['runner'].run_spark_script('resources/enrollmentrecords.sql')
    ubc_extract.run(spark['spark'], spark['runner'], '2017-04', True)
    global pharmacy_export, enrollment_export, pharmacyclaims_record_count, \
        enrollment_record_count
    pharmacy_export   = filter(lambda x: x.hvid is not None and len(x.hvid) == 32,
                            spark['sqlContext'].sql('select * from express_script_rx_norm_out') \
                                 .collect()
                        )
    enrollment_export = filter(lambda x: x.hvid is not None and len(x.hvid) == 32,
                            spark['sqlContext'].sql('select * from express_script_enrollment_out') \
                                 .collect()
                        )
    pharmacyclaims_record_count = spark['sqlContext'].sql('SELECT COUNT(*) FROM pharmacyclaims').collect()[0][0]
    enrollment_record_count     = spark['sqlContext'].sql('SELECT COUNT(*) FROM enrollmentrecords').collect()[0][0]

def test_hvid_obfuscation():
    """Test that HVIDs in both pharmacy claims and enrollment data are obfuscated, and that
    the obfuscation works identically on both sets of data"""
    hvid1 = '123456789'
    hvid2 = '912345678'
    assert filter(lambda x: x.claim_id == 'TBvIP6J0HBHPzTsyA2/xDbpENnzMYmjn5MGl2u3O61U=', pharmacy_export)[0].hvid \
        == hashlib.md5(hvid1 + 'UBC0').hexdigest().upper()
    assert filter(lambda x: x.date_end == datetime.strptime('2013-09-23', '%Y-%m-%d').date(), enrollment_export)[0].hvid \
        == hashlib.md5(hvid2 + 'UBC0').hexdigest().upper()
    assert filter(lambda x: x.claim_id == 'TBvIP6J0HBHPzTsyA2/xDbpENnzMYmjn5MGl2u3O61U=', pharmacy_export)[0].hvid \
        == filter(lambda x: x.date_end == datetime.strptime('2014-09-30', '%Y-%m-%d').date(), enrollment_export)[0].hvid \

def test_num_records_exported():
    """Test that only the relevant pharmacy claims were exported, and that all
    of the enrollment records were exported"""
    assert len(pharmacy_export) == 5
    assert len(pharmacy_export) != pharmacyclaims_record_count
    assert len(enrollment_export) == enrollment_record_count

def test_file_name_prefixes():
    """Test that the exported part files have the expected prefix in their
    name"""
    pharmacy_prefix = 'pharmacyclaims_2017-03_2017-04_'
    part_files = filter(
        lambda f: not f.endswith('.crc'),
        os.listdir(
            '/tmp/ubc_pharmacy_data/'
        )
    )
    for f in part_files:
        assert f.startswith(pharmacy_prefix)

    enrollment_prefix = 'enrollmentrecords_2017-03_2017-04_'
    part_files = filter(
        lambda f: not f.endswith('.crc'),
        os.listdir(
            '/tmp/ubc_enrollment_data/'
        )
    )
    for f in part_files:
        assert f.startswith(enrollment_prefix)

def test_cleanup(spark):
    spark['sqlContext'].sql('drop table express_script_enrollment_out')
    spark['sqlContext'].sql('drop table express_script_rx_norm_out')

