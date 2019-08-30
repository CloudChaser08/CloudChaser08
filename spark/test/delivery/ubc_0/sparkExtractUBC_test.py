import pytest
import hashlib
import spark.delivery.ubc_0.sparkExtractUBC as ubc_extract
import os
import shutil

from datetime import datetime, date

pharmacy_final_export       = []
pharmacy_prelim_export      = []
enrollment_export           = []
pharmacyclaims_record_count = 0
enrollment_record_count     = 0
@pytest.mark.usefixtures("spark")
def test_init(spark):
    spark['runner'].run_spark_script('resources/pharmacyclaims.sql')
    spark['runner'].run_spark_script('resources/enrollmentrecords.sql')
    ubc_extract.run(spark['spark'], spark['runner'], '2017-05', True)
    global pharmacy_prelim_export, pharmacy_final_export, enrollment_export, \
            pharmacyclaims_record_count, enrollment_record_count
    pharmacy_final_export  = [x for x in spark['sqlContext'].sql('select * from express_scripts_rx_norm_final_out') \
                                     .collect() if x.hvid is not None and len(x.hvid) == 32]
    pharmacy_prelim_export = [x for x in spark['sqlContext'].sql('select * from express_scripts_rx_norm_prelim_out') \
                                     .collect() if x.hvid is not None and len(x.hvid) == 32]
    enrollment_export      = [x for x in spark['sqlContext'].sql('select * from express_scripts_enrollment_out') \
                                      .collect() if x.hvid is not None and len(x.hvid) == 32]
    pharmacyclaims_record_count = spark['sqlContext'].sql('SELECT COUNT(*) FROM pharmacyclaims_old').collect()[0][0]
    enrollment_record_count     = spark['sqlContext'].sql('SELECT COUNT(*) FROM enrollmentrecords').collect()[0][0]

def test_hvid_obfuscation():
    """Test that HVIDs in both pharmacy claims and enrollment data are obfuscated, and that
    the obfuscation works identically on both sets of data"""
    hvid1 = '123456789'
    hvid2 = '912345678'
    assert [x for x in pharmacy_prelim_export if x.claim_id == 'TBvIP6J0HBHPzTsyA2/xDbpENnzMYmjn5MGl2u3O61U='][0].hvid \
        == hashlib.md5(hvid1.encode('UTF-8') + 'UBC0'.encode('UTF-8')).hexdigest().upper()
    assert [x for x in enrollment_export if x.date_end == datetime.strptime('2013-09-23', '%Y-%m-%d').date()][0].hvid \
        == hashlib.md5(hvid2.encode('UTF-8') + 'UBC0'.encode('UTF-8')).hexdigest().upper()
    assert [x for x in pharmacy_prelim_export if x.claim_id == 'TBvIP6J0HBHPzTsyA2/xDbpENnzMYmjn5MGl2u3O61U='][0].hvid \
        == [x for x in enrollment_export if x.date_end == datetime.strptime('2014-09-30', '%Y-%m-%d').date()][0].hvid \

def test_num_records_exported():
    """Test that only the relevant pharmacy claims were exported, and that all
    of the enrollment records were exported"""
    assert len(pharmacy_prelim_export) == 3
    assert len(pharmacy_prelim_export) != pharmacyclaims_record_count
    assert len(pharmacy_final_export) == 1
    assert len(enrollment_export) == enrollment_record_count

def test_file_name_prefixes():
    """Test that the exported part files have the expected prefix in their
    name"""
    pharmacy_prefix = 'pharmacyclaims_2017-03_final_'
    part_files = [f for f in os.listdir('/tmp/ubc_pharmacy_final_data/')
        if not f.endswith('.crc') and not f.startswith(".hive-staging")]
    for f in part_files:
        assert f.startswith(pharmacy_prefix)

    pharmacy_prefix = 'pharmacyclaims_2017-04_prelim_'
    part_files = [f for f in os.listdir('/tmp/ubc_pharmacy_prelim_data/')
        if not f.endswith('.crc') and not f.startswith(".hive-staging")]
    for f in part_files:
        assert f.startswith(pharmacy_prefix)

    enrollment_prefix = 'enrollmentrecords_2017-04_'
    part_files = [f for f in os.listdir('/tmp/ubc_enrollment_data/')
        if not f.endswith('.crc') and not f.startswith(".hive-staging")]
    for f in part_files:
        assert f.startswith(enrollment_prefix)

def test_cleanup(spark):
    spark['sqlContext'].sql('drop table if exists express_scripts_enrollment_out')
    spark['sqlContext'].sql('drop table if exists express_scripts_rx_norm_final_out')
    spark['sqlContext'].sql('drop table if exists express_scripts_rx_norm_prelim_out')

    try:
        shutil.rmtree('/tmp/ubc_pharmacy_final_data/')
    except:
        pass

    try:
        shutil.rmtree('/tmp/ubc_pharmacy_prelim_data/')
    except:
        pass

    try:
        shutil.rmtree('/tmp/ubc_enrollment_data/')
    except:
        pass
