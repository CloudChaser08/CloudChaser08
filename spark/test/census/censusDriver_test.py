import pytest
from spark.common.censusDriver import CensusDriver
from spark.helpers.udf.general_helpers import obfuscate_hvid

class testDriver(CensusDriver):
    CLIENT_NAME    = 'TEST'
    OPPORTUNITY_ID = 'TEST123'

    def __init__(self, spark_fixture):
        super(testDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY_ID, spark_fixture=spark_fixture)

driver = None

@pytest.mark.usefixtures("spark")
def test_default_paths_templates(spark):
    global driver
    driver = testDriver(spark)

    assert driver._records_path_template == \
        "s3a://salusv/incoming/census/TEST/TEST123/{year}/{month:02d}/{day:02d}/"
    assert driver._matching_path_template == \
        "s3a://salusv/matching/payload/census/TEST/TEST123/{year}/{month:02d}/{day:02d}/"
    assert driver._output_path_template == \
        "s3a://salusv/deliverable/TEST/TEST123/{year}/{month:02d}/{day:02d}/"

    assert driver._output_file_name_template     == 'response_{year}{month:02d}{day:02d}.gz'
    assert driver._records_module_name           == 'records_schemas'
    assert driver._matching_payloads_module_name == 'matching_paylods_schemas'

def test_property_overwrites():
    global driver 

    driver.records_path_template = '123456'
    assert driver._records_path_template == '123456'

    driver.matching_path_template = '123457'
    assert driver._matching_path_template == '123457'

    driver.output_path_template = '123458'
    assert driver._output_path_template == '123458'

    driver.records_module_name = '123459'
    assert driver._records_module_name == '123459'

    driver.matching_payloads_module_name = '123460'
    assert driver._matching_payloads_module_name == '123460'

#TODO test_load()

def test_transform():
    global driver

    driver._spark.sql("SELECT '1' as hvid, '2' as claimId").createOrReplaceTempView('matching_payload')

    results = driver.transform().collect()

    # header
    assert results[0]['hvid']  == 'hvid'
    assert results[0]['rowid'] == 'rowid'

    # content
    assert results[1]['hvid']  == obfuscate_hvid('1', 'hvidTEST123')
    assert results[1]['rowid'] == '2'

#TODO test_save()

#TODO test_copy_to_s3()
