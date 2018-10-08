import pytest
from spark.common.censusDriver import CensusDriver
from spark.helpers.udf.general_helpers import obfuscate_hvid
from datetime import date

CLIENT_NAME    = 'TEST'
OPPORTUNITY_ID = 'TEST123'

@pytest.fixture
@pytest.mark.usefixtures("spark")
def test_driver(spark):
    return CensusDriver(CLIENT_NAME, OPPORTUNITY_ID, spark_fixture=spark, test=True)

@pytest.fixture
@pytest.mark.usefixtures("spark")
def e2e_driver(spark):
    return CensusDriver(CLIENT_NAME, OPPORTUNITY_ID, spark_fixture=spark, end_to_end_test=True)

@pytest.fixture
@pytest.mark.usefixtures("spark")
def prod_driver(spark):
    return CensusDriver(CLIENT_NAME, OPPORTUNITY_ID, spark_fixture=spark)

@pytest.mark.usefixtures("test_driver", "e2e_driver", "prod_driver")
def test_default_paths_templates(test_driver, e2e_driver, prod_driver):
    """
    Check that all the various templates are set correctly
    """
    assert test_driver._records_path_template == \
        '../test/census/TEST/TEST123/resources/input/{year}/{month:02d}/{day:02d}/'
    assert test_driver._matching_path_template == \
        '../test/census/TEST/TEST123/resources/matching/{year}/{month:02d}/{day:02d}/'
    assert test_driver._output_path_template == \
        '../test/census/TEST/TEST123/resources/output/{year}/{month:02d}/{day:02d}/'

    assert e2e_driver._records_path_template == \
        's3://salusv/testing/dewey/airflow/e2e/TEST/TEST123/records/{year}/{month:02d}/{day:02d}/'
    assert e2e_driver._matching_path_template == \
        's3://salusv/testing/dewey/airflow/e2e/TEST/TEST123/matching/{year}/{month:02d}/{day:02d}/'
    assert e2e_driver._output_path_template == \
        's3://salusv/testing/dewey/airflow/e2e/TEST/TEST123/output/{year}/{month:02d}/{day:02d}/'

    assert prod_driver._records_path_template == \
        "s3a://salusv/incoming/census/TEST/TEST123/{year}/{month:02d}/{day:02d}/"
    assert prod_driver._matching_path_template == \
        "s3a://salusv/matching/payload/census/TEST/TEST123/{year}/{month:02d}/{day:02d}/"
    assert prod_driver._output_path_template == \
        "s3a://salusv/deliverable/TEST/TEST123/{year}/{month:02d}/{day:02d}/"

    assert test_driver._output_file_name_template     == 'response_{year}{month:02d}{day:02d}.gz'
    assert test_driver._records_module_name           == 'records_schemas'
    assert test_driver._matching_payloads_module_name == 'matching_paylods_schemas'

@pytest.mark.usefixtures("test_driver")
def test_property_overwrites(test_driver):
    """
    Check that property overwriting works
    """
    test_driver.records_path_template = '123456'
    assert test_driver._records_path_template == '123456'

    test_driver.matching_path_template = '123457'
    assert test_driver._matching_path_template == '123457'

    test_driver.output_path_template = '123458'
    assert test_driver._output_path_template == '123458'

    test_driver.records_module_name = '123459'
    assert test_driver._records_module_name == '123459'

    test_driver.matching_payloads_module_name = '123460'
    assert test_driver._matching_payloads_module_name == '123460'

@pytest.mark.usefixtures("test_driver")
def test_load(test_driver):
    """
    Check that a matching_payload table is created from the files in the
    matching_path directory
    """
    test_driver._matching_path_template = '../test/resources/foo/'
    test_driver.load(date(2018, 1, 1))

    matching_tbl = test_driver._spark.table('matching_payload')

    assert len(matching_tbl.collect()) == 10
    assert 'hvid' in matching_tbl.columns
    assert 'claimId' in matching_tbl.columns

@pytest.mark.usefixtures("test_driver")
def test_transform(test_driver):
    """
    Check that the matching_payload table is transformed into one of hvid-rowid
    pairs. hvids should be obfuscated
    """
    test_driver._spark.sql("SELECT '1' as hvid, '2' as claimId").createOrReplaceTempView('matching_payload')

    results = test_driver.transform().collect()

    # first row should be a header
    assert results[0]['hvid']  == 'hvid'
    assert results[0]['rowid'] == 'rowid'

    # content
    assert results[1]['hvid']  == obfuscate_hvid('1', 'hvidTEST123')
    assert results[1]['rowid'] == '2'
