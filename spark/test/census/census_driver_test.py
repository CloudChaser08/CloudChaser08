import pytest
import spark.common.census_driver
import gzip
import subprocess

from spark.common.census_driver import CensusDriver
from spark.helpers.udf.general_helpers import obfuscate_hvid
from datetime import date

CLIENT_NAME = 'TEST'
OPPORTUNITY_ID = 'TEST123'
CUSTOM_SALT = 'custom_salt'

@pytest.fixture
@pytest.mark.usefixtures("patch_spark_init")
def test_driver(patch_spark_init):
    return CensusDriver(CLIENT_NAME, OPPORTUNITY_ID, test=True)


@pytest.fixture
@pytest.mark.usefixtures("patch_spark_init")
def e2e_driver(patch_spark_init):
    return CensusDriver(CLIENT_NAME, OPPORTUNITY_ID, end_to_end_test=True)


@pytest.fixture
@pytest.mark.usefixtures("patch_spark_init")
def prod_driver(patch_spark_init):
    return CensusDriver(CLIENT_NAME, OPPORTUNITY_ID)


@pytest.fixture
@pytest.mark.usefixtures("patch_spark_init")
def salt_def_driver(patch_spark_init):
    return CensusDriver(CLIENT_NAME, OPPORTUNITY_ID, salt=CUSTOM_SALT, test=True)


@pytest.mark.usefixtures("test_driver", "e2e_driver", "prod_driver", "salt_def_driver")
def test_default_paths_templates(test_driver, e2e_driver, prod_driver, salt_def_driver):
    """
    Ensure that all the various templates are set correctly
    """
    assert test_driver._records_path_template == \
        '../test/census/TEST/TEST123/resources/input/{batch_id_path}/'
    assert test_driver._matching_path_template == \
        '../test/census/TEST/TEST123/resources/matching/{batch_id_path}/'
    assert test_driver._output_path == \
        '../test/census/TEST/TEST123/resources/output/'

    assert e2e_driver._records_path_template == \
        's3://salusv/testing/dewey/airflow/e2e/TEST/TEST123/records/{batch_id_path}/'
    assert e2e_driver._matching_path_template == \
        's3://salusv/testing/dewey/airflow/e2e/TEST/TEST123/matching/{batch_id_path}/'
    assert e2e_driver._output_path == \
        's3://salusv/testing/dewey/airflow/e2e/TEST/TEST123/output/'

    assert prod_driver._records_path_template == \
        "s3a://salusv/incoming/census/TEST/TEST123/{batch_id_path}/"
    assert prod_driver._matching_path_template == \
        "s3a://salusv/matching/payload/census/TEST/TEST123/{batch_id_path}/"
    assert prod_driver._output_path == \
        "s3a://salusv/deliverable/TEST/TEST123/"

    assert salt_def_driver._records_path_template == \
        '../test/census/TEST/TEST123/resources/input/{batch_id_path}/'
    assert salt_def_driver._matching_path_template == \
        '../test/census/TEST/TEST123/resources/matching/{batch_id_path}/'
    assert salt_def_driver._output_path == \
        '../test/census/TEST/TEST123/resources/output/'

    assert test_driver._output_file_name_template == '{batch_id_value}_response_{{part_num}}.csv.gz'
    assert test_driver._records_module_name == 'records_schemas'
    assert test_driver._matching_payloads_module_name == 'matching_payloads_schemas'


@pytest.mark.usefixtures("test_driver")
def test_property_overwrites(test_driver):
    """
    Ensure that property overwriting works
    """
    test_driver.records_path_template = '123456'
    assert test_driver._records_path_template == '123456'

    test_driver.matching_path_template = '123457'
    assert test_driver._matching_path_template == '123457'

    test_driver.output_path = '123458'
    assert test_driver._output_path == '123458'

    test_driver.records_module_name = '123459'
    assert test_driver._records_module_name == '123459'

    test_driver.matching_payloads_module_name = '123460'
    assert test_driver._matching_payloads_module_name == '123460'


@pytest.mark.usefixtures("test_driver", "salt_def_driver")
def test_salt_overwrites(test_driver, salt_def_driver):
    """
    Ensure that when a custom salt is specified it
    overrides the default behaviour of setting the
    salt to the opp_id
    """
    assert test_driver._salt == OPPORTUNITY_ID
    assert salt_def_driver._salt == CUSTOM_SALT


@pytest.mark.usefixtures("test_driver")
def test_load(test_driver):
    """
    Ensure that a matching_payload table is created from the files in the
    matching_path directory
    """
    test_driver._matching_path_template = '../test/resources/foo/'
    test_driver.load(date(2018, 1, 1), None)

    matching_tbl = test_driver._spark.table('matching_payload')

    assert len(matching_tbl.collect()) == 10
    assert 'hvid' in matching_tbl.columns
    assert 'claimId' in matching_tbl.columns


@pytest.mark.usefixtures("test_driver")
def test_transform(test_driver):
    """
    Ensure that the matching_payload table is transformed into one of hvid-rowid
    pairs. hvids should be obfuscated
    """
    test_driver._spark.sql("SELECT '1' as hvid, '2' as claimId").createOrReplaceTempView('matching_payload')

    results = test_driver.transform(date(2018, 1, 1), None).collect()

    # content
    assert results[0]['hvid'] == obfuscate_hvid('1', 'hvidTEST123')
    assert results[0]['rowid'] == '2'


@pytest.mark.usefixtures("test_driver")
def test_save(test_driver, monkeypatch):
    """
    Ensure that the output file and path are of the expected formats
    """
    monkeypatch.setattr(spark.common.census_driver, 'SAVE_PATH', '/tmp/')

    df = test_driver._spark.sql("SELECT '1' as hvid, '2' as rowid")
    test_driver.save(df, date(2018, 1, 1), None)

    with gzip.open('/tmp/2018/01/01/20180101_response_00000.csv.gz', 'rt') as fin:
        content = fin.readlines()

    row = content[1].strip()
    (hvid, rowid) = row.split('|')

    # Should only be 1 row of data, and a header row
    assert len(content) == 2

    # Should be 2 columns, pipe separated
    assert len(row.split('|')) == 2

    # First columns should be the hvid, 2nd the rowid. Both in quotes
    assert hvid == '"1"'
    assert rowid == '"2"'


@pytest.mark.usefixtures("prod_driver")
def test_copy_to_s3(prod_driver, monkeypatch):
    """
    Ensure that the s3 copy command is called with the expected parameters and
    that following the s3 copy, the source directory is removed
    """
    subprocess_calls = []
    def capture_subprocess_calls(params):
        subprocess_calls.append(params)
        return

    monkeypatch.setattr(subprocess, 'check_call', capture_subprocess_calls)

    prod_driver.copy_to_s3()

    # Should be an s3-dist-cp call
    assert subprocess_calls[0] == [
        's3-dist-cp',
        '--s3ServerSideEncryption',
        '--deleteOnSuccess',
        '--src',
        '/staging/',
        '--dest',
        's3a://salusv/deliverable/TEST/TEST123/']
