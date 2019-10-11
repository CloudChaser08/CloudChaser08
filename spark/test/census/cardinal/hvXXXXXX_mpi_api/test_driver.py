from datetime import date
from spark.census.cardinal.hvXXXXXX_mpi_api.driver import CardinalAPICensusDriver

import pytest
import os
import gzip


def all_tables(spark):
    return [el['tableName'] for el in spark['sqlContext'].sql("SHOW TABLES").collect()]


def rows_for_table(spark, table_name):
    return spark['sqlContext'].sql("SELECT * FROM " + table_name).collect()


def columns_for_table(spark, table_name):
    return spark['sqlContext'].table(table_name).columns


EXPECTED_OUTPUT_FILE = 'test/census/cardinal/hvXXXXXX_mpi_api/resources/output/expected_output.json.gz'

@pytest.fixture
@pytest.mark.usefixtures("patch_spark_init")
def driver(patch_spark_init):
    driver = CardinalAPICensusDriver(opportunity_id='hvXXXXXX_mpi_api', test=True)

    # Use test path locations
    driver.matching_path_template = "../test/census/{client}/{opp_id}/resources/matching/{{batch_id_path}}/".format(
        opp_id=driver._opportunity_id,
        client=driver._client_name
    )

    driver.records_path_template = "../test/census/{client}/{opp_id}/resources/transactions/{{batch_id_path}}/".format(
        opp_id=driver._opportunity_id,
        client=driver._client_name
    )

    driver._output_file_name_template = "../test/census/{client}/{opp_id}/resources/output/".format(
        opp_id=driver._opportunity_id,
        client=driver._client_name
    )

    return driver


@pytest.mark.usefixtures("spark")
def test_run(driver, spark):

    # ~ With superfluous log output disabled
    spark['spark'].sparkContext.setLogLevel("OFF")
    batch_id = "20190618"
    batch_date = date(2019, 6, 18)

    # ~ When matching payload is loaded
    driver.load(batch_date=batch_date, batch_id=batch_id)

    # ~ Then a populated table "matching_payload" exists
    assert 'matching_payload' in all_tables(spark)
    assert 'cardinal_mpi_api_transactions' in all_tables(spark)
    assert len(rows_for_table(spark, 'matching_payload')) > 0
    assert len(rows_for_table(spark, 'cardinal_mpi_api_transactions')) > 0

    # ~ When transformation scripts are run
    df = driver.transform()

    # ~ Then normalized table exists
    assert 'normalize' in all_tables(spark)
    assert len(rows_for_table(spark, 'normalize')) > 0

    # ~ Then verify output schema
    assert ["message_body", "metadata"] == columns_for_table(spark, 'normalize')

    # ~ When the file is saved
    driver.save(df, batch_date=batch_date, batch_id=batch_id)

    # ~ Then the return file should exist
    output_file = "/tmp/staging/{batch_id}/{batch_id}_response.json.gz".format(
        batch_id=batch_id
    )
    assert os.path.isfile(output_file)

    with gzip.open(output_file) as actual_file, gzip.open(EXPECTED_OUTPUT_FILE) as expected_file:
        assert sorted(actual_file.readlines()) == sorted(expected_file.readlines())


if __name__ == "__main__":
    pytest.main()
