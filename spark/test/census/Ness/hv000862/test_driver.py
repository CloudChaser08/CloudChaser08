from spark.census.Ness.hv000862.driver import UbcNessCensusDriver
import pytest
from datetime import date

def all_tables(spark):
    return [el['tableName'] for el in spark['sqlContext'].sql("SHOW TABLES").collect()]


def rows_for_table(spark, table_name):
    return spark['sqlContext'].sql("SELECT * FROM " + table_name).collect()


def columns_for_table(spark, table_name):
    return spark['sqlContext'].table(table_name).columns


@pytest.fixture
@pytest.mark.usefixtures("patch_spark_init")
def driver(patch_spark_init):
    driver = UbcNessCensusDriver(salt="UBC-0")

    # Use test path locations
    driver.matching_path_template = "./test/census/{client}/{opp_id}/resources/matching/{{batch_id_path}}/".format(
        opp_id=driver._opportunity_id,
        client=driver._client_name
    )

    driver.records_path_template = "./test/census/{client}/{opp_id}/resources/transactions/{{batch_id_path}}/".format(
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

    # ~ Then a populated table "payload" exists
    assert 'matching_payload' in all_tables(spark)
    assert 'transactions' in all_tables(spark)
    assert len(rows_for_table(spark, 'matching_payload')) > 0
    assert len(rows_for_table(spark, 'transactions')) > 0

    # ~ When transformation scripts are run
    _ = driver.transform(batch_date, batch_id)

    # ~ Then census_result table exists
    assert 'census_result' in all_tables(spark)
    assert len(rows_for_table(spark, 'census_result')) > 0

    # ~ Then verify output schema
    assert ["HVID", "GUID", "UniqueRecordNumebr", "UBCApp", "UBCDB", "UBCProgram"] == columns_for_table(spark, 'census_result')

    # ~ Verify table contents
    rows = rows_for_table(spark, 'census_result')
    expected_hvid = ['8f3e12b9e985540963e5a5f2df5db86e', 'bd523724b25b27e122ba857ac5a1b10e',
                     'ad0d917fbb71843aeac366908539727f']

    persons = []
    apps = []
    ubcdbs = []
    programs = []

    for i in range(0, len(rows)):
        persons.append("person-{}".format(i))
        apps.append("UBCapp{}".format(i))
        ubcdbs.append("DB{}".format(i))
        programs.append("Prog{}".format(i))

    for index, row in enumerate(rows):
        assert row.HVID in expected_hvid
        assert row.GUID in persons
        assert row.UBCApp in apps
        assert row.UBCDB in ubcdbs
        assert row.UBCProgram in programs


if __name__ == "__main__":
    pytest.main()
