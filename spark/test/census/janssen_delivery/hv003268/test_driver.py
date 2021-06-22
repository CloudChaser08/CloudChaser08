import datetime
import pytest
from spark.census.janssen.internal.hv003268.driver import JanssenInternalCensusDriver

@pytest.fixture
@pytest.mark.usefixtures("patch_spark_init")
def driver():
    census_driver = JanssenInternalCensusDriver(salt="JAN3268", test=True)
    return census_driver


@pytest.mark.usefixtures("spark")
def test_run(driver, spark):
    # Disable superfluous log output
    spark["spark"].sparkContext.setLogLevel("OFF")

    batch_id = "VAC31518COV3001_202106140005"
    batch_date = datetime.date(2021, 3, 15)

    driver.load(batch_date=batch_date, batch_id=batch_id)

    tables = [r["tableName"] for r in spark["sqlContext"].sql("show tables").collect()]
    assert "matching_payload" in tables
    assert spark["sqlContext"].sql("select * from matching_payload").count() == 10
    assert "records" in tables
    assert spark["sqlContext"].sql("select * from records").count() == 10

    _ = driver.transform(batch_date, batch_id)

    tables = [r["tableName"] for r in spark["sqlContext"].sql("show tables").collect()]
    assert "census_result" in tables
    results = spark["sqlContext"].sql("select * from census_result").collect()
    assert len(results) == 1
    assert results[0]["hvid"] == "999"
    assert results[0]["claimId"] == "10"
    assert results[0]["study"] == "3001"


@pytest.mark.usefixtures("spark")
def test_run_study_id_not_found(driver, spark):
    # Disable superfluous log output
    spark["spark"].sparkContext.setLogLevel("OFF")

    batch_id = "VAC31518_1234"
    batch_date = datetime.date(2021, 3, 15)

    driver.load(batch_date=batch_date, batch_id=batch_id)

    tables = [r["tableName"] for r in spark["sqlContext"].sql("show tables").collect()]
    assert "matching_payload" in tables
    assert spark["sqlContext"].sql("select * from matching_payload").count() == 10
    assert "records" in tables
    assert spark["sqlContext"].sql("select * from records").count() == 10

    _ = driver.transform(batch_date, batch_id)

    tables = [r["tableName"] for r in spark["sqlContext"].sql("show tables").collect()]
    assert "census_result" in tables
    results = spark["sqlContext"].sql("select * from census_result").collect()
    assert len(results) == 1
    assert results[0]["hvid"] == "999"
    assert results[0]["claimId"] == "10"
    assert results[0]["study"] == ""
