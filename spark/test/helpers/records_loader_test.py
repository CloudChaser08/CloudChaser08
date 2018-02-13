import pytest

from pyspark.sql.types import StructType, StructField, LongType, FloatType, IntegerType, StringType, DateType
import spark.helpers.records_loader as records_loader
import spark.helpers.file_utils as file_utils

csv_location = file_utils.get_abs_path(
    __file__, '../resources/records.csv'
)
psv_location = file_utils.get_abs_path(
    __file__, '../resources/records.psv'
)
quoted_csv_location = file_utils.get_abs_path(
    __file__, '../resources/quoted_records.csv'
)
incomplete_csv_location = file_utils.get_abs_path(
    __file__, '../resources/incomplete_records.csv'
)

columns = ['id', 'diagnosis_code', 'diagnosis_code_qual', 'prescribing_npi', 'notes']

@pytest.mark.usefixtures("spark")
def test_csv(spark):
    """
    Test that a simple CSV is loaded correctly
    """
    record = records_loader.load(spark['runner'], csv_location, columns, 'csv', ',').collect()[0]

    assert record.id == '1234567'
    assert record.diagnosis_code == 'I2150'
    assert record.diagnosis_code_qual == '02'
    assert record.prescribing_npi == '1032368193'
    assert record.notes == 'cookie'

def test_psv(spark):
    """
    Test that a simple PSV is loaded correctly
    """
    record = records_loader.load(spark['runner'], psv_location, columns, 'csv', '|').collect()[0]

    assert record.id == '1234567'
    assert record.diagnosis_code == 'I2150'
    assert record.diagnosis_code_qual == '02'
    assert record.prescribing_npi == '1032368193'
    assert record.notes == 'cookie'

def test_quoted_csv(spark):
    """
    Test that a CSV with quotes is loaded correctly
    """
    record = records_loader.load(spark['runner'], quoted_csv_location, columns, 'csv', ',').collect()[0]

    assert record.id == '1234567'
    assert record.diagnosis_code == 'I2150'
    assert record.diagnosis_code_qual == '02'
    assert record.prescribing_npi == '1032368193'
    assert record.notes == 'cookie'

def test_incomplete_csv(spark):
    """
    Test that a CSV with missing fields is loaded correctly
    """
    record = records_loader.load(spark['runner'], incomplete_csv_location, columns, 'csv', ',').collect()[0]

    assert record.id == '1234567'
    assert record.diagnosis_code == 'I2150'
    assert record.diagnosis_code_qual == '02'
    assert record.prescribing_npi is None
    assert record.notes is None

