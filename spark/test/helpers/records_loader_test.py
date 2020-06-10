import pytest

from pyspark.sql.types import StructType, StructField, LongType, FloatType, IntegerType, StringType, DateType
import spark.helpers.records_loader as records_loader
import spark.helpers.file_utils as file_utils
import spark.test.resources.transactions as transactions

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
empty_csv_location = file_utils.get_abs_path(
    __file__, '../resources/blank.csv'
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

def test_empty_csv(spark):
    """
    Test that an empty CSV still creates a DataFrame
    """
    df = records_loader.load(spark['runner'], empty_csv_location, columns, 'csv', ',')

    assert df.count() == 0
    assert df.columns == columns

def test_error_wrong_number_of_columns_csv(spark):
    """
    Test that attempting to read a CSV with too many columns results in an error
    """
    with pytest.raises(Exception) as e:
        records_loader.load(spark['runner'], csv_location, columns[:-1], 'csv', ',')

    assert "Number of columns in data file (5) exceeds expected schema (4)" in str(e.value)

def test_unsupported_file_type(spark):
    """
    Test that the loader throws an error when an unsupported file type is passed
    in for loading
    """
    with pytest.raises(ValueError) as e:
        records_loader.load(spark['runner'], empty_csv_location, columns, 'tsv')

    assert str(e.value) == 'Unsupported file type: tsv'

def test_load_and_clean_all(spark):
    records_loader.load_and_clean_all(spark['runner'], csv_location, transactions, 'csv', ',')
    record = spark['runner'].sqlContext.table('raw').collect()[0]

    assert record.id == '1234567'
    assert record.diagnosis_code == 'I2150'
    assert record.diagnosis_code_qual == '02'
    assert record.prescribing_npi == '1032368193'
    assert record.notes == 'cookie'
