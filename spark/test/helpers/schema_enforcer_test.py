import pytest
import spark.helpers.schema_enforcer as schema_enforcer
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import Row

master_schema = StructType([
    StructField('patient_id', IntegerType(), True),
    StructField('diagnosis_code', StringType(), True),
    StructField('procedure_code', StringType(), True),
    StructField('rendering_npi', IntegerType(), True)
])

record = Row(*master_schema.names)

@pytest.mark.usefixtures("spark")
def test_apply_schema_no_aliases(spark):
    """
        Check that when the original DataFrame has the same number of columns
        as the target schema but the columns names are different, the new
        DataFrame has the correct column names
    """
    global master_schema

    org_df = spark['sqlContext'].sql("SELECT '1', 'I10', '49999', '1231232123'")

    new_df = schema_enforcer.apply_schema(org_df, master_schema)

    assert master_schema.names == new_df.columns

@pytest.mark.usefixtures("spark")
def test_apply_schema_aliased_subset(spark):
    """
        Check that when the original DataFrame uses the same column names as
        the target schema but only has a subset of the columns in the schema,
        the new DataFrame has all the columns with the missing columns filled
        with NULLs
    """
    global master_schema

    org_df = spark['sqlContext'].sql("""
        SELECT '1' AS patient_id, 'I10' AS diagnosis_code,
        '1231232123' as rendering_npi
    """)

    new_df = schema_enforcer.apply_schema(org_df, master_schema)

    assert master_schema.names == new_df.columns
    assert new_df.where('procedure_code IS NULL').count() == 1


@pytest.mark.usefixtures("spark")
def test_apply_schema_explicit_subset(spark):
    """
        Check that when the original DataFrame only has a subset of the columns
        in the schema and a list of which columns are in the original DataFrame
        is supplied to apply_schema, the new DataFrame has all the columns with
        the missing columns filled with NULLs
    """
    global master_schema

    org_df = spark['sqlContext'].sql("SELECT '1', '49999', '1231232123'")

    new_df = schema_enforcer.apply_schema(org_df, master_schema, [
        'patient_id', 'procedure_code', 'rendering_npi'
    ])

    assert master_schema.names == new_df.columns
    assert new_df.where('diagnosis_code IS NULL').count() == 1

@pytest.mark.usefixtures("spark")
def test_apply_schema_out_of_order(spark):
    """
        Check that when the original DataFrame has a subset of the columns in
        the schema in a different order than the schema, the target schema is
        still applied correctly
    """
    global master_schema

    org_df = spark['sqlContext'].sql("SELECT '49999', '1231232123', 'I10'")

    new_df = schema_enforcer.apply_schema(org_df, master_schema, [
        'procedure_code', 'rendering_npi', 'diagnosis_code'
    ])

    assert new_df.collect()[0] == record(None, 'I10', '49999', 1231232123)

    org_df = spark['sqlContext'].sql("""
        SELECT '49999' AS procedure_code, '1231232123' AS rendering_npi,
        'I10' AS diagnosis_code
    """)

    new_df = schema_enforcer.apply_schema(org_df, master_schema)

    assert new_df.collect()[0] == record(None, 'I10', '49999', 1231232123)

@pytest.mark.usefixtures("spark")
def test_apply_schema_fails_extra_column(spark):
    """
        Check that when the original DataFrame has a different number of
        columns than the schema, there is no explicit list of columns to
        use, and at least one of the columns is not in the schema, an
        exception is thrown
    """
    global master_schema

    org_df = spark['sqlContext'].sql("""
        SELECT '49999' AS procedure_code, '1231232123' AS rendering_npi,
        '1056813579' as pharmacy_npi
    """)

    with pytest.raises(ValueError) as err:
        new_df = schema_enforcer.apply_schema(org_df, master_schema)

    assert str(err.value) == 'Column pharmacy_npi is not part of the schema'


def test_apply_schema_cols_to_keep(spark):
    """
        Check that when the original DataFrame has a different number of
        columns than the schema, there is no explicit list of columns
        to use, and at least one of the columns is not in the schema,
        but that column exists in cols_to_keep, the column is in the
        resulting df
    """
    global master_schema

    org_df = spark['sqlContext'].sql("""
        SELECT '49999' AS procedure_code, '1231232123' AS rendering_npi,
        '1056813579' as pharmacy_npi
    """)

    new_df = schema_enforcer.apply_schema(org_df, master_schema, columns_to_keep=['pharmacy_npi'])

    assert 'pharmacy_npi' in new_df.columns


@pytest.mark.usefixtures("spark")
def test_apply_schema_type_casting(spark):
    """
        Check that when the target schema is applied to the original DataFrame
        columns are cast into the target type
    """
    global master_schema

    org_df = spark['sqlContext'].sql("""
        SELECT '49999' AS procedure_code, '1231232123' AS rendering_npi
    """)

    new_df = schema_enforcer.apply_schema(org_df, master_schema)

    assert type(new_df.collect()[0].rendering_npi) == int


@pytest.mark.usefixtures("spark")
def test_apply_schema_func(spark):
    """
        Check that a schema applying function can be set up independently
        and then applied to a data frame
    """
    global master_schema

    org_df = spark['sqlContext'].sql("SELECT '1', 'I10', '49999', '1231232123'")

    schema_applying_func = schema_enforcer.apply_schema_func(master_schema)

    new_df = schema_applying_func(org_df)

    assert master_schema.names == new_df.columns
