import pytest
import spark.helpers.privacy.medicalclaims as medical_priv
from pyspark.sql.types import StructField, StructType, StringType, Row


@pytest.mark.usefixtures("spark")
def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['1', 'I', 'V24', '02', '2017-01-01', 'notransform', '300', 'PA', None, '%^01$^'],
        ['2', 'I', 'V24', '01', '2015-01-01', 'notransform', '200', 'ca', None, '###LONG_CODE###'],
        ['3', 'I', 'V24', None, '2017-01-01', 'notransform', '100', 'BAD STATE', None, None],
        ['4', 'P', 'V24', '02', '2017-01-01', 'notransform', None, 'PA', '5', '%^01$^'],
        ['5', 'P', 'V24', '01', '2015-01-01', 'notransform', None, 'ca', 'good pos', '###LONG_CODE###'],
        ['6', 'P', 'V24', None, '2017-01-01', 'notransform', None, 'BAD STATE', 'good pos', None]
    ]).toDF(StructType([
        StructField('id', StringType()),
        StructField('claim_type', StringType()),
        StructField('diagnosis_code', StringType()),
        StructField('diagnosis_code_qual', StringType()),
        StructField('date_service', StringType()),
        StructField('notransform', StringType()),
        StructField('inst_type_of_bill_std_id', StringType()),
        StructField('prov_rendering_state', StringType()),
        StructField('place_of_service_std_id', StringType()),
        StructField('procedure_modifier_1', StringType())
    ]))

    Claim = Row(*test_df.columns)

    # assert privacy filtering is being applied
    assert medical_priv.filter(test_df).collect() \
        == [Claim('1', 'I', None, '02', '2017-01-01', 'notransform', 'X00', None, None, '01'),
            Claim('2', 'I', 'V24', '01', '2015-01-01', 'notransform', '200', 'CA', None, 'LO'),
            Claim('3', 'I', None, None, '2017-01-01', 'notransform', '100', None, None, None),
            Claim('4', 'P', None, '02', '2017-01-01', 'notransform', None, None, '99', '01'),
            Claim('5', 'P', 'V24', '01', '2015-01-01', 'notransform', None, 'CA', 'good pos', 'LO'),
            Claim('6', 'P', None, None, '2017-01-01', 'notransform', None, None, 'good pos', None)]
