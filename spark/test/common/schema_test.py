from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType
from spark.common.schema import Schema

STRUCTURE = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_medcl_clm_pymt_sumry_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True)
])


def test_schema():
    schema = Schema(name="test", schema_structure=STRUCTURE, output_directory='/test')
    assert schema.name == "test"
    assert schema.schema_structure == STRUCTURE
    assert schema.output_directory == '/test'
