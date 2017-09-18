import pytest

from pyspark.sql.types import StructType, StructField, StringType

import spark.helpers.file_utils as file_utils
import spark.delivery.foresite_hv000321.sparkExtractForesite as foresite

script_path = __file__

def cleanup(spark):
    sqlContext.sql('DROP TABLES HERE')

@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    # load ndc code ref table
    spark['spark'].sparkContext.parallelize([
        ['metformin_ndc', 'metformin', 'metformin_brand']
    ]).toDF(
        StructType([
            StructField('ndc_code', StringType(), True),
            StructField('nonproprietary_name', StringType(), True),
            StructField('proprietary_name', StringType(), True)
        ])
    ).write.saveAsTable("default.ref_ndc_code")

    # load icd10 code ref table
    spark['spark'].sparkContext.parallelize([
        ['E112', 'header', 'long description'],
        ['E113', 'header', 'long description']
    ]).toDF(
        StructType([
            StructField('code', StringType(), True),
            StructField('header', StringType(), True),
            StructField('long_description', StringType(), True)
        ])
    ).write.saveAsTable("default.ref_icd10_diagnosis")

    # load ref_marketplace_to_warehouse
    spark['spark'].sparkContext.parallelize([
        ['express_scripts', 'pharmacy', 'esi']
    ]).toDF(
        StructType([
            StructField('warehouse_feed_name', StringType(), True),
            StructField('datatype', StringType(), True),
            StructField('marketplace_feed_name', StringType(), True)
        ])
    ).write.saveAsTable("default.ref_marketplace_to_warehouse")

    # load pharmacyclaims table
    spark['runner'].run_spark_script('../../../common/pharmacyclaims_common_model_v3.sql', [
        ['table_name', 'pharmacyclaims', False],
        ['properties', "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
         + "STORED AS TEXTFILE "
         + "LOCATION '{}' ".format(file_utils.get_abs_path(script_path, './resources/pharmacyclaims/')),
         False],
        ['external', '', False]
    ], script_path)

    foresite.run(spark['spark'], spark['runner'], '2017-05-01', True)




