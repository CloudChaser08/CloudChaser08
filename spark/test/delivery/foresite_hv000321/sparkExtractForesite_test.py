import pytest
import shutil
import logging

from pyspark.sql.types import *

import spark.helpers.file_utils as file_utils
import spark.delivery.foresite_hv000321.sparkExtractForesite as foresite

script_path = __file__


def cleanup(spark):
    spark['sqlContext'].sql('DROP TABLE IF EXISTS external_ref_icd10_diagnosis')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS external_ref_ndc_code')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS external_ref_marketplace_to_warehouse')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS external_pharmacyclaims')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS external_enrollmentrecords')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS external_ref_calendar')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS external_mkt_def_calendar')
    spark['sqlContext'].sql('DROP DATABASE IF EXISTS {} CASCADE'.format(foresite.FORESITE_SCHEMA))

    try:
        shutil.rmtree(file_utils.get_abs_path(script_path, './resources/output'))
    except:
        logging.warn("No output dir found, nothing removed.")

    try:
        shutil.rmtree(file_utils.get_abs_path(script_path, './resources/tmp'))
    except:
        logging.warn("No tmp dir found, nothing removed.")


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    spark['sqlContext'].sql('CREATE DATABASE {}'.format(foresite.FORESITE_SCHEMA))

    # load ndc code ref table
    spark['spark'].sparkContext.parallelize([
        ['metformin_ndc', 'metformin', 'metformin_brand']
    ]).toDF(
        StructType([
            StructField('ndc_code', StringType(), True),
            StructField('nonproprietary_name', StringType(), True),
            StructField('proprietary_name', StringType(), True)
        ])
    ).write.saveAsTable("external_ref_ndc_code")

    # load icd10 code ref table
    spark['spark'].sparkContext.parallelize([
        ['1', 'E112', 'header', 'long description'],
        ['2', 'E113', 'header', 'long description']
    ]).toDF(
        StructType([
            StructField('ordernum', StringType(), True),
            StructField('code', StringType(), True),
            StructField('header', StringType(), True),
            StructField('long_description', StringType(), True)
        ])
    ).write.saveAsTable("external_ref_icd10_diagnosis")

    # load ref_marketplace_to_warehouse
    spark['spark'].sparkContext.parallelize([
        ['express_scripts', 'pharmacy', 'esi', '16']
    ]).toDF(
        StructType([
            StructField('warehouse_feed_name', StringType(), True),
            StructField('data_type', StringType(), True),
            StructField('marketplace_feed_name', StringType(), True),
            StructField('marketplace_feed_id', StringType(), True)
        ])
    ).write.saveAsTable("external_ref_marketplace_to_warehouse")

    # load mkt_def_calendar
    spark['spark'].sparkContext.parallelize([
        ['2017-09-08', '2017-09-08', '2017-09-19']
    ]).toDF(
        StructType([
            StructField('delivery_date', StringType(), True),
            StructField('start_date', StringType(), True),
            StructField('end_date', StringType(), True)
        ])
    ).write.saveAsTable("external_mkt_def_calendar")

    # load ref_calendar
    spark['spark'].sparkContext.parallelize([
        ['2017-09-08'],
        ['2017-09-09'],
        ['2017-09-10'],
        ['2017-09-11'],
        ['2017-09-12'],
        ['2017-09-13'],
        ['2017-09-14'],
        ['2017-09-15'],
        ['2017-09-16'],
        ['2017-09-17'],
        ['2017-09-18'],
        ['2017-09-19'],
        ['2017-09-20']
    ]).toDF(
        StructType([
            StructField('calendar_date', StringType(), True)
        ])
    ).createTempView("external_ref_calendar")

    # load pharmacyclaims table
    spark['runner'].run_spark_script('../../../common/pharmacyclaims/sql/pharmacyclaims_common_model_v3.sql', [
        ['table_name', 'external_pharmacyclaims', False],
        ['properties', "PARTITIONED BY (part_provider string, part_processdate string) "
         + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
         + "STORED AS TEXTFILE "
         + "LOCATION '{}' ".format(file_utils.get_abs_path(script_path, './resources/pharmacyclaims/')),
         False],
        ['external', 'EXTERNAL', False]
    ], script_path)

    spark['sqlContext'].sql('msck repair table external_pharmacyclaims')

    # load enrollmentrecords table
    spark['runner'].run_spark_script('../../../common/enrollmentrecords/sql/enrollment_common_model.sql', [
        ['table_name', 'external_enrollmentrecords', False],
        ['properties', "PARTITIONED BY (part_provider string, part_processdate string) "
         + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
         + "STORED AS TEXTFILE "
         + "LOCATION '{}' ".format(file_utils.get_abs_path(script_path, './resources/enrollmentrecords/')),
         False],
        ['external', 'EXTERNAL', False]
    ], script_path)

    spark['sqlContext'].sql('msck repair table external_enrollmentrecords')

    foresite.run(spark['spark'], spark['runner'], '2017-09-08', True)


def test_pharmacyclaims_extract(spark):
    pharmacyclaims_results = spark['sqlContext'].sql('select * from {}.pharmacy_claims_t2d'.format(
        foresite.FORESITE_SCHEMA
    )).collect()

    assert len(pharmacyclaims_results) == 2

def test_enrollment_extract(spark):
    enrollment_results = spark['sqlContext'].sql('select * from {}.enrollment_t2d'.format(
        foresite.FORESITE_SCHEMA
    )).collect()

    assert len(enrollment_results) == 5
