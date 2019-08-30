import pytest
import shutil
import hashlib

from pyspark.sql.types import StructType, StructField, StringType

import spark.helpers.file_utils as file_utils
import spark.delivery.celgene_hv000242.sparkExtractCelgene as celgene

script_path = __file__

pharmacy_results = None
nppes_results = None

def cleanup(spark):
    spark['sqlContext'].sql('DROP TABLE IF EXISTS default.ref_nppes')

    try:
        shutil.rmtree(file_utils.get_abs_path(script_path, './resources/output'))
    except:
        pass

    try:
        shutil.rmtree(file_utils.get_abs_path(script_path, './resources/tmp'))
    except:
        pass


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    # load pharmacyclaims table
    spark['runner'].run_spark_script('../../../common/pharmacyclaims_common_model_v4.sql', [
        ['table_name', 'pharmacyclaims', False],
        ['properties', "PARTITIONED BY (part_provider string, part_best_date string) "
         + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
         + "STORED AS TEXTFILE "
         + "LOCATION '{}' ".format(file_utils.get_abs_path(script_path, './resources/pharmacyclaims/')),
         False],
        ['external', 'EXTERNAL', False],
        ['additional_columns', [], False]
    ], script_path)
    spark['sqlContext'].sql('msck repair table pharmacyclaims')

    # load ref_nppes table
    spark['spark'].sparkContext.parallelize([
        ['prescribing_npi1', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['prescribing_npi2', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['prescribing_npix', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
    ]).toDF(
        StructType([
            StructField('npi', StringType(), True),
            StructField('entity_type', StringType(), True),
            StructField('replacement_npi', StringType(), True),
            StructField('ein', StringType(), True),
            StructField('org_name', StringType(), True),
            StructField('last_name', StringType(), True),
            StructField('first_name', StringType(), True),
            StructField('middle_name', StringType(), True),
            StructField('name_prefix', StringType(), True),
            StructField('name_suffix', StringType(), True),
            StructField('practice_address1', StringType(), True),
            StructField('practice_address2', StringType(), True),
            StructField('practice_city', StringType(), True),
            StructField('practice_state', StringType(), True),
            StructField('practice_postal', StringType(), True),
            StructField('practice_country', StringType(), True),
            StructField('practice_phone', StringType(), True),
            StructField('taxonomy_code_1', StringType(), True)
        ])
    ).write.saveAsTable("default.ref_nppes")

    celgene.run(spark['spark'], spark['runner'], '2017-12-26', True)

    global pharmacy_results, nppes_results
    pharmacy_results = spark['sqlContext'].sql(
        'select * from pharmacyclaims_extract'
    ).collect()
    nppes_results = spark['sqlContext'].sql(
        'select * from nppes_extract'
    ).collect()


def test_pharmacyclaims_extract(spark):

    assert len(pharmacy_results) == 2

    assert sorted([(res.prov_prescribing_npi, res.ndc_code) for res in pharmacy_results]) \
        == [('prescribing_npi1', '59572063106'), ('prescribing_npi2', '59572063255')]

    hash_generator = hashlib.md5()
    hash_generator.update("hvid-1".encode('UTF-8') + "hvidHV242".encode('UTF-8'))
    intended_hvid = hash_generator.hexdigest()

    for res in pharmacy_results:
        assert res.hvid == intended_hvid


def test_nppes_extract(spark):
    assert len(nppes_results) == 2
    assert sorted([res.npi for res in nppes_results]) == ['prescribing_npi1', 'prescribing_npi2']
