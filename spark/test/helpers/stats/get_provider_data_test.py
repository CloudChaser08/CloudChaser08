import pytest

from pyspark.sql import Row

import spark.helpers.file_utils as file_utils
import spark.helpers.stats.utils as stats_utils


def cleanup(spark):
    spark['sqlContext'].dropTempTable('actual_table')
    spark['sqlContext'].sql('DROP SCHEMA custom_schema CASCADE')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    spark['sqlContext'].sql('''
        CREATE EXTERNAL TABLE IF NOT EXISTS actual_table (
          col string,
          part_provider string
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES(
          "separatorChar" = "|"
        )
        STORED AS TEXTFILE
        LOCATION '{}'
    '''.format(file_utils.get_abs_path(__file__, 'resources/provider_data_test/actual/')))
    spark['sqlContext'].sql('''
        CREATE SCHEMA IF NOT EXISTS custom_schema
    ''')
    spark['sqlContext'].sql('''
        CREATE EXTERNAL TABLE custom_schema.custom_table (
          col string,
          part_provider string
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES(
          "separatorChar" = "|"
        )
        STORED AS TEXTFILE
        LOCATION '{}'
    '''.format(file_utils.get_abs_path(__file__, 'resources/provider_data_test/custom/')))


def test_default(spark):
    assert stats_utils.get_provider_data(spark['sqlContext'], 'actual_table', 'a').count() == 10


def test_custom_table(spark):
    assert stats_utils.get_provider_data(spark['sqlContext'], 'acutal_table', 'a', custom_schema='custom_schema'
                                         , custom_table='custom_table').count() == 5


def test_cleanup(spark):
    cleanup(spark)
