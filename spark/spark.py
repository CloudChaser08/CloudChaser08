from pyspark.sql import HiveContext, SparkSession
from helpers.udf.general_helpers \
  import extract_number, extract_date
from helpers.udf.post_normalization_cleanup import *
from helpers.udf.date_functions import *


def init(provider, local=False):
    spark = SparkSession.builder                                              \
                    .master("local[*]" if local else "yarn")                  \
                    .appName(provider + " Normalization")                     \
                    .config('spark.sql.catalogImplementation', 'hive')        \
                    .config('spark.sql.crossJoin.enabled', 'true')            \
                    .getOrCreate()

    sqlContext = HiveContext(spark.sparkContext)

    # register privacy filters
    sqlContext.registerFunction(
        'filter_due_to_place_of_service', filter_due_to_place_of_service
    )
    sqlContext.registerFunction(
        'obscure_place_of_service', obscure_place_of_service
    )
    sqlContext.registerFunction(
        'clean_up_diagnosis_code', clean_up_diagnosis_code
    )
    sqlContext.registerFunction(
        'clean_up_procedure_code', clean_up_procedure_code
    )
    sqlContext.registerFunction(
        'clean_up_ndc_code', clean_up_ndc_code
    )
    sqlContext.registerFunction(
        'scrub_discharge_status', scrub_discharge_status
    )
    sqlContext.registerFunction(
        'nullify_drg_blacklist', nullify_drg_blacklist
    )
    sqlContext.registerFunction(
        'cap_age', cap_age
    )
    sqlContext.registerFunction(
        'cap_year_of_birth', cap_year_of_birth
    )
    sqlContext.registerFunction(
        'yyyyMMdd_to_date', yyyyMMdd_to_date
    )

    # helper functions for cleaning up data
    sqlContext.registerFunction(
        'extract_number', extract_number
    )
    sqlContext.registerFunction(
        'extract_date', extract_date
    )

    return spark, sqlContext
