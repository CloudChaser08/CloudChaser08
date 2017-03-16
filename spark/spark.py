from pyspark.sql import HiveContext, SparkSession
from helpers.udf.post_normalization_cleanup \
  import clean_up_diagnosis_code, obscure_place_of_service, \
  filter_due_to_place_of_service, cap_age, cap_year_of_birth
from helpers.udf.general_helpers \
  import extract_number, extract_date


def init(provider, local=False):
    spark = SparkSession.builder                                       \
                    .master("local[*]" if local else "yarn")           \
                    .appName(provider + " Normalization")              \
                    .config('spark.sql.catalogImplementation', 'hive') \
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
        'cap_age', cap_age
    )
    sqlContext.registerFunction(
        'cap_year_of_birth', cap_year_of_birth
    )

    # helper functions for cleaning up data
    sqlContext.registerFunction(
        'extract_number', extract_number
    )
    sqlContext.registerFunction(
        'extract_date', extract_date
    )

    return spark, sqlContext
