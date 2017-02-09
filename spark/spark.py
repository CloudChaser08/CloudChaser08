from pyspark.sql import HiveContext, SparkSession
from helpers.udf.post_normalization_cleanup \
  import clean_up_diagnosis_code, obscure_place_of_service, \
  filter_due_to_place_of_service


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

    return spark, sqlContext
