from pyspark.sql import HiveContext, SparkSession
from spark.helpers.udf.user_defined_functions \
    import get_diagnosis_with_priority, string_set_diff, uniquify
from spark.helpers.udf.post_normalization_cleanup \
    import clean_up_diagnosis_code, obscure_place_of_service, \
    filter_due_to_place_of_service


def init(provider, local=False):
    spark = SparkSession.builder                                       \
                    .master("local[*]" if local else "yarn")           \
                    .appName(provider + " Normalization")              \
                    .config('spark.sql.catalogImplementation', 'hive') \
                    .getOrCreate()

    sqlContext = HiveContext(spark.sparkContext)

    # register udfs
    sqlContext.registerFunction(
        'get_diagnosis_with_priority', get_diagnosis_with_priority
    )
    sqlContext.registerFunction(
        'string_set_diff', string_set_diff
    )
    sqlContext.registerFunction(
        'uniquify', uniquify
    )

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
