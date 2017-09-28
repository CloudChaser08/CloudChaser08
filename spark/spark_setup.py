from pyspark.sql import SQLContext, SparkSession
from spark.helpers.udf.post_normalization_cleanup import *
from spark.helpers.udf.general_helpers import *
from spark.helpers.udf.medicalclaims_helpers import *
import spark.helpers.file_utils as file_utils


def init(provider, local=False):
    spark = SparkSession.builder                                              \
                    .master("local[*]" if local else "yarn")                  \
                    .appName(provider + " Normalization")                     \
                    .config('spark.sql.catalogImplementation', 'hive')        \
                    .config('spark.sql.crossJoin.enabled', 'true')            \
                    .config('spark.driver.extraClassPath', '{}:{}'.format(
                        file_utils.get_abs_path(
                            __file__,
                            'common/json-serde-1.3.7-jar-with-dependencies.jar'
                        ),
                        file_utils.get_abs_path(
                            __file__,
                            'common/HiveJDBC41.jar'
                        )
                    ))                                                        \
                    .getOrCreate()

    sqlContext = SQLContext(spark.sparkContext)

    if local:
        spark.sparkContext \
             .addPyFile(file_utils.get_abs_path(__file__, 'target/dewey.zip'))

    # register privacy filters
    sqlContext.registerFunction(
        'filter_due_to_place_of_service', filter_due_to_place_of_service
    )
    sqlContext.registerFunction(
        'obscure_place_of_service', obscure_place_of_service
    )
    sqlContext.registerFunction(
        'filter_due_to_inst_type_of_bill', filter_due_to_inst_type_of_bill
    )
    sqlContext.registerFunction(
        'obscure_inst_type_of_bill', obscure_inst_type_of_bill
    )
    sqlContext.registerFunction(
        'clean_up_diagnosis_code', clean_up_diagnosis_code
    )
    sqlContext.registerFunction(
        'clean_up_procedure_code', clean_up_procedure_code
    )
    sqlContext.registerFunction(
        'mask_zip_code', mask_zip_code
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
        'clean_up_numeric_code', clean_up_numeric_code
    )
    sqlContext.registerFunction(
        'mask_zip_code', mask_zip_code
    )

    # helper functions for cleaning up data
    sqlContext.registerFunction(
        'extract_number', extract_number
    )
    sqlContext.registerFunction(
        'cap_date', cap_date
    )
    sqlContext.registerFunction(
        'extract_date', extract_date
    )
    sqlContext.registerFunction(
        'extract_currency', extract_currency
    )
    sqlContext.registerFunction(
        'create_range', create_range
    )
    sqlContext.registerFunction(
        'obfuscate_hvid', obfuscate_hvid
    )
    sqlContext.registerFunction(
        'slightly_obfuscate_hvid', slightly_obfuscate_hvid
    )
    sqlContext.registerFunction(
        'slightly_deobfuscate_hvid', slightly_deobfuscate_hvid
    )
    sqlContext.registerFunction(
        'to_json', to_json
    )
    sqlContext.registerFunction(
        'clean_up_freetext', clean_up_freetext
    )

    # helper functions for normalizing medical claims
    sqlContext.registerFunction(
        'get_diagnosis_with_priority', get_diagnosis_with_priority
    )
    sqlContext.registerFunction(
        'uniquify', uniquify
    )
    sqlContext.registerFunction(
        'string_set_diff', string_set_diff
    )

    return spark, sqlContext
