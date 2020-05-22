from pyspark import SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import ArrayType, StringType, DateType, MapType, IntegerType
from spark.helpers.udf.post_normalization_cleanup import *
from spark.helpers.udf.general_helpers import *
from spark.helpers.udf.medicalclaims_helpers import *
import spark.helpers.file_utils as file_utils
from spark.common.utility import logger


JSON_SERDE_JAR_PATH = \
        file_utils.get_abs_path(
            __file__,
            'common/json-serde-1.3.7-jar-with-dependencies.jar'
        )

HIVE_JDBC_JAR_PATH = \
        file_utils.get_abs_path(
            __file__,
            'common/HiveJDBC41.jar'
        )

DEFAULT_SPARK_PARAMETERS = {
    'spark.sql.catalogImplementation': 'hive',
    'spark.sql.crossJoin.enabled': 'true',
    'spark.driver.extraClassPath': "{}:{}".format(JSON_SERDE_JAR_PATH, HIVE_JDBC_JAR_PATH)
}


def init(provider, local=False, conf_parameters=None):
    if conf_parameters:
        # This is a bad way to combine the dicts as it makes DEFAULT_SPARK_PARAMETERS
        # mutable. We should be using the ** syntax instead, but that's not
        # supported by the Python version the cluster uses.
        DEFAULT_SPARK_PARAMETERS.update(conf_parameters)

    parameters = DEFAULT_SPARK_PARAMETERS

    formatted_parameters = [(k, v) for k, v in parameters.items()]

    spark_conf = \
            SparkConf() \
            .setMaster("local[*]" if local else "yarn") \
            .setAppName(provider + " Normalization") \
            .setAll(formatted_parameters)

    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    sqlContext = SQLContext(spark.sparkContext)

    if local:
        spark.sparkContext \
             .addPyFile(file_utils.get_abs_path(__file__, 'target/dewey.zip'))

    spark.sparkContext.setCheckpointDir('/tmp/checkpoint/')

    logger.log_spark_state()

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
        'clean_up_loinc_code', clean_up_loinc_code
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
        'is_int', is_int
    )
    sqlContext.registerFunction(
        'clean_up_numeric_code', clean_up_numeric_code
    )
    sqlContext.registerFunction(
        'clean_up_alphanumeric_code', clean_up_alphanumeric_code
    )
    sqlContext.registerFunction(
        'mask_zip_code', mask_zip_code
    )

    # helper functions for cleaning up data
    sqlContext.registerFunction(
        'extract_number', extract_number
    )
    sqlContext.registerFunction(
        'convert_value', convert_value
    )
    sqlContext.registerFunction(
        'cap_date', cap_date, DateType()
    )
    sqlContext.registerFunction(
        'extract_date', extract_date, DateType()
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
        'clean_up_gender', clean_up_gender
    )
    sqlContext.registerFunction(
        'validate_age', validate_age
    )
    sqlContext.registerFunction(
        'validate_state_code', validate_state_code
    )
    sqlContext.registerFunction(
        'clean_up_npi_code', clean_up_npi_code
    )
    sqlContext.registerFunction(
        'slightly_obfuscate_hvid', slightly_obfuscate_hvid
    )
    sqlContext.registerFunction(
        'slightly_deobfuscate_hvid', slightly_deobfuscate_hvid
    )
    sqlContext.registerFunction(
        'obfuscate_candidate_hvids', obfuscate_candidate_hvids
    )
    sqlContext.registerFunction(
        'to_json', to_json
    )
    sqlContext.registerFunction(
        'clean_up_freetext', clean_up_freetext
    )
    sqlContext.registerFunction(
        'densify_scalar_array', densify_scalar_array, ArrayType(StringType())
    )
    sqlContext.registerFunction(
        'densify_2d_array', densify_2d_array, ArrayType(ArrayType(StringType()))
    )
    sqlContext.registerFunction(
        'densify_2d_array_by_key', densify_2d_array_by_key, ArrayType(ArrayType(StringType()))
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

    sqlContext.registerFunction(
        'clean_up_vital_sign', clean_up_vital_sign
    )

    spark.udf.register('find_descendants_recursively', find_descendants_recursively,
                       MapType(IntegerType(), ArrayType(IntegerType())))
    return spark, sqlContext
