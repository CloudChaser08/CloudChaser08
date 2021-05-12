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


def init_udfs(spark):
      # ---- privacy related functions ----
    spark.udf.register('filter_due_to_place_of_service', filter_due_to_place_of_service)
    spark.udf.register('obscure_place_of_service', obscure_place_of_service)
    
    spark.udf.register('filter_due_to_inst_type_of_bill', filter_due_to_inst_type_of_bill)
    spark.udf.register('obscure_inst_type_of_bill', obscure_inst_type_of_bill)
    
    spark.udf.register('clean_up_ndc_code', clean_up_ndc_code)

    spark.udf.register('clean_up_vital_sign', clean_up_vital_sign)
    spark.udf.register('clean_up_diagnosis_code', clean_up_diagnosis_code)
    spark.udf.register('clean_up_procedure_code', clean_up_procedure_code)
    
    spark.udf.register('scrub_discharge_status', scrub_discharge_status)
    
    spark.udf.register('nullify_drg_blacklist', nullify_drg_blacklist)
    
    spark.udf.register('cap_age', cap_age)
    spark.udf.register('cap_year_of_birth', cap_year_of_birth)
    spark.udf.register('mask_zip_code', mask_zip_code)
    
    spark.udf.register('obfuscate_hvid', obfuscate_hvid)
    spark.udf.register('slightly_obfuscate_hvid', slightly_obfuscate_hvid)
    spark.udf.register('slightly_deobfuscate_hvid', slightly_deobfuscate_hvid)
    spark.udf.register('obfuscate_candidate_hvids', obfuscate_candidate_hvids)

    # ---- helper functions ----
    # cleaning
    spark.udf.register('clean_up_gender', clean_up_gender)
    spark.udf.register('clean_up_freetext', clean_up_freetext)
    spark.udf.register('clean_up_npi_code', clean_up_npi_code)
    spark.udf.register('clean_up_loinc_code', clean_up_loinc_code)
    spark.udf.register('clean_up_numeric_code', clean_up_numeric_code)
    spark.udf.register('clean_up_alphanumeric_code', clean_up_alphanumeric_code)

    # extraction
    spark.udf.register('extract_currency', extract_currency)
    spark.udf.register('extract_number', extract_number)
    spark.udf.register('extract_date', extract_date, DateType())

    # conversion
    spark.udf.register('is_int', is_int)
    spark.udf.register('to_json', to_json)
    spark.udf.register('convert_value', convert_value)
    
    # capping
    spark.udf.register('cap_date', cap_date, DateType())
    
    # validation
    spark.udf.register('validate_age', validate_age)
    spark.udf.register('validate_state_code', validate_state_code)
    
    # comma-separated string operations
    spark.udf.register('create_range', create_range)
    
    # colon-separated string operations
    spark.udf.register('uniquify', uniquify)
    spark.udf.register('string_set_diff', string_set_diff)
    
    # array operations
    spark.udf.register('densify_scalar_array', densify_scalar_array, ArrayType(StringType()))
    spark.udf.register('densify_2d_array', densify_2d_array, ArrayType(ArrayType(StringType())))
    spark.udf.register('densify_2d_array_by_key', densify_2d_array_by_key, ArrayType(ArrayType(StringType())))
    
    spark.udf.register(
        'find_descendants_recursively', 
        find_descendants_recursively, 
        MapType(
            IntegerType(), 
            ArrayType(
                IntegerType()
            )
        )
    )

    # normalizing medical claims
    spark.udf.register('get_diagnosis_with_priority', get_diagnosis_with_priority)


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

    init_udfs(spark)

    return spark, sqlContext
