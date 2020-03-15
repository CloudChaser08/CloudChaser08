import spark.helpers.udf.post_normalization_cleanup as post_normalization_cleanup
from pyspark.sql.functions import col, lit, udf

ANALYTICS_DB_CONN = 'jdbc:hive2://analytics.aws.healthverity.com:10000'
HIVE_DRIVER = 'com.amazon.hive.jdbc41.HS2Driver'
def _get_table_as_df(sqlContext, schema, table_name):
    return sqlContext.read.jdbc(
        url=ANALYTICS_DB_CONN + '/' + schema,
        table=table_name,
        properties={
            "user"   : "hadoop",
            "driver" : HIVE_DRIVER
        }
    )

def load_analytics_db_table(sqlContext, schema, table_name, local_alias):
    _get_table_as_df(sqlContext, schema, table_name).createOrReplaceTempView(local_alias)

# table_d=
# [
#   {"schema" : "<schema>", "table_name" : "<table_name>", "local_alias" : "<local_alias>"}
# ]
def load_analytics_db_tables(sqlContext, table_d):
    for td in table_d:
        load_analytics_db_table(sqlContext, **td)

def load_icd_diag_codes(sqlContext):
    _get_table_as_df(sqlContext, 'default', 'ref_icd9_diagnosis') \
        .select(udf(post_normalization_cleanup.clean_up_diagnosis_code)(col('code'), lit('01'), lit(None)) \
            .alias('code') \
        ) \
        .union(_get_table_as_df(sqlContext, 'default', 'ref_icd10_diagnosis') \
            .select(udf(post_normalization_cleanup.clean_up_diagnosis_code)(col('code'), lit('02'), lit(None)) \
                .alias('code') \
            ) \
        ) \
        .distinct() \
        .cache() \
        .createOrReplaceTempView('icd_diag_codes')
    sqlContext.table('icd_diag_codes').count()

def load_icd_proc_codes(sqlContext):
    _get_table_as_df(sqlContext, 'default', 'ref_icd9_procedure') \
        .select('code') \
        .union(_get_table_as_df(sqlContext, 'default', 'ref_icd10_procedure').select('code')) \
        .cache() \
        .createOrReplaceTempView('icd_proc_codes')
    sqlContext.table('icd_proc_codes').count()

def load_hcpcs_codes(sqlContext):
    _get_table_as_df(sqlContext, 'default', 'ref_hcpcs') \
        .select('hcpc') \
        .distinct() \
        .cache() \
        .createOrReplaceTempView('hcpcs_codes')
    sqlContext.table('hcpcs_codes').count()

def load_cpt_codes(sqlContext):
    _get_table_as_df(sqlContext, 'default', 'ref_cpt') \
        .select('code') \
        .cache() \
        .createOrReplaceTempView('cpt_codes')
    sqlContext.table('cpt_codes').count()

def load_loinc_codes(sqlContext):
    _get_table_as_df(sqlContext, 'default', 'ref_loinc') \
        .select('loinc_num') \
        .cache() \
        .createOrReplaceTempView('loinc_codes')
    sqlContext.table('loinc_codes').count()

def load_ref_gen_ref(sqlContext):
    _get_table_as_df(sqlContext, 'dw', 'ref_gen_ref') \
        .cache() \
        .createOrReplaceTempView('ref_gen_ref')
    sqlContext.table('ref_gen_ref').count()
