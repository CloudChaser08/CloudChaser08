import spark.helpers.schema_enforcer as schema_enforcer
from spark.common.medicalclaims_common_model import schema_v7 as med_schema
from spark.common.pharmacyclaims_common_model_v6 import schema as pharma_schema

def prepare(runner, hvids, start_dt, end_dt):
    for table_name in ['hvm_emr_diag', 'hvm_emr_enc', 'hvm_emr_medctn', 'hvm_emr_proc']:
        df = runner.sqlContext.table('dw.' + table_name) \
            .where("part_mth >= '2016-04'") \
            .join(hvids, 'hvid', 'left') \
            .where(hvids['hvid'].isNotNull())
        df = df[df.part_hvm_vdr_feed_id.isin(*SUPPLIERS)]

        df.cache() \
            .createOrReplaceTempView(table_name)

        runner.sqlContext.table(table_name).count()

    runner.run_all_spark_scripts()
    df = schema_enforcer.apply_schema(
        runner.sqlContext.table('synthetic_medicalclaims').repartition(100),
        med_schema,
        columns_to_keep=['part_provider', 'part_processdate']
    ).checkpoint().cache()
#    df.write.parquet('/debug/medclaims/')
#    df = runner.sqlContext.read.parquet('/debug/medclaims/')
    df.createOrReplaceTempView('synthetic_medicalclaims')
    df.count()

    df = schema_enforcer.apply_schema(
        runner.sqlContext.table('synthetic_pharmacyclaims').repartition(100),
        pharma_schema,
        columns_to_keep=['part_provider', 'part_processdate']
    ).checkpoint().cache()
#    df.write.parquet('/debug/pharmaclaims/')
#    df = runner.sqlContext.read.parquet('/debug/pharmaclaims/')
    df.createOrReplaceTempView('synthetic_pharmacyclaims')
    df.count()


SUPPLIERS = [
    '25',
    '35'
]
