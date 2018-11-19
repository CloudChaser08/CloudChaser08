import spark.helpers.schema_enforcer as schema_enforcer
from spark.common.medicalclaims_common_model import schema_v7 as med_schema
from spark.common.pharmacyclaims_common_model_v6 import schema as pharma_schema
from datetime import timedelta
import pyspark.sql.functions as F

def prepare(runner, hvids, start_dt, is_prod=False):
    for table_name in ['hvm_emr_diag', 'hvm_emr_enc', 'hvm_emr_medctn', 'hvm_emr_proc']:
        # Because this is EMR data, we are going back an additional year in the
        # partitions. This will be cut down to the the appropriate date ranges
        # in the extract scripts
        df = runner.sqlContext.table('dw.' + table_name) \
            .where(F.col('part_mth') >= (start_dt - timedelta(days=366))) \
            .join(hvids, 'hvid', 'left') \
            .where(hvids['hvid'].isNotNull())

        # Humana has not yet approved this for production, so null it out

        # Humana requested that these claims would not be included in UAT
        # responses for now either
        df = runner.sqlContext.createDataFrame([], df.schema)

        df = df[df.part_hvm_vdr_feed_id.isin(*SUPPLIERS)]

        df.cache() \
            .createOrReplaceTempView(table_name)

        runner.sqlContext.table(table_name).count()

    runner.run_all_spark_scripts()
    df = schema_enforcer.apply_schema(
        runner.sqlContext.table('synthetic_medicalclaims').repartition(100),
        med_schema,
        columns_to_keep=['part_provider', 'part_processdate']
    ).checkpoint()

    df.createOrReplaceTempView('synthetic_medicalclaims')
    df.count()

    df = schema_enforcer.apply_schema(
        runner.sqlContext.table('synthetic_pharmacyclaims').repartition(100),
        pharma_schema,
        columns_to_keep=['part_provider', 'part_processdate']
    ).checkpoint()

    df.createOrReplaceTempView('synthetic_pharmacyclaims')
    df.count()


SUPPLIERS = [
    '25',
    '35'
]
