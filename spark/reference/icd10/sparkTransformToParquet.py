import argparse

import spark.helpers.postprocessor as postprocessor
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.types import StructType, StructField
from spark.helpers import external_table_loader
from spark.runner import Runner
from spark.spark_setup import init


def run(spark, runner, execution_date):
    PCS_INPUT = 's3://salusv/incoming/reference/icd10/pcs/{}/'.format(execution_date)
    PCS_OUTPUT = 's3://salusv/reference/parquet/icd10/{}/pcs/'.format(execution_date)
    CM_INPUT = 's3://salusv/incoming/reference/icd10/cm/{}/'.format(execution_date)
    CM_OUTPUT = 's3://salusv/reference/parquet/icd10/{}/cm/'.format(execution_date)

    PCS_GRP_INPUT = 's3://salusv/incoming/reference/icd10/pcs-cat/{}/'.format(execution_date)
    PCS_GRP_OUTPUT = 's3://salusv/reference/parquet/icd10/{}/pcs-cat/'.format(execution_date)
    CM_GRP_INPUT = 's3://salusv/incoming/reference/icd10/cm-cat/{}/'.format(execution_date)
    CM_GRP_OUTPUT = 's3://salusv/reference/parquet/icd10/{}/cm-cat/'.format(execution_date)

    pcs = spark.read.text(PCS_INPUT)
    cm = spark.read.text(CM_INPUT)
    """
    Fixed Width Layout for ICD10 Reference Data:

    Position | Length | Contents
    ---------+--------+---------------------------------------------------------------------------------------------------------
        1    |     5  |  Order number, right justified, zero filled.
        6    |     1  |  Blank
        7    |     7  |  ICD-10-CM or ICD-10-PCS code. Dots are not included.
       14    |     1  |  Blank
       15    |     1  |  0 if the code is a header -not valid for HIPAA-covered transactions.  1 if the code is valid. 
       16    |     1  |  Blank
       17    |    60  |  Short description
       77    |     1  |  Blank
       78    |   323  |  Long description
    """

    external_table_loader.load_icd_diag_codes(runner.sqlContext)
    external_table_loader.load_analytics_db_table(runner.sqlContext, "default", "ref_icd10_diagnosis", "ref_icd10_diagnosis")
    external_table_loader.load_analytics_db_table(runner.sqlContext, "default", "ref_icd10_procedure", "ref_icd10_procedure")

    trimmed_pcs = (postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )
        (
        pcs.select(
            pcs.value.substr(1, 5).alias('ordernum'),
            pcs.value.substr(7, 7).alias('code'),
            pcs.value.substr(15, 1).alias('header'),
            pcs.value.substr(17, 60).alias('short_description'),
            pcs.value.substr(78, 323).alias('long_description')
        )
    ))

    current_pcs_codes_table = spark.table('ref_icd10_procedure')

    missing_pcs_codes = current_pcs_codes_table.join(trimmed_pcs, current_pcs_codes_table.code == trimmed_pcs.code,
                                                     'leftanti')

    all_pcs_codes = trimmed_pcs.union(missing_pcs_codes)

    all_pcs_codes.repartition(1).write.parquet(PCS_OUTPUT, mode='overwrite')

    trimmed_cm = (
        postprocessor.compose(
            postprocessor.trimmify,
            postprocessor.nullify
        )
            (
            cm.select(
                cm.value.substr(1, 5).alias('ordernum'),
                cm.value.substr(7, 7).alias('code'),
                cm.value.substr(15, 1).alias('header'),
                cm.value.substr(17, 60).alias('short_description'),
                cm.value.substr(78, 323).alias('long_description')
            )
        )
    )

    current_cm_codes_table = spark.table('ref_icd10_diagnosis')

    missing_cm_codes = current_cm_codes_table.join(trimmed_cm, current_cm_codes_table.code == trimmed_cm.code, 'leftanti')

    all_cm_codes = trimmed_cm.union(missing_cm_codes)

    all_cm_codes.repartition(1).write.parquet(CM_OUTPUT, mode='overwrite')

    # Groups processing

    schema_pcs = StructType([
        StructField("section", StringType()),
        StructField("section_desc", StringType()),
        StructField("position_num", DoubleType()),
        StructField("position_value", StringType()),
        StructField("position_description", StringType())
    ])

    pcs_grp = spark.read.csv(PCS_GRP_INPUT, sep=",", quote='"', header=True, schema=schema_pcs)

    trimmed_pcs_grp = (
        postprocessor.compose(
            postprocessor.trimmify,
            postprocessor.nullify
        )(pcs_grp))

    trimmed_pcs_grp.repartition(1).write.parquet(PCS_GRP_OUTPUT, mode='overwrite')

    schema_cm = StructType([
        StructField("level_num", DoubleType()),
        StructField("start_num", StringType()),
        StructField("stop_num", StringType()),
        StructField("description", StringType()),
    ])

    cm_grp = spark.read.csv(CM_GRP_INPUT, sep=",", quote='"', header=True, schema=schema_cm)
    trimmed_cm_grp = (
        postprocessor.compose(
            postprocessor.trimmify,
            postprocessor.nullify
        )(cm_grp))
    trimmed_cm_grp.repartition(1).write.parquet(CM_GRP_OUTPUT, mode='overwrite')


def main(args):
    spark, sqlContext = init('Reference ICD10')

    runner = Runner(sqlContext)
    run(spark, runner, args.year)

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=str)
    args = parser.parse_args()
    main(args)
