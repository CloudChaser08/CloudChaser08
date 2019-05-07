import spark.helpers.records_loader as records_loader
import pyspark.sql.functions as F

# Append HVIDs to the demographics table
def prepare(spark, runner, s3_crosswalk_reference):
    dem = spark.table('new_demographics') \
            .withColumn('patientpseudonym', F.lit(None).cast('string')) # NULL out this column. It can contain DeID data
    mat = spark.table('matching_payload') \
            .withColumn('reportingenterpriseid', F.regexp_extract('input_file_name', '(HV|NG)_LSSA_([^_]*)_[^\.]*.txt', 2))
    dem_merged = dem.join(mat, ((dem.nextgengroupid == mat.hvJoinKey)
                & (dem.reportingenterpriseid == mat.reportingenterpriseid)), 'left_outer') \
        .select(*([dem[c] for c in dem.columns] + [mat['hvid']]))

    cross = spark.read.parquet(s3_crosswalk_reference).select(*CROSSWALK_COLUMNS)
    dem_w_hvid = dem_merged.where("hvid is NOT NULL")
    dem_wo_hvid = dem_merged.where("hvid is NULL")
    dem_wo_hvid = (
        dem_wo_hvid
        .withColumn(
            'ngid',
            F.concat_ws('_', F.lit('118'), F.col('reportingenterpriseid'), F.col('nextgengroupid'))
        )
    )
    dem_xwalked = dem_wo_hvid.join(cross, 'ngid', 'left_outer') \
        .select(*([dem_wo_hvid[c] for c in dem_wo_hvid.columns if c != 'hvid' and c != 'ngid'] + [cross['hvid']]))

    dem_xwalked = dem_xwalked.select(*dem_w_hvid.columns)
    dem_w_hvid.union(dem_xwalked).createOrReplaceTempView('new_demographics')

CROSSWALK_COLUMNS = [
    'hvid',
    'ngid'
]

