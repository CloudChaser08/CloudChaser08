import spark.helpers.records_loader as records_loader
from pyspark.sql.functions import regexp_extract

# Append HVIDs to the demographics table
def prepare(spark, runner, s3_crosswalk_reference):
    dem = spark.table('new_demographics')
    mat = spark.table('matching_payload') \
            .withColumn('reportingenterpriseid', regexp_extract('input_file_name', 'NG_LSSA_([^_]*)_[^\.]*.txt', 1))
    dem_merged = dem.join(mat, ((dem.nextgengroupid == mat.hvJoinKey)
                & (dem.reportingenterpriseid == mat.reportingenterpriseid)), 'left_outer') \
        .select(*([dem[c] for c in dem.columns] + [mat['hvid']]))

    cross = records_loader \
            .load(runner, s3_crosswalk_reference, CROSSWALK_COLUMNS, 'orc')

    dem_w_hvid = dem_merged.where("hvid is NOT NULL")
    dem_wo_hvid = dem_merged.where("hvid is NULL")
    dem_xwalked = dem_wo_hvid.join(cross, ['nextgengroupid', 'reportingenterpriseid'], 'left_outer') \
        .select(*([demo_wo_hvid[c] for c in dem_wo_hvid.columns if c != 'hvid'] + [cross['hvid']]))

    dem_w_hvid.union(dem_xwalked).createOrReplaceTempView('new_demographics')

CROSSWALK_COLUMNS = [
    'nextgengroupid',
    'reportingenterpriseid',
    'hvid'
]

