import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
from datetime import timedelta

def extract(spark, runner, hvids, timestamp, start_dt, end_dt, pharmacy_extract):
    t1 = runner.sqlContext.table('enrollmentrecords')
    t2 = runner.sqlContext.table('dw.ref_vdr_feed')
    t2 = t2[t2.hvm_tile_nm.isin(*SUPPLIERS)]

    date_in_range = spark.createDataFrame(
            [{'cal_date' : ((start_dt + timedelta(days=i)).isoformat())} for i in range((end_dt - start_dt).days + 1)],
        ).select(F.col('cal_date').cast('date').alias('calendar_date')).repartition(1)

    t3 = hvids.crossJoin(date_in_range)
    t4 = pharmacy_extract \
        .select(
            'hvid', F.col('data_feed'),
            F.col('date_service').alias('calendar_date'),
            F.lit('Y').alias('enrolled_flag'),
            F.col('humana_group_id')
        )
    
    # Extract conditions
    ext = t1.join(t2, t1['data_feed'] == t2['hvm_vdr_feed_id'], 'inner') \
        .join(t3, (t1['hvid'] == t3['hvid']) & (t3['calendar_date'].between(t1['date_start'], t1['date_end'])), 'inner') \
        .select(t1['hvid'], 'calendar_date', 'hvm_vdr_feed_id', F.lit('Y').alias('enrolled_flag'), 'humana_group_id')

    # Hashing
    ext = ext.withColumn('hvid', F.md5(F.concat(t1['hvid'], F.lit('hvid'), F.lit('hv000468'), F.lit(repr(timestamp)), F.col('humana_group_id'))))

    # Rename columns
    ext = ext.withColumn('data_feed', F.col('hvm_vdr_feed_id'))

    # Reorder
    return ext.select(*EXTRACT_COLUMNS).union(t4).distinct()

EXTRACT_COLUMNS = [
    'hvid',                     # Hashed
    'data_feed',                # Feed ID#
    'calendar_date',
    'enrolled_flag',
    'humana_group_id'
]

SUPPLIERS = [
    'Private Source 17',
    'Private Source 22',
    'PDX, Inc.'
]
