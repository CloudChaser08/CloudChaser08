"""extract enrollment records"""
import pyspark.sql.functions as FN
from datetime import timedelta


def extract(spark, runner, hvids, timestamp, start_dt, end_dt, pharmacy_extract):
    t1 = runner.sqlContext.table('dw.hvm_enrollment_v06')
    t2 = runner.sqlContext.table('dw.ref_vdr_feed')
    t2 = t2[t2.hvm_vdr_feed_id.isin(*SUPPLIER_FEED_IDS)]

    date_in_range = spark.createDataFrame(
        [{'cal_date': ((start_dt + timedelta(days=i)).isoformat())} for i in range((end_dt - start_dt).days + 1)],
    ).select(FN.col('cal_date').cast('date').alias('calendar_date')).repartition(1)

    t3 = hvids.crossJoin(date_in_range)
    t4 = pharmacy_extract \
        .select(
            'hvid', FN.col('data_feed'),
            FN.col('date_service').alias('calendar_date'),
            FN.lit('Y').alias('enrolled_flag'),
            FN.col('humana_group_id')
        )
    
    # Extract conditions
    ext = t1.join(t2, t1['data_feed'] == t2['hvm_vdr_feed_id'], 'inner').join(
        t3, (t1['hvid'] == t3['hvid']) & (t3['calendar_date'].between(t1['date_start'], t1['date_end'])), 'inner')\
        .select(t1['hvid'], 'calendar_date', 'hvm_vdr_feed_id', FN.lit('Y').alias('enrolled_flag'), 'humana_group_id')

    # Hashing
    ext = ext.withColumn('hvid', FN.md5(FN.concat(t1['hvid'], FN.lit('hvid'), FN.lit('hv005061')
                                                  , FN.lit(repr(timestamp)), FN.col('humana_group_id'))))

    # Rename columns
    ext = ext.withColumn('data_feed', FN.col('hvm_vdr_feed_id'))

    # Reorder
    return ext.select(*EXTRACT_COLUMNS).union(t4).distinct()


EXTRACT_COLUMNS = [
    'hvid',                     # Hashed
    'data_feed',                # Feed ID#
    'calendar_date',
    'enrolled_flag',
    'humana_group_id'
]

# Feed tile names as of 11/09/21
SUPPLIER_FEED_IDS = [
    '61',  # Private Source 17
    '36',  # Private Source 22
    '147', # Private Source 33
    '177', # Private Source 20
    '86'   # Kroger
]
