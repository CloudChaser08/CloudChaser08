"""
dedup
"""
import pyspark.sql.functions as FN
from pyspark.sql import Window


def deduplicate(runner, test=False):
    old_encounter = runner.sqlContext.table('old_encounter').drop('nextrecorddate')
    new_encounter = runner.sqlContext.table('new_encounter')
    encounter_union = old_encounter.union(new_encounter)
    window = Window.orderBy('recorddate').partitionBy('encounterid', 'reportingenterpriseid')

    encounter_union.withColumn('nextrecorddate', FN.lead(FN.col('recorddate')).over(window))\
        .where('nextrecorddate IS NULL').drop('nextrecorddate')\
        .repartition(1 if test else 5000, 'nextgengroupid').cache_and_track('encounter_dedup')\
        .createOrReplaceTempView('encounter_dedup')

    old_demographics = runner.sqlContext.table('old_demographics').drop('nextrecorddate')
    new_demographics = runner.sqlContext.table('new_demographics')
    demographics_union = old_demographics.union(new_demographics)

    # Remove duplicate demographics information that only differs in the date
    # on which the file was sent to us
    cols1 = demographics_union.columns
    cols1.remove('referencedatetime')
    demographics_union = demographics_union.\
        groupBy(*cols1).agg(FN.max('referencedatetime').alias('referencedatetime'))
    cols1.remove('recorddate')
    cols1.remove('dataset')
    wnd = Window.orderBy('recorddate')\
        .partitionBy('nextgengroupid', 'reportingenterpriseid')
    demographics_union = demographics_union\
        .withColumn('md5', FN.md5(FN.concat_ws('|', *[FN.coalesce(FN.col(c), FN.lit('')) for c in cols1]))) \
        .withColumn('prevmd5', FN.lag(FN.col('md5')).over(wnd)) \
        .where("md5 != prevmd5 OR prevmd5 IS NULL") \
        .select(*[c for c in new_demographics.columns])

    window = Window.orderBy('recorddate').partitionBy('nextgengroupid', 'reportingenterpriseid')

    demographics_union.withColumn('nextrecorddate', FN.lead(FN.col('recorddate')).over(window)) \
        .repartition(1 if test else 5000, 'nextgengroupid') \
        .createOrReplaceTempView('demographics_local')
