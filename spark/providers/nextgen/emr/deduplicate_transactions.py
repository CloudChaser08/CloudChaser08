import pyspark.sql.functions as F
from pyspark.sql import Window

def deduplicate(runner):
    old_encounter = runner.sqlContext.table('old_encounter').drop('nextrecorddate')
    new_encounter = runner.sqlContext.table('new_encounter')
    encounter_union = old_encounter.union(new_encounter)
    window = Window.orderBy('recorddate').partitionBy('encounterid', 'reportingenterpriseid')

    encounter_union.withColumn('nextrecorddate', F.lead(F.col('recorddate')).over(window)) \
            .where('nextrecorddate IS NULL').drop('nextrecorddate') \
            .repartition(5000).cache_and_track('encounter_dedup') \
            .createOrReplaceTempView('encounter_dedup')

    old_demographics = runner.sqlContext.table('old_demographics').drop('nextrecorddate')
    new_demographics = runner.sqlContext.table('new_demographics')
    demographics_union = old_demographics.union(new_demographics)
    window = Window.orderBy('recorddate').partitionBy('nextgengroupid', 'reportingenterpriseid')

    demographics_union.withColumn('nextrecorddate', F.lead(F.col('recorddate')).over(window)) \
            .repartition(5000).cache_and_track('demographics_local') \
            .createOrReplaceTempView('demographics_local')

    runner.sqlContext.table('demographics_local').count()
