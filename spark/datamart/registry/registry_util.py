import pyspark.sql.functions as FN
import spark.common.utility.logger as logger

cohort_loc = 's3://salusv/warehouse/datamart/registry/_mom_cohort/part_provider={}/'
masterset_loc = 's3://salusv/warehouse/datamart/registry/_mom_masterset/part_provider={}/'

mp_clmns = ['age', 'claimid', 'gender', 'hvid', 'state', 'threedigitzip', 'yearofbirth']


def get_mom_masterset(spark, provider_partition_name):
    # Cohort processing
    loc = masterset_loc.format(provider_partition_name)
    logger.log('    -Collecting registry masterset from {}'.format(loc))
    recent_masterset_created_dt = spark.read.parquet(loc).agg({'created_date': 'max'}).collect()[0][0]

    if not recent_masterset_created_dt:
        raise Exception("Alert!!!! Registry masterset does not exist")

    loc = loc + 'created_date={}/'.format(recent_masterset_created_dt)
    logger.log('    -Loading recent registry masterset from {}'.format(loc))

    df_collect = spark.read.parquet(loc).distinct().repartition(1)

    df_cnt = df_collect.count()

    if df_cnt > 0:
        logger.log('    -Number of records from masterset table {}'.format(str(df_cnt)))
    else:
        raise Exception("Alert!!!! Registry masterset is empty or invalid")

    return df_collect


def get_mom_cohort(spark, provider_partition_name):
    # Cohort processing
    loc = cohort_loc.format(provider_partition_name)
    logger.log('    -Collecting registry cohort from {}'.format(loc))
    recent_cohort_created_dt = spark.read.parquet(loc).agg({'created_date': 'max'}).collect()[0][0]

    if not recent_cohort_created_dt:
        raise Exception("Alert!!!! Registry cohort does not exist")

    loc = loc + 'created_date={}/'.format(recent_cohort_created_dt)
    logger.log('    -Loading recent registry cohort from {}'.format(loc))

    df_collect = spark.read.parquet(loc).select(FN.col("hvid")).distinct().repartition(1)

    df_cnt = df_collect.count()

    if df_cnt > 0:
        logger.log('    -Number of hvids from cohort table {}'.format(str(df_cnt)))
    else:
        raise Exception("Alert!!!! Registry cohort is empty or hvids are null")

    return df_collect


def get_mom_cohort_filtered_mp(spark, provider_partition_name, mp_tbl='matching_payload'):
    # Cohort processing
    registry_df_out = get_mom_cohort(spark, provider_partition_name)

    # matching payload processing
    logger.log('    -Filtering {} table from registry cohort'.format(mp_tbl))
    mp_df = spark.table(mp_tbl)
    filtered_mp_df_out = mp_df.join(registry_df_out, ['hvid'])\
        .select(*[mp_df[c] for c in mp_df.columns if c.lower() in mp_clmns]).repartition(100)

    mp_df_cnt = filtered_mp_df_out.count()

    if mp_df_cnt > 0:
        logger.log('    -Number of records from matching payload table {}'.format(str(mp_df_cnt)))
    else:
        raise Exception("Alert!!!! Registry matching payload table is empty or invalid")

    return registry_df_out, filtered_mp_df_out
