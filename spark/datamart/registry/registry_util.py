import pyspark.sql.functions as FN
import spark.common.utility.logger as logger

registry_cohort = 's3://salusv/warehouse/datamart/registry/_mom_cohort/part_provider={}/'
mp_clmns = ['age', 'claimid', 'gender', 'hvid', 'state', 'threedigitzip', 'yearofbirth']


def get_mom_cohort_filtered_mp(spark, provider_partition_name, mp_tbl='matching_payload'):
    # Cohort processing
    registry_loc = registry_cohort.format(provider_partition_name)
    logger.log('    -Collecting registry cohort from {}'.format(registry_loc))
    recent_cohort_created_dt = spark.read.parquet(registry_loc).agg({'created_date': 'max'}).collect()[0][0]

    if not recent_cohort_created_dt:
        raise Exception("Alert!!!! Registry cohort does not exist")

    registry_loc = registry_loc + 'created_date={}/'.format(recent_cohort_created_dt)
    logger.log('    -Loading recent registry cohort from {}'.format(registry_loc))

    registry_df_out = spark.read.parquet(registry_loc).select(FN.col("hvid")).distinct().repartition(1)

    registry_df_cnt = registry_df_out.count()

    if registry_df_cnt > 0:
        logger.log('    -Number of hvids from cohort table {}'.format(str(registry_df_cnt)))
    else:
        raise Exception("Alert!!!! Registry cohort is empty or hvids are null")

    # matching payload processing
    logger.log('    -Filtering {} table from registry cohort'.format(mp_tbl))
    mp_df = spark.table(mp_tbl)
    filtered_mp_df_out = mp_df.join(registry_df_out, ['hvid']) \
        .select(*[mp_df[c] for c in mp_df.columns if c.lower() in mp_clmns]).repartition(10)

    mp_df_cnt = filtered_mp_df_out.count()

    if mp_df_cnt > 0:
        logger.log('    -Number of records from matching payload table {}'.format(str(mp_df_cnt)))
    else:
        raise Exception("Alert!!!! Registry matching payload table is empty or invalid")

    return registry_df_out, filtered_mp_df_out
