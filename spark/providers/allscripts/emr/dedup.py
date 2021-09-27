from pyspark.sql import functions as f
from pyspark.sql import catalog as c
from pyspark.sql import Window
from spark.runner import Runner
from spark.spark_setup import init

import subprocess

conf_parameters = {
    'spark.executor.memoryOverhead': 4096,
    'spark.driver.memoryOverhead': 4096,
    'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
    'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
    'spark.task.maxFailures': 8,
    'spark.max.executor.failures': 800
}


def dedup_table(df, dup_ids):
    # HV_May12_01
    df = df.withColumn('data_set_nm_dt', f.to_date(df['data_set_nm'].substr(3, 5), 'MMMyy'))

    window = Window.partitionBy(dup_ids).orderBy(
        f.col('data_captr_dt').desc(),
        f.col('data_set_nm_dt').desc(),
        f.col('crt_dt').desc()
    )
    df = df.withColumn(
        'rn',
        f.row_number().over(window)
    ).filter(
        f.col('rn') == 1
    ).drop('rn')

    return df


tables = {
    'lab_result': ['vdr_lab_test_id']
    , 'clinical_observation': ['vdr_clin_obsn_id', 'clin_obsn_substc_cd']
    , 'lab_order': ['vdr_lab_ord_id']
    , 'procedure': ['vdr_proc_id', 'proc_diag_cd']
    , 'medication': ['vdr_medctn_ord_id', 'medctn_alt_substc_cd']
    , 'provider_order': ['vdr_prov_ord_id', 'prov_ord_diag_cd']
    , 'vital_sign': ['vdr_vit_sign_id']
    , 'diagnosis': ['vdr_diag_id', 'diag_cd']
    , 'encounter': ['vdr_enc_id', 'vdr_alt_enc_id']
}

# s3-dist-cp --s3ServerSideEncryption --deleteOnSuccess --src /staging/ --dest
# s3://salusv/warehouse/transformed/allscripts_dedup/
s3_path_template = "s3://salusv/warehouse/transformed/allscripts_restate/emr/2017-08-23/{table}/part_hvm_vdr_feed_id=25/"
s3_path_template_out = "s3://salusv/warehouse/transformed/allscripts_dedup/emr/2017-08-23/{table}/part_hvm_vdr_feed_id=25/"

staging_location = '/staging/'
copy_command_template = 's3-dist-cp --s3ServerSideEncryption --src {src} --dest {dest}'


def dist_cp(src, dest):
    copy_command = [
        's3-dist-cp',
        '-Dmapreduce.job.reduces=2000',
        '--s3ServerSideEncryption',
        '--src',
        src,
        '--dest',
        dest
    ]

    subprocess.call(copy_command)


def rm_staging():
    copy_command = [
        'hadoop',
        'fs',
        '-rm',
        '-r',
        '/staging/'
    ]

    subprocess.call(copy_command)


# for each table
for table, dup_ids in tables.items():
    s3_path_in = s3_path_template.format(table=table)
    s3_path_out = s3_path_template_out.format(table=table)

    context_name = 'Allscripts EMR Dedup {}'.format(table)
    spark, sql_context = init(context_name, False, conf_parameters)
    runner = Runner(sql_context)

    print("------------------------> Step 1 start <----------------------------")
    ### Step 1 -- pull data
    # create table locally
    df = spark.read.parquet(s3_path_in)
    print("{} start count:".format(table), df.count())

    print("------------------------> Step 1 end <----------------------------")

    ### Step 2 -- order by duplicate ids, repartition, export
    print("------------------------> Step 2 start <----------------------------")
    df = dedup_table(df, dup_ids).withColumn(
        'part_mth_r',
        f.concat(
            df['part_mth'],
            (
                f.when(
                    df['part_mth'] == '0_PREDATES_HVM_HISTORY', f.rand() * 200
                ).otherwise(
                    f.rand() * 20
                )
            ).cast('int')
        )
    )

    # repartition and export
    df.repartition(5000, 'part_mth_r').drop('part_mth_r').write.parquet(staging_location,
                                                                        mode='append',
                                                                        compression='gzip',
                                                                        partitionBy='part_mth')
    df = spark.read.parquet(staging_location)
    print("{} end count:".format(table), df.count())
    print("------------------------> Step 2 end <----------------------------")

    ### Step 3 -- output to s3
    spark.stop()
    print("------------------------> Step 3 start <----------------------------")
    dist_cp(staging_location, s3_path_out)
    print("------------------------> Step 3 end <----------------------------")

    rm_staging()
