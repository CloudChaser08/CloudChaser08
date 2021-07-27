import argparse
import spark.providers.inovalon.medicalclaims.transactional_schemas_v1 as historic_source_table_schemas
import spark.providers.inovalon.medicalclaims.transactional_schemas_v2 as jan_feb_2020_schemas
import spark.providers.inovalon.medicalclaims.transactional_schemas_v3 as mar_2020_schemas
import spark.providers.inovalon.medicalclaims.transactional_schemas_v4 as full_hist_restate_schemas

from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims import schemas as medicalclaims_schemas
import pyspark.sql.functions as FN
import spark.common.utility.logger as logger
import subprocess
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
from datetime import datetime
from dateutil.relativedelta import relativedelta

from pyspark.sql.functions import lit

v_cutoff_date = '2021-02-01'

if __name__ == "__main__":
    # This script has a custom run.sh -> inovalon_run.sh to account memory requirements

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'inovalon'
    output_table_names_to_schemas = {
        'inovalon_25_dx_norm_final': medicalclaims_schemas['schema_v10'],
    }
    provider_partition_name = provider_name

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    parser.add_argument('--skip_filter_duplicates', default=False, action='store_true')

    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test
    skip_filter_duplicates = args.skip_filter_duplicates
    tmp_location = '/tmp/reference/'
    # npi temp locations
    npi_stage_temp_location = '/tmp/inovalon_npi_stage/'
    npi_temp_location = '/tmp/inovalon_w_npi/'

    for this_location in [tmp_location, npi_stage_temp_location, npi_temp_location]:
        subprocess.check_call(['hadoop', 'fs', '-rm', '-r', '-f', this_location])
        subprocess.check_call(['hadoop', 'fs', '-mkdir', this_location])

    ref_claims_location = 's3://salusv/reference/inovalon/medicalclaims/claims/'

    # the vendor sent a different schema for the following dates
    is_schema_v2 = date_input in ['2020-03-03', '2020-03-04']
    is_schema_v3 = date_input == '2020-03-25'
    is_schema_v1 = \
        not is_schema_v2 and not is_schema_v3 \
        and datetime.strptime(date_input, '%Y-%m-%d').date() < datetime.strptime(v_cutoff_date, '%Y-%m-%d').date()

    if is_schema_v2:
        logger.log('Using the Jan/Feb 2020 refresh schema (v2)')
        source_table_schema = jan_feb_2020_schemas
    elif is_schema_v3:
        logger.log('Using the Mar 2020 refresh schema (v3)')
        source_table_schema = mar_2020_schemas
    elif is_schema_v1:
        logger.log('Using the historic schema (v1)')
        source_table_schema = historic_source_table_schemas
    else:
        logger.log('Using the future restate schema (v4)')
        source_table_schema = full_hist_restate_schemas

    # non-historic runs on inovalon are stable if run in chunks. This routine chunks the data by
    # looking at the last 2 characters of claimuid in clams and claimcode
    chunks = ['00', '20', '40', '60', '80']
    for chunk in chunks:
        # Create and run driver
        driver = MarketplaceDriver(
            provider_name,
            provider_partition_name,
            source_table_schema,
            output_table_names_to_schemas,
            date_input,
            end_to_end_test,
            load_date_explode=False,
            unload_partition_count=200,
            vdr_feed_id=176,
            use_ref_gen_values=True,
            output_to_transform_path=True  # Inform James Barker when changing
        )

        conf_parameters = {
            'spark.executor.memoryOverhead': 4096,
            'spark.driver.memoryOverhead': 4096
        }
        driver.init_spark_context(conf_parameters=conf_parameters)

        # create a range to compare the last 2 characters of claim and claimcode against
        # example: 0-32, 33-65, 66-99
        top = int(chunk) + 20
        rng = [str(i).zfill(2) for i in range(int(chunk), top)]
        logger.log('running: ' + str(rng))

        driver.load()
        logger.log('Chunking input tables')

        if is_schema_v2:
            logger.log('Adding missing Jan/Feb 2020 columns')
            clm = driver.spark.table('clm')\
                .withColumn('billedamount', lit('')) \
                .withColumn('allowedamount', lit('')) \
                .withColumn('copayamount', lit('')) \
                .withColumn('costamount', lit('')) \
                .withColumn('paidamount', lit(''))
        elif is_schema_v3:
            logger.log('Adding missing Mar 2020 columns')
            clm = driver.spark.table('clm')\
                .withColumn('billedamount', lit('')) \
                .withColumn('allowedamount', lit('')) \
                .withColumn('copayamount', lit('')) \
                .withColumn('costamount', lit(''))
        else:
            clm = driver.spark.table('clm')

        clm = clm.filter(FN.substring(clm.claimuid, -2, 2).isin(rng))

        ccd = driver.spark.table('ccd')
        ccd = ccd.filter(FN.substring(ccd.claimuid, -2, 2).isin(rng))

        # Inovalon sends duplicate claims from one package to the next. We have to dedup the claims
        # when ingesting. Run with the skip_filter_duplicates argument to skip
        if not skip_filter_duplicates:
            # load reference locations
            logger.log('Loading reference claims table')
            ref = driver.spark.read.parquet(ref_claims_location).createOrReplaceTempView('ref')

            # filter out any claims that already exist in the reference location
            logger.log('Filtering input tables')
            clm.createOrReplaceTempView('clm')
            ccd.createOrReplaceTempView('ccd')
            filter_query = '''SELECT {tbl}.* FROM {tbl} 
                LEFT JOIN ref 
                ON RIGHT({tbl}.claimuid, 2) = ref.last_char_claim_id and {tbl}.claimuid = ref.claimuid
                WHERE ref.claimuid is NULL'''
            clm = driver.spark.sql(filter_query.format(tbl='clm'))
            ccd = driver.spark.sql(filter_query.format(tbl='ccd'))

        logger.log('Timmify/Nullify the filtered tables')
        clm = (postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(clm)
               .cache_and_track(clm)).createOrReplaceTempView('clm')
        ccd = (postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(ccd)
               .cache_and_track(ccd)).createOrReplaceTempView('ccd')

        logger.log('Filter payloads')
        relevant_payload_fields = [
            'age', 'claimid', 'gender', 'hvid', 'state', 'threedigitzip', 'yearofbirth'
        ]

        driver.spark.table('matching_payload').select(*relevant_payload_fields).cache_and_track(
            'matching_payload').createOrReplaceTempView('matching_payload')

        logger.log('Running the normalization SQL scripts')
        logger.log(chunk)
        driver.runner.run_all_spark_scripts([
            ['VDR_FILE_DT', str(driver.date_input), False],
            ['AVAILABLE_START_DATE', driver.available_start_date, False],
            ['EARLIEST_SERVICE_DATE', driver.earliest_service_date, False],
            ['CHUNK', chunk, False]],
            directory_path=driver.provider_directory_path
        )

        driver.save_to_disk()

        # save transactions to disk. They will be added to the s3 reference location
        logger.log('Saving filtered input tables to hdfs')
        query = "SELECT distinct " \
                "clm.claimuid, RIGHT(clm.claimuid, 2) as last_char_claim_id, '{}' as date_input FROM clm" \
            .format(date_input)

        partition = ['date_input', 'last_char_claim_id']
        driver.spark.sql(query).repartition(100).write \
            .parquet(tmp_location, partitionBy=partition, compression='gzip', mode="append")

        driver.stop_spark()
        driver.log_run()
        driver.copy_to_output_path()

        # Copy claims to reference location
        logger.log("Writing claims to the reference location for future duplication checking")
        normalized_records_unloader.distcp(ref_claims_location, src=tmp_location)
