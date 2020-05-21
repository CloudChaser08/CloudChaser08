import argparse
import spark.providers.inovalon.medicalclaims.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims_common_model import schemas as medicalclaims_schemas
import pyspark.sql.functions as F
import spark.common.utility.logger as logger
import subprocess
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor

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
    parser.add_argument('--chunk', type=str)
    parser.add_argument('--skip_filter_duplicates', default=False, action='store_true')

    args = parser.parse_args()
    date_input = args.date
    end_to_end_test = args.end_to_end_test
    skip_filter_duplicates = args.skip_filter_duplicates
    tmp_location = '/tmp/reference/'
    subprocess.check_call(['hadoop', 'fs', '-rm', '-r', '-f', tmp_location])
    subprocess.check_call(['hadoop', 'fs', '-mkdir', tmp_location])
    ref_claims_location = 's3://salusv/reference/inovalon/medicalclaims/claims2/'

    # non-historic runs on inovalon are stable if run in chunks. This routine chunks the data by
    # looking at the last 2 characters of claimuid in clams and claimcode
    chunks = ['00', '20', '40', '60', '80']
    for chunk in chunks:

        # Create and run driver
        driver = MarketplaceDriver(
            provider_name,
            provider_partition_name,
            source_table_schemas,
            output_table_names_to_schemas,
            date_input,
            end_to_end_test,
            load_date_explode=False,
            unload_partition_count=200,
            vdr_feed_id=176,
            use_ref_gen_values=True
        )
        driver.init_spark_context()

        # create a range to compare the last 2 characters of claim and claimcode against
        # example: 0-32, 33-65, 66-99
        top = int(chunk) + 20
        rng = [str(i).zfill(2) for i in range(int(chunk), top)]
        logger.log('running: ' + str(rng))

        driver.load()

        logger.log('Chunking input tables')
        clm = driver.spark.table('clm')
        clm = clm.filter(F.substring(clm.claimuid, -2, 2).isin(rng))

        ccd = driver.spark.table('ccd')
        ccd = ccd.filter(F.substring(ccd.claimuid, -2, 2).isin(rng))

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
        query = "SELECT distinct clm.claimuid, RIGHT(clm.claimuid, 2) as last_char_claim_id, '{}' as date_input FROM clm" \
            .format(date_input)

        partition = ['date_input', 'last_char_claim_id']
        driver.spark.sql(query).repartition(100).write \
            .parquet(tmp_location, partitionBy=partition, compression='gzip', mode="append")

        driver.log_run()
        driver.stop_spark()
        driver.copy_to_output_path()

        # Copy claims to reference location
        logger.log("Writing claims to the reference location for future duplication checking")
        normalized_records_unloader.distcp(ref_claims_location, src=tmp_location)

