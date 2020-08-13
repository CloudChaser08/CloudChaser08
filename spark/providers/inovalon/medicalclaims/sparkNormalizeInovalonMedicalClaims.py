import argparse
import spark.providers.inovalon.medicalclaims.transactional_schemas_v1 as historic_source_table_schemas
import spark.providers.inovalon.medicalclaims.transactional_schemas_v2 as jan_feb_2020_schemas
import spark.providers.inovalon.medicalclaims.transactional_schemas_v3 as mar_2020_schemas

from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims_common_model import schemas as medicalclaims_schemas
import pyspark.sql.functions as F
import spark.common.utility.logger as logger
import subprocess
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor

from pyspark.sql.functions import lit

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

    args = parser.parse_args()
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

    ref_claims_location = 's3://salusv/reference/inovalon/medicalclaims/claims2/'

    # the vendor sent a different schema for the following dates
    is_schema_v2 = date_input in ['2020-03-03', '2020-03-04']
    is_schema_v3 = date_input == '2020-03-25'

    if is_schema_v2:
        logger.log('Using the Jan/Feb 2020 refresh schema (v2)')
        source_table_schema = jan_feb_2020_schemas
    elif is_schema_v3:
        logger.log('Using the Mar 2020 refresh schema (v3)')
        source_table_schema = mar_2020_schemas
    else:
        logger.log('Using the historic schema (v1)')
        source_table_schema = historic_source_table_schemas

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
            output_to_transform_path=False
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

        if is_schema_v2:
            logger.log('Adding missing Jan/Feb 2020 columns')
            clm = clm.withColumn('billedamount', lit('')) \
                .withColumn('allowedamount', lit('')) \
                .withColumn('copayamount', lit('')) \
                .withColumn('costamount', lit('')) \
                .withColumn('paidamount', lit(''))
        elif is_schema_v3:
            logger.log('Adding missing Mar 2020 columns')
            clm = clm.withColumn('billedamount', lit('')) \
                .withColumn('allowedamount', lit('')) \
                .withColumn('copayamount', lit('')) \
                .withColumn('costamount', lit(''))

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
        query = "SELECT distinct clm.claimuid, RIGHT(clm.claimuid, 2) as last_char_claim_id, '{}' as date_input FROM clm" \
            .format(date_input)

        partition = ['date_input', 'last_char_claim_id']
        driver.spark.sql(query).repartition(100).write \
            .parquet(tmp_location, partitionBy=partition, compression='gzip', mode="append")

        """
        Build Inovalon NPI Stage and NPI Table
        """
        # read delta (current chunk)
        npi_delta = driver.spark.read.parquet('/staging/medicalclaims/2018-06-06/').createOrReplaceTempView('npi_delta')

        # warehouse locations
        if driver.output_to_transform_path:
            npi_stage_location = 's3://salusv/reference/inovalon/medicalclaims/inovalon_npi_stage/'
        else:
            npi_stage_location = driver.output_path + 'medicalclaims/inovalon_npi_stage/'

        npi_location = driver.output_path + 'medicalclaims/inovalon_w_npi/'

        # save NPI Stage transactions to disk. They will be added to the s3 NPI Stage location
        logger.log('Saving NPI Stage input tables to hdfs')

        query = """
            SELECT
                medicalclaims.service_line_id
                ,special.pos_cnt
                ,special.tob_cnt
                ,medicalclaims.part_provider
                ,'{part_file_date}' as part_file_date
                ,medicalclaims.part_best_date
            FROM npi_delta medicalclaims
                INNER JOIN
                (
                    SELECT
                        service_line_id
                        ,COUNT(DISTINCT place_of_service_std_id) AS pos_cnt
                        ,COUNT(DISTINCT inst_type_of_bill_std_id)
                            - MAX
                            (
                                CASE
                                    WHEN SUBSTR(inst_type_of_bill_std_id, 1, 1) = 'X'
                                        AND inst_type_of_bill_std_id IS NOT NULL THEN 1
                                ELSE 0 END
                            ) AS tob_cnt
                        ,part_best_date
                    FROM npi_delta
                    GROUP BY service_line_id, part_best_date
                ) special
                ON medicalclaims.part_best_date = special.part_best_date
                    AND medicalclaims.service_line_id = special.service_line_id
            GROUP BY 
                    medicalclaims.service_line_id
                    , special.pos_cnt
                    , special.tob_cnt
                    , medicalclaims.part_provider
                    , medicalclaims.part_best_date
            """.format(part_file_date=date_input)

        partition = ['part_provider', 'part_file_date', 'part_best_date']
        driver.spark.sql(query).repartition(5).write\
            .parquet(npi_stage_temp_location, partitionBy=partition, compression='gzip', mode="append")

        # save NPI transactions to disk. They will be added to the s3 NPI location
        logger.log('Saving NPI input tables to hdfs')
        npi_stage = driver.spark.read.parquet(npi_stage_temp_location).createOrReplaceTempView('npi_stage')

        query = """
            SELECT
                hv_enc_id,
                hvid,
                created,
                model_version,
                data_set,
                data_feed,
                data_vendor,
                patient_gender,
                patient_age,
                patient_year_of_birth,
                patient_zip3,
                patient_state,
                claim_type,
                date_received,
                date_service,
                date_service_end,
                inst_discharge_status_std_id,
                inst_type_of_bill_std_id,
                inst_drg_std_id,
                inst_drg_vendor_id,
                inst_drg_vendor_desc,
                place_of_service_std_id,
                delta.service_line_id,
                diagnosis_code,
                diagnosis_code_qual,
                admit_diagnosis_ind,
                procedure_code,
                procedure_code_qual,
                procedure_units_billed,
                procedure_modifier_1,
                revenue_code,
                line_charge,
                line_allowed,
                prov_rendering_npi,
                prov_billing_npi,
                prov_rendering_vendor_id,
                prov_rendering_name_1,
                prov_rendering_address_1,
                prov_rendering_address_2,
                prov_rendering_city,
                prov_rendering_state,
                prov_rendering_zip,
                prov_billing_vendor_id,
                prov_billing_name_1,
                prov_billing_address_1,
                prov_billing_address_2,
                prov_billing_city,
                prov_billing_state,
                prov_billing_zip,
                logical_delete_reason,
                stg.pos_cnt,
                stg.tob_cnt,
                delta.part_provider,
                delta.part_best_date
            FROM
                npi_delta delta
                INNER JOIN npi_stage stg
                    ON
                        delta.service_line_id = stg.service_line_id
                        AND delta.part_best_date = stg.part_best_date
            """

        partition = ['part_provider', 'part_best_date']
        driver.spark.sql(query).repartition(driver.unload_partition_count).write\
            .parquet(npi_temp_location, partitionBy=partition, compression='gzip', mode="append")

        logger.log("Renaming NPI files")
        for this_location in [npi_stage_temp_location, npi_temp_location]:
            part_files_cmd = [
                'hadoop', 'fs', '-ls', '-R', this_location
            ]
            # add a prefix to part file names
            try:
                part_files = subprocess.check_output(part_files_cmd).decode().strip().split("\n")
            except:
                part_files = []

            driver.spark.sparkContext.parallelize(part_files).repartition(5000).foreach(
                normalized_records_unloader.mk_move_file(date_input, False)
            )

        driver.log_run()
        driver.stop_spark()
        driver.copy_to_output_path()

        # Copy claims to reference location
        logger.log("Writing claims to the reference location for future duplication checking")
        normalized_records_unloader.distcp(ref_claims_location, src=tmp_location)

        # Copy NPI Stage claims to NPI Stage location
        logger.log("Writing claims to the NPI Stage location for DI Request")
        normalized_records_unloader.distcp(npi_stage_location, src=npi_stage_temp_location)

        # Copy NPI claims to NPI location
        logger.log("Writing claims to the NPI location for the New View Model")
        normalized_records_unloader.distcp(npi_location, src=npi_temp_location)
