import spark.common.utility.logger as logger
import spark.helpers.file_utils as file_utils
import spark.datamart.covid19.context as context
import spark.datamart.datamart_util as dmutil
import os

"""
Builder will construct Covid19 Datamart Facts and References
The Core functionalities are: 
    -extract source data from S3 into HDFS Location
    -run pre-defined Covid19 SQL
        -to build snapshot or reference, history data extract from s3 datamart
    -create external tables on HDFS Location
"""

script_path = __file__
dir_path = os.path.dirname(os.path.realpath(__file__))


class Covid19LabBuilder:
    path_prefix_pos = dir_path.find('/spark/target/')
    path_suffix_pos = dir_path.find('/datamart/covid19/labtests')
    sql_path = '{}/spark{}/sql/'.format(dir_path[:path_prefix_pos], dir_path[path_suffix_pos:])

    _lab_part_provider_list = context.LAB_PART_PROVIDER_LIST
    _lab_big_part_provider_list = context.LAB_BIG_PART_PROVIDER
    _number_of_months_per_extract = context.NUMBER_OF_MONTHS_PER_EXTRACT
    _number_of_months_per_extract_in_hdfs = context.NUMBER_OF_MONTHS_PER_EXTRACT_IN_HDFS

    _lab_db = context.LAB_DW_SCHEMA
    _lab_table = context.LAB_DW_TABLE_NAME
    _lab_is_partitioned_table = context.LAB_DW_TABLE_IS_PARTITIONED
    _lab_result_db = context.LAB_RESULTS_SCHEMA
    _lab_result_table = context.LAB_RESULTS_TABLE_NAME
    _lab_result_is_partitioned_table = context.LAB_RESULTS_TABLE_IS_PARTITIONED
    _lab_partitions = context.LAB_PARTITIONS

    _hdfs_output_path = context.HDFS_OUTPUT_PATH
    _lab_fact_all_tests = '{}{}/'.format(_hdfs_output_path, context.LAB_FACT_ALL_TESTS)
    _lab_fact_covid_tests = '{}{}/'.format(_hdfs_output_path, context.LAB_FACT_COVID_TESTS)
    _lab_fact_covid_cleansed = '{}{}/'.format(_hdfs_output_path, context.LAB_FACT_COVID_CLEANSED)
    _lab_ref_covid = '{}{}/'.format(_hdfs_output_path, context.LAB_REF_COVID)
    _lab_covid_snapshot = '{}{}/'.format(_hdfs_output_path, context.LAB_COVID_SNAPSHOT)
    _lab_covid_sum = '{}{}/'.format(_hdfs_output_path, context.LAB_COVID_SUM)

    _lab_datamart_db = context.LAB_DW_SCHEMA
    _lab_fact_covid_cleansed_table = context.LAB_DW_COVID_CLEANSED_TABLE_NAME
    _lab_fact_covid_cleansed_is_partitioned_table = context.LAB_DW_COVID_CLEANSED_TABLE_IS_PARTITIONED

    def __init__(self,
                 spark,
                 runner,
                 requested_list_of_months,
                 test=False
                 ):
        self.spark = spark
        self.runner = runner
        self.requested_list_of_months = requested_list_of_months
        self.test = test

    def get_nbr_of_buckets(self, part_provider_lower=''):
        """
        :param part_provider_lower: standard part providers
        :return: number of buckets
        """
        if part_provider_lower in context.LAB_BIG_PART_PROVIDER:
            nbr_of_buckets = 100
        elif part_provider_lower in context.LAB_MEDIUM_PART_PROVIDER:
            nbr_of_buckets = 10
        elif part_provider_lower in context.LAB_SMALL_PART_PROVIDER:
            nbr_of_buckets = 1
        else:
            nbr_of_buckets = context.LAB_NBR_OF_BUCKETS
        return nbr_of_buckets

    def build_all_tests(self):
        """
        :return: Status True / False
        """
        """
        Build All Lab Tests:
            1. Get requested Months and Collect pre-configured covid19 Lab providers
            2. Apply MSCK Repair for both input/source tables (external partitioned)
            3. Extract LabTests data from DW and create HDFS table [sql/1_lab_collect_tests.sql]
            4. Extract Results data from aet2575 and create HDFS local table [sql/2_lab_collect_results2575.sql]
            5. Merge LabTests and Results [sql/3_lab_build_all_tests.sql]
            6. Build all Tests HDFS Local Table (partitioned)

        Input/Dependency - Production Tables:
            dw._labtests_nbc
            aet2575.hvrequest_output_002575

        Special Considerations:
            if any providers are configured in LAB_BIG_PART_PROVIDER list
                then extract will be chunk by chunk [NUMBER_OF_MONTHS_PER_EXTRACT]
        Output:
            Load into HDFS
                Later this will be transferred to S3 for Reporting
        """
        logger.log('    -build_all_tests: started')
        file_utils.clean_up_output_hdfs(self._lab_fact_all_tests)

        """
        (Sometimes we lost MSCK repair applied on source tables. So better
        apply msck repair on source tables before start extract)
        
        Apply MSCK repair for the source/input tables to catch-up
        all loaded new dataset
        """
        dmutil.table_repair(self.spark, self.runner, self._lab_db, self._lab_table
                            , self._lab_is_partitioned_table)

        dmutil.table_repair(self.spark, self.runner, self._lab_result_db, self._lab_result_table
                            , self._lab_result_is_partitioned_table)
        """
        msck repair DONE
        """

        for part_provider in self._lab_part_provider_list:
            part_provider_lower = part_provider.lower()
            current_part_mth = []
            for part_mth in self.requested_list_of_months:
                current_part_mth.append(part_mth)
                idx_cnt = self.requested_list_of_months.index(part_mth) + 1
                if (idx_cnt == len(self.requested_list_of_months) or
                        (part_provider_lower in self._lab_big_part_provider_list
                         and idx_cnt % self._number_of_months_per_extract == 0)
                ):
                    list_of_part_mth = "','".join(current_part_mth)
                    del current_part_mth[:]

                    logger.log('        -loading: extracting provider={} part months=[''{}'']'.format(
                        part_provider, list_of_part_mth))

                    nbr_of_buckets = self.get_nbr_of_buckets(part_provider_lower)
                    if not self.test:
                        self.runner.run_spark_script(
                            '1_lab_collect_tests.sql', [
                                ['part_provider', part_provider],
                                ['list_of_part_mth', list_of_part_mth],
                                ['nbr_of_buckets', str(nbr_of_buckets)]
                            ], source_file_path=self.sql_path, return_output=True).repartition(
                            'part_mth', 'claim_bucket_id').createOrReplaceTempView('_temp_lab_tests')

                        self.runner.run_spark_script(
                            '2_lab_collect_results2575.sql', [
                                ['part_provider', part_provider],
                                ['list_of_part_mth', list_of_part_mth],
                                ['nbr_of_buckets', str(nbr_of_buckets)]
                            ], source_file_path=self.sql_path, return_output=True).repartition(
                            'part_mth', 'claim_bucket_id').cache().createOrReplaceTempView('_temp_lab_results')

                    local_all_tests_view = '_temp_lab_all_tests'

                    self.runner.run_spark_script('3_lab_build_all_tests.sql'
                                                 , source_file_path=self.sql_path
                                                 , return_output=True).createOrReplaceTempView(local_all_tests_view)

                    output_table = self.spark.table(local_all_tests_view)
                    logger.log('        -loading: writing provider = {} part months [''{}'']'.format(
                        part_provider, list_of_part_mth))

                    output_table.repartition(
                        'part_mth', 'claim_bucket_id').write.parquet(
                        self._lab_fact_all_tests, compression='gzip', mode='append', partitionBy=self._lab_partitions)

                    self.runner.run_spark_query('drop view {}'.format(local_all_tests_view))

        logger.log('    -build_all_tests: completed')

    def build_covid_tests(self):
        """
        Build Covid Tests:
            1. Read Input from Previous Step generated output "build_all_tests"
            2. Filter/Collect Covid Tests only [/4_lab_build_covid_tests.sql]
            5. Build Covid Tests HDFS Local Table (partitioned)

        Dependency/Input:
            Previous Step "build_all_tests"
        Special Considerations:
            Change parallelism to 500
                input partitioned by part_mth and part_provider (
                    7 months * 5 providers * 20 nbr_of_buckets = ~700)
        Output:
            Load into HDFS
                Later this will be transferred to S3 for Reporting
        """

        logger.log('    -build_covid_tests: started')
        file_utils.clean_up_output_hdfs(self._lab_fact_covid_tests)

        """
        For Optimization and fast data shuffle, 
            Please reset spark parallelism and partitions based on input parameters
            nbr_of_part_parallel is greater than or equal to from the below calculations
                number-of-buckets * requested-list-of-months * number-of-providers
            Ex:
                20 buckets * 7 months refresh * 5 providers = set 700 partitions/parallelism
                20 buckets * 8 months refresh * 5 providers = set 800 partitions/parallelism
                20 buckets * 7 months refresh * 6 providers = set 840 partitions/parallelism
        """
        nbr_of_part_parallel = \
            self.requested_list_of_months * self.get_nbr_of_buckets() * len(self._lab_part_provider_list)
        self.spark.sql("SET spark.default.parallelism={}".format(nbr_of_part_parallel))
        self.spark.sql("SET spark.shuffle.partitions={}".format(nbr_of_part_parallel))

        local_all_tests_view = '_temp_lab_all_tests'
        local_covid_tests_view = '_temp_lab_covid_tests'
        self.spark.read.parquet(self._lab_fact_all_tests).repartition(
            'part_mth', 'part_provider', 'covid19_ind').createOrReplaceTempView(local_all_tests_view)

        current_part_mth = []
        for part_mth in self.requested_list_of_months:
            current_part_mth.append(part_mth)
            idx_cnt = self.requested_list_of_months.index(part_mth) + 1
            if idx_cnt == len(self.requested_list_of_months) \
                    or idx_cnt % self._number_of_months_per_extract_in_hdfs == 0:
                list_of_part_mth = "','".join(current_part_mth)
                del current_part_mth[:]

                logger.log(
                    '        -loading: extracting covid tests part months [''{}'']'.format(list_of_part_mth))

                self.runner.run_spark_script(
                    '4_lab_build_covid_tests.sql', [
                        ['list_of_part_mth', list_of_part_mth],
                        ['claim_bucket_id_0', '0'],
                        ['claim_bucket_id_1', '1'],
                        ['claim_bucket_id_2', '2'],
                        ['claim_bucket_id_3', '4'],
                        ['claim_bucket_id_4', '5'],
                        ['claim_bucket_id_5', '6'],
                        ['claim_bucket_id_6', '6'],
                        ['claim_bucket_id_7', '7'],
                        ['claim_bucket_id_8', '8'],
                        ['claim_bucket_id_9', '9'],
                        ['claim_bucket_id_10', '10'],
                        ['claim_bucket_id_11', '11'],
                        ['claim_bucket_id_12', '12'],
                        ['claim_bucket_id_13', '13'],
                        ['claim_bucket_id_14', '14'],
                        ['claim_bucket_id_15', '15'],
                        ['claim_bucket_id_16', '16'],
                        ['claim_bucket_id_17', '17'],
                        ['claim_bucket_id_18', '18'],
                        ['claim_bucket_id_19', '19']
                    ], source_file_path=self.sql_path, return_output=True).createOrReplaceTempView(
                    local_covid_tests_view)

                output_table = self.spark.table(local_covid_tests_view)

                logger.log(
                    '        -loading: writing covid tests for part months [''{}'']'.format(list_of_part_mth))

                output_table.repartition(10).write.parquet(self._lab_fact_covid_tests
                                                          , compression='gzip', mode='append'
                                                          , partitionBy=self._lab_partitions)

                self.runner.run_spark_query('drop view {}'.format(local_covid_tests_view))

        self.runner.run_spark_query('drop view {}'.format(local_all_tests_view))
        logger.log('    -build_covid_tests: completed')

    def build_covid_tests_cleansed(self):
        """
        Build Covid Tests Cleansed:
            1. Read Input from Previous Step generated output "build_covid_tests"
            2. Generate covid tests cleansed DELTA [5_lab_cleanup_covid_tests.sql]
            3. Collect covid tests cleansed HISTORY (non DELTA Months)
                from dw.lab_fact_covid_cleansed table
                    using [5a_lab_collect_cleansed_covid_history.sql]
            4. covid tests cleansed DELTA union all with HISTORY
            5. Build Covid Tests Cleansed HDFS Local Table (partitioned)

        Dependency/Input:
            Delta from Previous Step "build_covid_tests"
            History from Datamart Production table
        Output:
            Load into HDFS
                Later this will be transferred to S3 for Reporting  (full refresh)
        """
        logger.log('    -build_covid_tests_cleansed: started')
        local_covid_tests_view = '_temp_lab_covid_tests'
        local_covid_tests_cleansed_view = '_temp_lab_covid_tests_cleansed'

        file_utils.clean_up_output_hdfs(self._lab_fact_covid_cleansed)

        logger.log('        -loading: lab covid tests cleansed - reading DELTA')
        self.spark.read.parquet(self._lab_fact_covid_tests).createOrReplaceTempView(local_covid_tests_view)

        covid_tests_cleansed_delta_df = self.runner.run_spark_script(
            '5_lab_cleanup_covid_tests.sql', source_file_path=self.sql_path, return_output=True)

        if dmutil.has_table(self.spark, self._lab_datamart_db, self._lab_fact_covid_cleansed_table):
            list_of_part_mth = "','".join(self.requested_list_of_months)

            logger.log('        -loading: lab covid ref -reading cleansed HISTORY '
                       'data from [{}.{}] except part months  [''{}''] '.format(self._lab_datamart_db
                                                                                , self._lab_fact_covid_cleansed_table
                                                                                , list_of_part_mth))

            dmutil.table_repair(self.spark, self.runner, self._lab_datamart_db, self._lab_fact_covid_cleansed_table
                                , self._lab_fact_covid_cleansed_is_partitioned_table)

            covid_tests_cleansed_master_df = dmutil.df_union_all(
                self.runner.run_spark_script(
                    '5a_lab_collect_cleansed_covid_history.sql', [
                        ['list_of_part_mth', list_of_part_mth]
                    ], source_file_path=self.sql_path, return_output=True).repartition(
                    'part_mth', 'part_provider'),
                covid_tests_cleansed_delta_df)
        else:
            logger.log('        -loading: lab covid ref -there is NO cleansed HISTORY data')
            covid_tests_cleansed_master_df = covid_tests_cleansed_delta_df

        covid_tests_cleansed_master_df.createOrReplaceTempView(local_covid_tests_cleansed_view)

        output_table = self.spark.table(local_covid_tests_cleansed_view)

        logger.log('        -loading: lab covid tests cleansed - writing DELTA')

        output_table.write.parquet(
            self._lab_fact_covid_cleansed, compression='gzip', mode='append', partitionBy=self._lab_partitions)

        # output_table.repartition(
        #     'part_mth', 'part_provider', 'claim_bucket_id').write.parquet(
        #     self._lab_fact_covid_cleansed, compression='gzip', mode='append', partitionBy=self._lab_partitions)

        self.runner.run_spark_query('drop view {}'.format(local_covid_tests_view))
        self.runner.run_spark_query('drop view {}'.format(local_covid_tests_cleansed_view))
        logger.log('    -build_covid_tests_cleansed: completed')

    def build_covid_ref(self):
        """
        Build Covid Ref:
            1. Read Input from Previous Step generated output "build_covid_tests_cleansed"
            2. Generate covid ref using [6_lab_build_covid_ref.sql]
            5. Build Covid Ref HDFS Local Table (non partitioned)

        Dependency/Input:
            Previous Step "build_covid_tests_cleansed"
        Output:
            Load into HDFS
                Later this will be transferred to S3 for Reporting (full refresh)
        """
        logger.log('    -build_covid_ref: started')
        local_covid_ref_view = '_temp_lab_covid_ref'
        local_covid_tests_cleansed_view = '_temp_lab_covid_tests_cleansed'

        file_utils.clean_up_output_hdfs(self._lab_ref_covid)

        logger.log('        -loading: lab covid ref -reading cleansed (DELTA + HISTORY) data')
        covid_tests_cleansed_master_df = self.spark.read.parquet(self._lab_fact_covid_cleansed)
        covid_tests_cleansed_master_df.repartition('part_mth', 'part_provider', 'claim_bucket_id')

        covid_tests_cleansed_master_df.createOrReplaceTempView(local_covid_tests_cleansed_view)

        self.runner.run_spark_script('6_lab_build_covid_ref.sql', source_file_path=self.sql_path
                                     , return_output=True).createOrReplaceTempView(local_covid_ref_view)

        output_table = self.spark.table(local_covid_ref_view)

        logger.log('        -loading: lab covid ref writing')

        output_table.repartition(1).write.parquet(self._lab_ref_covid, compression='gzip', mode='append')

        self.runner.run_spark_query('drop view {}'.format(local_covid_tests_cleansed_view))
        self.runner.run_spark_query('drop view {}'.format(local_covid_ref_view))

        logger.log('    -build_covid_ref: completed')

    def build_covid_snapshot(self):
        """
        Build Covid Ref:
            1. Read/Cache Input from Previous Step generated output "build_covid_tests_cleansed"
            2. Read/Cache Input from Previous Step generated outut "build_covid_ref"
            2. Generate covid Snapshot using [7_lab_build_covid_snapshot.sql]
            5. Build Covid Ref HDFS Local Table (non partitioned)

        Dependency/Input:
            Previous Step "build_covid_tests_cleansed"
        Output:
            Load into HDFS
                Later this will be transferred to S3 for Reporting (full refresh)
        """
        logger.log('    -build_covid_snapshot: started')

        file_utils.clean_up_output_hdfs(self._lab_covid_snapshot)

        local_covid_tests_cleansed_view = '_temp_lab_covid_tests_cleansed'
        local_covid_ref_view = '_temp_lab_covid_ref'
        local_covid_snapshot_view = '_temp_lab_covid_snapshot'

        logger.log('        -loading: lab covid snapshot - reading covid ref')
        covid_ref_df = self.spark.read.parquet(self._lab_ref_covid)

        covid_ref_df.repartition(
            'part_provider', 'test_ordered_name', 'result_name', 'hv_method_flag', 'result_comments', 'result')
        covid_ref_df.cache().createOrReplaceTempView(local_covid_ref_view)

        logger.log('        -loading: lab covid snapshot - reading covid tests cleansed')
        covid_tests_cleansed_master_df = self.spark.read.parquet(self._lab_fact_covid_cleansed)
        covid_tests_cleansed_master_df.repartition(
            'claim_bucket_id', 'date_service', 'part_provider', 'test_ordered_name', 'result_name'
            , 'hv_method_flag', 'result_comments', 'result')
        covid_tests_cleansed_master_df.cache().createOrReplaceTempView(local_covid_tests_cleansed_view)

        self.runner.run_spark_script('7_lab_build_covid_snapshot.sql', [
            ['claim_bucket_id_low_1', '0'], ['claim_bucket_id_up_1', '1'],
            ['claim_bucket_id_low_2', '2'], ['claim_bucket_id_up_2', '3'],
            ['claim_bucket_id_low_3', '4'], ['claim_bucket_id_up_3', '5'],
            ['claim_bucket_id_low_4', '6'], ['claim_bucket_id_up_4', '7'],
            ['claim_bucket_id_low_5', '8'], ['claim_bucket_id_up_5', '9'],
            ['claim_bucket_id_low_6', '10'], ['claim_bucket_id_up_6', '19'],
        ], source_file_path=self.sql_path
                                     , return_output=True).createOrReplaceTempView(local_covid_snapshot_view)

        output_table = self.spark.table(local_covid_snapshot_view)

        # output_table.write.parquet(self._lab_covid_snapshot, compression='gzip', mode='append')
        output_table.repartition(10).write.parquet(self._lab_covid_snapshot, compression='gzip', mode='append')

        covid_tests_cleansed_master_df.unpersist()
        covid_ref_df.unpersist()
        self.runner.run_spark_query('drop view {}'.format(local_covid_tests_cleansed_view))
        self.runner.run_spark_query('drop view {}'.format(local_covid_ref_view))
        self.runner.run_spark_query('drop view {}'.format(local_covid_snapshot_view))
        self.runner.sqlContext.clearCache()

        logger.log('    -build_covid_snapshot: completed')

    def build_covid_sum(self):

        """
        Build Covid Ref:
            1. Read/Cache Input from Previous Step generated output "build_covid_snapshot"
            2. Generate covid Snapshot using [8_lab_build_covid_sum.sql]
            5. Build Covid Ref HDFS Local Table (non partitioned)

        Dependency/Input:
            Previous Step "build_covid_snapshot"
        Output:
            Load into HDFS
                Later this will be transferred to S3 for Reporting (full refresh)
        """
        logger.log('    -build_covid_sum: started')
        file_utils.clean_up_output_hdfs(self._lab_covid_sum)

        local_covid_sum_view = '_temp_lab_covid_sum'
        local_covid_snapshot_view = '_temp_lab_covid_snapshot'

        covid_snapshot_df = \
            self.spark.read.parquet(self._lab_covid_snapshot).repartition(
                'date_service', 'part_provider', 'hv_test_flag')

        covid_snapshot_df.cache().createOrReplaceTempView(local_covid_snapshot_view)

        self.runner.run_spark_script('8_lab_build_covid_sum.sql', source_file_path=self.sql_path
                                , return_output=True).createOrReplaceTempView(local_covid_sum_view)

        output_table = self.spark.table(local_covid_sum_view)

        output_table.repartition(1).write.parquet(self._lab_covid_sum, compression='gzip', mode='append')

        covid_snapshot_df.unpersist()
        self.runner.run_spark_query('drop view {}'.format(local_covid_snapshot_view))
        self.runner.run_spark_query('drop view {}'.format(local_covid_sum_view))
        logger.log('    -build_covid_sum: completed')
