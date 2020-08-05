import os
import spark.common.utility.logger as logger
import spark.datamart.covid19.context as context
import spark.datamart.datamart_util as dmutil
import spark.helpers.normalized_records_unloader as normalized_records_unloader

"""
Publisher module will 
    handle publish external table and covid data status
    Steps:    
        -Create external tables if not exist
        -apply msck repair for partitioned tables
        -refresh table for non-partitioned tables
        -collect datamart max dataset date and publish metrics and datastatus
            Status Objects: Table: dw.mdata   View :   dw.v_mdata
"""

script_path = __file__
dir_path = os.path.dirname(os.path.realpath(__file__))


class Covid19LabPublisher:
    path_prefix_pos = dir_path.find('/spark/target/')
    path_suffix_pos = dir_path.find('/datamart/covid19/labtests')
    sql_path = '{}/spark{}/sql/'.format(dir_path[:path_prefix_pos], dir_path[path_suffix_pos:])

    part_mth = ['part_mth']

    _tables_list = context.LAB_TABLES_LIST
    _datamart_path_full = '{}{}/'.format(context.PRODUCTION, context.LAB_DATAMART_PATH)
    _partitioned_tables_list = context.LAB_PARTTITIONED_TABLES_LIST

    _dw_db = context.DW_SCHEMA

    _mdata_table_location = context.MDATA_TABLE_LOCATION
    _mdata_table = context.MDATA_TABLE
    _mdata_view = context.MDATA_VIEW
    _mdata_db_table = '{}.{}'.format(_dw_db, _mdata_table)
    _mdata_db_view = '{}.{}'.format(_dw_db, _mdata_view)

    _hdfs_output_path = context.HDFS_OUTPUT_PATH
    _mdata_hdfs_location = '{}{}/'.format(_hdfs_output_path, _mdata_table)

    _all_tests_table = context.LAB_FACT_ALL_TESTS
    _all_tests_db_table = '{}.{}'.format(_dw_db, _all_tests_table)

    _covid_tests_table = context.LAB_FACT_COVID_TESTS
    _covid_tests_db_table = '{}.{}'.format(_dw_db, _covid_tests_table)

    def __init__(self,
                 refresh_time_id,
                 requested_list_of_months,
                 output_to_transform_path,
                 asset_typ,
                 data_typ,
                 load_ind,
                 datamart_desc,
                 test=False
                 ):
        self.refresh_time_id = refresh_time_id
        self.requested_list_of_months = requested_list_of_months
        self.output_to_transform_path = output_to_transform_path
        self.asset_typ = asset_typ
        self.data_typ = data_typ
        self.load_ind = load_ind
        self.datamart_desc = datamart_desc
        self.test = test

    def create_table_if_not_and_repair(self, spark, runner):
        """
        :return:
        """

        """
            Create/Repair Tables
                1. Collect covid Table lists [self._tables_list]
                2. Collect covid partitioned Table List [self._partitioned_tables_list]
                3. Create Table of Not Exists
                    3.1 Repair Table (if Partitioned)
                    3.2 Refresh Table 
                4. Done
        """
        logger.log('    -create_table_if_not_and_repair: started')
        for table in self._tables_list:
            table_sql = 'xt_{}_{}.sql'.format(self._dw_db, table)
            table_location = self._datamart_path_full
            if table in [self._all_tests_table, self._covid_tests_table]:
                table_location = table_location.replace('s3:', 's3a:')

            """
                1. Validate given table is exist or not
                2. if table is not exist
                        run external table create SQL file
                3. apply table refresh and get table exist status
            """
            logger.log('    -table re-create if not exist {}.{}: started'.format(self._dw_db, table))
            if not dmutil.has_table(spark, self._dw_db, table):
                runner.run_spark_script(table_sql, [
                    ['table_location', table_location.rstrip('/') + '/']
                ], source_file_path=self.sql_path, return_output=False)
                logger.log('        -table does not exist and re-created')

            runner.run_spark_query('refresh {}.{}'.format(self._dw_db, table))
            table_status = dmutil.has_table(spark, self._dw_db, table)

            repair_status = False
            if table_status:
                repair_status = dmutil.table_repair(
                    spark, runner, self._dw_db, table, table in self._partitioned_tables_list)

            if not repair_status or not table_status:
                logger.log(
                    '        -main: ALERT! Failed to create/msck '
                    'repair on {}.{} and Location is {}'.format(self._dw_db, table, table_location))
        logger.log('    -create_table_if_not_and_repair: completed')

    def update_mdata(self, spark, runner):
        """
        :param spark:
        :param runner:
        :return: Return Status True/ False
        """

        """
            Update Log Status
                a. Create External MDATA Table (If not Exist) on DW Environment
                b. Create Log View (If not Exist) on DW Environment
                c. Insert Log Information
                    1. Collect Covid Start and End Time ID (Data Availability)
                    2. Insert into Log Table with Refresh Time ID
                    3. Refresh Table and View
                d. Done
        """
        logger.log('    -update_mdata: started')

        tbl_sql = "CREATE EXTERNAL TABLE IF NOT EXISTS {mdata_db_table} (" \
                  "asset_typ STRING" \
                  ", data_typ STRING" \
                  ", load_ind STRING" \
                  ", last_refresh_time_id STRING" \
                  ", data_start_id STRING" \
                  ", data_end_id STRING" \
                  ", notes STRING " \
                  ") " \
                  "PARTITIONED BY (part_mth STRING) " \
                  "STORED AS PARQUET " \
                  "LOCATION '{mdata_table_location}'"

        viw_sql = "CREATE OR REPLACE VIEW {mdata_db_view} AS SELECT " \
                  "asset_typ" \
                  ", data_typ" \
                  ", load_ind" \
                  ", last_refresh_time_id" \
                  ", data_start_id" \
                  ", data_end_id" \
                  ", notes " \
                  ", part_mth " \
                  "FROM {mdata_db_table} WHERE " \
                  "     (asset_typ, data_typ, load_ind, last_refresh_time_id) " \
                  "     IN " \
                  "     (" \
                  "         SELECT " \
                  "         asset_typ, data_typ, load_ind, max(last_refresh_time_id) AS last_refresh_time_id " \
                  "         FROM {mdata_db_table}" \
                  "         GROUP BY asset_typ, data_typ, load_ind" \
                  "     )"

        ins_sql = "SELECT " \
                  "'{asset_typ}' AS asset_typ" \
                  ", '{data_typ}' AS data_typ" \
                  ", '{load_ind}' AS load_ind " \
                  ", '{last_refresh_time_id}' AS last_refresh_time_id" \
                  ", '{data_start_id}' AS data_start_id" \
                  ", '{data_end_id}' AS data_end_id " \
                  ", '{datamart_desc}' AS notes " \
                  ", '{part_mth}' AS part_mth"

        logger.log('            -create table/view if not exists')

        if not dmutil.has_table(spark, self._dw_db, self._mdata_table):
            runner.run_spark_query(
                tbl_sql.format(mdata_db_table=self._mdata_db_table
                               , mdata_table_location=self._mdata_table_location))

            logger.log('                -table does not exist and re-created')

        dmutil.table_repair(spark, runner, self._dw_db, self._mdata_table, True)

        runner.run_spark_query(viw_sql.format(
            mdata_db_view=self._mdata_db_view,  mdata_db_table=self._mdata_db_table))

        runner.run_spark_query('refresh {}'.format(self._mdata_db_view))
        logger.log('            -view refresh: completed')

        did_sql = "select " \
                  "date_format(min(date_service), 'yyyy-MM-dd') as start_id" \
                  ", date_format(max(date_service), 'yyyy-MM-dd') as end_id " \
                  "from {} ".format(self._covid_tests_db_table)

        data_id = spark.sql(did_sql).select('start_id', 'end_id').collect()

        data_start_id = str(data_id[0].start_id)
        data_end_id = str(data_id[0].end_id)
        data_part_mth = self.refresh_time_id[:7]

        mdata_temp_df = \
            runner.run_spark_query(
                ins_sql.format(mdata_db_table=self._mdata_db_table
                               , asset_typ=self.asset_typ
                               , data_typ=self.data_typ
                               , load_ind=self.load_ind
                               , last_refresh_time_id=self.refresh_time_id
                               , data_start_id=data_start_id
                               , data_end_id=data_end_id
                               , datamart_desc=self.datamart_desc
                               , part_mth=data_part_mth
                               ), return_output=True).createOrReplaceTempView('_mdata_temp')

        # directly writing into s3
        # spark.table('_mdata_temp').write.parquet(self._mdata_table_location, compression='gzip', mode='append')

        # writing into local then transfer into s3
        spark.table('_mdata_temp').repartition(1).write.parquet(
            self._mdata_hdfs_location, compression='gzip', mode='append', partitionBy=self.part_mth)

        normalized_records_unloader.distcp(self._mdata_table_location, self._mdata_hdfs_location)

        logger.log('            -update mdata : completed')

        dmutil.table_repair(spark, runner, self._dw_db, self._mdata_table, True)

        runner.run_spark_query('refresh {}'.format(self._mdata_db_view))
        logger.log('            -view refresh: completed')

        logger.log('    -update_mdata: completed')

