import spark.common.utility.logger as logger
import spark.datamart.covid19_custom.context as context
import spark.datamart.datamart_util as dmutil
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import subprocess
import re
from datetime import datetime, timedelta

"""
Transfer Module will move data from HDFS to S3 Location
The core steps are:
    -create _stage on datamart location and copy all the data from HDFS to S3(stage)
    --Requested Months
        -Copy existing production dataset into Datmart Archive location (if available)
        -Remove existing production dataset (if available)
        -Move data from S3-stage location into S3-prouction location
"""


class Covid19LabTransformer:
    _hdfs_output_path = context.HDFS_OUTPUT_PATH
    _tables_list = context.LAB_TABLES_LIST_CUSTOM
    _partitioned_tables_list = context.LAB_PARTTITIONED_TABLES_LIST_CUSTOM
    _pat_mth_pattern = context.PART_MTH_PATTERN
    _full_archive_requested_days = context.FULL_ARCHIVE_REQUESTED_DAYS
    _full_archive_suffix_key = context.FULL_ARCHIVE_SUFFIX_KEY
    _incremental_archive_suffix_key = context.INCREMENTAL_ARCHIVE_SUFFIX_KEY

    def __init__(self,
                 requested_list_of_months,
                 output_path,
                 output_stage_path,
                 output_archive_path,
                 archive_timeid,
                 test=False
                 ):
        self.requested_list_of_months = requested_list_of_months
        self.output_path = output_path
        self.output_stage_path = output_stage_path
        self.output_archive_path = output_archive_path
        self.archive_timeid = archive_timeid
        self.test = test

    def part_files_ops(self, action, src_path, target_path=''):
        """
        :param action: refer below; aws s3 commandsk
        :param src_path:  action processing month
        :param target_path: used for all aws action except "aws rm"
        :return:
        """

        """
        This module receive aws s3 <action> and submit it.
            action with recursive mode
        """
        ops_status = True
        try:
            if len(src_path) > 0 and len(target_path) > 0:
                if action in ['sync', 'cp', 'mv']:
                    subprocess.check_call(['aws', 's3', action, '--recursive', src_path, target_path])
                elif action == 'sw':
                    subprocess.check_call(['aws', 's3', 'rm', '--recursive', target_path])
                    subprocess.check_call(['aws', 's3', 'mv', '--recursive', src_path, target_path])
            elif action == 'rm' and len(src_path) > 0:
                subprocess.check_call(['aws', 's3', 'rm', '--recursive', src_path])
        except Exception as e:
            ops_status = False
            logger.log('           -part_files_ops failed to execute command: '
                       'aws s3 {} --recursive {} \n Error: {}'.format(action, src_path, e))
        return ops_status

    def cleanup_stage_if_exists(self):
        """
        delete given datamart stage path
            Check if exists or not
                remove with recursive
        """
        logger.log('            -cleanup_stage_if_exists: started')
        try:
            failed_stage_path = subprocess.check_output([
                'aws', 's3', 'ls', self.output_stage_path]).decode().split("\n")
            if len(failed_stage_path) > 0:
                self.part_files_ops('rm', self.output_stage_path)
        except Exception as e:
            logger.log('           -stage does not exist {} \n Error: {}'.format(self.output_stage_path, e))

        logger.log('        -cleanup_stage_if_exists: completed')

    def trans_local_to_s3stage(self):
        """
        transfer data from local (HDFS) to Datamart S3 Stage location
            -use distcp to transfer with delete on success True
        """
        logger.log("    -trans_local_to_s3stage: Moving files hdfs {}-> s3 {}".format(
            self._hdfs_output_path, self.output_stage_path))
        normalized_records_unloader.distcp(self.output_stage_path, self._hdfs_output_path)
        logger.log('    -trans_local_to_s3stage: completed')

    def trans_local_to_s3prod(self):
        """
        transfer data from local (HDFS) to Datamart S3 Output location
        bypass stage location, directly data transfer into temp/prod
            -use distcp to transfer with delete on success True
        """
        logger.log("    -trans_local_to_s3stage: Moving files hdfs {}-> s3 {}".format(
            self._hdfs_output_path, self.output_path))
        normalized_records_unloader.distcp(self.output_path, self._hdfs_output_path)
        logger.log('    -trans_local_to_s3stage: completed')

    def archive_current_prod(self):
        """
            Archive Current Prod: (FULL)
                1. Collect list of covid tables [self._tables_list]
                2. Get New Archive Time ID 
                3. Read each Table
                    3.1 S3 Copy from S3 Prod to S3-Archive 
                        (For partitioned Tables Read month by month) - [self._partitioned_tables_list]
                        * Month Format YYYY-MM
                4. Done
        """
        logger.log('    -archive_current_prod: started')

        this_day = str((datetime.utcnow() - timedelta(hours=4)).strftime("%A"))
        RUN_FULL_ARCHIVE = this_day.lower() in [fa.lower() for fa in self._full_archive_requested_days]

        if RUN_FULL_ARCHIVE:
            ARCHIVE_TIME_ID = '{}{}/'.format(self.archive_timeid, self._full_archive_suffix_key)
            logger.log('           -archive_current_prod: Requested for Full Archive')
        else:
            ARCHIVE_TIME_ID = '{}{}/'.format(self.archive_timeid, self._incremental_archive_suffix_key)
            logger.log('           -archive_current_prod: Requested for Incremental Archive')

        for table in self._tables_list:
            s3_path = self.output_path + table + '/'
            s3_archive_path = self.output_archive_path + ARCHIVE_TIME_ID + table + '/'

            logger.log(
                '        -archive_current_prod: Copy files s3 {}-> s3 archive {}'.format(s3_path, s3_archive_path))

            b_s3_path_exist = False
            get_s3_path_list = []
            try:
                get_s3_path_list = subprocess.check_output(['aws', 's3', 'ls', s3_path])
                if len(get_s3_path_list) > 0:
                    b_s3_path_exist = True
            except Exception as e:
                logger.log('           -archive_current_prod: '
                           'files does not exist in {}  \n Error: {}'.format(s3_path, e))

            if b_s3_path_exist:
                if table in self._partitioned_tables_list:
                    part_mth_list = [
                        datetime.strptime(re.sub('[^0-9-]+', '', row), '%Y-%m') for row in
                        get_s3_path_list.decode().split("\n")
                        if self._pat_mth_pattern in row
                    ]

                    if len(part_mth_list) > 0:
                        for part_mth in part_mth_list:
                            if RUN_FULL_ARCHIVE or part_mth.strftime('%Y-%m') in self.requested_list_of_months:
                                self.part_files_ops(
                                    'cp', '{}{}{}/'.format(s3_path, self._pat_mth_pattern, part_mth.strftime('%Y-%m'))
                                    , '{}{}{}/'.format(s3_archive_path
                                                       , self._pat_mth_pattern, part_mth.strftime('%Y-%m'))
                                    )
                else:
                    self.part_files_ops('cp', s3_path, s3_archive_path)
            else:
                logger.log('           -archive_current_prod: there is no files to '
                           'archive. process skipped for {}'.format(table))
        logger.log('    -archive_current_prod: completed')

    def move_stage_to_prod(self):
        """
            Move Stage to Prod:
                1. Collect list of covid tables [self._tables_list]
                2. Collect "requested Delta Months" Find S3-Prod path for that month
                    2.1 Delete existing data
                    2.2 Move from S3-Stage to S3-Prod
                (For partitioned Tables Read month by month) - [self._partitioned_tables_list]
                * Month Format YYYY-MM
                4. Done
        """

        logger.log('    -move_stage_to_prod: started')
        for table in self._tables_list:
            s3_stage_path = self.output_stage_path + table + '/'
            s3_path = self.output_path + table + '/'

            if table in self._partitioned_tables_list:
                for part_mth in self.requested_list_of_months:
                    self.part_files_ops('rm', s3_path + self._pat_mth_pattern + part_mth + '/')

                b_s3_stage_path_exist = False
                get_s3_stage_path_list = []
                try:
                    get_s3_stage_path_list = subprocess.check_output(['aws', 's3', 'ls', s3_stage_path])
                    if len(get_s3_stage_path_list) > 0:
                        b_s3_stage_path_exist = True
                except Exception as e:
                    logger.log('           -files does not exist in {} \n Error: {}'.format(s3_stage_path, e))

                if b_s3_stage_path_exist:
                    part_mth_list = [
                        datetime.strptime(re.sub('[^0-9-]+', '', row), '%Y-%m') for row in
                        get_s3_stage_path_list.decode().split("\n")
                        if self._pat_mth_pattern in row
                    ]

                    if len(part_mth_list) > 0:
                        for part_mth in part_mth_list:
                            self.part_files_ops(
                                'sw', s3_stage_path + self._pat_mth_pattern + part_mth.strftime('%Y-%m') + '/',
                                s3_path + self._pat_mth_pattern + part_mth.strftime('%Y-%m') + '/')
                else:
                    logger.log('           -there is no files to archive. process skipped for {}'.format(table))
            else:
                self.part_files_ops('sw', s3_stage_path, s3_path)
        logger.log('    -move_stage_to_prod: completed')

    def table_location_switch(self, spark, runner, db, table, table_location):
        # DISABLED - Not Used
        """
        :param spark:
        :param runner:
        :param db:  DB Name
        :param table: Table Name
        :param table_location: New Location
        :return:
        """
        """
            Table Location Switch
                1. Collect Spark and table details (db, table and table location)
                2. Check if that table is not exists re-create table using prodsql [prodsql/xt_<db>_<Table>.sql]
                    2.1 get table location
                    2.2 alter table command to switch the table
                3. Final Check if table is exists or not
        """
        logger.log('            -table_location_switch: started')
        new_table_location = table_location.rstrip('/') + '/'
        table_exists = dmutil.has_table(spark, db, table)
        if not table_exists:
            kv_list = [
                ['db', db],
                ['table', table],
                ['table_location', new_table_location]
            ]

            runner.run_spark_script('prodsql/xt_{}_{}.sql'.format(db, table), kv_list, return_output=False)
            logger.log(' -table_switch: table does not exist and re-created')
        else:
            runner.run_spark_query("alter table {}.{} set location '{}'".format(db, table, new_table_location))

        runner.run_spark_query('msck repair table {}.{}'.format(db, table))
        logger.log('           -table_location_switch: Completed')


