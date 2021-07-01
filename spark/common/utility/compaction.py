"""
    Compact parquet files in given s3 directory.
    - Take retention Backup
    - Apply Compaction in temporary directories
    - Maintain partition structure
    - Rename files to a compacted naming convention in temp folder.
        - YYYY-MM-DD_c_part-{numeration}-{UUID}.c000.gz.parquet
    IF continue_to_source=True:
        - Swap Data (Delete existing and Move new)
        - If failure during data swap, sync original back to source and remove any partial compacted
            file transfers.

    The 'compaction_plan' parameter is of the structure:
        {
                "partition_name": partition,
                "source_path": source_path,
                "backup_path": backup_path,
                "rename_path": rename_path,
                "compact_path": compact_path
        }

    ~ Douglas King <dhking@healthverity.com>
"""
import subprocess
from datetime import datetime
from uuid import uuid4
import os
from urllib.parse import urlparse
import logging
import boto3
from spark.helpers.s3_utils import list_folders, has_s3_subdirectories, list_files
from spark.helpers.file_utils import get_optimal_s3_partition_count

PARQUET_FILE_SIZE = 1024 * 1024 * 250
BACKUP_ROOT = "s3://salusv/warehouse/backup/weekly/"
MAX_DELETE_RANGE = 1000  # Max files allowed in s3.delete_objects()
S3_CLIENT = boto3.client('s3')


def _s3_bulk_delete(s3_urls):
    """
    Deletes files from list of s3 urls in chunks up to MAX_DELETE_RANGE
    WARNING: Does not handle multiple buckets.
    :param s3_urls: List of full s3 paths to delete
    :type s3_urls: list[str]
    :return: None
    """
    for i in range(0, len(s3_urls), MAX_DELETE_RANGE):
        chunk = s3_urls[i:i + MAX_DELETE_RANGE]
        url_parsed_files = [urlparse(s3_file) for s3_file in chunk]
        files_as_dict = [{'Key': k.path.lstrip('/')} for k in url_parsed_files]

        S3_CLIENT.delete_objects(
            Bucket=url_parsed_files[0].netloc,
            Delete={'Objects': files_as_dict}
        )


def _backup_files(compaction_plan):
    """
    Moves files from source to backup directory
    :param compaction_plan: Paths for work - Partition Name, Source, Backup, Rename, Compact
    :type compaction_plan: dict[str, str]
    :return:
    """
    logging.info("...backing up files from %s", compaction_plan['source_path'])
    subprocess.check_output(
        ['aws', 's3', 'cp', '--recursive', '--exclude="*"', '--include="*.parquet"',
         compaction_plan['source_path'], compaction_plan['backup_path']])


def _compact_files(compaction_plan, spark_session):
    """
    Calculates expected partitions and repartitions to compact directory
    :param compaction_plan: Paths for work - Partition Name, Source, Backup, Rename, Compact
    :type compaction_plan: dict[str, str]
    :param spark_session: Spark Session to use for operations
    :type spark_session: pyspark.sql.session.SparkSession
    :return: None
    """
    logging.info("...calculating number of partitions for %s", compaction_plan['backup_path'])
    repartition_count = get_optimal_s3_partition_count(compaction_plan['backup_path'],
                                                       PARQUET_FILE_SIZE)

    logging.info("...will partition into %s files", repartition_count)
    spark_session.read.parquet(compaction_plan['backup_path'])\
        .repartition(repartition_count).write \
        .parquet(compaction_plan['compact_path'], mode='append', compression='gzip')
    logging.info("...compaction written to %s", compaction_plan['compact_path'])


def _confirm_record_counts(compaction_plan, spark_session):
    """
    Compares backup path and compact path to confirm record counts
    :param compaction_plan: Paths for work - Partition Name, Source, Backup, Rename, Compact
    :type compaction_plan: dict[str, str]
    :param spark_session: Spark Session to use for operations
    :type spark_session: pyspark.sql.session.SparkSession
    :return: bool
    """
    logging.info("...comparing counts for %s", compaction_plan['partition_name'])

    backup_count = spark_session.read.parquet(compaction_plan['backup_path']).count()
    logging.info("...Original Row Count: %s", backup_count)
    compact_count = spark_session.read.parquet(compaction_plan['compact_path']).count()
    logging.info("...Compact Row Count: %s", compact_count)

    return backup_count == compact_count


def _rename_files(compaction_plan, file_counter, process_date, file_uuid):
    """
    Renames files to rename_path
    :param compaction_plan: Paths for work - Partition Name, Source, Backup, Rename, Compact
    :type compaction_plan: dict[str, str]
    :param file_counter: Incrementing file counter for part-file names
    :type file_counter: int
    :param process_date: Date for this compaction
    :type process_date: str
    :param file_uuid: Parquet part-file name UUID
    :type file_uuid: str
    :return: running total of files
    :rtype: int
    """
    logging.info("...listing compact files in dir: %s", compaction_plan['compact_path'])
    ls_output = subprocess.Popen(['aws', 's3', 'ls', compaction_plan['compact_path']],
                                 stdout=subprocess.PIPE)
    file_list_out = subprocess.check_output(['grep', '-ioP', 'part-.+.gz.parquet'],
                                            stdin=ls_output.stdout)
    ls_output.wait()

    file_list = [f for f in file_list_out.decode().split("\n") if f]
    for file_name in file_list:
        resolved_file_name = "{process_date}_c_part-{file_counter:05}-{file_uuid}.c000.gz.parquet" \
            .format(process_date=process_date,
                    file_counter=file_counter,
                    file_uuid=file_uuid)
        logging.info("...renaming %s as %s", file_name, resolved_file_name)
        subprocess.check_output(['aws', 's3', 'mv', "{compaction_path}{file_name}"
                                .format(compaction_path=compaction_plan['compact_path'],
                                        file_name=file_name
                                        ),
                                 "{rename_path}{resolved_file_name}".format(
                                     rename_path=compaction_plan['rename_path'],
                                     resolved_file_name=resolved_file_name
                                 )])
        file_counter += 1

    logging.info("...files renamed to %s", compaction_plan['rename_path'])
    return file_counter


def _s3_filtered_files_at_level(s3_url, starts_with: str = '', ends_with: str = ''):
    """
    Non-recursive files in s3 path. Optional inclusive filters for starts_with and ends_with
    :param s3_url: Path to search for files
    :type s3_url: str
    :param starts_with: Prefix for search inclusion
    :type starts_with: str
    :param ends_with: Suffix for search inclusion
    :type ends_with: str
    :return: filenames with given prefix removed.
    :rtype: list[str]
    """
    return list([file_name[len(s3_url):] for file_name in
                 list_files(s3_url, recursive=False, full_path=True)
                 if file_name[len(s3_url):].startswith(starts_with)
                 and file_name[len(s3_url):].endswith(ends_with)
                 ])


def _move_completed_files(compaction_plan):
    """
    Rename_path files get moved to source_path
    :param compaction_plan: Paths for work: Partition Name, Source, Backup, Rename, Compact
    :type compaction_plan: dict[str, str]
    :return: None
    :rtype: None
    """
    # TODO: I think this can be made into a single step with the rename/move being the same
    logging.info("...moving %s files back to source path", compaction_plan['rename_path'])
    subprocess.check_output(
        ['aws', 's3', 'mv', '--recursive', '--exclude="*"', '--include="*.gz.parquet"',
         compaction_plan['rename_path'], compaction_plan['source_path']])


def compact_s3_path(s3_source_path,
                    spark_session,
                    file_uuid=None,
                    process_date=None,
                    continue_to_source=False,
                    ):
    """
    Takes an s3 path and will discover, backup, compact, and rename partitions. Will operate on a
    single path or a path with subfolders.
    :param s3_source_path: Starting s3 url
    :param s3_source_path: str
    :param spark_session: Spark Session to use for operations
    :type spark_session: pyspark.sql.session.SparkSession
    :param file_uuid: Specified UUID for parquet files and backup directory
    :type file_uuid: str
    :param process_date: Specified date for operation, used to prefix parquet files
    :type process_date: str
    :param continue_to_source: Should we delete originals and move compacted files back?
    :type continue_to_source: bool
    :return: None
    :rtype: None
    """
    # TODO: LATER handle recursive partitions
    logging.info("Compacting %s", s3_source_path)
    if not file_uuid:
        file_uuid = str(uuid4())
    logging.info("Run id: %s", file_uuid)
    if not process_date:
        process_date = "{today:%Y-%m-%d}".format(today=datetime.now())
    logging.info("Operation date: %s", process_date)

    compaction_plans = []
    # Grabs last path part from s3_source_path
    path_ext = os.path.dirname(s3_source_path).split('/')[-1]
    logging.info("...Path extension: %s", path_ext)

    # Common path for Backup, Compact, and Rename actions
    operations_root_path = os.path.join(BACKUP_ROOT, file_uuid, path_ext)
    logging.info("...Operations root path: %s", operations_root_path)

    backup_path_root = os.path.join(operations_root_path, 'backup') + '/'
    logging.info("...BACKUP DIR: %s", backup_path_root)

    compact_path_root = os.path.join(operations_root_path, 'compact') + '/'
    logging.info("...COMPACT DIR: %s", compact_path_root)

    rename_path_root = os.path.join(operations_root_path, 'rename') + '/'
    logging.info("...RENAME DIR: %s", rename_path_root)

    # Checks for single directory, or partitions directory
    if has_s3_subdirectories(s3_source_path):
        logging.info('...gathering partitions.')
        best_part_dates = [path[len(s3_source_path):] for path in list_folders(s3_source_path,
                                                                               full_path=True)]

        for partition in sorted(best_part_dates):
            # TODO: Skip paths where optimization is not helpful.
            # TODO: Able to restart with same UUID and pickup where left off.

            source_path = s3_source_path + partition

            backup_path = os.path.join(backup_path_root, partition)
            compact_path = os.path.join(compact_path_root, partition)
            rename_path = os.path.join(rename_path_root, partition)
            compaction_plans.append({
                "partition_name": partition,
                "source_path": source_path,
                "backup_path": backup_path,
                "rename_path": rename_path,
                "compact_path": compact_path
            })
    else:
        # For a single directory it is the partition
        compaction_plans.append({
            "partition_name": path_ext,
            "source_path": s3_source_path,
            "backup_path": backup_path_root,
            "rename_path": rename_path_root,
            "compact_path": compact_path_root
        })

    # TODO: Split these off into asynchronous chunks
    file_counter = 0
    for plan in compaction_plans:
        logging.info("Operating on %s", plan['partition_name'])
        _backup_files(plan)
        _compact_files(plan, spark_session)
        if not _confirm_record_counts(plan, spark_session):
            raise Exception("File counts before and after compaction do not match for %s." %
                            plan['partition_name'])

        logging.info("...record counts match")
        file_counter = _rename_files(plan, file_counter, process_date, file_uuid)

        if continue_to_source:
            logging.info("...continuing to move Delete/Move stages for %s", plan['partition_name'])

            original_file_names = _s3_filtered_files_at_level(plan['backup_path'])
            original_file_full_paths = [os.path.join(plan['source_path'], f) for f in
                                        original_file_names]

            try:
                _s3_bulk_delete(original_file_full_paths)
                logging.info("...%s files written total", file_counter)
                _move_completed_files(plan)

            except Exception as err:
                logging.critical("%s", str(err))

                # Delete failed, restore from backup
                logging.critical("...failed final moves. Syncing files from backup.")
                subprocess.check_output(
                    ['aws', 's3', 'sync', '--exclude="*"', '--include="*.gz.parquet"',
                     plan['backup_path'], plan['source_path']]
                )

                # and remove partial transfers
                logging.critical("...failed final moves. Deleting partially transferred files.")

                files_already_moved = _s3_filtered_files_at_level(
                    plan['backup_path'],
                    starts_with="{process_date}_c_part-".format(process_date=process_date),
                    ends_with="-{file_uuid}.c000.gz.parquet".format(file_uuid=file_uuid))

                files_already_moved_full_paths = [os.path.join(plan['source_path'], f) for f in
                                                  files_already_moved]

                _s3_bulk_delete(files_already_moved_full_paths)

        logging.info("Completed %s", plan['partition_name'])


__all__ = ['compact_s3_path']
