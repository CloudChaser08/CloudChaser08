"""
Set of utility functions for interacting with s3
"""
import os
import re
import subprocess
import boto3
from spark.common.utility import logger


def parse_s3_path(path):
    """
    Split s3 path into bucket and key

    :param path:
    :type path: str
    :return: (bucket, prefix)
    :rtype: (str, str)
    """
    items = path.replace('s3a', 's3')[5:].split('/')
    return items[0], os.path.join(*items[1:])


def delete_file(path):
    """
    Delete file.

    :param path: s3 path (with s3 or s3a prefix)
    :type path: str
    :return:
    :rtype: none
    """
    bucket, key = parse_s3_path(path)
    client = boto3.client('s3')
    client.delete_object(Bucket=bucket, Key=key)


def list_path_pages(bucket, key, recursive=False):
    """
    Get a list of page iterators on s3 for a bucket and key. You won't need this most of the time.

    :param bucket: s3 bucket (ex: salusv)
    :type bucket: str
    :param key: s3 key (ex: warehouse/parquet)
    :type key: str
    :param recursive: whether to list files recursively, default `false`
    :type recursive: bool
    :return: A generator with the page iterator object for the given directory. You can iterate over
     it directly or get a list with `list()`
    :rtype: collections.Iterable[dict]
    """
    paginator = boto3.client('s3').get_paginator('list_objects_v2')

    response_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=key,
        Delimiter="" if recursive else '/',
    )

    for page in response_iterator:
        yield page


def list_folders(path, recursive=False, full_path=False):
    """
    Get a list of folders on s3 for a path.

    :param path: s3 path (with s3 or s3a prefix)
    :type path: str
    :param recursive: whether to list files recursively, default `false`
    :type recursive: bool
    :param full_path: should this return the full path?
    :type full_path: bool
    :return: A generator with the folders that are within the given directory.
    :rtype: collections.Iterable[str]
    """
    bucket, key = parse_s3_path(path)
    for page in list_path_pages(bucket, key, recursive=recursive):
        prefixes = page.get('CommonPrefixes', [])
        for prefix in prefixes:
            prefix_name = prefix['Prefix']
            if prefix_name.endswith('/'):
                yield "s3://{}/{}".format(bucket, prefix_name) if full_path else prefix_name


def list_files(path, keys=None, recursive=False, full_path=False):
    """
    Get a list of files on s3 for a path. If `keys` is empty, just returns path else returns
    a tuple of path, List[expected key values]

    :param path: s3 path (with s3 or s3a prefix)
    :type path: str
    :param keys: any of 'LastModified', 'ETag', 'Size', 'StorageClass', 'Owner'
    :type keys: List[str]
    :param recursive: whether to list files recursively, default `false`
    :type recursive: bool
    :param full_path: whether to list the full s3 path
    :type full_path: bool
    :return: A generator with the files that are within the given directory. You can iterate over
        it directly or get a list with `list()`
    :rtype: Union[collections.Iterable[str], collections.Iterable[str, List[str]]]:
    """
    bucket, key = parse_s3_path(path)
    for page in list_path_pages(bucket, key, recursive=recursive):
        for item in page['Contents']:
            item_path = "s3://{}/{}".format(bucket, item["Key"]) if full_path else item["Key"]
            if keys:
                yield item_path, \
                      [
                          item[expected_key]
                          for expected_key in keys
                          if expected_key in keys
                      ]
            else:
                yield item_path


def get_file_path_size(path, recursive=False):
    """
    Returns the size(in bytes) of file OR all files within a directory on s3.

    :param path: s3 path (with s3 or s3a prefix)
    :type path: str
    :param recursive: whether to list files recursively. Defaults to `False`
    :type recursive: bool
    :return: The size(bytes) files that are within the given directory or given file
    :rtype: int
    """
    return sum([item[1][0] for item in list_files(path, keys=['Size'], recursive=recursive)])


def get_s3_file_count(path, recursive=False):
    """
    Returns the len(number of files) of path OR all files within a directory on s3.

    :param path: s3 path (with s3 or s3a prefix)
    :type path: str
    :param recursive: whether to list files recursively. Defaults to `False`
    :type recursive: bool
    :return: The number of files(count) that are within the given directory or given file
    :rtype: int
    """
    try:
        files_cnt = len([item for item in list_files(path, recursive=recursive)])
    except:
        files_cnt = 0
    return files_cnt


def get_list_of_2c_subdir(s3_path, include_parent_dir=False):
    """
    Returns the subdirectory paths within a directory on s3
       start from number 2 (2nd millenia).

    :param s3_path: s3 path (with s3 or s3a prefix)
    :type s3_path: str
    :param include_parent_dir:
    :type include_parent_dir: bool
    :return: include_parent_dir=False
                ['/2017/01/21/','/2017/02/05/' ]
            include_parent_dir=True
                ['/example/2017/01/21/','/example/2017/02/05/' ]
    :rtype: List[str]
    """
    s3_path_full = s3_path + '/' if s3_path[-1] != '/' else s3_path
    input_path = s3_path_full.replace('s3a:', 's3:')
    dates_full = []
    regex_query = r'2[0-9]{3}/../..'
    try:
        files = list_files(input_path, recursive=True)
        dates = [
            re.findall(regex_query, x)[0]
            for x in files
            if re.search(regex_query, x)
        ]
        dates = set(dates)
        if include_parent_dir:
            dates_full = ["{}{}/".format(s3_path_full, sub) for sub in dates]
        else:
            dates_full = ["{}/".format(sub) for sub in dates]
    except Exception as error:
        logger.log(
            "Unable to collect list of subdir: {}\nError encountered: {}".format(
                s3_path, str(error)
            )
        )

    return sorted(dates_full)


def delete_success_file(s3_path):
    """
    Delete success file from s3

    :param s3_path: s3 path (with s3 or s3a prefix)
    :type s3_path: str
    :return: None
    :rtype: None
    """
    delete_file(s3_path + '_SUCCESS')


def copy_file_from_local(src, dest):
    """
    Stop gap for copying files from local file systion to s3 location

    :param src: Source file location
    :type src: str
    :param dest: Destination location
    :type dest: str
    :return: None
    :rtype: none
    """
    subprocess.check_call(['aws', 's3', 'cp', src, dest])


def has_s3_subdirectories(s3_path):
    """
    Does this s3 path have any sub directories?
    NOTE: 'directories' don't really exist in s3. This checks to see if there are prefixes with
    delimiters '/' beyond the given path.

    :param s3_path: s3 path (with s3 or s3a prefix)
    :type s3_path: str
    :return: True if any next level prefixes
    :rtype: bool
    """
    return any(list_folders(s3_path))
