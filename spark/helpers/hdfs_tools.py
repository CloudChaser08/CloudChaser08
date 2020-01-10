import subprocess


def get_hdfs_file_count(src):
    """Returns the count of all files within a directory on HDFS.
    In order to determine the file count, this function will recursively list out
    all files contained within the source directory and then count them.

    Args:
        src (string): The URI of the HDFS directory.

    Returns:
        int: The number files that are within the given directory.
    """

    file_count = \
            subprocess \
            .check_output(['hadoop fs -ls -R {} | wc -l'.format(src)], shell=True) \
            .decode() \
            .strip() \
            .split('\n')

    return int(file_count[0])
