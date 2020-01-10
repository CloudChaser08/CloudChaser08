import subprocess


def list_parquet_files(src):
    """Recursively lists all parquet files that are contained within a given location.

    Args:
        src (str): The HDFS directory whose parquet files will be listed.

        NOTE:
            This is the directory name itself and not a URI.

    Returns:
        files ([str]): The parquet files stored at the given directory.
    """

    list_hdfs_cmd = ['hadoop', 'fs', '-ls', '-R', 'hdfs://{}'.format(src)]
    grep_cmd = ['grep', '-E', '(part\-|c000)']
    awk_cmd = ['awk', '{ print $8 }']

    ls = subprocess.Popen(list_hdfs_cmd, stdout=subprocess.PIPE)
    grep = subprocess.Popen(grep_cmd, stdin=ls.stdout, stdout=subprocess.PIPE)
    result = subprocess.check_output(awk_cmd, stdin=grep.stdout)

    return result.decode().strip().split("\n")

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
