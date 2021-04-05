import subprocess


def list_parquet_files(src, pattern='(part\-|c000)'):
    """Recursively lists all parquet files that are contained within a given location.

    Args:
        src (str): The HDFS directory whose parquet files will be listed.

        NOTE:
            This is the directory name itself and not a URI.

    Returns:
        files ([str]): The parquet files stored at the given directory.
    """

    list_hdfs_cmd = ['hadoop', 'fs', '-ls', '-R', 'hdfs://{}'.format(src)]
    grep_cmd = ['grep', '-E', pattern]
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
        subprocess.check_output(['hadoop fs -ls -R {} | wc -l'.format(src)], shell=True).decode().strip().split('\n')

    return int(file_count[0])


def get_files_from_hdfs_path(path):
    files = [
        f.split(' ')[-1].strip().split('/')[-1]
        for f in
        str(subprocess.check_output(['hdfs', 'dfs', '-ls', path])).split('\\n')
        if f.split(' ')[-1].startswith('hdfs')
    ]

    return [f.split(' ')[-1].strip().split('/')[-1] for f in files]


def get_hdfs_file_path_size(path):
    """Returns the size(in bytes) of file OR all files within a directory on HDFS.
    Displays sizes of files and directories contained in the
    given directory or the size of a file in case its just a file.
    Args:
        path (string): /staging

    Returns:
        int: The size(bytes) files that are within the given directory or given file
    """

    file_count = \
        subprocess.check_output(['hadoop fs -du -s {}'.format(path)], shell=True).decode().strip().split(' ')

    return int(file_count[0])


def get_s3_file_path_size(path):
    """Returns the size(in bytes) of file OR all files within a directory on s3.
    Displays sizes of files and directories contained in the
    given directory or the size of a file in case its just a file.
    Args:
        path (string): /staging

    Returns:
        int: The size(bytes) files that are within the given directory or given file
    """
    file_count = \
        subprocess.check_output(
            ['aws s3 ls --summarize {}'.format(path.replace('s3a:', 's3:'))], shell=True).decode().strip().split('Total Size: ', 1)

    return int(file_count[1])
