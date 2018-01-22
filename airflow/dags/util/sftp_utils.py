#
# Operators for interacting with SFTP
#
import pysftp


def fetch_file(path, dest_path, host, user, password):
    """
    Fetch a file from SFTP
    """
    with pysftp.Connection(host, username=user, password=password) as conn:
        conn.get(path, dest_path)


def upload_file(src_path, path, host, user, password):
    """
    Upload a file to SFTP
    """
    with pysftp.Connection(host, username=user, password=password) as conn:
        with conn.cd('/'.join(path.split('/')[:-1])):
            conn.put(src_path)


def list_path(path, host, user, password):
    """
    List files in a path
    """
    with pysftp.Connection(host, username=user, password=password) as conn:
        return conn.listdir_attr(path)


def file_exists(path, host, user, password):
    """
    Determine if a file exists on an sftp server
    """
    all_files = list_path(path.split('/')[:-1], host, user, password)
    return all_files.contains(path.split('/')[-1])
