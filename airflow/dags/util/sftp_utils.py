#
# Operators for interacting with SFTP
#
import pysftp


def fetch_file(path, dest_path, **conn_kwargs):
    """
    Fetch a file from SFTP
    """
    with pysftp.Connection(**conn_kwargs) as conn:
        conn.get(path, dest_path)


def upload_file(src_path, path, **conn_kwargs):
    """
    Upload a file to SFTP
    """
    with pysftp.Connection(**conn_kwargs) as conn:
        with conn.cd('/'.join(path.split('/')[:-1])):
            conn.put(src_path)


def list_path(path, **conn_kwargs):
    """
    List files in a path
    """
    with pysftp.Connection(**conn_kwargs) as conn:
        return conn.listdir(path)


def file_exists(path, **conn_kwargs):
    """
    Determine if a file exists on an sftp server
    """
    all_files = list_path(path.split('/')[:-1], **conn_kwargs)
    return all_files.contains(path.split('/')[-1])
