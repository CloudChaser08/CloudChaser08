#
# Operators for interacting with SFTP
#
import pysftp


def fetch_file(abs_external_filepath, abs_internal_path, external_host, user, password):
    """
    Fetch a file from SFTP
    """
    with pysftp.Connection(external_host, username=user, password=password) as conn:
        conn.get(abs_external_filepath, abs_internal_path)


def upload_file(abs_internal_filepath, abs_external_path, external_host, user, password):
    """
    Upload a file to SFTP
    """
    with pysftp.Connection(external_host, username=user, password=password) as conn:
        with conn.cd('/'.join(abs_external_path.split('/')[:-1])):
            conn.put(abs_internal_filepath)


def list_path(abs_external_path, external_host, user, password):
    """
    List files in a path
    """
    with pysftp.Connection(external_host, username=user, password=password) as conn:
        return conn.listdir_attr(abs_external_path)


def file_exists(abs_external_filepath, external_host, user, password):
    """
    Determine if a file exists on an sftp server
    """
    all_files = list_path(abs_external_filepath.split('/')[:-1], external_host, user, password)
    return all_files.contains(abs_external_filepath.split('/')[-1])
