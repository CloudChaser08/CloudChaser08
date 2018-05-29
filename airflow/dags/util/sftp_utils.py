#
# Operators for interacting with SFTP
#
import pysftp


def fetch_file(path, ignore_host_key=False, dest_path, **conn_kwargs):
    """
    Fetch a file from SFTP
    """
    with _get_connection(ignore_host_key=ignore_host_key, **conn_kwargs) as conn:
        conn.get(path, dest_path)


def upload_file(src_path, path, ignore_host_key=False, **conn_kwargs):
    """
    Upload a file to SFTP
    """
    with _get_connection(ignore_host_key=ignore_host_key, **conn_kwargs) as conn:
        with conn.cd('/'.join(path.split('/')[:-1])):
            conn.put(src_path)


def list_path(path, ignore_host_key=False, **conn_kwargs):
    """
    List files in a path
    """
    with _get_connection(ignore_host_key=ignore_host_key, **conn_kwargs) as conn:
        return conn.listdir(path)


def file_exists(path, **conn_kwargs):
    """
    Determine if a file exists on an sftp server
    """
    all_files = list_path(path.split('/')[:-1], **conn_kwargs)
    return all_files.contains(path.split('/')[-1])

def _get_connection(ignore_host_key, **conn_kwargs):
    if ignore_host_key:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

    if 'port' in conn_kwargs:
        conn_kwargs['port'] = int(conn_kwargs['port'])

    return pysftp.Connection(**conn_kwargs)
