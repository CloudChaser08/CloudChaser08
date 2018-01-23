import subprocess

INCOMING_FILES_DIR = 's3://salusv/testing/dewey/airflow/e2e/gather_incoming_files'


def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', INCOMING_FILES_DIR + '/dest'
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'gather_incoming_files'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'gather_incoming_files',
        '-s', '2018-01-22T12:00:00',
        '-e', '2018-01-22T12:00:00',
        '-I'
    ])


def test_files_moved():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', '{}/dest'.format(INCOMING_FILES_DIR)
    ])) > 0


def test_cleanup():
    cleanup()
